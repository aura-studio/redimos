package meta

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Weekly orphan sweeper (redimos task 11.2).
//
// The lazy deleter (deleter.go) is the primary reclamation path: DeleteMeta removes
// a key's meta item and enqueues the pk, and the background deleter reclaims the
// data members. That path is best-effort, though — the bounded queue drops pks when
// it fills, and a member reclaim can fail — so some data members can be left behind
// as "orphans" whose owning key has no meta item. They are already invisible to
// clients (a missing meta item makes the key logically absent) but still consume
// storage. DynamoDB's native TTL only cleans up the meta item and can lag up to
// 48h, so it is not a reliable backstop either.
//
// The Sweeper closes that gap. On a configurable ticker (default 7 days) it invokes
// the storage sweep primitive, which scans the whole table for orphan members and
// reclaims them. It runs as a Start/Stop background goroutine like the deleter and
// exposes counters (sweep runs, orphans reclaimed, failures) for observability.
//
//	sw := meta.NewSweeper(store, meta.SweeperConfig{})
//	sw.Start(ctx)
//	defer sw.Stop()

// DefaultSweepInterval is the sweep period used when SweeperConfig leaves Interval
// unset. Orphans are a rare, bounded backstop condition, so a weekly cadence keeps
// the extra full-table Scan cost negligible.
const DefaultSweepInterval = 7 * 24 * time.Hour

// OrphanSweeper is the storage seam the Sweeper drives once per tick. storage.Store
// satisfies it (via SweepOrphans); unit tests inject a fake so the ticker and
// lifecycle can be exercised without a live DynamoDB.
type OrphanSweeper interface {
	// SweepOrphans scans for orphan data members (whose owning key has no meta
	// item) and reclaims them, returning the number of members reclaimed.
	SweepOrphans(ctx context.Context) (reclaimed int, err error)
}

// SweeperConfig configures a Sweeper.
type SweeperConfig struct {
	// Interval is the sweep period. A value <= 0 selects DefaultSweepInterval.
	// Tests inject a short interval to exercise the ticker without waiting a week.
	Interval time.Duration

	// SweepOnStart runs one sweep immediately when Start is called, before the
	// first tick elapses. Off by default so startup does not trigger a full-table
	// Scan on every deploy/restart.
	SweepOnStart bool

	// InitialDelay, when > 0 (and SweepOnStart is false), schedules the FIRST sweep
	// this many seconds after Start instead of a full Interval later. It fixes the
	// "never sweeps" hole: with only a fresh Interval ticker, a process that restarts
	// more often than the interval (e.g. a proxy redeployed daily against a weekly
	// sweep) resets the timer every time and the orphan sweep never fires, so orphans
	// accumulate unbounded. A short, jittered InitialDelay guarantees each process
	// lifetime performs a sweep soon after startup while spreading the full-table Scan
	// across a fleet. Ignored when SweepOnStart is true (which already sweeps at t=0).
	InitialDelay time.Duration

	// OnError, if set, is called for each failed sweep. It runs on the worker
	// goroutine, so it must not block. A failed sweep is counted and the Sweeper
	// simply waits for the next tick to try again.
	OnError func(err error)

	// Logger, if set, receives structured error events (op="orphan_sweep", error) and
	// takes precedence over OnError. Prefer it for machine-parseable worker logs.
	Logger Logger
}

// Sweeper periodically drives the storage orphan-sweep primitive on a ticker. It is
// safe to Start once and Stop once. The zero value is not usable; construct one
// with NewSweeper.
type Sweeper struct {
	sweeper      OrphanSweeper
	interval     time.Duration
	onStart      bool
	initialDelay time.Duration
	onError      func(err error)
	logger       Logger

	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool
	quit      chan struct{} // closed by Stop to request shutdown
	done      chan struct{} // closed by the worker when it has fully exited

	runs      atomic.Int64 // total sweeps executed (success or failure)
	reclaimed atomic.Int64 // total orphan members reclaimed across all sweeps
	failures  atomic.Int64 // sweeps that returned an error
}

// NewSweeper builds a Sweeper over the given sweep seam. The returned Sweeper is
// idle until Start is called.
func NewSweeper(s OrphanSweeper, cfg SweeperConfig) *Sweeper {
	interval := cfg.Interval
	if interval <= 0 {
		interval = DefaultSweepInterval
	}

	return &Sweeper{
		sweeper:      s,
		interval:     interval,
		onStart:      cfg.SweepOnStart,
		initialDelay: cfg.InitialDelay,
		onError:      cfg.OnError,
		logger:       cfg.Logger,
		quit:         make(chan struct{}),
		done:         make(chan struct{}),
	}
}

// Start launches the background worker. It is idempotent: only the first call
// starts a goroutine. The worker stops when ctx is cancelled or Stop is called.
func (s *Sweeper) Start(ctx context.Context) {
	s.startOnce.Do(func() {
		s.started.Store(true)
		go s.run(ctx)
	})
}

// Stop requests a graceful shutdown and blocks until the worker has exited. It is
// idempotent and safe to call even if Start was never called (returns immediately).
func (s *Sweeper) Stop() {
	if !s.started.Load() {
		return
	}

	s.stopOnce.Do(func() { close(s.quit) })
	<-s.done
}

// Runs returns the total number of sweeps executed (successful or failed).
func (s *Sweeper) Runs() int64 { return s.runs.Load() }

// Reclaimed returns the total number of orphan members reclaimed across all sweeps.
func (s *Sweeper) Reclaimed() int64 { return s.reclaimed.Load() }

// Failures returns the number of sweeps that returned an error.
func (s *Sweeper) Failures() int64 { return s.failures.Load() }

// run is the worker loop. It optionally sweeps once on start, then sweeps on every
// ticker tick until ctx is cancelled or Stop is called.
func (s *Sweeper) run(ctx context.Context) {
	defer close(s.done)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	if s.onStart {
		s.sweep(ctx)
	} else if s.initialDelay > 0 {
		// First sweep after a short delay (then the regular ticker) so a process that
		// restarts more often than Interval still sweeps at least once per lifetime,
		// without the immediate-on-every-restart Scan that SweepOnStart would cause.
		timer := time.NewTimer(s.initialDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-s.quit:
			timer.Stop()
			return
		case <-timer.C:
			s.sweep(ctx)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.quit:
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

// sweep runs a single sweep, recording metrics and reporting errors. A failed sweep
// is counted and reported but not retried immediately; the next tick tries again.
func (s *Sweeper) sweep(ctx context.Context) {
	s.runs.Add(1)

	n, err := s.sweeper.SweepOrphans(ctx)
	if err != nil {
		s.failures.Add(1)

		if s.logger != nil {
			s.logger.Error("orphan_sweep", map[string]any{"error": err.Error()})
		} else if s.onError != nil {
			s.onError(err)
		}

		return
	}

	s.reclaimed.Add(int64(n))
}
