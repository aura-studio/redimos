package meta

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Lazy delete queue + background deleter (redimos task 11.1).
//
// Deleting a key is split into two phases so the client-visible DEL stays O(1)
// while the (potentially large) member cleanup happens off the request path:
//
//  1. MetaStore.DeleteMeta removes the meta item, making the key immediately
//     logically absent, and hands the pk to a DeletionEnqueuer (this Deleter).
//  2. The Deleter's background goroutine drains the in-memory queue and, for each
//     pk, reclaims the key's remaining data items via the injected MemberDeleter
//     (Query pk + BatchWriteItem), rate-limited so it does not exhaust write
//     capacity.
//
// The queue is bounded: if it fills (deleter falling behind, or DynamoDB slow),
// Enqueue drops the pk rather than blocking the command path. Dropped pks are not
// lost forever — the weekly sweeper (task 11.2) rescans for orphan members with no
// meta and reclaims them. Failed deletions are likewise left for the sweeper.
//
// The Deleter implements DeletionEnqueuer, so a MetaStore is wired to it directly:
//
//	d := meta.NewDeleter(store, meta.DeleterConfig{RatePerSecond: 50})
//	ms := meta.NewMetaStore(store, d)
//	d.Start(ctx)
//	defer d.Stop()

// MemberDeleter reclaims a key's data-member items. storage.Store satisfies this
// (via its DeleteMembersIfDead method); unit tests inject a fake so the background
// deleter can be exercised without a live DynamoDB.
type MemberDeleter interface {
	// DeleteMembersIfDead removes all data items under pk (everything except the meta
	// item) ONLY while the key is dead (its #meta item absent), atomically with that
	// liveness check. If pk was recreated after being enqueued (a DEL-then-recreate) it
	// aborts (aborted=true) and leaves the new incarnation's data intact — the atomic
	// fence that makes DEL-then-recreate linearizable. It returns the number deleted and
	// must be safe to call for a pk with no members.
	DeleteMembersIfDead(ctx context.Context, pk string) (deleted int, aborted bool, err error)
}

// DefaultQueueCapacity is the bounded queue size used when DeleterConfig leaves
// QueueCapacity unset.
const DefaultQueueCapacity = 1024

// DeleterConfig configures a Deleter.
type DeleterConfig struct {
	// QueueCapacity bounds the in-memory delete queue. When the queue is full,
	// Enqueue drops the pk (counted via Dropped) instead of blocking the caller. A
	// value <= 0 selects DefaultQueueCapacity.
	QueueCapacity int

	// RatePerSecond caps how many pks the background worker processes per second,
	// limiting the member-deletion write rate against DynamoDB. A value <= 0
	// disables rate limiting (drain as fast as possible).
	RatePerSecond float64

	// OnError, if set, is called for each pk whose member deletion fails. It runs
	// on the worker goroutine, so it must not block. Failed pks are left for the
	// weekly sweeper to reclaim.
	OnError func(pk string, err error)

	// Logger, if set, receives structured error events (op="lazy_delete", pk, error)
	// and takes precedence over OnError. Prefer it for machine-parseable worker logs.
	Logger Logger
}

// Deleter is the in-memory lazy-delete queue plus the background goroutine that
// drains it. It is safe for concurrent Enqueue calls from many connection
// goroutines. The zero value is not usable; construct one with NewDeleter.
type Deleter struct {
	deleter MemberDeleter
	queue   chan string
	rate    float64
	onError func(pk string, err error)
	logger  Logger

	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool
	quit      chan struct{} // closed by Stop to request shutdown
	done      chan struct{} // closed by the worker when it has fully exited

	dropped  atomic.Int64 // enqueue attempts dropped because the queue was full
	deleted  atomic.Int64 // total members reclaimed across all processed pks
	failures atomic.Int64 // pks whose member deletion returned an error
	aborted  atomic.Int64 // pks whose reclaim was skipped because the key was recreated (fenced)
}

// compile-time assertion that Deleter can be used as the MetaStore's enqueuer.
var _ DeletionEnqueuer = (*Deleter)(nil)

// NewDeleter builds a Deleter over the given member-deletion seam. The returned
// Deleter is idle until Start is called; Enqueue may be called before Start (pks
// buffer in the bounded queue up to capacity).
func NewDeleter(md MemberDeleter, cfg DeleterConfig) *Deleter {
	capacity := cfg.QueueCapacity
	if capacity <= 0 {
		capacity = DefaultQueueCapacity
	}

	return &Deleter{
		deleter: md,
		queue:   make(chan string, capacity),
		rate:    cfg.RatePerSecond,
		onError: cfg.OnError,
		logger:  cfg.Logger,
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// Enqueue schedules pk's data items for asynchronous deletion. It never blocks:
// if the bounded queue is full it drops the pk (incrementing Dropped) so the
// calling command path is never stalled. Safe for concurrent use.
func (d *Deleter) Enqueue(pk string) {
	select {
	case d.queue <- pk:
	default:
		d.dropped.Add(1)
	}
}

// Start launches the background worker. It is idempotent: only the first call
// starts a goroutine. The worker stops when ctx is cancelled or Stop is called.
func (d *Deleter) Start(ctx context.Context) {
	d.startOnce.Do(func() {
		d.started.Store(true)
		go d.run(ctx)
	})
}

// Stop requests a graceful shutdown and blocks until the worker has exited,
// draining any pks still queued at shutdown. It is idempotent and safe to call
// even if Start was never called (in which case it returns immediately).
func (d *Deleter) Stop() {
	if !d.started.Load() {
		return
	}

	d.stopOnce.Do(func() { close(d.quit) })
	<-d.done
}

// Dropped returns the number of Enqueue calls dropped because the queue was full.
func (d *Deleter) Dropped() int64 { return d.dropped.Load() }

// Deleted returns the total number of members reclaimed across all processed pks.
func (d *Deleter) Deleted() int64 { return d.deleted.Load() }

// Failures returns the number of pks whose member deletion returned an error.
func (d *Deleter) Failures() int64 { return d.failures.Load() }

// Aborted returns the number of pks whose reclaim was skipped by the atomic fence because
// the key had been recreated (a DEL-then-recreate) since it was enqueued.
func (d *Deleter) Aborted() int64 { return d.aborted.Load() }

// QueueLen returns the number of pks currently waiting in the queue.
func (d *Deleter) QueueLen() int { return len(d.queue) }

// run is the worker loop. It consumes pks from the queue, applies the optional
// rate limit before each deletion, and exits on ctx cancellation or Stop. On Stop
// it drains the remaining queued pks (best effort) before returning.
func (d *Deleter) run(ctx context.Context) {
	defer close(d.done)

	var tick <-chan time.Time

	if d.rate > 0 {
		ticker := time.NewTicker(time.Duration(float64(time.Second) / d.rate))
		defer ticker.Stop()
		tick = ticker.C
	}

	for {
		var pk string

		select {
		case <-ctx.Done():
			return
		case <-d.quit:
			d.drain(ctx)
			return
		case pk = <-d.queue:
		}

		// Rate-limit gate: wait for a tick before reclaiming this pk's members.
		if tick != nil {
			select {
			case <-ctx.Done():
				return
			case <-d.quit:
				// Honour the pk we already dequeued, then drain the rest.
				d.process(ctx, pk)
				d.drain(ctx)

				return
			case <-tick:
			}
		}

		d.process(ctx, pk)
	}
}

// drain processes whatever pks remain in the queue without rate limiting, so a
// graceful Stop does not strand already-queued deletions. It stops early if ctx is
// cancelled.
func (d *Deleter) drain(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		select {
		case pk := <-d.queue:
			d.process(ctx, pk)
		default:
			return
		}
	}
}

// process reclaims a single pk's members, recording metrics and reporting errors.
// A failed deletion is counted and reported but not re-queued; the weekly sweeper
// is the backstop for pks that fail here or are dropped by a full queue.
func (d *Deleter) process(ctx context.Context, pk string) {
	// Fenced reclaim (async path only): DeleteMembersIfDead deletes pk's members ATOMICALLY
	// with a check that pk is still dead (#meta absent). If pk was recreated since it was
	// enqueued (a DEL-then-recreate), the reclaim aborts and the new incarnation's data is
	// left intact — no window in which a concurrent SET is silently wiped.
	n, aborted, err := d.deleter.DeleteMembersIfDead(ctx, pk)
	if err != nil {
		d.failures.Add(1)

		if d.logger != nil {
			d.logger.Error("lazy_delete", map[string]any{"pk": pk, "error": err.Error()})
		} else if d.onError != nil {
			d.onError(pk, err)
		}

		return
	}

	if aborted {
		d.aborted.Add(1)
	}

	d.deleted.Add(int64(n))
}
