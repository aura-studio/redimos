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
// (via its DeleteMembers method); unit tests inject a fake so the background
// deleter can be exercised without a live DynamoDB.
type MemberDeleter interface {
	// DeleteMembers removes all data items under pk (everything except the meta
	// item) and returns the number deleted. It must be safe to call for a pk with
	// no members.
	DeleteMembers(ctx context.Context, pk string) (deleted int, err error)
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

	// IsLive, if set, reports whether pk currently has a live meta item. The deleter
	// calls it before reclaiming a pk's members and SKIPS the reclaim when the key is
	// live: a key recreated after being enqueued (a DEL-then-recreate) is not an orphan,
	// and reclaiming it would wipe the new incarnation's data — a linearizability
	// violation (a write acknowledged after the DEL could be silently undone). Wire it to
	// the meta store's existence check. When nil, members are reclaimed unconditionally.
	// This guard lives ONLY on the async lazy-delete path; the synchronous live-collection
	// rewrite (LReplaceAll) never routes through the deleter and so is unaffected.
	IsLive func(ctx context.Context, pk string) (bool, error)

	// Synchronous, when true, makes the Deleter reclaim members INLINE on the caller's
	// goroutine instead of queueing them for a background worker: Enqueue(pk) runs the
	// same process(pk) (the IsLive recreate-guard, then DeleteMembers, with the same
	// metrics) synchronously and returns only once the members are gone, and Start
	// becomes a no-op (no background goroutine is ever spawned). The default (false)
	// is today's asynchronous behaviour — a bounded queue drained by a background
	// worker — so this field is purely additive and every existing caller is unchanged.
	//
	// This is the mode the in-process embedding (redimos.NewInProcessClient) uses so a
	// DEL is fully synchronous (members reclaimed before DEL returns) and no background
	// goroutine exists. SyncContext supplies the context handed to process; when nil a
	// context.Background() is used. Rate limiting (RatePerSecond) and QueueCapacity are
	// ignored in synchronous mode.
	Synchronous bool

	// SyncContext is the context passed to the inline reclaim (IsLive + DeleteMembers)
	// when Synchronous is true. A nil value uses context.Background(). It is ignored in
	// the default asynchronous mode, where each reclaim uses the worker's context.
	SyncContext context.Context
}

// Deleter is the in-memory lazy-delete queue plus the background goroutine that
// drains it. It is safe for concurrent Enqueue calls from many connection
// goroutines. The zero value is not usable; construct one with NewDeleter.
type Deleter struct {
	deleter     MemberDeleter
	queue       chan string
	rate        float64
	onError     func(pk string, err error)
	logger      Logger
	isLive      func(ctx context.Context, pk string) (bool, error)
	synchronous bool
	syncCtx     context.Context

	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool
	quit      chan struct{} // closed by Stop to request shutdown
	done      chan struct{} // closed by the worker when it has fully exited

	dropped      atomic.Int64 // enqueue attempts dropped because the queue was full
	deleted      atomic.Int64 // total members reclaimed across all processed pks
	failures     atomic.Int64 // pks whose member deletion returned an error
	isLiveErrors atomic.Int64 // recreate-guard (IsLive) checks that returned an error
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

	syncCtx := cfg.SyncContext
	if syncCtx == nil {
		syncCtx = context.Background()
	}

	return &Deleter{
		deleter:     md,
		queue:       make(chan string, capacity),
		rate:        cfg.RatePerSecond,
		onError:     cfg.OnError,
		logger:      cfg.Logger,
		isLive:      cfg.IsLive,
		synchronous: cfg.Synchronous,
		syncCtx:     syncCtx,
		quit:        make(chan struct{}),
		done:        make(chan struct{}),
	}
}

// Enqueue schedules pk's data items for deletion.
//
// In the default asynchronous mode it never blocks: if the bounded queue is full it
// drops the pk (incrementing Dropped) so the calling command path is never stalled.
//
// In synchronous mode (DeleterConfig.Synchronous) it instead reclaims the members
// INLINE on the caller's goroutine by running the same process(pk) — the IsLive
// recreate-guard, then DeleteMembers, with the same metrics — and returns only once
// the members are gone. No background worker is involved. Safe for concurrent use in
// both modes (process only touches the injected deleter and atomic counters).
func (d *Deleter) Enqueue(pk string) {
	if d.synchronous {
		d.process(d.syncCtx, pk)
		return
	}

	select {
	case d.queue <- pk:
	default:
		d.dropped.Add(1)
	}
}

// Start launches the background worker. It is idempotent: only the first call
// starts a goroutine. The worker stops when ctx is cancelled or Stop is called.
//
// In synchronous mode (DeleterConfig.Synchronous) Start is a no-op: reclamation
// happens inline in Enqueue, so no background goroutine is ever spawned. Stop is
// likewise a no-op then (started stays false).
func (d *Deleter) Start(ctx context.Context) {
	if d.synchronous {
		return
	}
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

// IsLiveErrors returns the number of recreate-guard (IsLive) checks that returned
// an error. A rising count means the guard is degraded — reclaims are running
// unguarded and could race a recreate — so it is worth surfacing/alerting on.
func (d *Deleter) IsLiveErrors() int64 { return d.isLiveErrors.Load() }

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
	// Recreate guard (async path only): if pk was recreated since it was enqueued (its
	// #meta is live again), it is not an orphan — reclaiming its members would wipe the new
	// incarnation's data. Skip.
	if d.isLive != nil {
		live, err := d.isLive(ctx, pk)
		switch {
		case err != nil:
			// The liveness check itself failed. We proceed to reclaim (best-effort; the
			// weekly sweeper is the backstop), but this is NOT free of risk: if the pk was
			// in fact recreated, an unguarded reclaim could wipe fresh data. Count and log
			// it so a persistently failing check (which silently degrades the recreate
			// guard) is visible, rather than swallowed.
			d.isLiveErrors.Add(1)
			if d.logger != nil {
				d.logger.Error("lazy_delete_islive", map[string]any{"pk": pk, "error": err.Error()})
			}
		case live:
			return
		}
	}

	n, err := d.deleter.DeleteMembers(ctx, pk)
	if err != nil {
		d.failures.Add(1)

		if d.logger != nil {
			d.logger.Error("lazy_delete", map[string]any{"pk": pk, "error": err.Error()})
		} else if d.onError != nil {
			d.onError(pk, err)
		}

		return
	}

	d.deleted.Add(int64(n))
}
