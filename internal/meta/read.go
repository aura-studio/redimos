package meta

import (
	"context"
	"time"
)

// This file implements the read path (design "算法 2：读路径"): meta and data are
// read in parallel, meta is awaited first, and the data result is only surfaced
// when the key's meta exists and is not expired. Expiry is judged purely from
// meta.exp and an injectable clock via IsExpired, independent of when DynamoDB's
// native TTL physically removes the item (requirements 11.4, 11.5, 16.1, 16.2).
//
// ctx note: ReadPath derives a cancellable context and cancels it as soon as the
// meta lookup shows the key is absent or expired, signalling the in-flight data
// read to stop. This cancellation is best-effort: the redimo fork v1.7 reads still
// call context.TODO() internally, so the underlying DynamoDB calls do not yet
// observe the cancellation. The data goroutine's result is drained into a buffered
// channel so the goroutine never leaks even when its result is discarded.

// nowEpochSeconds returns the current time in epoch seconds. It is the default
// clock used by NewReader when no clock is injected.
func nowEpochSeconds() int64 { return time.Now().Unix() }

// Reader binds a MetaStore to a clock so ReadPath can evaluate expiry
// deterministically. The clock seam lets unit tests pin "now" without touching the
// wall clock; production callers pass nil to use time.Now().Unix().
type Reader struct {
	ms  *MetaStore
	now func() int64
}

// NewReader builds a Reader over ms. A nil now is replaced with the wall-clock
// (time.Now().Unix()), so production callers can omit the clock while tests inject
// a fixed one.
func NewReader(ms *MetaStore, now func() int64) *Reader {
	if now == nil {
		now = nowEpochSeconds
	}

	return &Reader{ms: ms, now: now}
}

// ReadPath executes the read path for pk. It launches the meta Load and readData
// concurrently, awaits the meta result first, and then decides:
//
//   - meta read failed        → (zero, false, err); the data read is cancelled.
//   - meta absent             → (zero, false, nil); the data read is cancelled.
//   - meta present but expired → (zero, false, nil); the data read is cancelled and
//     pk is enqueued for lazy deletion exactly once.
//   - meta present, unexpired → the data result is awaited and returned as
//     (data, true, nil), or (zero, false, err) if the data read failed.
//
// found reports whether the key is live: it is false for a missing or expired key,
// which command handlers surface to clients as "key does not exist" (e.g. GET's
// $-1). Expiry uses the Reader's injected clock and IsExpired only, independent of
// DynamoDB's native TTL cleanup timing.
//
// readData receives a context that ReadPath cancels once meta shows the key is
// absent or expired; see the file-level ctx note on the best-effort nature of that
// cancellation.
func ReadPath[T any](
	ctx context.Context,
	r *Reader,
	pk string,
	readData func(ctx context.Context) (T, error),
) (T, bool, error) {
	now := r.now()

	// Derive a cancellable context so we can signal the data read to stop the
	// moment we learn the key is absent or expired.
	dataCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type dataResult struct {
		val T
		err error
	}
	// Buffered so the data goroutine can always deliver its result and exit, even
	// when we discard it (absent/expired) and never receive from the channel.
	dataCh := make(chan dataResult, 1)
	go func() {
		val, err := readData(dataCtx)
		dataCh <- dataResult{val: val, err: err}
	}()

	type metaResult struct {
		meta   Meta
		exists bool
		err    error
	}
	metaCh := make(chan metaResult, 1)
	go func() {
		m, exists, err := r.ms.Load(ctx, pk)
		metaCh <- metaResult{meta: m, exists: exists, err: err}
	}()

	// Await meta first: it decides whether the data result is even relevant.
	mr := <-metaCh

	var zero T

	if mr.err != nil {
		cancel() // stop the in-flight data read; its result is irrelevant.
		return zero, false, mr.err
	}

	if !mr.exists {
		cancel() // key does not exist: discard the data read.
		return zero, false, nil
	}

	if IsExpired(mr.meta, now) {
		cancel()                 // expired: discard the data read.
		r.ms.enqueue.Enqueue(pk) // lazy delete: enqueue exactly once.
		return zero, false, nil
	}

	// Meta is present and unexpired: the data result is authoritative.
	dr := <-dataCh
	if dr.err != nil {
		return zero, false, dr.err
	}

	return dr.val, true, nil
}
