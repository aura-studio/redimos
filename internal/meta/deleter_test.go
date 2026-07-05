package meta

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeMemberDeleter is an in-memory MemberDeleter double. It records the pks it is
// asked to reclaim and (optionally) signals each call on a channel so tests can
// wait for the background worker to consume the queue without sleeping.
type fakeMemberDeleter struct {
	mu      sync.Mutex
	pks     []string
	perPK   int   // members reported deleted per call
	err     error // when non-nil, every call fails with this error
	aborted bool  // when true, every call reports the fenced abort (key recreated)

	calls chan string // buffered signal, one send per DeleteMembersIfDead call (optional)
}

func (f *fakeMemberDeleter) DeleteMembersIfDead(_ context.Context, pk string) (int, bool, error) {
	f.mu.Lock()
	f.pks = append(f.pks, pk)
	f.mu.Unlock()

	if f.calls != nil {
		f.calls <- pk
	}

	if f.err != nil {
		return 0, false, f.err
	}

	if f.aborted {
		return 0, true, nil
	}

	return f.perPK, false, nil
}

func (f *fakeMemberDeleter) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, len(f.pks))
	copy(out, f.pks)

	return out
}

var _ MemberDeleter = (*fakeMemberDeleter)(nil)

// TestDeleter_RecordsFencedAbort verifies the deleter records the atomic fence's abort: when
// DeleteMembersIfDead reports aborted=true (the key was recreated after being enqueued, so the
// backend transaction deleted nothing), the deleter counts it via Aborted() and reclaims no
// members — a DEL-then-recreate cannot wipe the new incarnation's data.
func TestDeleter_RecordsFencedAbort(t *testing.T) {
	md := &fakeMemberDeleter{perPK: 1, aborted: true, calls: make(chan string, 4)}

	d := NewDeleter(md, DeleterConfig{RatePerSecond: 1000})
	d.Start(context.Background())

	d.Enqueue("recreated")
	if got := waitFor(t, md.calls); got != "recreated" {
		t.Fatalf("processed %q, want recreated", got)
	}

	d.Stop() // wait for the worker to finish recording metrics for this pk

	if got := d.Aborted(); got != 1 {
		t.Fatalf("Aborted() = %d, want 1 (recreated key's reclaim fenced off)", got)
	}
	if got := d.Deleted(); got != 0 {
		t.Fatalf("Deleted() = %d, want 0 (nothing reclaimed when the fence aborts)", got)
	}
	if got := d.Failures(); got != 0 {
		t.Fatalf("Failures() = %d, want 0 (an abort is not a failure)", got)
	}
}

// waitFor reads one value from ch or fails the test after a deadline. It keeps the
// concurrency tests fast on success and bounded on failure.
func waitFor(t *testing.T, ch <-chan string) string {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for background deleter to consume a pk")
		return ""
	}
}

func TestDeleter_EnqueueConsumesAndDeletesMembers(t *testing.T) {
	md := &fakeMemberDeleter{perPK: 3, calls: make(chan string, 1)}
	d := NewDeleter(md, DeleterConfig{})

	ctx := context.Background()
	d.Start(ctx)
	defer d.Stop()

	d.Enqueue("0:k")

	if got := waitFor(t, md.calls); got != "0:k" {
		t.Fatalf("background deleter reclaimed pk %q, want 0:k", got)
	}

	// Deleted count reflects the members the member-deleter reported.
	// Stop drains and guarantees the worker has finished processing.
	d.Stop()

	if got := d.Deleted(); got != 3 {
		t.Fatalf("Deleted() = %d, want 3", got)
	}
	if got := d.Failures(); got != 0 {
		t.Fatalf("Failures() = %d, want 0", got)
	}
}

func TestDeleter_BoundedQueueDropsWhenFull(t *testing.T) {
	// Worker is never started, so nothing drains the queue: only QueueCapacity pks
	// fit and the rest are dropped rather than blocking Enqueue.
	d := NewDeleter(&fakeMemberDeleter{}, DeleterConfig{QueueCapacity: 2})

	for i := 0; i < 5; i++ {
		d.Enqueue("0:k")
	}

	if got := d.QueueLen(); got != 2 {
		t.Fatalf("QueueLen() = %d, want 2 (capacity)", got)
	}
	if got := d.Dropped(); got != 3 {
		t.Fatalf("Dropped() = %d, want 3 (5 enqueued - capacity 2)", got)
	}
}

func TestDeleter_DefaultQueueCapacity(t *testing.T) {
	d := NewDeleter(&fakeMemberDeleter{}, DeleterConfig{QueueCapacity: 0})
	if cap(d.queue) != DefaultQueueCapacity {
		t.Fatalf("queue capacity = %d, want default %d", cap(d.queue), DefaultQueueCapacity)
	}
}

func TestDeleter_GracefulStopDrainsQueue(t *testing.T) {
	md := &fakeMemberDeleter{}
	// No rate limit; large enough queue to hold everything before Start.
	d := NewDeleter(md, DeleterConfig{QueueCapacity: 16})

	pks := []string{"0:a", "0:b", "0:c", "0:d", "0:e"}
	for _, pk := range pks {
		d.Enqueue(pk)
	}

	// Start then immediately request shutdown: the worker must drain everything it
	// already accepted (via the normal loop and/or the drain path) before exiting.
	d.Start(context.Background())
	d.Stop()

	if got := len(md.recorded()); got != len(pks) {
		t.Fatalf("reclaimed %d pks after graceful stop, want %d (all queued drained)", got, len(pks))
	}
	if got := d.QueueLen(); got != 0 {
		t.Fatalf("QueueLen() = %d after Stop, want 0", got)
	}
	if got := d.Dropped(); got != 0 {
		t.Fatalf("Dropped() = %d, want 0 (queue was never full)", got)
	}
}

func TestDeleter_StopBeforeStartIsSafe(t *testing.T) {
	d := NewDeleter(&fakeMemberDeleter{}, DeleterConfig{})
	// Must return promptly without blocking on the never-closed done channel.
	done := make(chan struct{})

	go func() {
		d.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() blocked when the worker was never started")
	}
}

func TestDeleter_ContextCancellationStopsWorker(t *testing.T) {
	md := &fakeMemberDeleter{calls: make(chan string, 1)}
	d := NewDeleter(md, DeleterConfig{})

	ctx, cancel := context.WithCancel(context.Background())
	d.Start(ctx)

	d.Enqueue("0:k")
	_ = waitFor(t, md.calls)

	// Cancelling ctx must terminate the worker; Stop then returns cleanly.
	cancel()

	stopped := make(chan struct{})

	go func() {
		d.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not exit after context cancellation")
	}
}

func TestDeleter_ReportsAndCountsFailures(t *testing.T) {
	boom := errors.New("batch write failed")

	var (
		mu       sync.Mutex
		gotPK    string
		gotErr   error
		reported = make(chan struct{}, 1)
	)

	md := &fakeMemberDeleter{err: boom}
	d := NewDeleter(md, DeleterConfig{
		OnError: func(pk string, err error) {
			mu.Lock()
			gotPK, gotErr = pk, err
			mu.Unlock()
			reported <- struct{}{}
		},
	})

	d.Start(context.Background())
	defer d.Stop()

	d.Enqueue("0:bad")

	select {
	case <-reported:
	case <-time.After(2 * time.Second):
		t.Fatal("OnError was not invoked for a failed deletion")
	}

	mu.Lock()
	defer mu.Unlock()

	if gotPK != "0:bad" || !errors.Is(gotErr, boom) {
		t.Fatalf("OnError(%q, %v), want (0:bad, %v)", gotPK, gotErr, boom)
	}

	// Give the counter a moment to settle (it is incremented before OnError).
	if got := d.Failures(); got != 1 {
		t.Fatalf("Failures() = %d, want 1", got)
	}
	if got := d.Deleted(); got != 0 {
		t.Fatalf("Deleted() = %d, want 0 on failure", got)
	}
}

func TestDeleter_RateLimitedStillProcesses(t *testing.T) {
	md := &fakeMemberDeleter{calls: make(chan string, 2)}
	// A generous rate keeps the test fast while still exercising the tick gate.
	d := NewDeleter(md, DeleterConfig{RatePerSecond: 1000})

	d.Start(context.Background())
	defer d.Stop()

	d.Enqueue("0:a")
	d.Enqueue("0:b")

	first := waitFor(t, md.calls)
	second := waitFor(t, md.calls)

	if first == second {
		t.Fatalf("expected two distinct pks, got %q twice", first)
	}
}

// TestDeleter_WiredToMetaStore exercises the full seam: MetaStore.DeleteMeta
// removes the meta item and enqueues the pk on the Deleter, whose background
// worker then reclaims the members via the injected MemberDeleter.
func TestDeleter_WiredToMetaStore(t *testing.T) {
	md := &fakeMemberDeleter{perPK: 2, calls: make(chan string, 1)}
	d := NewDeleter(md, DeleterConfig{})
	d.Start(context.Background())
	defer d.Stop()

	store := &fakeStore{deleteExisted: true}
	ms := NewMetaStore(store, d)

	existed, err := ms.DeleteMeta(context.Background(), "0:wired")
	if err != nil || !existed {
		t.Fatalf("DeleteMeta = (%v, %v), want (true, nil)", existed, err)
	}

	if got := waitFor(t, md.calls); got != "0:wired" {
		t.Fatalf("deleter reclaimed pk %q, want 0:wired", got)
	}
}
