package meta

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeOrphanSweeper is an in-memory OrphanSweeper double. It reports a fixed number
// of reclaimed members per call (or a fixed error) and signals each call on a
// channel so tests can observe ticker-driven sweeps without sleeping for a week.
type fakeOrphanSweeper struct {
	mu        sync.Mutex
	callCount int
	perCall   int   // orphan members reported reclaimed per sweep
	err       error // when non-nil, every sweep fails with this error

	calls chan struct{} // buffered signal, one send per SweepOrphans call (optional)
}

func (f *fakeOrphanSweeper) SweepOrphans(_ context.Context) (int, error) {
	f.mu.Lock()
	f.callCount++
	f.mu.Unlock()

	if f.calls != nil {
		f.calls <- struct{}{}
	}

	if f.err != nil {
		return 0, f.err
	}

	return f.perCall, nil
}

func (f *fakeOrphanSweeper) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.callCount
}

var _ OrphanSweeper = (*fakeOrphanSweeper)(nil)

// waitSignal reads one value from ch or fails the test after a deadline.
func waitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the sweeper to run")
	}
}

func TestSweeper_TickerTriggersSweep(t *testing.T) {
	fs := &fakeOrphanSweeper{perCall: 4, calls: make(chan struct{}, 4)}
	// Short interval so the ticker fires quickly; we never wait a real week.
	sw := NewSweeper(fs, SweeperConfig{Interval: 5 * time.Millisecond})

	sw.Start(context.Background())
	defer sw.Stop()

	// Observe at least two ticker-driven sweeps.
	waitSignal(t, fs.calls)
	waitSignal(t, fs.calls)

	if got := sw.Reclaimed(); got < 4 {
		t.Fatalf("Reclaimed() = %d, want >= 4 after at least one sweep", got)
	}
	if got := sw.Failures(); got != 0 {
		t.Fatalf("Failures() = %d, want 0", got)
	}
	if got := sw.Runs(); got < 2 {
		t.Fatalf("Runs() = %d, want >= 2", got)
	}
}

func TestSweeper_SweepOnStartRunsImmediately(t *testing.T) {
	fs := &fakeOrphanSweeper{perCall: 7, calls: make(chan struct{}, 1)}
	// Long interval: the only sweep we can observe must come from SweepOnStart.
	sw := NewSweeper(fs, SweeperConfig{Interval: time.Hour, SweepOnStart: true})

	sw.Start(context.Background())
	defer sw.Stop()

	waitSignal(t, fs.calls)

	sw.Stop() // guarantees the worker finished recording the sweep

	if got := sw.Runs(); got != 1 {
		t.Fatalf("Runs() = %d, want exactly 1 (start sweep, no tick yet)", got)
	}
	if got := sw.Reclaimed(); got != 7 {
		t.Fatalf("Reclaimed() = %d, want 7", got)
	}
}

func TestSweeper_NoSweepOnStartByDefault(t *testing.T) {
	fs := &fakeOrphanSweeper{}
	// Long interval and SweepOnStart off: no sweep should occur before Stop.
	sw := NewSweeper(fs, SweeperConfig{Interval: time.Hour})

	sw.Start(context.Background())
	sw.Stop()

	if got := fs.count(); got != 0 {
		t.Fatalf("SweepOrphans called %d times, want 0 (no sweep on start, no tick)", got)
	}
	if got := sw.Runs(); got != 0 {
		t.Fatalf("Runs() = %d, want 0", got)
	}
}

func TestSweeper_DefaultInterval(t *testing.T) {
	sw := NewSweeper(&fakeOrphanSweeper{}, SweeperConfig{Interval: 0})
	if sw.interval != DefaultSweepInterval {
		t.Fatalf("interval = %v, want default %v", sw.interval, DefaultSweepInterval)
	}
}

func TestSweeper_CountsAndReportsFailures(t *testing.T) {
	boom := errors.New("scan failed")

	var (
		mu       sync.Mutex
		gotErr   error
		reported = make(chan struct{}, 1)
	)

	fs := &fakeOrphanSweeper{err: boom}
	sw := NewSweeper(fs, SweeperConfig{
		Interval:     5 * time.Millisecond,
		SweepOnStart: true,
		OnError: func(err error) {
			mu.Lock()
			gotErr = err
			mu.Unlock()

			select {
			case reported <- struct{}{}:
			default:
			}
		},
	})

	sw.Start(context.Background())
	defer sw.Stop()

	select {
	case <-reported:
	case <-time.After(2 * time.Second):
		t.Fatal("OnError was not invoked for a failed sweep")
	}

	mu.Lock()
	defer mu.Unlock()

	if !errors.Is(gotErr, boom) {
		t.Fatalf("OnError got %v, want %v", gotErr, boom)
	}
	if got := sw.Failures(); got < 1 {
		t.Fatalf("Failures() = %d, want >= 1", got)
	}
	if got := sw.Reclaimed(); got != 0 {
		t.Fatalf("Reclaimed() = %d, want 0 on failure", got)
	}
}

func TestSweeper_StopBeforeStartIsSafe(t *testing.T) {
	sw := NewSweeper(&fakeOrphanSweeper{}, SweeperConfig{})

	done := make(chan struct{})

	go func() {
		sw.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() blocked when the worker was never started")
	}
}

func TestSweeper_StartIsIdempotent(t *testing.T) {
	fs := &fakeOrphanSweeper{perCall: 1, calls: make(chan struct{}, 8)}
	sw := NewSweeper(fs, SweeperConfig{Interval: 5 * time.Millisecond})

	// Multiple Start calls must not launch multiple worker goroutines.
	sw.Start(context.Background())
	sw.Start(context.Background())
	defer sw.Stop()

	waitSignal(t, fs.calls)
}

func TestSweeper_ContextCancellationStopsWorker(t *testing.T) {
	fs := &fakeOrphanSweeper{perCall: 1, calls: make(chan struct{}, 4)}
	sw := NewSweeper(fs, SweeperConfig{Interval: 5 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	sw.Start(ctx)

	waitSignal(t, fs.calls)

	// Cancelling ctx must terminate the worker; Stop then returns cleanly.
	cancel()

	stopped := make(chan struct{})

	go func() {
		sw.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not exit after context cancellation")
	}
}

// TestSweeper_WiredToStore exercises the seam with the same fakeStore the rest of
// the meta tests use: the Sweeper drives storage.Store.SweepOrphans and records the
// reclaimed count it reports.
func TestSweeper_WiredToStore(t *testing.T) {
	store := &fakeStore{sweepReclaimed: 5}
	sw := NewSweeper(store, SweeperConfig{Interval: time.Hour, SweepOnStart: true})

	sw.Start(context.Background())
	sw.Stop()

	if got := sw.Runs(); got != 1 {
		t.Fatalf("Runs() = %d, want 1", got)
	}
	if got := sw.Reclaimed(); got != 5 {
		t.Fatalf("Reclaimed() = %d, want 5", got)
	}
}
