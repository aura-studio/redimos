package migrate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakePika is an injectable PikaClient for tests. It records the argv of every
// mirror write and signals each one on a channel so a test can await the
// asynchronous delivery without sleeping. Optional hooks let a test make Do
// slow or failing.
type fakePika struct {
	mu   sync.Mutex
	got  [][][]byte
	done chan struct{} // buffered; one token per completed Do

	block chan struct{} // if non-nil, Do waits on it before returning
	err   error         // if non-nil, Do returns it
}

func newFakePika(buffer int) *fakePika {
	return &fakePika{done: make(chan struct{}, buffer)}
}

func (f *fakePika) Do(ctx context.Context, args [][]byte) error {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	f.got = append(f.got, args)
	f.mu.Unlock()
	f.done <- struct{}{}
	return f.err
}

func (f *fakePika) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.got)
}

// awaitN waits for n completed Do calls or fails the test on timeout.
func (f *fakePika) awaitN(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-f.done:
		case <-deadline:
			t.Fatalf("timed out waiting for mirror write %d/%d (got %d)", i+1, n, f.count())
		}
	}
}

func argv(parts ...string) [][]byte {
	out := make([][]byte, len(parts))
	for i, p := range parts {
		out[i] = []byte(p)
	}
	return out
}

func TestMirrorWrite_MatchingPrefixIsMirroredAsync(t *testing.T) {
	fake := newFakePika(4)
	w := NewDualWriter(DualWriteConfig{
		Enabled:  true,
		Target:   "pika",
		Prefixes: []string{"user:"},
		Workers:  2,
	}, fake)
	defer w.Stop()

	w.MirrorWrite("user:42", argv("SET", "user:42", "v"))
	fake.awaitN(t, 1)

	if got := fake.count(); got != 1 {
		t.Fatalf("expected 1 mirrored write, got %d", got)
	}
	if s := w.Stats(); s.Mirrored != 1 || s.Skipped != 0 || s.Dropped != 0 || s.Failed != 0 {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

func TestMirrorWrite_NonMatchingPrefixIsNotMirrored(t *testing.T) {
	fake := newFakePika(4)
	w := NewDualWriter(DualWriteConfig{
		Enabled:  true,
		Prefixes: []string{"user:"},
		Workers:  1,
	}, fake)
	defer w.Stop()

	w.MirrorWrite("order:1", argv("SET", "order:1", "v"))

	// Give any (erroneous) async delivery a chance to happen.
	select {
	case <-fake.done:
		t.Fatal("non-matching prefix should not be mirrored")
	case <-time.After(100 * time.Millisecond):
	}

	if s := w.Stats(); s.Skipped != 1 || s.Mirrored != 0 {
		t.Fatalf("expected 1 skipped and 0 mirrored, got %+v", s)
	}
}

func TestMirrorWrite_EmptyPrefixMirrorsEverything(t *testing.T) {
	fake := newFakePika(4)
	w := NewDualWriter(DualWriteConfig{Enabled: true, Workers: 1}, fake)
	defer w.Stop()

	w.MirrorWrite("anything", argv("DEL", "anything"))
	fake.awaitN(t, 1)

	if s := w.Stats(); s.Mirrored != 1 || s.Skipped != 0 {
		t.Fatalf("expected everything mirrored, got %+v", s)
	}
}

func TestMirrorWrite_SlowPikaDoesNotBlockCaller(t *testing.T) {
	fake := newFakePika(4)
	fake.block = make(chan struct{}) // Do blocks until released
	w := NewDualWriter(DualWriteConfig{
		Enabled:   true,
		QueueSize: 8,
		Workers:   1,
	}, fake)

	// The caller must return promptly even though the single worker is stuck
	// inside a blocked Do.
	start := time.Now()
	w.MirrorWrite("k1", argv("SET", "k1", "v")) // grabbed by the worker, blocks
	w.MirrorWrite("k2", argv("SET", "k2", "v")) // queued
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("MirrorWrite blocked on slow Pika for %v", elapsed)
	}

	// Release the worker and confirm both writes eventually land, then stop.
	close(fake.block)
	fake.awaitN(t, 2)
	w.Stop()
}

func TestMirrorWrite_QueueFullDropsAreCounted(t *testing.T) {
	fake := newFakePika(16)
	fake.block = make(chan struct{}) // pin the worker so the queue fills
	w := NewDualWriter(DualWriteConfig{
		Enabled:   true,
		QueueSize: 1,
		Workers:   1,
	}, fake)

	// First write is picked up by the worker (which blocks); the next fills the
	// size-1 queue. With a single pinned worker and a size-1 queue, at most two
	// writes can be in flight, so the remaining writes are dropped.
	w.MirrorWrite("k0", argv("SET", "k0", "v"))
	w.MirrorWrite("k1", argv("SET", "k1", "v"))

	for i := 0; i < 20; i++ {
		w.MirrorWrite("k", argv("SET", "k", "v"))
	}

	waitUntil(t, func() bool { return w.Stats().Dropped > 0 })
	if s := w.Stats(); s.Dropped == 0 {
		t.Fatalf("expected some dropped writes on a full queue, got %+v", s)
	}

	close(fake.block)
	w.Stop()
}

func TestMirrorWrite_FailingPikaIsCountedNotFatal(t *testing.T) {
	fake := newFakePika(4)
	fake.err = errors.New("pika down")
	w := NewDualWriter(DualWriteConfig{Enabled: true, Workers: 1}, fake)
	defer w.Stop()

	w.MirrorWrite("k", argv("SET", "k", "v"))
	fake.awaitN(t, 1)

	waitUntil(t, func() bool { return w.Stats().Failed == 1 })
	if s := w.Stats(); s.Failed != 1 || s.Mirrored != 0 {
		t.Fatalf("expected 1 failed and 0 mirrored, got %+v", s)
	}
}

func TestStop_DrainsQueuedWrites(t *testing.T) {
	fake := newFakePika(64)
	w := NewDualWriter(DualWriteConfig{
		Enabled:   true,
		QueueSize: 64,
		Workers:   2,
	}, fake)

	const n = 40
	for i := 0; i < n; i++ {
		w.MirrorWrite("k", argv("SET", "k", "v"))
	}
	// Stop must drain everything already queued before returning.
	w.Stop()

	if got := fake.count(); got != n {
		t.Fatalf("Stop did not drain: mirrored %d of %d", got, n)
	}
	if s := w.Stats(); s.Mirrored != n {
		t.Fatalf("expected %d mirrored after drain, got %+v", n, s)
	}
}

func TestStop_IsIdempotentAndNilSafe(t *testing.T) {
	var nilW *DualWriter
	nilW.Stop()                // must not panic
	nilW.MirrorWrite("k", nil) // must not panic
	if nilW.Enabled() {
		t.Fatal("nil writer must not be enabled")
	}

	fake := newFakePika(4)
	w := NewDualWriter(DualWriteConfig{Enabled: true, Workers: 1}, fake)
	w.Stop()
	w.Stop() // second Stop is a no-op

	// After Stop, MirrorWrite is a no-op and does not panic.
	w.MirrorWrite("k", argv("SET", "k", "v"))
}

func TestNewDualWriter_DisabledIsNoOp(t *testing.T) {
	fake := newFakePika(4)

	// Disabled by flag.
	w := NewDualWriter(DualWriteConfig{Enabled: false}, fake)
	if w.Enabled() {
		t.Fatal("expected disabled writer")
	}
	w.MirrorWrite("k", argv("SET", "k", "v"))

	// Enabled but nil client -> still disabled.
	w2 := NewDualWriter(DualWriteConfig{Enabled: true}, nil)
	if w2.Enabled() {
		t.Fatal("expected disabled writer with nil client")
	}
	w2.MirrorWrite("k", argv("SET", "k", "v"))

	select {
	case <-fake.done:
		t.Fatal("disabled writer must not mirror")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestMirrorWrite_CopiesArgv(t *testing.T) {
	fake := newFakePika(4)
	w := NewDualWriter(DualWriteConfig{Enabled: true, Workers: 1}, fake)
	defer w.Stop()

	cmd := argv("SET", "k", "original")
	w.MirrorWrite("k", cmd)
	// Mutate the caller's buffer immediately after enqueue.
	copy(cmd[2], []byte("mutatedd"))
	fake.awaitN(t, 1)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got := string(fake.got[0][2]); got != "original" {
		t.Fatalf("argv was not copied: mirror saw %q", got)
	}
}

func TestParseDualWriteFlag(t *testing.T) {
	cases := []struct {
		in      string
		enabled bool
		target  string
	}{
		{"", false, ""},
		{"off", false, ""},
		{"OFF", false, ""},
		{"none", false, ""},
		{"disabled", false, ""},
		{"pika", true, "pika"},
		{"PIKA", true, "pika"},
		{" pika ", true, "pika"},
	}
	for _, tc := range cases {
		got := ParseDualWriteFlag(tc.in)
		if got.Enabled != tc.enabled || got.Target != tc.target {
			t.Errorf("ParseDualWriteFlag(%q) = {Enabled:%v Target:%q}, want {Enabled:%v Target:%q}",
				tc.in, got.Enabled, got.Target, tc.enabled, tc.target)
		}
	}
}

func TestMatchAnyPrefix(t *testing.T) {
	cases := []struct {
		key      string
		prefixes []string
		want     bool
	}{
		{"user:1", nil, true},                     // empty list => match all
		{"user:1", []string{}, true},              // empty list => match all
		{"user:1", []string{"user:"}, true},       // matching prefix
		{"order:1", []string{"user:"}, false},     // non-matching
		{"user:1", []string{"a:", "user:"}, true}, // any-of match
		{"ab", []string{"abc"}, false},            // key shorter than prefix
		{"anything", []string{""}, true},          // empty prefix is a wildcard
	}
	for _, tc := range cases {
		if got := matchAnyPrefix(tc.key, tc.prefixes); got != tc.want {
			t.Errorf("matchAnyPrefix(%q, %v) = %v, want %v", tc.key, tc.prefixes, got, tc.want)
		}
	}
}

// waitUntil polls cond until it is true or a short deadline elapses.
func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("condition not met before deadline")
		case <-time.After(2 * time.Millisecond):
		}
	}
}
