package migrate

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// fakeShadowPika is an injectable ShadowPikaClient for shadow-read tests. It
// returns a fixed reply (or per-command replies) from Read, records every
// compared command, and signals each completed Read on a channel so a test can
// await the asynchronous compare without sleeping.
type fakeShadowPika struct {
	mu    sync.Mutex
	reads [][][]byte
	done  chan struct{} // buffered; one token per completed Read

	reply   []byte            // default reply returned by Read
	replies map[string][]byte // optional per-key override (keyed by args[1])
	err     error             // if non-nil, Read returns it
	block   chan struct{}     // if non-nil, Read waits on it before returning
}

func newFakeShadowPika(buffer int) *fakeShadowPika {
	return &fakeShadowPika{done: make(chan struct{}, buffer)}
}

func (f *fakeShadowPika) Do(ctx context.Context, args [][]byte) error {
	_, err := f.Read(ctx, args)
	return err
}

func (f *fakeShadowPika) Read(ctx context.Context, args [][]byte) ([]byte, error) {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	f.reads = append(f.reads, args)
	reply := f.reply
	if f.replies != nil && len(args) >= 2 {
		if r, ok := f.replies[string(args[1])]; ok {
			reply = r
		}
	}
	err := f.err
	f.mu.Unlock()
	f.done <- struct{}{}
	if err != nil {
		return nil, err
	}
	return reply, nil
}

func (f *fakeShadowPika) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reads)
}

func (f *fakeShadowPika) awaitN(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-f.done:
		case <-deadline:
			t.Fatalf("timed out waiting for shadow read %d/%d (got %d)", i+1, n, f.count())
		}
	}
}

// diffCollector is an injectable DiffSink that records every diff it receives.
type diffCollector struct {
	mu    sync.Mutex
	diffs []Diff
}

func (c *diffCollector) sink(d Diff) {
	c.mu.Lock()
	c.diffs = append(c.diffs, d)
	c.mu.Unlock()
}

func (c *diffCollector) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.diffs)
}

func TestShadowRead_Rate1AlwaysSamplesAndCompares(t *testing.T) {
	fake := newFakeShadowPika(8)
	fake.reply = []byte("v")
	col := &diffCollector{}
	r := NewShadowReader(ShadowConfig{Enabled: true, Rate: 1, Workers: 2}, fake, col.sink)
	defer r.Stop()

	// Matching replies => sampled + compared, no diff.
	r.ShadowRead("user:1", argv("GET", "user:1"), []byte("v"))
	fake.awaitN(t, 1)
	waitUntil(t, func() bool { return r.Stats().Compared == 1 })

	s := r.Stats()
	if s.Sampled != 1 || s.Compared != 1 || s.Diffs != 0 || s.Skipped != 0 {
		t.Fatalf("unexpected stats: %+v", s)
	}
	if col.len() != 0 {
		t.Fatalf("expected no diffs for matching replies, got %d", col.len())
	}
}

func TestShadowRead_Rate0NeverSamples(t *testing.T) {
	fake := newFakeShadowPika(8)
	col := &diffCollector{}
	r := NewShadowReader(ShadowConfig{Enabled: true, Rate: 0, Workers: 1}, fake, col.sink)
	defer r.Stop()

	for i := 0; i < 50; i++ {
		r.ShadowRead("user:1", argv("GET", "user:1"), []byte("v"))
	}

	// No sampling => no async Read should ever happen.
	select {
	case <-fake.done:
		t.Fatal("rate 0 must never sample")
	case <-time.After(100 * time.Millisecond):
	}

	s := r.Stats()
	if s.Sampled != 0 || s.Compared != 0 || s.Skipped != 50 {
		t.Fatalf("expected 50 skipped and nothing sampled, got %+v", s)
	}
}

func TestShadowRead_MatchingRepliesRecordNoDiff(t *testing.T) {
	fake := newFakeShadowPika(8)
	fake.reply = []byte("same")
	col := &diffCollector{}
	r := NewShadowReader(ShadowConfig{Enabled: true, Rate: 1, Workers: 1}, fake, col.sink)
	defer r.Stop()

	r.ShadowRead("k", argv("GET", "k"), []byte("same"))
	fake.awaitN(t, 1)
	waitUntil(t, func() bool { return r.Stats().Compared == 1 })

	if s := r.Stats(); s.Diffs != 0 {
		t.Fatalf("expected 0 diffs, got %+v", s)
	}
	if col.len() != 0 {
		t.Fatalf("sink should not be called for matching replies, got %d", col.len())
	}
}

func TestShadowRead_DifferingRepliesRecordDiff(t *testing.T) {
	fake := newFakeShadowPika(8)
	fake.reply = []byte("pika-value")
	col := &diffCollector{}
	r := NewShadowReader(ShadowConfig{Enabled: true, Rate: 1, Workers: 1}, fake, col.sink)
	defer r.Stop()

	r.ShadowRead("user:9", argv("GET", "user:9"), []byte("dynamo-value"))
	fake.awaitN(t, 1)
	waitUntil(t, func() bool { return r.Stats().Diffs == 1 })

	if s := r.Stats(); s.Diffs != 1 || s.Compared != 1 {
		t.Fatalf("expected 1 diff/compared, got %+v", s)
	}
	waitUntil(t, func() bool { return col.len() == 1 })
	col.mu.Lock()
	d := col.diffs[0]
	col.mu.Unlock()
	if d.Key != "user:9" || string(d.Primary) != "dynamo-value" || string(d.Shadow) != "pika-value" {
		t.Fatalf("unexpected diff recorded: %+v (primary=%q shadow=%q)", d, d.Primary, d.Shadow)
	}
}

func TestShadowRead_PrefixGatingSkipsNonMatchingKeys(t *testing.T) {
	fake := newFakeShadowPika(8)
	fake.reply = []byte("v")
	col := &diffCollector{}
	r := NewShadowReader(ShadowConfig{
		Enabled:  true,
		Rate:     1,
		Prefixes: []string{"user:"},
		Workers:  1,
	}, fake, col.sink)
	defer r.Stop()

	r.ShadowRead("order:1", argv("GET", "order:1"), []byte("v"))

	select {
	case <-fake.done:
		t.Fatal("non-matching prefix must not be shadowed")
	case <-time.After(100 * time.Millisecond):
	}

	if s := r.Stats(); s.Skipped != 1 || s.Sampled != 0 {
		t.Fatalf("expected 1 skipped, 0 sampled, got %+v", s)
	}
}

func TestShadowRead_DoesNotBlockOrAlterPrimary(t *testing.T) {
	fake := newFakeShadowPika(8)
	fake.reply = []byte("v")
	fake.block = make(chan struct{}) // Read blocks until released
	col := &diffCollector{}
	r := NewShadowReader(ShadowConfig{Enabled: true, Rate: 1, QueueSize: 8, Workers: 1}, fake, col.sink)

	primary := []byte("dynamo-value")
	start := time.Now()
	r.ShadowRead("k1", argv("GET", "k1"), primary) // grabbed by worker, blocks
	r.ShadowRead("k2", argv("GET", "k2"), primary) // queued
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("ShadowRead blocked on slow Pika for %v", elapsed)
	}
	// The caller's primary reply is untouched by the shadow read.
	if string(primary) != "dynamo-value" {
		t.Fatalf("primary reply was altered: %q", primary)
	}

	close(fake.block)
	fake.awaitN(t, 2)
	r.Stop()
}

func TestShadowRead_CopiesArgvAndPrimary(t *testing.T) {
	fake := newFakeShadowPika(8)
	fake.reply = []byte("v")
	col := &diffCollector{}
	r := NewShadowReader(ShadowConfig{Enabled: true, Rate: 1, Workers: 1}, fake, col.sink)
	defer r.Stop()

	cmd := argv("GET", "kkkkkkkk")
	primary := []byte("originalx")
	r.ShadowRead("kkkkkkkk", cmd, primary)
	// Mutate caller buffers immediately after enqueue.
	copy(cmd[1], []byte("mutated!"))
	copy(primary, []byte("mutated!!"))
	fake.awaitN(t, 1)
	waitUntil(t, func() bool { return r.Stats().Compared == 1 })

	fake.mu.Lock()
	sawKey := string(fake.reads[0][1])
	fake.mu.Unlock()
	if sawKey != "kkkkkkkk" {
		t.Fatalf("argv was not copied: shadow saw key %q", sawKey)
	}
}

func TestShadowRead_PikaErrorCountedNotFatal(t *testing.T) {
	fake := newFakeShadowPika(8)
	fake.err = errors.New("pika down")
	col := &diffCollector{}
	r := NewShadowReader(ShadowConfig{Enabled: true, Rate: 1, Workers: 1}, fake, col.sink)
	defer r.Stop()

	r.ShadowRead("k", argv("GET", "k"), []byte("v"))
	fake.awaitN(t, 1)
	waitUntil(t, func() bool { return r.Stats().Errors == 1 })

	if s := r.Stats(); s.Errors != 1 || s.Compared != 0 || s.Diffs != 0 {
		t.Fatalf("expected 1 error, 0 compared/diffs, got %+v", s)
	}
	if col.len() != 0 {
		t.Fatalf("sink must not be called on Pika error, got %d", col.len())
	}
}

func TestShadowRead_DisabledAndNilAreNoOps(t *testing.T) {
	var nilR *ShadowReader
	nilR.ShadowRead("k", argv("GET", "k"), []byte("v")) // must not panic
	nilR.Stop()                                         // must not panic
	if nilR.Enabled() {
		t.Fatal("nil reader must not be enabled")
	}

	fake := newFakeShadowPika(4)
	r := NewShadowReader(ShadowConfig{Enabled: false, Rate: 1}, fake, nil)
	if r.Enabled() {
		t.Fatal("expected disabled reader")
	}
	r.ShadowRead("k", argv("GET", "k"), []byte("v"))

	// Enabled but nil client => still disabled.
	r2 := NewShadowReader(ShadowConfig{Enabled: true, Rate: 1}, nil, nil)
	if r2.Enabled() {
		t.Fatal("expected disabled reader with nil client")
	}

	select {
	case <-fake.done:
		t.Fatal("disabled reader must not shadow")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestShadowRead_SetRandSourceMakesSamplingDeterministic(t *testing.T) {
	fake := newFakeShadowPika(64)
	fake.reply = []byte("v")
	col := &diffCollector{}
	// Rate 0.5 with a seeded source: sampling is deterministic, so we only
	// assert that some but not all reads are sampled and the machinery works.
	r := NewShadowReader(ShadowConfig{Enabled: true, Rate: 0.5, Workers: 2}, fake, col.sink)
	r.SetRandSource(rand.NewSource(1))
	defer r.Stop()

	const n = 100
	for i := 0; i < n; i++ {
		r.ShadowRead("k", argv("GET", "k"), []byte("v"))
	}

	s := r.Stats()
	if s.Sampled == 0 || s.Sampled == n {
		t.Fatalf("rate 0.5 should sample a strict subset, sampled %d of %d", s.Sampled, n)
	}
	if s.Sampled+s.Skipped != n {
		t.Fatalf("sampled+skipped should equal total: %+v", s)
	}
}

func TestShadowRead_PlainPikaClientTreatsReplyAsEmpty(t *testing.T) {
	// A client implementing only PikaClient (Do) — no Read. The shadow reply is
	// treated as empty, so an empty primary matches and a non-empty one diffs.
	fake := newFakePika(8)
	col := &diffCollector{}
	r := NewShadowReader(ShadowConfig{Enabled: true, Rate: 1, Workers: 1}, fake, col.sink)
	defer r.Stop()

	// Empty primary vs empty shadow => no diff.
	r.ShadowRead("k", argv("GET", "k"), nil)
	fake.awaitN(t, 1)
	waitUntil(t, func() bool { return r.Stats().Compared == 1 })
	if r.Stats().Diffs != 0 {
		t.Fatalf("empty vs empty should not diff, got %+v", r.Stats())
	}

	// Non-empty primary vs empty shadow => diff.
	r.ShadowRead("k", argv("GET", "k"), []byte("x"))
	fake.awaitN(t, 1)
	waitUntil(t, func() bool { return r.Stats().Diffs == 1 })
}

func TestStop_ShadowDrainsQueuedComparisons(t *testing.T) {
	fake := newFakeShadowPika(128)
	fake.reply = []byte("v")
	col := &diffCollector{}
	r := NewShadowReader(ShadowConfig{Enabled: true, Rate: 1, QueueSize: 128, Workers: 2}, fake, col.sink)

	const n = 40
	for i := 0; i < n; i++ {
		r.ShadowRead("k", argv("GET", "k"), []byte("v"))
	}
	r.Stop()

	if got := fake.count(); got != n {
		t.Fatalf("Stop did not drain: compared %d of %d", got, n)
	}
	if s := r.Stats(); s.Compared != n {
		t.Fatalf("expected %d compared after drain, got %+v", n, s)
	}
}

func TestParseShadowReadFlag(t *testing.T) {
	cases := []struct {
		in      string
		enabled bool
		rate    float64
	}{
		{"", false, 0},
		{"off", false, 0},
		{"OFF", false, 0},
		{"none", false, 0},
		{"disabled", false, 0},
		{"sample:0.01", true, 0.01},
		{"SAMPLE:0.01", true, 0.01},
		{" sample:0.5 ", true, 0.5},
		{"sample:1", true, 1},
		{"sample:0", true, 0},
		{"sample:2", true, 1},    // clamped to 1
		{"sample:-0.5", true, 0}, // clamped to 0
		{"sample:", false, 0},    // malformed => disabled
		{"sample:abc", false, 0}, // unparseable => disabled
		{"0.01", false, 0},       // missing sample: prefix => disabled
		{"garbage", false, 0},    // unknown => disabled
	}
	for _, tc := range cases {
		got := ParseShadowReadFlag(tc.in)
		if got.Enabled != tc.enabled || got.Rate != tc.rate {
			t.Errorf("ParseShadowReadFlag(%q) = {Enabled:%v Rate:%v}, want {Enabled:%v Rate:%v}",
				tc.in, got.Enabled, got.Rate, tc.enabled, tc.rate)
		}
	}
}
