package migrate

import (
	"context"
	"sync"
	"testing"
	"time"
)

// hooks_integration_test.go exercises the migration hooks *together* through
// the Hooks aggregate, validating the end-to-end flows of requirement 17:
//
//   - 17.1 dual-write: a matching write is mirrored to Pika asynchronously,
//     prefix-gated, observed via Stats/await;
//   - 17.2 shadow-read: a sampled read whose Pika reply differs from the
//     primary records a diff via the sink; a matching reply records none;
//   - 17.3 fallback: a DynamoDB miss where Pika holds the value returns it and
//     backfills; a miss where Pika lacks it returns not-found.
//
// Unlike the per-hook unit tests, these wire all three hooks over a *single*
// shared fake Pika (implementing both PikaClient and ShadowPikaClient) plus a
// shared fake Backfiller, so the aggregate wiring and the hooks' cooperation
// are what is under test. Async paths use channel/await synchronization (no
// sleeps), mirroring the existing hook unit tests.

// sharedFakePika is a single fake backend that plays the role of Pika for all
// three hooks at once: DualWriter mirrors via Do, ShadowReader and Fallback
// read via Read (the ShadowPikaClient extension). It behaves like a tiny
// key/value store so a write mirrored through the dual-write path can later be
// observed by a shadow read or fallback read of the same key — letting the
// combined scenario tie the hooks together.
//
// Writes (Do) are keyed by args[1] and store args[2] as the value; reads (Read)
// return the stored value for args[1] (nil when absent, i.e. a Pika miss).
// Every Do and Read signals a buffered channel so tests await the asynchronous
// hooks without sleeping.
type sharedFakePika struct {
	mu     sync.Mutex
	store  map[string][]byte
	writes [][][]byte
	reads  [][][]byte

	writeDone chan struct{} // buffered; one token per completed Do
	readDone  chan struct{} // buffered; one token per completed Read
}

func newSharedFakePika(buffer int) *sharedFakePika {
	return &sharedFakePika{
		store:     make(map[string][]byte),
		writeDone: make(chan struct{}, buffer),
		readDone:  make(chan struct{}, buffer),
	}
}

// seed pre-populates the store, modeling data that already lives in Pika (for
// shadow-read diffs and fallback hits) without going through a mirror write.
func (f *sharedFakePika) seed(key string, value []byte) {
	f.mu.Lock()
	f.store[key] = append([]byte(nil), value...)
	f.mu.Unlock()
}

// Do plays a mirrored write into the store. It records the argv and, for a
// command shaped like SET <key> <value>, stores the value under the key so a
// later Read observes it.
func (f *sharedFakePika) Do(ctx context.Context, args [][]byte) error {
	f.mu.Lock()
	f.writes = append(f.writes, args)
	if len(args) >= 3 {
		f.store[string(args[1])] = append([]byte(nil), args[2]...)
	}
	f.mu.Unlock()
	f.writeDone <- struct{}{}
	return nil
}

// Read returns the stored value for args[1], or nil when the key is absent
// (a Pika miss). It records the read argv and signals completion.
func (f *sharedFakePika) Read(ctx context.Context, args [][]byte) ([]byte, error) {
	f.mu.Lock()
	f.reads = append(f.reads, args)
	var reply []byte
	if len(args) >= 2 {
		if v, ok := f.store[string(args[1])]; ok {
			reply = append([]byte(nil), v...)
		}
	}
	f.mu.Unlock()
	f.readDone <- struct{}{}
	return reply, nil
}

func (f *sharedFakePika) writeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.writes)
}

func (f *sharedFakePika) get(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.store[key]
	return v, ok
}

// awaitWrites waits for n completed Do calls or fails on timeout.
func (f *sharedFakePika) awaitWrites(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-f.writeDone:
		case <-deadline:
			t.Fatalf("timed out waiting for mirror write %d/%d (got %d)", i+1, n, f.writeCount())
		}
	}
}

// awaitReads waits for n completed Read calls or fails on timeout.
func (f *sharedFakePika) awaitReads(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-f.readDone:
		case <-deadline:
			t.Fatalf("timed out waiting for shadow read %d/%d", i+1, n)
		}
	}
}

// newIntegrationHooks wires all three hooks over the shared fake Pika and a
// shared backfiller, with rate=1 shadow sampling (deterministic) and the given
// prefix allowlist applied uniformly across the hooks.
func newIntegrationHooks(fake *sharedFakePika, sink DiffSink, bf Backfiller, prefixes []string) *Hooks {
	return &Hooks{
		DualWriter: NewDualWriter(DualWriteConfig{
			Enabled:  true,
			Target:   "pika",
			Prefixes: prefixes,
			Workers:  2,
		}, fake),
		ShadowReader: NewShadowReader(ShadowConfig{
			Enabled:  true,
			Rate:     1,
			Prefixes: prefixes,
			Workers:  2,
		}, fake, sink),
		Fallback: NewFallback(FallbackConfig{
			Enabled:  true,
			Prefixes: prefixes,
		}, fake, bf),
	}
}

// stopHooks tears down the async hooks (drains queues, joins workers).
func stopHooks(h *Hooks) {
	h.DualWriter.Stop()
	h.ShadowReader.Stop()
}

// TestHooks_DualWritePath validates requirement 17.1: a write to a key matching
// the prefix allowlist is mirrored to Pika asynchronously (observed via
// await + Stats), while a write outside the allowlist is skipped and never
// reaches Pika.
//
// **Validates: Requirements 17.1**
func TestHooks_DualWritePath(t *testing.T) {
	fake := newSharedFakePika(8)
	col := &diffCollector{}
	bf := &fakeBackfiller{}
	h := newIntegrationHooks(fake, col.sink, bf, []string{"user:"})
	defer stopHooks(h)

	// Matching prefix: mirrored asynchronously and lands in Pika.
	h.DualWriter.MirrorWrite("user:1", argv("SET", "user:1", "v1"))
	fake.awaitWrites(t, 1)

	// Non-matching prefix: skipped, never mirrored.
	h.DualWriter.MirrorWrite("order:1", argv("SET", "order:1", "x"))

	// Give any erroneous async delivery a chance to happen, then assert none.
	select {
	case <-fake.writeDone:
		t.Fatal("non-matching prefix must not be mirrored")
	case <-time.After(100 * time.Millisecond):
	}

	if got, ok := fake.get("user:1"); !ok || string(got) != "v1" {
		t.Fatalf("mirrored write did not land in Pika: got=%q ok=%v", got, ok)
	}
	if s := h.DualWriter.Stats(); s.Mirrored != 1 || s.Skipped != 1 || s.Dropped != 0 || s.Failed != 0 {
		t.Fatalf("unexpected dual-write stats: %+v", s)
	}
}

// TestHooks_ShadowReadDiffPath validates requirement 17.2: a sampled read whose
// Pika reply differs from the primary records a diff via the sink and counts a
// diff; a matching reply records none.
//
// **Validates: Requirements 17.2**
func TestHooks_ShadowReadDiffPath(t *testing.T) {
	fake := newSharedFakePika(8)
	col := &diffCollector{}
	bf := &fakeBackfiller{}
	h := newIntegrationHooks(fake, col.sink, bf, nil)
	defer stopHooks(h)

	// Pika holds a value that differs from what the primary (DynamoDB) returned.
	fake.seed("user:diff", []byte("pika-value"))
	h.ShadowReader.ShadowRead("user:diff", argv("GET", "user:diff"), []byte("dynamo-value"))
	fake.awaitReads(t, 1)
	waitUntil(t, func() bool { return h.ShadowReader.Stats().Diffs == 1 })

	waitUntil(t, func() bool { return col.len() == 1 })
	col.mu.Lock()
	d := col.diffs[0]
	col.mu.Unlock()
	if d.Key != "user:diff" || string(d.Primary) != "dynamo-value" || string(d.Shadow) != "pika-value" {
		t.Fatalf("unexpected diff recorded: key=%q primary=%q shadow=%q", d.Key, d.Primary, d.Shadow)
	}

	// A matching reply records no diff.
	fake.seed("user:same", []byte("agree"))
	h.ShadowReader.ShadowRead("user:same", argv("GET", "user:same"), []byte("agree"))
	fake.awaitReads(t, 1)
	waitUntil(t, func() bool { return h.ShadowReader.Stats().Compared == 2 })

	if s := h.ShadowReader.Stats(); s.Diffs != 1 {
		t.Fatalf("expected exactly 1 diff after a matching read, got %+v", s)
	}
	if col.len() != 1 {
		t.Fatalf("sink must not be called for the matching read, got %d diffs", col.len())
	}
}

// TestHooks_FallbackBackfillPath validates requirement 17.3: a DynamoDB miss
// where Pika holds the value returns it to the caller and backfills it into the
// primary store; a miss where Pika also lacks the value returns not-found and
// does not backfill.
//
// **Validates: Requirements 17.3**
func TestHooks_FallbackBackfillPath(t *testing.T) {
	fake := newSharedFakePika(8)
	col := &diffCollector{}
	bf := &fakeBackfiller{}
	h := newIntegrationHooks(fake, col.sink, bf, nil)
	defer stopHooks(h)

	// Pika holds a value DynamoDB missed on -> returned + backfilled.
	fake.seed("user:hit", []byte("from-pika"))
	value, found, err := h.Fallback.FallbackOnMiss(context.Background(), "user:hit", argv("GET", "user:hit"))
	if err != nil || !found || string(value) != "from-pika" {
		t.Fatalf("expected fallback hit, got value=%q found=%v err=%v", value, found, err)
	}
	if bf.count() != 1 {
		t.Fatalf("expected exactly 1 backfill, got %d", bf.count())
	}
	bf.mu.Lock()
	call := bf.got[0]
	bf.mu.Unlock()
	if call.key != "user:hit" || string(call.value) != "from-pika" {
		t.Fatalf("unexpected backfill: key=%q value=%q", call.key, call.value)
	}

	// Pika also misses -> not found, no backfill.
	value, found, err = h.Fallback.FallbackOnMiss(context.Background(), "user:absent", argv("GET", "user:absent"))
	if err != nil || found || value != nil {
		t.Fatalf("expected clean miss, got value=%q found=%v err=%v", value, found, err)
	}
	if bf.count() != 1 {
		t.Fatalf("miss must not backfill, backfill count=%d", bf.count())
	}
	if s := h.Fallback.Stats(); s.Fallbacks != 2 || s.Hits != 1 || s.Backfills != 1 || s.Errors != 0 {
		t.Fatalf("unexpected fallback stats: %+v", s)
	}
}

// TestHooks_CombinedMigrationFlow ties the three hooks together over the shared
// fake Pika: a dual-write mirrors a value into Pika, a fallback on a different
// (pre-seeded) key returns and backfills it, and a shadow read of the
// dual-written key agrees (no diff) because the mirror already landed. It then
// asserts the aggregate Hooks fields cooperate and each hook's Stats reflect
// its operation.
//
// **Validates: Requirements 17.1, 17.2, 17.3**
func TestHooks_CombinedMigrationFlow(t *testing.T) {
	fake := newSharedFakePika(16)
	col := &diffCollector{}
	bf := &fakeBackfiller{}
	h := newIntegrationHooks(fake, col.sink, bf, []string{"user:"})
	defer stopHooks(h)

	// 1) Dual-write mirrors user:100=hello into Pika.
	h.DualWriter.MirrorWrite("user:100", argv("SET", "user:100", "hello"))
	fake.awaitWrites(t, 1)
	waitUntil(t, func() bool { return h.DualWriter.Stats().Mirrored == 1 })

	// 2) Fallback on a *different* key that already lives in Pika: returned and
	//    backfilled into the primary store.
	fake.seed("user:200", []byte("legacy"))
	value, found, err := h.Fallback.FallbackOnMiss(context.Background(), "user:200", argv("GET", "user:200"))
	if err != nil || !found || string(value) != "legacy" {
		t.Fatalf("combined: fallback hit expected, got value=%q found=%v err=%v", value, found, err)
	}

	// 3) Shadow read of the dual-written key: since the mirror already landed,
	//    Pika agrees with the primary and no diff is recorded.
	h.ShadowReader.ShadowRead("user:100", argv("GET", "user:100"), []byte("hello"))
	fake.awaitReads(t, 1)
	waitUntil(t, func() bool { return h.ShadowReader.Stats().Compared == 1 })

	// Aggregate assertions: each hook's Stats reflect exactly its operation.
	if s := h.DualWriter.Stats(); s.Mirrored != 1 || s.Skipped != 0 {
		t.Fatalf("combined dual-write stats: %+v", s)
	}
	if s := h.Fallback.Stats(); s.Fallbacks != 1 || s.Hits != 1 || s.Backfills != 1 {
		t.Fatalf("combined fallback stats: %+v", s)
	}
	if s := h.ShadowReader.Stats(); s.Compared != 1 || s.Diffs != 0 {
		t.Fatalf("combined shadow stats: %+v", s)
	}
	if col.len() != 0 {
		t.Fatalf("combined: no diff expected once the mirror landed, got %d", col.len())
	}
	if bf.count() != 1 {
		t.Fatalf("combined: exactly 1 backfill expected, got %d", bf.count())
	}
}

// TestHooks_DisabledHooksAreNilSafe asserts the aggregate is safe to call when
// individual hooks are disabled (nil fields): every hook method is nil-safe, so
// the command layer can hold a partially-populated Hooks and call through it
// without branching on nil.
func TestHooks_DisabledHooksAreNilSafe(t *testing.T) {
	// A Hooks with every migration hook disabled (all fields nil).
	h := &Hooks{}

	// None of these must panic, and all must report the disabled/no-op result.
	h.DualWriter.MirrorWrite("user:1", argv("SET", "user:1", "v"))
	h.DualWriter.Stop()
	if h.DualWriter.Enabled() {
		t.Fatal("nil DualWriter must not be enabled")
	}
	if h.DualWriter.ShouldMirror("user:1") {
		t.Fatal("nil DualWriter must not mirror")
	}

	h.ShadowReader.ShadowRead("user:1", argv("GET", "user:1"), []byte("v"))
	h.ShadowReader.Stop()
	if h.ShadowReader.Enabled() {
		t.Fatal("nil ShadowReader must not be enabled")
	}

	value, found, err := h.Fallback.FallbackOnMiss(context.Background(), "user:1", argv("GET", "user:1"))
	if value != nil || found || err != nil {
		t.Fatalf("nil Fallback must be a clean no-op, got value=%q found=%v err=%v", value, found, err)
	}
	if h.Fallback.Enabled() || h.Fallback.ShouldFallback("user:1") {
		t.Fatal("nil Fallback must not be enabled or fallback")
	}

	// The big-key counter is also nil-safe through the aggregate.
	h.BigKeys.Inc()
	if got := h.BigKeys.Interceptions(); got != 0 {
		t.Fatalf("nil BigKeys must report 0, got %d", got)
	}

	// Stats accessors on nil hooks return zero values.
	if (h.DualWriter.Stats() != Stats{}) {
		t.Fatal("nil DualWriter Stats must be zero")
	}
	if (h.ShadowReader.Stats() != ShadowStats{}) {
		t.Fatal("nil ShadowReader Stats must be zero")
	}
	if (h.Fallback.Stats() != FallbackStats{}) {
		t.Fatal("nil Fallback Stats must be zero")
	}
}

// TestHooks_ShadowReadSamplingBoundaries validates the deterministic sampling
// boundaries of requirement 17.2 through the integration harness: a reader at
// rate 0 never samples (no Pika read, no compare), while a reader at rate 1
// always samples. Both boundaries are deterministic, so no seeded RNG is
// needed.
//
// **Validates: Requirements 17.2**
func TestHooks_ShadowReadSamplingBoundaries(t *testing.T) {
	// rate 0: never samples, so the shared Pika is never read.
	t.Run("rate0_never", func(t *testing.T) {
		fake := newSharedFakePika(8)
		col := &diffCollector{}
		r := NewShadowReader(ShadowConfig{Enabled: true, Rate: 0, Workers: 2}, fake, col.sink)
		defer r.Stop()

		fake.seed("user:x", []byte("pika"))
		for i := 0; i < 50; i++ {
			r.ShadowRead("user:x", argv("GET", "user:x"), []byte("dynamo"))
		}

		// No sampling => no async Read should ever fire.
		select {
		case <-fake.readDone:
			t.Fatal("rate 0 must never sample/read Pika")
		case <-time.After(100 * time.Millisecond):
		}
		if s := r.Stats(); s.Sampled != 0 || s.Compared != 0 || s.Skipped != 50 {
			t.Fatalf("rate 0: expected 50 skipped, nothing sampled, got %+v", s)
		}
		if col.len() != 0 {
			t.Fatalf("rate 0: expected no diffs, got %d", col.len())
		}
	})

	// rate 1: always samples, so every eligible read is compared.
	t.Run("rate1_always", func(t *testing.T) {
		fake := newSharedFakePika(8)
		col := &diffCollector{}
		r := NewShadowReader(ShadowConfig{Enabled: true, Rate: 1, Workers: 2}, fake, col.sink)
		defer r.Stop()

		fake.seed("user:y", []byte("agree"))
		const n = 5
		for i := 0; i < n; i++ {
			r.ShadowRead("user:y", argv("GET", "user:y"), []byte("agree"))
		}
		fake.awaitReads(t, n)
		waitUntil(t, func() bool { return r.Stats().Compared == n })
		if s := r.Stats(); s.Sampled != n || s.Diffs != 0 {
			t.Fatalf("rate 1: expected all %d sampled and no diffs, got %+v", n, s)
		}
	})
}

// TestHooks_FallbackIsReadOnly validates the read-only guarantee of requirement
// 17.3 through the integration harness: a fallback hit reads from Pika and
// backfills into the *primary* store (via the Backfiller), but must NEVER issue
// a write (Do) against Pika itself. Asserting the shared fake's writeCount stays
// zero proves the fallback path is read-only with respect to the source of
// truth.
//
// **Validates: Requirements 17.3**
func TestHooks_FallbackIsReadOnly(t *testing.T) {
	fake := newSharedFakePika(8)
	bf := &fakeBackfiller{}
	// Fallback only; no dual-writer, so any Pika write could only come from the
	// fallback path itself.
	f := NewFallback(FallbackConfig{Enabled: true, Prefixes: []string{"user:"}}, fake, bf)

	fake.seed("user:ro", []byte("legacy"))
	value, found, err := f.FallbackOnMiss(context.Background(), "user:ro", argv("GET", "user:ro"))
	if err != nil || !found || string(value) != "legacy" {
		t.Fatalf("expected fallback hit, got value=%q found=%v err=%v", value, found, err)
	}

	// The value was backfilled into the primary store...
	if bf.count() != 1 {
		t.Fatalf("expected exactly 1 backfill into the primary store, got %d", bf.count())
	}
	// ...but the source-of-truth (Pika) was only read, never written.
	if wc := fake.writeCount(); wc != 0 {
		t.Fatalf("fallback must be read-only against Pika, but observed %d writes", wc)
	}
}

// TestHooks_DisabledHooksAreNoOps validates that hooks explicitly disabled by
// flag (Enabled:false) — as opposed to nil fields — are complete no-ops through
// the aggregate: no goroutines mirror, no Pika reads happen, and no backfills
// occur. This complements TestHooks_DisabledHooksAreNilSafe (which covers nil
// fields) and mirrors the "disabled writer/reader/fallback is a no-op" coverage
// called for by requirement 17.
func TestHooks_DisabledHooksAreNoOps(t *testing.T) {
	fake := newSharedFakePika(8)
	col := &diffCollector{}
	bf := &fakeBackfiller{}
	h := &Hooks{
		DualWriter:   NewDualWriter(DualWriteConfig{Enabled: false}, fake),
		ShadowReader: NewShadowReader(ShadowConfig{Enabled: false, Rate: 1}, fake, col.sink),
		Fallback:     NewFallback(FallbackConfig{Enabled: false}, fake, bf),
	}
	defer stopHooks(h)

	if h.DualWriter.Enabled() || h.ShadowReader.Enabled() || h.Fallback.Enabled() {
		t.Fatal("all hooks were constructed disabled but report Enabled()")
	}

	fake.seed("user:z", []byte("legacy"))
	h.DualWriter.MirrorWrite("user:z", argv("SET", "user:z", "v"))
	h.ShadowReader.ShadowRead("user:z", argv("GET", "user:z"), []byte("dynamo"))
	value, found, err := h.Fallback.FallbackOnMiss(context.Background(), "user:z", argv("GET", "user:z"))
	if value != nil || found || err != nil {
		t.Fatalf("disabled fallback must be a clean miss, got value=%q found=%v err=%v", value, found, err)
	}

	// Give any erroneous async delivery a chance, then assert nothing touched Pika.
	select {
	case <-fake.writeDone:
		t.Fatal("disabled dual-writer must not mirror")
	case <-fake.readDone:
		t.Fatal("disabled shadow-reader must not read Pika")
	case <-time.After(100 * time.Millisecond):
	}

	if fake.writeCount() != 0 {
		t.Fatalf("disabled hooks must not write Pika, got %d writes", fake.writeCount())
	}
	if bf.count() != 0 {
		t.Fatalf("disabled fallback must not backfill, got %d", bf.count())
	}
	if s := h.DualWriter.Stats(); (s != Stats{}) {
		t.Fatalf("disabled dual-writer stats must be zero, got %+v", s)
	}
	if s := h.ShadowReader.Stats(); (s != ShadowStats{}) {
		t.Fatalf("disabled shadow-reader stats must be zero, got %+v", s)
	}
}
