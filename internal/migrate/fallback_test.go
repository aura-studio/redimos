package migrate

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// fakeBackfiller is an injectable Backfiller for tests. It records every
// backfilled (key, value) pair and can be made to fail.
type fakeBackfiller struct {
	mu  sync.Mutex
	got []backfillCall
	err error
}

type backfillCall struct {
	key   string
	value []byte
}

func (b *fakeBackfiller) Backfill(ctx context.Context, key string, value []byte) error {
	b.mu.Lock()
	b.got = append(b.got, backfillCall{key: key, value: append([]byte(nil), value...)})
	err := b.err
	b.mu.Unlock()
	return err
}

func (b *fakeBackfiller) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.got)
}

func TestFallbackOnMiss_PikaHasValueBackfillsAndReturns(t *testing.T) {
	fake := newFakeShadowPika(4)
	fake.reply = []byte("pika-value")
	bf := &fakeBackfiller{}
	f := NewFallback(FallbackConfig{Enabled: true}, fake, bf)

	value, found, err := f.FallbackOnMiss(context.Background(), "user:1", argv("GET", "user:1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected found=true when Pika holds the value")
	}
	if string(value) != "pika-value" {
		t.Fatalf("expected returned value %q, got %q", "pika-value", value)
	}
	if bf.count() != 1 {
		t.Fatalf("expected exactly 1 backfill, got %d", bf.count())
	}
	bf.mu.Lock()
	call := bf.got[0]
	bf.mu.Unlock()
	if call.key != "user:1" || string(call.value) != "pika-value" {
		t.Fatalf("unexpected backfill call: key=%q value=%q", call.key, call.value)
	}
	if s := f.Stats(); s.Fallbacks != 1 || s.Hits != 1 || s.Backfills != 1 || s.Errors != 0 || s.Skipped != 0 {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

func TestFallbackOnMiss_PikaLacksValueNoBackfill(t *testing.T) {
	fake := newFakeShadowPika(4)
	fake.reply = nil // Pika also misses
	bf := &fakeBackfiller{}
	f := NewFallback(FallbackConfig{Enabled: true}, fake, bf)

	value, found, err := f.FallbackOnMiss(context.Background(), "user:2", argv("GET", "user:2"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("expected found=false when Pika lacks the value")
	}
	if value != nil {
		t.Fatalf("expected nil value, got %q", value)
	}
	if bf.count() != 0 {
		t.Fatalf("expected no backfill on a genuine miss, got %d", bf.count())
	}
	if s := f.Stats(); s.Fallbacks != 1 || s.Hits != 0 || s.Backfills != 0 {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

func TestFallbackOnMiss_PrefixGatingSkipsNonMatching(t *testing.T) {
	fake := newFakeShadowPika(4)
	fake.reply = []byte("v")
	bf := &fakeBackfiller{}
	f := NewFallback(FallbackConfig{Enabled: true, Prefixes: []string{"user:"}}, fake, bf)

	value, found, err := f.FallbackOnMiss(context.Background(), "order:1", argv("GET", "order:1"))
	if err != nil || found || value != nil {
		t.Fatalf("expected clean miss for non-matching prefix, got value=%q found=%v err=%v", value, found, err)
	}
	if fake.count() != 0 {
		t.Fatalf("Pika must not be read for non-matching prefix, reads=%d", fake.count())
	}
	if bf.count() != 0 {
		t.Fatalf("no backfill expected for non-matching prefix, got %d", bf.count())
	}
	if s := f.Stats(); s.Skipped != 1 || s.Fallbacks != 0 {
		t.Fatalf("expected 1 skipped and 0 fallbacks, got %+v", s)
	}
	if f.ShouldFallback("order:1") {
		t.Fatal("ShouldFallback should be false for non-matching prefix")
	}
	if !f.ShouldFallback("user:9") {
		t.Fatal("ShouldFallback should be true for matching prefix")
	}
}

func TestFallbackOnMiss_PikaReadErrorCountedNotFatal(t *testing.T) {
	fake := newFakeShadowPika(4)
	fake.err = errors.New("pika down")
	bf := &fakeBackfiller{}
	f := NewFallback(FallbackConfig{Enabled: true}, fake, bf)

	value, found, err := f.FallbackOnMiss(context.Background(), "user:1", argv("GET", "user:1"))
	if err == nil {
		t.Fatal("expected the Pika read error to be surfaced")
	}
	if found || value != nil {
		t.Fatalf("expected no value on read error, got value=%q found=%v", value, found)
	}
	if bf.count() != 0 {
		t.Fatalf("no backfill expected on read error, got %d", bf.count())
	}
	if s := f.Stats(); s.Errors != 1 || s.Hits != 0 || s.Fallbacks != 1 {
		t.Fatalf("expected 1 error/fallback and 0 hits, got %+v", s)
	}
}

func TestFallbackOnMiss_BackfillErrorCountedButValueReturned(t *testing.T) {
	fake := newFakeShadowPika(4)
	fake.reply = []byte("pika-value")
	bf := &fakeBackfiller{err: errors.New("dynamo write failed")}
	f := NewFallback(FallbackConfig{Enabled: true}, fake, bf)

	value, found, err := f.FallbackOnMiss(context.Background(), "user:1", argv("GET", "user:1"))
	if err != nil {
		t.Fatalf("backfill error must be non-fatal, got err=%v", err)
	}
	if !found || string(value) != "pika-value" {
		t.Fatalf("value from Pika must still be returned, got value=%q found=%v", value, found)
	}
	if s := f.Stats(); s.Hits != 1 || s.Backfills != 0 || s.Errors != 1 {
		t.Fatalf("expected hit counted, backfill error counted, got %+v", s)
	}
}

func TestFallbackOnMiss_NilBackfillerReturnsValueWithoutBackfill(t *testing.T) {
	fake := newFakeShadowPika(4)
	fake.reply = []byte("pika-value")
	f := NewFallback(FallbackConfig{Enabled: true}, fake, nil)

	value, found, err := f.FallbackOnMiss(context.Background(), "user:1", argv("GET", "user:1"))
	if err != nil || !found || string(value) != "pika-value" {
		t.Fatalf("expected value returned with nil backfiller, got value=%q found=%v err=%v", value, found, err)
	}
	if s := f.Stats(); s.Hits != 1 || s.Backfills != 0 || s.Errors != 0 {
		t.Fatalf("nil backfiller: expected hit, no backfill, no error, got %+v", s)
	}
}

func TestFallbackOnMiss_DisabledAndNilAreNoOps(t *testing.T) {
	var nilF *Fallback
	value, found, err := nilF.FallbackOnMiss(context.Background(), "k", argv("GET", "k"))
	if value != nil || found || err != nil {
		t.Fatalf("nil fallback must be a clean no-op, got value=%q found=%v err=%v", value, found, err)
	}
	if nilF.Enabled() {
		t.Fatal("nil fallback must not be enabled")
	}
	if nilF.ShouldFallback("k") {
		t.Fatal("nil fallback ShouldFallback must be false")
	}

	fake := newFakeShadowPika(4)
	fake.reply = []byte("v")

	// Disabled by flag.
	f := NewFallback(FallbackConfig{Enabled: false}, fake, &fakeBackfiller{})
	if f.Enabled() {
		t.Fatal("expected disabled fallback")
	}
	if _, found, _ := f.FallbackOnMiss(context.Background(), "k", argv("GET", "k")); found {
		t.Fatal("disabled fallback must report not found")
	}

	// Enabled but nil client => still disabled.
	f2 := NewFallback(FallbackConfig{Enabled: true}, nil, &fakeBackfiller{})
	if f2.Enabled() {
		t.Fatal("expected disabled fallback with nil client")
	}

	if fake.count() != 0 {
		t.Fatalf("disabled fallback must not read Pika, reads=%d", fake.count())
	}
}

func TestFallbackOnMiss_EmptyPrefixMatchesEverything(t *testing.T) {
	fake := newFakeShadowPika(4)
	fake.reply = []byte("v")
	f := NewFallback(FallbackConfig{Enabled: true}, fake, &fakeBackfiller{})

	if _, found, err := f.FallbackOnMiss(context.Background(), "anything", argv("GET", "anything")); err != nil || !found {
		t.Fatalf("empty prefix should match everything, got found=%v err=%v", found, err)
	}
}

func TestFallbackOnMiss_CopiesReplyBeforeReturning(t *testing.T) {
	fake := newFakeShadowPika(4)
	fake.reply = []byte("original")
	bf := &fakeBackfiller{}
	f := NewFallback(FallbackConfig{Enabled: true}, fake, bf)

	value, _, _ := f.FallbackOnMiss(context.Background(), "k", argv("GET", "k"))
	// Mutating the fake's underlying reply must not affect the returned value.
	copy(fake.reply, []byte("mutated!"))
	if string(value) != "original" {
		t.Fatalf("returned value was not copied: got %q", value)
	}
}

func TestBigKeyCounter_ReflectsInjectedSource(t *testing.T) {
	var live uint64
	c := NewBigKeyCounter(func() uint64 { return live })

	if got := c.Interceptions(); got != 0 {
		t.Fatalf("expected 0 initially, got %d", got)
	}
	live = 42
	if got := c.Interceptions(); got != 42 {
		t.Fatalf("expected injected source value 42, got %d", got)
	}
	if got := c.Stats().Interceptions; got != 42 {
		t.Fatalf("Stats should reflect source, got %d", got)
	}
	// Local increments are not reflected when a source is authoritative.
	c.Inc()
	c.Add(5)
	if got := c.Interceptions(); got != 42 {
		t.Fatalf("local increments must not override injected source, got %d", got)
	}
}

func TestBigKeyCounter_LocalIncrementsWhenNoSource(t *testing.T) {
	c := NewBigKeyCounter(nil)

	if got := c.Interceptions(); got != 0 {
		t.Fatalf("expected 0 initially, got %d", got)
	}
	c.Inc()
	c.Inc()
	c.Add(3)
	if got := c.Interceptions(); got != 5 {
		t.Fatalf("expected 5 after local increments, got %d", got)
	}
	if got := c.Stats().Interceptions; got != 5 {
		t.Fatalf("Stats should reflect local counter, got %d", got)
	}
}

func TestBigKeyCounter_NilIsNoOp(t *testing.T) {
	var c *BigKeyCounter
	c.Inc()   // must not panic
	c.Add(10) // must not panic
	if got := c.Interceptions(); got != 0 {
		t.Fatalf("nil counter must report 0, got %d", got)
	}
	if got := c.Stats().Interceptions; got != 0 {
		t.Fatalf("nil counter Stats must report 0, got %d", got)
	}
}
