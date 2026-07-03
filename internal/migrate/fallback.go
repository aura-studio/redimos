package migrate

import (
	"context"
	"sync/atomic"
	"time"
)

// Backfiller is the seam the fallback hook uses to write a value read from the
// legacy Pika source-of-truth back into the primary store (DynamoDB), so a
// subsequent read is served from DynamoDB without another source-of-truth trip.
//
// The migrate package deliberately depends only on this small interface, never
// on internal/storage or internal/command, so it carries no storage dependency
// and the backfill path can be exercised in isolation. The command layer
// supplies a concrete Backfiller during assembly (task 23.1) that performs the
// appropriate typed write for the command that missed.
//
// Backfill may block or fail: FallbackOnMiss runs it inline on the (cold) miss
// path and treats any error as non-fatal — the value read from Pika is still
// returned to the client, and the failure is counted in FallbackStats.Errors.
type Backfiller interface {
	Backfill(ctx context.Context, key string, value []byte) error
}

// BackfillerFunc adapts a plain function to the Backfiller interface, so the
// command layer can supply a closure instead of a named type.
type BackfillerFunc func(ctx context.Context, key string, value []byte) error

// Backfill implements Backfiller.
func (fn BackfillerFunc) Backfill(ctx context.Context, key string, value []byte) error {
	return fn(ctx, key, value)
}

// FallbackConfig configures the read-only source-of-truth fallback hook. The
// zero value is a disabled fallback.
type FallbackConfig struct {
	// Enabled turns the read-only fallback on. When false, NewFallback returns
	// a no-op fallback whose FallbackOnMiss always reports "not found".
	Enabled bool

	// Prefixes is the key-prefix allowlist mirroring dual-write/shadow gating:
	// only keys beginning with one of these prefixes are eligible for a
	// source-of-truth fallback. An empty list makes every key eligible (no
	// prefix gate). Requirement 17.3 pairs with the same gradual-rollout
	// semantics as 17.1/17.2.
	Prefixes []string

	// ReadTimeout bounds the Pika read plus backfill performed on a miss.
	// Values <= 0 mean no timeout. Because the fallback runs on the client's
	// read path (it must return the value the client sees), the timeout is
	// derived from the caller's context rather than a detached background one.
	ReadTimeout time.Duration
}

// FallbackStats is a point-in-time snapshot of read-only fallback counters.
type FallbackStats struct {
	// Fallbacks is the number of misses for which a source-of-truth fallback
	// was attempted (the key matched the prefix allowlist and a Pika read was
	// issued).
	Fallbacks uint64
	// Hits is the number of fallbacks where Pika held the value (so the client
	// saw a value it would otherwise have missed).
	Hits uint64
	// Backfills is the number of hits successfully written back into the
	// primary store via the Backfiller.
	Backfills uint64
	// Errors is the number of fallbacks where the Pika read or the backfill
	// failed. These are non-fatal: a read error yields "not found"; a backfill
	// error still returns the value read from Pika.
	Errors uint64
	// Skipped is the number of misses not eligible for a fallback because the
	// key did not match the prefix allowlist.
	Skipped uint64
}

// Fallback implements read-only source-of-truth fallback with backfill: on a
// read where the primary store (DynamoDB) has no value, FallbackOnMiss reads
// the key from the legacy Pika source-of-truth and, if Pika holds the value,
// backfills it into the primary store and returns it so the client sees the
// value. This supports cold-start / gradual migration where data still lives in
// Pika (requirement 17.3).
//
// Unlike the dual-write and shadow-read hooks, the fallback is synchronous: it
// runs on the client's read path because its result is what the client sees.
//
// A nil *Fallback and a disabled fallback are both valid no-ops, so the command
// layer can hold one unconditionally and call FallbackOnMiss without branching.
type Fallback struct {
	enabled    bool
	prefixes   []string
	timeout    time.Duration
	client     ShadowPikaClient
	backfiller Backfiller

	fallbacks atomic.Uint64
	hits      atomic.Uint64
	backfills atomic.Uint64
	errors    atomic.Uint64
	skipped   atomic.Uint64
}

// NewFallback builds a Fallback from cfg. When cfg.Enabled is false, or client
// is nil, it returns a disabled (no-op) fallback; this lets the caller
// construct one unconditionally from parsed flags. The Backfiller may be nil:
// a fallback with no backfiller still reads from Pika and returns the value to
// the client, it simply does not write the value back into the primary store
// (no backfill is counted).
func NewFallback(cfg FallbackConfig, client ShadowPikaClient, backfiller Backfiller) *Fallback {
	return &Fallback{
		enabled:    cfg.Enabled && client != nil,
		prefixes:   append([]string(nil), cfg.Prefixes...),
		timeout:    cfg.ReadTimeout,
		client:     client,
		backfiller: backfiller,
	}
}

// Enabled reports whether the fallback will attempt source-of-truth reads. It
// is nil-safe.
func (f *Fallback) Enabled() bool {
	return f != nil && f.enabled
}

// ShouldFallback reports whether a miss on key is eligible for a fallback, i.e.
// the fallback is enabled and key matches the prefix allowlist. It is nil-safe.
func (f *Fallback) ShouldFallback(key string) bool {
	if !f.Enabled() {
		return false
	}
	return matchAnyPrefix(key, f.prefixes)
}

// FallbackOnMiss is the entry point the command layer's read path calls when
// the primary store (DynamoDB) has no value for key. cmd is the original read
// command argv (args[0] is the command name), replayed verbatim against Pika.
//
// Behavior (requirement 17.3):
//   - Disabled fallback or nil receiver: returns (nil, false, nil) — the caller
//     proceeds as a normal miss.
//   - key not in the prefix allowlist: not eligible (FallbackStats.Skipped++),
//     returns (nil, false, nil).
//   - Otherwise the command is read from Pika. If Pika has no value, returns
//     (nil, false, nil). If the Pika read errors, the error is counted
//     (FallbackStats.Errors++) and returned as (nil, false, err) — non-fatal for
//     the caller, which may treat it as a miss.
//   - If Pika holds the value (FallbackStats.Hits++), it is backfilled into the
//     primary store via the Backfiller (FallbackStats.Backfills++ on success; a
//     backfill error is counted in Errors but is non-fatal), and the value is
//     returned as (value, true, nil) so the client sees it.
//
// A nil reply from Pika means "absent"; a non-nil reply (including an empty
// slice) means "present", matching how a bulk-string reply distinguishes a
// missing key ($-1) from an empty value.
func (f *Fallback) FallbackOnMiss(ctx context.Context, key string, cmd [][]byte) (value []byte, found bool, err error) {
	if !f.Enabled() {
		return nil, false, nil
	}
	if !matchAnyPrefix(key, f.prefixes) {
		f.skipped.Add(1)
		return nil, false, nil
	}

	f.fallbacks.Add(1)

	if f.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, f.timeout)
		defer cancel()
	}

	reply, readErr := f.client.Read(ctx, cmd)
	if readErr != nil {
		f.errors.Add(1)
		return nil, false, readErr
	}
	if reply == nil {
		// Pika does not have the value either — a genuine miss.
		return nil, false, nil
	}

	// Pika holds the value: the client will see it.
	f.hits.Add(1)
	out := cloneBytes(reply)

	// Backfill into the primary store so the next read is served from DynamoDB.
	// A backfill failure is non-fatal: we still return the value read from Pika.
	if f.backfiller != nil {
		if bfErr := f.backfiller.Backfill(ctx, key, out); bfErr != nil {
			f.errors.Add(1)
		} else {
			f.backfills.Add(1)
		}
	}

	return out, true, nil
}

// Stats returns a snapshot of the read-only fallback counters. It is nil-safe.
func (f *Fallback) Stats() FallbackStats {
	if f == nil {
		return FallbackStats{}
	}
	return FallbackStats{
		Fallbacks: f.fallbacks.Load(),
		Hits:      f.hits.Load(),
		Backfills: f.backfills.Load(),
		Errors:    f.errors.Load(),
		Skipped:   f.skipped.Load(),
	}
}

// BigKeyCounter surfaces the big-key / over-limit interception count for
// migration visibility (requirement 17.4) WITHOUT importing internal/guard or
// internal/metrics, keeping the migrate package decoupled from both. It mirrors
// the injectable-source pattern used by metrics for the same counter:
//
//   - If a source func is injected (via NewBigKeyCounter), Interceptions reads
//     it verbatim. During assembly (task 23.1) main wires this to
//     guard.Interceptions so the migrate-side view always reflects the live
//     size-guard rejection counter — the same value metrics exports.
//   - Otherwise the count is backed by an internal atomic that the command or
//     guard layer advances via Inc on each rejected write.
//
// A nil *BigKeyCounter is a valid no-op (Interceptions returns 0, Inc does
// nothing), so the command layer can hold one unconditionally.
type BigKeyCounter struct {
	// source, when non-nil, is the authoritative live interception source
	// (wired to guard.Interceptions in task 23.1). When set, local increments
	// are not reflected by Interceptions.
	source func() uint64

	// local backs Interceptions when no source is injected; advanced by Inc.
	local atomic.Uint64
}

// NewBigKeyCounter builds a BigKeyCounter. Pass a non-nil source to reflect an
// external live counter (task 23.1 wires guard.Interceptions here); pass nil to
// use the internal atomic advanced by Inc.
func NewBigKeyCounter(source func() uint64) *BigKeyCounter {
	return &BigKeyCounter{source: source}
}

// Inc advances the migrate-local interception counter by one. It is a no-op
// when an external source was injected (that source is authoritative) but
// remains safe to call, and it is nil-safe. The command/guard layer calls Inc
// on each rejected write when no live source is wired.
func (c *BigKeyCounter) Inc() {
	if c == nil {
		return
	}
	c.local.Add(1)
}

// Add advances the migrate-local interception counter by n, for callers that
// batch rejections. It is nil-safe.
func (c *BigKeyCounter) Add(n uint64) {
	if c == nil {
		return
	}
	c.local.Add(n)
}

// Interceptions returns the current big-key interception count: the injected
// source's value when a source was wired, otherwise the internal atomic. It is
// nil-safe.
func (c *BigKeyCounter) Interceptions() uint64 {
	if c == nil {
		return 0
	}
	if c.source != nil {
		return c.source()
	}
	return c.local.Load()
}

// BigKeyStats is a point-in-time snapshot of the big-key interception counter,
// mirroring the *Stats accessors of the other hooks for uniform reporting.
type BigKeyStats struct {
	// Interceptions is the number of key/member/value over-limit interceptions
	// observed (from the injected source or the local counter).
	Interceptions uint64
}

// Stats returns a snapshot of the big-key interception count. It is nil-safe.
func (c *BigKeyCounter) Stats() BigKeyStats {
	return BigKeyStats{Interceptions: c.Interceptions()}
}
