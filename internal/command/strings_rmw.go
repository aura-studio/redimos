package command

import (
	"context"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

// overwriteAnyType prepares a destructive String write (plain SET / SETEX /
// PSETEX): in Redis these commands replace a key of ANY type, so a key currently
// holding a non-String collection must have that collection cleared first, so the
// subsequent EnsureType(String) creates a fresh string instead of failing
// WRONGTYPE. An absent key, or one already holding a String, needs no action — the
// String's single value item is overwritten in place by the following SetString.
//
// The old collection's member items are reclaimed SYNCHRONOUSLY here (not via the
// DEL-style async enqueue) and this applies to an EXPIRED non-String key too. The
// async lazy-deleter guards every reclaim with an IsLive check to protect a
// DEL-then-recreate, and SweepOrphans only reclaims members whose pk has no meta;
// once EnsureType writes the fresh String meta, BOTH treat the stale members as
// live, so an async or swept reclaim would never fire and the members would linger
// forever as invisible orphans (and could even resurface if the pk later became a
// collection again). DeleteMembers removes every non-meta item under pk while the
// old meta still stands, before the new value is written, which is exactly the
// window in which the removal is unambiguous. (Not used by SETNX, which claims
// only a logically-absent key — see handleSet's NX branch — nor by GETSET, which
// reads the old value as a string and so keeps WRONGTYPE.)
func (r *Router) overwriteAnyType(ctx context.Context, pk string) error {
	m, ok, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		return err
	}
	if !ok || m.Type == meta.TypeString {
		return nil
	}
	if _, err := r.Storage.Store.DeleteMembers(ctx, pk); err != nil {
		return err
	}
	_, err = r.Storage.Meta.DeleteMeta(ctx, pk)
	return err
}

// --- APPEND / STRLEN / SETRANGE / GETRANGE (requirements 5.10, 5.11, 16.4) ----
//
// APPEND and SETRANGE are read-modify-write String mutations: they read the
// current live value, compute the new value, then write it back. Per requirements
// 5.10 / 15.2 / 16.4 the write is a conditional compare-and-set on the value the
// read observed, retried on conflict, so concurrent read-modify-writes cannot
// silently lose an update: two connections appending to the same key each land in
// turn, the loser's conditional write fails, and it re-reads and recomputes on top
// of the winner's value. The loop is bounded by storage.MaxRMWRetries; a run that
// exhausts it (pathological hot-key contention) surfaces the generic retryable
// "-ERR" reply from storage.ErrRMWMaxRetries. This does NOT rely on read
// consistency — the condition is evaluated at write time against the current item
// (requirement 15.2) — so it is correct even on an eventually-consistent read.
//
// The base value is read through the meta layer (Load + IsExpired), so an absent
// or expired key contributes an empty base (APPEND creates the key, SETRANGE
// zero-pads from empty) exactly as Redis/Pika treat a missing key. The
// compare-and-set precondition, by contrast, is asserted on the PHYSICAL value
// item (its bytes, or its absence) the read observed, so an expired-but-not-yet-
// swept stale value item is still overwritten deterministically. The resulting
// value size is validated through the guard before each write attempt, so an
// oversized result is rejected with no partial write (requirement 14.3).
//
// Scope note: this closes lost updates for the single-item String value. True
// cross-connection atomicity for the multi-item collection commands (e.g.
// LSET/LTRIM/LREM/LINSERT rebuilding a list) still needs a DynamoDB transaction
// and remains best-effort in P0; those commands are not part of this
// compare-and-set path.

// readCurrentString reads the current live String value at pk for a read-path or
// RMW command. It loads the meta item and evaluates expiry against the router's
// clock: an absent or expired key yields found=false with an empty value.
// wrongType is true when the key is live but not a String, which read commands
// (STRLEN/GETRANGE) surface as WRONGTYPE; RMW commands ignore it and let the
// subsequent EnsureType(TypeString) reject the write with the same error. When
// the key is a live String the stored value bytes are returned.
func (r *Router) readCurrentString(ctx context.Context, pk string) (val []byte, found, wrongType bool, err error) {
	m, ok, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		return nil, false, false, err
	}
	if !ok || meta.IsExpired(m, r.now()) {
		return nil, false, false, nil
	}
	if m.Type != meta.TypeString {
		return nil, false, true, nil
	}

	v, has, err := r.Storage.Store.GetString(ctx, pk)
	if err != nil {
		return nil, false, false, err
	}
	if !has {
		// Live String meta with no value item: treat as an empty string.
		return nil, true, false, nil
	}

	return v, true, false, nil
}

// readStringForRMW reads one attempt's worth of state for a read-modify-write
// String command (APPEND/SETRANGE). It returns two views of the value:
//
//   - base is the LOGICAL current value the command builds on — the stored bytes
//     when the key is a live (present, unexpired) String, or empty when the key is
//     absent, expired, or a live String with no value item yet. This matches how
//     Redis/Pika treat a missing key (APPEND creates it, SETRANGE zero-pads from
//     empty).
//   - physVal / physExists describe the PHYSICAL value item exactly as stored, for
//     the compare-and-set precondition: physExists reports whether a value item is
//     present and physVal its bytes. These differ from base only for an
//     expired-but-not-yet-swept key (base empty, but physExists true with the stale
//     bytes), so the conditional write still targets the concrete item the read saw.
//
// A live non-String key yields base empty with wrongType=true; the caller lets the
// subsequent EnsureType(TypeString) reject the write with WRONGTYPE rather than
// treating it as an empty base.
func (r *Router) readStringForRMW(ctx context.Context, pk string) (base, physVal []byte, physExists, wrongType bool, err error) {
	m, ok, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		return nil, nil, false, false, err
	}
	liveString := ok && !meta.IsExpired(m, r.now()) && m.Type == meta.TypeString
	wrongType = ok && !meta.IsExpired(m, r.now()) && m.Type != meta.TypeString

	v, has, err := r.Storage.Store.GetString(ctx, pk)
	if err != nil {
		return nil, nil, false, false, err
	}
	physVal, physExists = v, has

	if liveString && has {
		base = v
	}

	return base, physVal, physExists, wrongType, nil
}

// rmwString runs the bounded compare-and-set read-modify-write loop shared by
// APPEND and SETRANGE (requirements 15.2, 16.4). Each attempt reads the current
// value, hands the logical base to compute to derive the new value (compute also
// runs the size guard and returns its error, so an oversized result is rejected
// with no write), verifies/creates the String type via EnsureType, then writes the
// result back conditionally on the value item being unchanged since the read. A
// conflicting concurrent write makes the conditional fail; the loop re-reads and
// retries up to storage.MaxRMWRetries, then returns storage.ErrRMWMaxRetries. A
// live non-String key is rejected by EnsureType and surfaces as WRONGTYPE. On
// success it returns the new value's byte length (the integer both commands reply).
func (r *Router) rmwString(ctx context.Context, pk string, compute func(base []byte) ([]byte, error)) (newLen int, err error) {
	for attempt := 0; attempt < storage.MaxRMWRetries; attempt++ {
		base, physVal, physExists, _, rerr := r.readStringForRMW(ctx, pk)
		if rerr != nil {
			return 0, rerr
		}

		next, cerr := compute(base)
		if cerr != nil {
			return 0, cerr
		}

		// EnsureType creates/verifies the String type (rejecting a live non-String
		// key with WRONGTYPE) before the value write, atomically re-checked each
		// attempt so a concurrent type change is still caught.
		if _, eerr := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); eerr != nil {
			return 0, eerr
		}

		ok, werr := r.Storage.Store.SetStringIfEquals(ctx, pk, next, physVal, physExists)
		if werr != nil {
			return 0, werr
		}
		if ok {
			return len(next), nil
		}
		// Lost the compare-and-set race: another writer changed the value between
		// the read and the write. Re-read and recompute.
	}

	return 0, storage.ErrRMWMaxRetries
}

// handleAppend implements APPEND key value (requirements 5.10, 16.4): append value
// to the string at key, creating the key with value when it is absent or expired.
// It replies the new string length as an integer (":N"). The existing TTL is
// preserved (APPEND does not clear expiry). A live non-String key replies
// WRONGTYPE (from EnsureType). The read-modify-write is not yet atomic across
// connections; see the file section header.
func (r *Router) handleAppend(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key, val := args[1], args[2]
	pk := encodePK(c.DB(), key)

	newLen, err := r.rmwString(ctx, pk, func(base []byte) ([]byte, error) {
		// Compute the appended result. A fresh slice keeps base's backing array
		// intact so a retry recomputes cleanly from the freshly-read base.
		next := make([]byte, 0, len(base)+len(val))
		next = append(next, base...)
		next = append(next, val...)

		if err := guard.CheckWrite(key, nil, [][]byte{next}); err != nil {
			return nil, err
		}

		return next, nil
	})
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(newLen))
}

// handleSetRange implements SETRANGE key offset value (requirements 5.10, 16.4):
// overwrite the string at key starting at offset, zero-padding with NUL bytes when
// offset is beyond the current length, and reply the new string length. A negative
// offset replies "-ERR offset is out of range"; a non-integer offset replies the
// not-an-integer error. An empty value performs no write and replies the current
// length (0 when the key is absent), matching Redis. The existing TTL is preserved.
// A live non-String key replies WRONGTYPE (from EnsureType). The read-modify-write
// is not yet atomic across connections; see the file section header.
func (r *Router) handleSetRange(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key, val := args[1], args[3]

	offset, err := ParseInt(args[2])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}
	if offset < 0 {
		w.Error(resp.ErrOffsetOutOfRange)
		return
	}

	pk := encodePK(c.DB(), key)

	// An empty value never creates or grows the key; it just reports the current
	// length (Redis SETRANGE semantics). No write, so no read-modify-write is
	// needed — a single read of the current value suffices. Redis still checks the
	// type first: SETRANGE with an empty value against a live non-String key replies
	// WRONGTYPE (setrangeCommand runs checkType before the empty-value short-circuit),
	// so surface wrongType here rather than reporting length 0.
	if len(val) == 0 {
		cur, _, wrongType, rerr := r.readCurrentString(ctx, pk)
		if rerr != nil {
			r.writeStoreError(c, rerr)
			return
		}
		if wrongType {
			w.Error(resp.ErrWrongType)
			return
		}
		w.Int(int64(len(cur)))
		return
	}

	newLen, err := r.rmwString(ctx, pk, func(base []byte) ([]byte, error) {
		// Resulting length = max(current length, offset+len(value)). Compute it in
		// int64 first and reject an oversized result through the guard before
		// allocating, so a huge offset cannot trigger a huge allocation.
		end := offset + int64(len(val))
		nl := int64(len(base))
		if end > nl {
			nl = end
		}
		if gerr := guard.CheckWrite(key, nil, nil); gerr != nil {
			return nil, gerr
		}
		if gerr := guard.CheckValueSize(nl); gerr != nil {
			return nil, gerr
		}

		// Build the new value: copy the current bytes (make zero-fills the gap up
		// to offset when offset > len(base)), then overwrite from offset with value.
		buf := make([]byte, nl)
		copy(buf, base)
		copy(buf[offset:], val)

		return buf, nil
	})
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(newLen))
}
