package command

import (
	"context"
	"errors"
	"log"
	"strconv"
	"strings"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

// This file holds helpers shared by every data-command family (strings, and later
// keys/hashes/sets/zsets/lists). Centralizing them here keeps the per-family
// handler files focused on command semantics and gives every family the same key
// encoding, existence check and error mapping.

// encodePK encodes a logical key into its DynamoDB partition key for the given
// selected database. The pk is "{db}:{key}" — the decimal db index, a ':', then the
// key bytes verbatim (so binary-safe key names round-trip). The uniform "{n}:"
// prefix (db 0 -> "0:", db 1 -> "1:", ...) is collision-free: the ':' terminates the
// number, so one db's prefix is never a prefix of another's ("1:" is not a prefix of
// "12:"), and a db-0 key that happens to look like "1:foo" encodes to "0:1:foo",
// distinct from db-1's "1:foo".
func encodePK(db int, key []byte) string {
	return strconv.Itoa(db) + ":" + string(key)
}

// decodePK reverses encodePK: it strips the "{db}:" partition-key prefix for the
// selected database, returning the logical key name and ok=true when pk belongs to
// that database. A pk that does not carry the expected prefix (i.e. it belongs to a
// different database) returns ok=false so SCAN can filter the keyspace to the
// connection's selected db. The key bytes after the prefix are returned verbatim so
// binary-safe key names — including names that themselves contain ':' — round-trip.
func decodePK(db int, pk string) (string, bool) {
	prefix := strconv.Itoa(db) + ":"

	if !strings.HasPrefix(pk, prefix) {
		return "", false
	}

	return pk[len(prefix):], true
}

// keyLive reports whether the logical key at pk currently exists and is not
// expired. Existence is defined by the meta item (the source of truth for logical
// presence); expiry is evaluated against the router's injected clock via
// meta.IsExpired, independent of DynamoDB native-TTL timing. It backs the SET
// NX/XX and SETNX existence gate. A missing meta or an expired key both report
// false. Any store error is returned to the caller.
func (r *Router) keyLive(ctx context.Context, pk string) (bool, error) {
	m, found, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil || !found {
		return false, err
	}

	return !meta.IsExpired(m, r.now()), nil
}

// writeStoreError maps a storage/meta error to a RESP2 error reply. A type
// conflict from the meta conditional write becomes the byte-for-byte WRONGTYPE
// reply (requirement 3.6, 11.2); a size-guard rejection becomes the backend-limit
// reply (requirement 14.1, 14.2); a DynamoDB throttle that survived the SDK's
// retry/backoff becomes the retryable backend-throttled reply (requirement 18.8).
// Any other (unexpected backend) error is propagated as a generic "-ERR" reply,
// preserving retryable semantics for callers per the error-handling design
// (requirement 18.8).
func (r *Router) writeStoreError(c *server.Conn, err error) {
	w := resp.NewWriter(c.Redcon())
	switch {
	case errors.Is(err, meta.ErrWrongType):
		w.Error(resp.ErrWrongType)
	case errors.Is(err, guard.ErrSizeExceeded):
		w.Error(resp.ErrValueExceedsBackendLimit)
	case errors.Is(err, storage.ErrThrottled):
		// DynamoDB throttling (ProvisionedThroughputExceededException) exhausted
		// the SDK's bounded retry/backoff. Reply with a plain "-ERR" so the client
		// keeps retryable semantics and can retry after backing off (requirement
		// 18.8). The alerting hook has already fired at the storage seam.
		w.Error(resp.ErrBackendThrottled)
	case errors.Is(err, storage.ErrNotInteger):
		w.Error(resp.ErrNotInteger)
	case errors.Is(err, storage.ErrNotFloat):
		w.Error(resp.ErrNotValidFloat)
	case errors.Is(err, storage.ErrIncrOverflow):
		w.Error(resp.ErrIncrDecrOverflow)
	case errors.Is(err, storage.ErrIncrNaNOrInfinity):
		w.Error(resp.ErrIncrNaNOrInfinity)
	case errors.Is(err, storage.ErrHashNotInteger):
		w.Error(resp.ErrHashNotInteger)
	case errors.Is(err, storage.ErrHashNotFloat):
		w.Error(resp.ErrHashNotFloat)
	case errors.Is(err, storage.ErrRMWMaxRetries):
		// A meaningful, retryable redimos error (hot-key CAS exhaustion) — surface it
		// verbatim rather than collapsing it into the generic backend error below.
		w.Error(resp.ErrRMWMaxRetries)
	default:
		// Do not leak raw backend/SDK error text to the client (DynamoDB
		// validation/condition/attribute details are a reconnaissance surface and
		// non-Redis noise). Reply a fixed retryable error and log the real cause
		// server-side for operators.
		log.Printf("redimos: unmapped store error: %v", err)
		w.Error(resp.ErrBackendError)
	}
}

// resultCapExceeded reports whether a collection of the given member count exceeds the
// configured --max-collection-result cap, and if so writes the ErrCollectionTooLarge
// reply. Whole-collection reads (HGETALL/SMEMBERS/LRANGE/ZRANGE...) and *STORE operand
// loads call it with the key's maintained meta.cnt BEFORE doing the backend read, so an
// oversized key is rejected without ever materializing it in proxy memory. A cap of 0
// disables the check.
func (r *Router) resultCapExceeded(w *resp.Writer, count int64) bool {
	cap := r.Config.MaxCollectionResult
	if cap > 0 && count > int64(cap) {
		w.Error(resp.ErrCollectionTooLarge)
		return true
	}

	return false
}

// rangeResultCount returns how many elements an inclusive index range [start, stop]
// selects from a collection of clen elements, applying Redis' negative-index and
// clamping rules (lrangeCommand / genericZrangebyrankCommand). It is used to size
// the reply of the index-range reads (LRANGE / ZRANGE / ZREVRANGE) for the
// collection cap BEFORE the backend read, so a bounded sub-range of a huge key is
// NOT over-rejected on the whole-key size — only a range that actually selects more
// than the cap is refused. The result never exceeds clen and is 0 for an empty or
// inverted range. It is direction-agnostic: ZREVRANGE selects the same number of
// ranks as the forward range for the same start/stop.
func rangeResultCount(clen, start, stop int64) int64 {
	if clen <= 0 {
		return 0
	}
	if start < 0 {
		start += clen
	}
	if stop < 0 {
		stop += clen
	}
	if start < 0 {
		start = 0
	}
	if stop >= clen {
		stop = clen - 1
	}
	if start > stop {
		return 0
	}

	return stop - start + 1
}

// adjustCount applies a member-count delta to a collection key's meta item and,
// when a removal drives the count to zero, removes the key entirely (an empty
// collection does not exist in Redis, so a subsequent EXISTS/TYPE/HLEN reports it
// gone). It is the shared count-maintenance seam for the collection families
// (Hash here, and later Set/SortedSet/List): after the family's data mutation
// reports the NET number of members added (positive) or removed (negative), the
// handler calls adjustCount so meta.cnt stays exactly equal to the member count
// (requirement 6.4 for HLEN, and the analogous SCARD/ZCARD/LLEN counters).
//
// The delta is applied through EnsureType, whose conditional ADD is atomic and
// re-verifies the type, so a concurrent type change is still rejected. A zero
// delta is a no-op (no meta write). typ must be the collection type the caller
// already ensured before the data mutation.
func (r *Router) adjustCount(ctx context.Context, pk string, typ meta.KeyType, delta int64) error {
	if delta == 0 {
		return nil
	}

	newCount, err := r.Storage.Meta.EnsureType(ctx, pk, typ, delta)
	if err != nil {
		return err
	}

	// Only a removal can empty the collection; a positive delta never deletes. Use the
	// count returned by the SAME atomic EnsureType write, then delete the meta CONDITIONAL
	// on the count still being <= 0 (DeleteMetaIfEmpty). This closes the previous
	// load-then-delete TOCTOU: between an independent Load and DeleteMeta a concurrent add
	// could restore the count, and the unconditional delete would then strand that fresh
	// member under a removed meta (an invisible orphan). The conditional delete instead
	// fails when a racing add has raised the count, leaving the collection intact.
	if delta < 0 && newCount <= 0 {
		if _, err := r.Storage.Meta.DeleteMetaIfEmpty(ctx, pk); err != nil {
			return err
		}
	}

	return nil
}

// ensureTypeExpiring is EnsureType(pk, expected, 0) with Redis' expire-if-needed
// semantics folded in. A bare EnsureType rejects a key whose stored type differs from
// `expected` with WRONGTYPE — but if that key is only LOGICALLY expired (meta.exp <= now)
// and not yet reclaimed (native-TTL / lazy-delete is asynchronous), Redis treats it as
// absent and lets the write create the new type. So on a WRONGTYPE this reloads the meta
// and, when it is expired, reclaims the stale key (members + meta, like overwriteAnyType)
// and retries the type-ensure once. A genuinely LIVE wrong-type key still returns
// meta.ErrWrongType. The count is not touched here — callers apply the real delta via
// adjustCount afterwards (this ensure step always passes cntDelta 0).
func (r *Router) ensureTypeExpiring(ctx context.Context, pk string, expected meta.KeyType) error {
	_, err := r.Storage.Meta.EnsureType(ctx, pk, expected, 0)
	if !errors.Is(err, meta.ErrWrongType) {
		return err
	}

	m, ok, lerr := r.Storage.Meta.Load(ctx, pk)
	if lerr != nil {
		return lerr
	}
	if !ok || !meta.IsExpired(m, r.now()) {
		return meta.ErrWrongType // a live key of the wrong type — a real WRONGTYPE
	}

	// Logically expired: reclaim its members and drop its meta, then re-ensure the type,
	// so the write proceeds as if the key never existed (Redis expire-if-needed).
	if _, derr := r.Storage.Store.DeleteMembers(ctx, pk); derr != nil {
		return derr
	}
	if _, derr := r.Storage.Meta.DeleteMeta(ctx, pk); derr != nil {
		return derr
	}
	_, err = r.Storage.Meta.EnsureType(ctx, pk, expected, 0)
	return err
}

// writeScanReply writes the two-element single-pk SCAN reply [cursor, [items...]]
// shared by SSCAN and HSCAN. A nil items slice is normalized to a non-nil empty
// slice so the inner array always encodes as "*0" (empty array), never the null
// array "*-1", matching Redis/Pika.
func writeScanReply(c *server.Conn, cursor string, items [][]byte) {
	if items == nil {
		items = [][]byte{}
	}
	buf := resp.AppendArrayHeader(nil, 2)
	buf = resp.AppendBulkString(buf, []byte(cursor))
	buf = resp.AppendBulkArray(buf, items)
	c.Redcon().WriteRaw(buf)
}
