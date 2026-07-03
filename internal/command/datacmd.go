package command

import (
	"context"
	"errors"
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
// selected database. Per the design's data model the pk is "{db}:{key}": db 0
// (the P0 default) uses the fixed "0:" prefix, and a non-zero db selected via
// SELECT maps to the "d{n}:" prefix (requirement 2.9). The key bytes are appended
// verbatim so binary-safe key names round-trip.
func encodePK(db int, key []byte) string {
	if db == 0 {
		return "0:" + string(key)
	}

	return "d" + strconv.Itoa(db) + ":" + string(key)
}

// decodePK reverses encodePK: it strips the "{db}:" partition-key prefix for the
// selected database, returning the logical key name and ok=true when pk belongs to
// that database. A pk that does not carry the expected prefix (i.e. it belongs to a
// different database) returns ok=false so SCAN can filter the keyspace to the
// connection's selected db. Like encodePK, db 0 uses the "0:" prefix and any
// non-zero db uses "d{n}:". The key bytes after the prefix are returned verbatim so
// binary-safe key names — including names that themselves contain ':' — round-trip.
func decodePK(db int, pk string) (string, bool) {
	prefix := "0:"
	if db != 0 {
		prefix = "d" + strconv.Itoa(db) + ":"
	}

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
	default:
		w.Error("ERR " + err.Error())
	}
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

	if err := r.Storage.Meta.EnsureType(ctx, pk, typ, delta); err != nil {
		return err
	}

	// Only a removal can empty the collection; a positive delta never deletes.
	if delta < 0 {
		m, found, err := r.Storage.Meta.Load(ctx, pk)
		if err != nil {
			return err
		}
		if found && m.Count <= 0 {
			if _, err := r.Storage.Meta.DeleteMeta(ctx, pk); err != nil {
				return err
			}
		}
	}

	return nil
}
