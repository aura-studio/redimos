package storage

import (
	"context"

	redimo "github.com/aura-studio/redimo"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// rvBytes coerces a redimo ReturnValue to a Redis byte string regardless of how the
// value ("val") attribute was physically stored in DynamoDB. redimos itself always
// writes String/Hash values as Binary (B), so for redimos-native data this is
// exactly rv.Bytes(). But the SAME table is routinely populated directly by other
// producers (e.g. a game backend writing via the AWS SDK), which store "val" as
// String (S) or Number (N) — verified against the redimo_aurora_nano table, whose
// hash/string values land as S (JSON blobs) and N (timestamps/counters), never B.
// Reading those with rv.Bytes() alone returns nil and renders an EMPTY value in
// HGET/HGETALL/HMGET/HSTRLEN/GET/MGET/GETSET (and misparses the HINCRBY/INCRBY
// numeric read-modify-write). Coerce S→raw bytes and N→its canonical decimal text so
// the value survives the round-trip; a genuinely absent or non-scalar value still
// yields nil, preserving the prior behaviour for those.
func rvBytes(rv redimo.ReturnValue) []byte {
	switch av := rv.ToAV().(type) {
	case *ddbtypes.AttributeValueMemberB:
		return av.Value
	case *ddbtypes.AttributeValueMemberS:
		return []byte(av.Value)
	case *ddbtypes.AttributeValueMemberN:
		return []byte(av.Value)
	case *ddbtypes.AttributeValueMemberBOOL:
		if av.Value {
			return []byte("1")
		}
		return []byte("0")
	default:
		return nil
	}
}

// v1 line note: redimo v1.6.1 has NO meta item, NO type/cnt/TTL machinery, and NO
// WRONGTYPE detection. The Store interface is preserved verbatim so the command and
// meta layers compile unchanged, but the meta primitives below are re-expressed
// over rv1's raw high-level methods:
//
//   - There is no #meta item. Type is NOT tracked, so EnsureType/EnsureTypeExpiring
//     never return WRONGTYPE and the command layer's type-check sites are softened
//     accordingly (see internal/command/state.go, zsets_store.go).
//   - Counts come straight from rv1: every data type stores one DynamoDB item per
//     element under the key's partition (a String stores one item at sk=""), and
//     rv1.HLEN (== SCARD == ZCARD == LLEN's lLen) is a pure partition SelectCount
//     Query. So a single partition count serves LLEN/SCARD/ZCARD/HLEN and existence
//     for every type, with no type tag needed. LoadMeta returns that count.
//   - There is no TTL: SetExpire/Persist are no-ops (report existence via rv1), and
//     the TTL/EXPIRE/PERSIST/TYPE commands are GATED (unregistered) at the command
//     layer, so their Exp/Type reads are never reached on a live path.
//   - DeleteMeta uses rv1.DEL (which deletes the whole partition synchronously), so
//     the lazy-delete/sweeper seam degrades to a no-op: DeleteMembers/SweepOrphans
//     have nothing left to reclaim.

// maxBatchWriteItems is the DynamoDB BatchWriteItem per-call hard limit. rv1 does
// not export this constant (redimo v2's redimo.MaxBatchWriteItems is gone), so it
// is defined locally. It only bounds the (now no-op) delete-batch config.
const maxBatchWriteItems = 25

// New builds a redimo-backed Store from an AWS DynamoDB client, a table name and
// a consistency option. Construction performs no network calls.
func New(ddb *dynamodb.Client, opts Options) Store {
	c := redimo.NewClient(ddb)

	if opts.TableName != "" {
		c = c.Table(opts.TableName)
	}

	// P0 default: strongly consistent reads (read-your-writes). A caller must
	// explicitly opt out via EventuallyConsistent, so a bare Options{} is strong.
	if opts.EventuallyConsistent {
		c = c.EventuallyConsistent()
	} else {
		c = c.StronglyConsistent()
	}

	// Wrap the redimo-backed store in the throttle decorator so every operation's
	// error is classified: a DynamoDB throttle surfaces as ErrThrottled and fires
	// the OnThrottle alerting hook (requirement 18.8). Retry/backoff for throttling
	// is handled by the AWS SDK client's retryer (see throttle.go / ErrThrottled).
	base := &redimoStore{client: c, deleteBatchSize: clampBatchSize(opts.DeleteBatchSize)}
	return newThrottleStore(base, opts.OnThrottle, opts.Breaker)
}

// NewFromClient wraps an already-configured redimo.Client. Useful when the caller
// needs full control over the client (index/attribute names, transaction limits).
// Like New it applies the throttle decorator (with no alerting hook) so throttling
// errors are still surfaced as ErrThrottled for the command layer to map.
func NewFromClient(client redimo.Client) Store {
	base := &redimoStore{client: client, deleteBatchSize: maxBatchWriteItems}
	return newThrottleStore(base, nil, nil)
}

// clampBatchSize normalizes a configured delete batch size to the DynamoDB per-call
// limit. A value <= 0 (or above the limit) selects the maximum.
func clampBatchSize(n int) int {
	if n <= 0 || n > maxBatchWriteItems {
		return maxBatchWriteItems
	}

	return n
}

// partitionCount returns the number of DynamoDB items under the key's partition,
// which equals the collection's member/element count for any type (a String key
// has exactly one item). It is rv1's HLEN — a pure partition SelectCount Query —
// and is the type-agnostic count that backs LLEN/SCARD/ZCARD/HLEN and existence on
// the v1 line, replacing the dropped meta.cnt counter.
func (s *redimoStore) partitionCount(pk string) (int64, error) {
	n, err := s.client.HLEN(pk)
	return int64(n), err
}

// EnsureType on the v1 line does NOT write a #meta item and does NOT type-check
// (rv1 tracks no type, so WRONGTYPE is unenforceable). It returns the current
// partition count so count-adjusting callers (adjustCount) still reply an accurate
// length. cntDelta is ignored: the authoritative count is read from rv1 after the
// caller's data mutation, not accumulated in a counter. It never returns
// ErrWrongType.
func (s *redimoStore) EnsureType(ctx context.Context, pk, expected string, cntDelta int64) (int64, error) {
	return s.partitionCount(pk)
}

// EnsureTypeExpiring is EnsureType on the v1 line (no TTL, so nothing to expire; no
// type, so nothing to take over). tookOverExpired is always false.
func (s *redimoStore) EnsureTypeExpiring(ctx context.Context, pk, expected string, cntDelta, nowEpoch int64) (int64, bool, error) {
	n, err := s.partitionCount(pk)
	return n, false, err
}

// CreateTypeIfAbsent is the SETNX/SET NX existence gate on the v1 line. rv1 tracks
// no meta item, so "logically absent" collapses to "the partition has no items":
// created is true iff the key does not exist. NOTE: this is a best-effort gate — it
// is a read-then-decide, NOT atomic, and there is no SETCAS in rv1. The atomic NX
// guarantee for plain string SET NX is instead provided by rv1.SET(..., IfNotExists)
// itself in the string handler; this method backs the meta-side claim only.
func (s *redimoStore) CreateTypeIfAbsent(ctx context.Context, pk, expected string, cntDelta, nowEpoch int64) (bool, error) {
	exists, err := s.client.EXISTS(pk)
	if err != nil {
		return false, err
	}

	return !exists, nil
}

// LoadMeta reports logical existence and the key's member count on the v1 line.
// found is true iff the partition has at least one item. Type is left empty (rv1
// tracks none) and Exp is 0 (no TTL); the command layer's type/expiry checks are
// softened for the v1 line so those zero values are never surfaced as WRONGTYPE or
// as a live TTL. Count is the partition item count, which serves the LLEN/SCARD/
// ZCARD/HLEN replies that formerly read meta.cnt.
func (s *redimoStore) LoadMeta(ctx context.Context, pk string) (Meta, bool, error) {
	n, err := s.partitionCount(pk)
	if err != nil {
		return Meta{}, false, err
	}
	if n == 0 {
		return Meta{}, false, nil
	}

	return Meta{Type: "", Exp: 0, Count: n}, true, nil
}

// KeyType reports the Redis type of pk on the v1 line by delegating to rv1.7's
// Client.TypeOf, which infers the type from item shape (rv1 stores no type tag): the
// empty-SK string sentinel, the reserved list-metadata sibling, or the skN shape that
// separates a set (random int63 marker) from a sorted set (score). found is false for
// a missing key. It backs the (un-gated) TYPE command and performs only reads.
// set-vs-zset is a documented heuristic (see redimo introspect.go); every other type
// is distinguished exactly.
func (s *redimoStore) KeyType(ctx context.Context, pk string) (string, bool, error) {
	return s.client.TypeOf(pk)
}

// SetExpire is a no-op on the v1 line: rv1 has no TTL storage. It reports whether
// the key exists so EXPIRE-style callers (all GATED) would see the right found
// value if ever reached. It never persists an expiry.
func (s *redimoStore) SetExpire(ctx context.Context, pk string, expEpoch int64) (bool, error) {
	return s.client.EXISTS(pk)
}

// Persist is a no-op on the v1 line (there is no TTL to clear). It reports whether
// the key exists.
func (s *redimoStore) Persist(ctx context.Context, pk string) (bool, error) {
	return s.client.EXISTS(pk)
}

// DeleteMeta deletes the key on the v1 line. rv1.DEL removes EVERY item under the
// partition synchronously (there is no separate meta item to remove first), so this
// both makes the key absent AND reclaims its members in one call — the lazy-delete
// seam has nothing left to do. existed reports whether any item was deleted.
func (s *redimoStore) DeleteMeta(ctx context.Context, pk string) (bool, error) {
	deleted, err := s.client.DEL(pk)
	if err != nil {
		return false, err
	}

	return len(deleted) > 0, nil
}

// DeleteMetaIfEmpty deletes the key only if it currently has no members. On the v1
// line a count-adjusting caller that drove the collection empty already removed the
// last item, so the partition is empty and there is nothing to delete; this simply
// confirms emptiness. deleted is always false (rv1.DEL on an empty partition is a
// no-op returning no fields), which is correct — the key is already absent.
func (s *redimoStore) DeleteMetaIfEmpty(ctx context.Context, pk string) (bool, error) {
	deleted, err := s.client.DEL(pk)
	if err != nil {
		return false, err
	}

	return len(deleted) > 0, nil
}

// DeleteMembers reclaims all of a key's items. rv1.DEL deletes the whole partition,
// so this is the same physical effect as DeleteMeta; it returns the number removed.
// On the v1 line DeleteMeta already reclaims everything synchronously, so the async
// lazy-deleter that calls this generally finds nothing left (it is enqueued only
// after DeleteMeta ran). It remains defined so the MemberDeleter seam compiles and
// the LReplaceAll list rewrite (which clears members before re-pushing) works.
func (s *redimoStore) DeleteMembers(ctx context.Context, pk string) (int, error) {
	deleted, err := s.client.DEL(pk)
	if err != nil {
		return 0, err
	}

	return len(deleted), nil
}

// SweepOrphans is a no-op on the v1 line: there are no orphan members because
// DeleteMeta reclaims a key's items synchronously via rv1.DEL (no separate meta
// item can be removed while members linger). It always reports zero reclaimed.
func (s *redimoStore) SweepOrphans(ctx context.Context) (int, error) {
	return 0, nil
}

// casRetry runs the bounded optimistic-concurrency (compare-and-set) loop shared
// by the read-modify-write value writes. On the v1 line rv1 has NO conditional
// compare-and-set (no SETCAS/HSETCAS), so the INCR-family RMW below can no longer
// be lost-update-safe; casRetry is retained for structure but each attempt lands
// unconditionally (ok=true on the first try), so it runs exactly once. This is the
// accepted v1 tradeoff: best-effort INCR without SETCAS.
func casRetry(attempt func() (ok bool, err error)) error {
	for i := 0; i < MaxRMWRetries; i++ {
		ok, err := attempt()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}

	rmwExhausted.Add(1)

	return ErrRMWMaxRetries
}
