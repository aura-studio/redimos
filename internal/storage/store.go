// Package storage wraps the redimo fork (v1.7 branch) that maps Redis data
// structures to DynamoDB single-table items.
//
// It is the seam between the proxy's command/business layers and the storage
// engine: command handlers and the meta layer depend on the Store interface
// defined here, never on redimo directly. That keeps the redimo import contained
// to this package so the fork can be swapped or mocked, and so the rest of the
// proxy can be unit-tested against a fake Store without a live DynamoDB.
//
// Task 8.1 lands the meta primitives (EnsureType / LoadMeta / SetExpire / Persist
// / DeleteMeta). Data-structure operations (Strings/Hashes/Lists/Sets/SortedSets)
// are added to this interface by the later command tasks (tasks 9–16).
//
// ctx note: the Store methods accept a ctx so the proxy API is context-aware,
// and the redimo-backed implementation threads that ctx down to redimo via
// redimo.Client.WithContext(ctx), so a request's cancellation/deadline reaches
// the DynamoDB calls.
package storage

import (
	"context"
	"errors"
	"sync/atomic"

	redimo "github.com/aura-studio/redimo/v2"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ErrWrongType is the storage-seam sentinel for a meta conditional-write type
// conflict. It mirrors redimo.ErrWrongType so callers can detect the condition
// with errors.Is without importing redimo. The meta layer maps it onto its own
// sentinel, which command handlers ultimately translate to the RESP reply
// "-WRONGTYPE Operation against a key holding the wrong kind of value".
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// INCR-family value/overflow sentinels. The command layer maps each onto the
// exact Pika v3.2.2 wire text via writeStoreError (see internal/command). Their
// Error() text is descriptive only; the leading "-ERR" prefix and the precise
// wording live in the resp package constants.
var (
	// ErrNotInteger signals that an INCR/DECR/INCRBY/DECRBY target value is not a
	// base-10 signed 64-bit integer. Maps to resp.ErrNotInteger. Requirement 5.9.
	ErrNotInteger = errors.New("value is not an integer or out of range")

	// ErrNotFloat signals that an INCRBYFLOAT target value is not a valid float.
	// Maps to resp.ErrNotValidFloat. Requirement 5.9.
	ErrNotFloat = errors.New("value is not a valid float")

	// ErrIncrOverflow signals that an integer increment/decrement would exceed the
	// signed 64-bit range. Maps to resp.ErrIncrDecrOverflow. Requirement 5.8.
	ErrIncrOverflow = errors.New("increment or decrement would overflow")

	// ErrIncrNaNOrInfinity signals that an INCRBYFLOAT would produce a NaN or
	// infinite result. Maps to resp.ErrIncrNaNOrInfinity. Requirement 5.8.
	ErrIncrNaNOrInfinity = errors.New("increment would produce NaN or Infinity")

	// ErrHashNotInteger signals that an HINCRBY target field value is not a
	// base-10 signed 64-bit integer. Maps to resp.ErrHashNotInteger. Requirement
	// 6.1.
	ErrHashNotInteger = errors.New("hash value is not an integer")

	// ErrHashNotFloat signals that an HINCRBYFLOAT target field value is not a
	// valid float. Maps to resp.ErrHashNotFloat. Requirement 6.1.
	ErrHashNotFloat = errors.New("hash value is not a float")

	// ErrRMWMaxRetries signals that a read-modify-write command (APPEND / SETRANGE
	// or the INCR-family reconciliation) exhausted MaxRMWRetries optimistic-
	// concurrency attempts without landing its conditional write — every attempt
	// lost a race with another writer on the same key. It maps to a generic,
	// retryable "-ERR" reply at the command layer (requirements 15.2, 16.4).
	ErrRMWMaxRetries = errors.New("read-modify-write exceeded retry limit under contention")
)

// MaxRMWRetries bounds the optimistic-concurrency retry loop shared by the
// read-modify-write String commands (APPEND / SETRANGE) and the INCR-family
// reconciliation. Each attempt re-reads the current value, recomputes the result
// and re-issues the conditional write (SetStringIfEquals); a losing attempt
// retries until the write lands or this bound is hit, at which point the command
// surfaces ErrRMWMaxRetries (requirements 15.2, 16.4). The value is generous
// because real contention on a single key is rare and each retry is one extra
// round-trip; a run that reaches the bound indicates pathological hot-key
// contention rather than normal operation.
const MaxRMWRetries = 50

// HField is a single hash field/value pair. The field name is a string because a
// DynamoDB sort key (which stores the field) is always a string; the value is
// opaque binary to keep Redis' hash values binary-safe. It is the unit HSet
// accepts and HGetAll returns.
type HField struct {
	Field string
	Value []byte
}

// ZMember is a single sorted-set member/score pair. The member is a string
// because it is stored as a DynamoDB sort key; the score is an IEEE754 double
// matching Redis' score type. It is the unit the sorted-set primitives exchange
// (ZAdd accepts them, the range/rank reads return them in score order).
type ZMember struct {
	Member string
	Score  float64
}

// ScoreBound is one end of a ZRANGEBYSCORE / ZCOUNT / ZREMRANGEBYSCORE score
// interval. Value carries the bound and may be +Inf or -Inf (Redis' +inf/-inf);
// Exclusive selects the open-interval '(' semantics (score strictly greater than
// / less than Value) rather than the default inclusive bound.
type ScoreBound struct {
	Value     float64
	Exclusive bool
}

// MaxBatchGetItems is the DynamoDB hard limit on the number of keys in a single
// BatchGetItem call. MGetStrings splits a large key set into chunks of at most
// this many partition keys (design: MGET reads ≤100/batch).
const MaxBatchGetItems = 100

// Meta is the storage-layer view of a key's meta item (attributes t / exp / cnt).
// Type is the raw type string ("str"/"hash"/"list"/"set"/"zset"); the meta layer
// wraps it in its typed KeyType.
type Meta struct {
	Type  string // attribute t
	Exp   int64  // attribute exp, epoch seconds; 0 = never expires
	Count int64  // attribute cnt
}

// Store is the storage seam over the redimo fork v1.7. It currently exposes the
// meta primitives that underpin type checking, O(1) counters and TTL/expiry;
// data-structure operations are appended by later command tasks.
type Store interface {
	// EnsureType performs the meta conditional write (attribute_not_exists(t) OR
	// t = :expected) that atomically creates/verifies the key type and applies the
	// count delta. It returns ErrWrongType when the key exists with a different
	// type, leaving all items unmodified.
	//
	// Concurrency (requirement 16.3): this is a single DynamoDB UpdateItem whose
	// condition (the type check) and effect (SET t plus ADD cnt :delta) are applied
	// atomically, and cnt is maintained with DynamoDB's atomic ADD. So when several
	// connections write the same key concurrently, each write's type check and
	// count adjustment are serialized by the backend — the type check can never
	// race past a conflicting type, and the counter can never lose an increment.
	// No read-then-write window exists here, so unlike the String value RMW
	// (SetStringIfEquals) this needs no compare-and-set retry.
	//
	// It returns newCount, the member count AFTER the delta was applied, read back from
	// the same atomic write. Callers that empty a collection use this authoritative count
	// (instead of a second racy Load) to decide deletion — see DeleteMetaIfEmpty.
	EnsureType(ctx context.Context, pk, expected string, cntDelta int64) (newCount int64, err error)

	// CreateTypeIfAbsent atomically establishes the meta item for pk with the given
	// type ONLY IF the key is logically absent — no meta item, or a meta item that
	// is already expired relative to nowEpoch. It is the concurrency-safe gate for
	// SETNX / SET NX: a single conditional meta write claims the key, so racing
	// callers can never both win. created is false (no error) when the key is live
	// (of any type); the caller replies :0 / null without writing a value. On a
	// successful claim the count is reset to cntDelta (not added) and any stale
	// expiry is cleared.
	CreateTypeIfAbsent(ctx context.Context, pk, expected string, cntDelta, nowEpoch int64) (created bool, err error)

	// LoadMeta reads the meta item for pk. found is false when the key is logically
	// absent (no meta item).
	LoadMeta(ctx context.Context, pk string) (meta Meta, found bool, err error)

	// SetExpire writes exp (epoch seconds) on an existing key's meta item. found is
	// false when the key has no meta item (→ EXPIRE returns :0).
	SetExpire(ctx context.Context, pk string, expEpoch int64) (found bool, err error)

	// Persist removes the exp attribute, making the key never-expiring. found is
	// false when the key has no meta item.
	Persist(ctx context.Context, pk string) (found bool, err error)

	// DeleteMeta removes only the meta item, making the key immediately logically
	// absent. existed reports whether a meta item was present. Reclaiming the key's
	// data items is the lazy deleter's job (task 11.1).
	DeleteMeta(ctx context.Context, pk string) (existed bool, err error)

	// DeleteMetaIfEmpty removes the meta item ONLY IF its member count is absent or <= 0.
	// It is the concurrency-safe deletion for a collection that a count-adjusting write
	// just emptied: a concurrent write that raised the count makes the conditional fail,
	// so a freshly-added member is never stranded under a removed meta. deleted reports
	// whether a meta item was actually removed.
	DeleteMetaIfEmpty(ctx context.Context, pk string) (deleted bool, err error)

	// DeleteMembers reclaims all of a key's data-member items — every item under
	// the pk except the meta item (sk = "#meta"). It is the storage primitive
	// behind the lazy deleter (task 11.1): after DeleteMeta removes the meta item,
	// the background deleter calls DeleteMembers to Query the pk and
	// BatchWriteItem-delete its members. It returns the number of members deleted
	// and is safe to call when the key has none (returns 0).
	DeleteMembers(ctx context.Context, pk string) (deleted int, err error)

	// SweepOrphans scans the whole table for orphan data members — items whose
	// owning pk has no meta item (sk = "#meta") — and reclaims them. It is the
	// storage primitive behind the weekly sweeper (task 11.2), the backstop for pks
	// that the lazy deleter dropped (full queue) or failed to reclaim. It returns
	// the total number of orphan members submitted for deletion.
	SweepOrphans(ctx context.Context) (reclaimed int, err error)

	// --- String data operations (task 9.1) ---------------------------------
	//
	// These operate on a String key's single data-value item (the item stored
	// under the key's pk with the reserved empty sort key), independent of the
	// key's meta item. Type checking, key creation and TTL are owned by the meta
	// layer (EnsureType / SetExpire / Persist); these primitives only read and
	// write the value bytes, so command handlers compose meta + value writes in
	// the order the design's write path prescribes (guard -> EnsureType -> value
	// write -> TTL). Values are treated as opaque binary to preserve Redis'
	// binary-safe string semantics.

	// GetString reads the String value at pk. found is false when the value item
	// does not exist. It does not consult the meta item, so callers enforce
	// existence/expiry via the meta read path before surfacing the value.
	GetString(ctx context.Context, pk string) (val []byte, found bool, err error)

	// MGetStrings reads the String values for the given pks as a single logical
	// batch, transparently splitting the read into BatchGetItem-sized chunks of at
	// most MaxBatchGetItems (100) partition keys. It returns a map from pk to value
	// containing only the pks that have a String value item; a pk with no value
	// item is simply absent from the map. Like GetString it does not consult meta
	// items — callers enforce per-key existence/expiry and type via the meta layer
	// and only pass live String pks (see the MGET handler). Duplicate pks are
	// fetched once. It backs MGET.
	MGetStrings(ctx context.Context, pks []string) (vals map[string][]byte, err error)

	// SetString unconditionally writes val as the String value at pk, creating or
	// overwriting the value item. Existence and type conditions (NX/XX, WRONGTYPE)
	// are decided by the caller via the meta layer before this write.
	SetString(ctx context.Context, pk string, val []byte) error

	// GetSetString atomically writes val as the String value at pk and returns the
	// previous value. existed is false when no value item was present before the
	// write (mapping GETSET's reply to the null bulk string). It backs GETSET.
	GetSetString(ctx context.Context, pk string, val []byte) (old []byte, existed bool, err error)

	// SetStringIfEquals is the optimistic-concurrency (compare-and-set) write
	// behind the read-modify-write String commands APPEND / SETRANGE (requirements
	// 15.2, 16.4). It writes newVal as the String value at pk only if the value
	// item is still exactly as the caller last read it: when oldExists is true the
	// stored bytes must equal oldVal, and when oldExists is false no value item
	// must exist. It returns ok=true when the conditional write landed and
	// ok=false — with no write — when the precondition failed because a concurrent
	// writer changed the value; the command layer then re-reads, recomputes and
	// retries up to MaxRMWRetries before surfacing ErrRMWMaxRetries. Unlike a plain
	// read-then-SetString this cannot silently lose a concurrent update, and it
	// does not depend on read consistency: the condition is evaluated at write time
	// against the current item (requirement 15.2). Type checking and TTL remain the
	// meta layer's job; this touches only the value item.
	SetStringIfEquals(ctx context.Context, pk string, newVal, oldVal []byte, oldExists bool) (ok bool, err error)

	// --- INCR-family atomic counters (task 9.3) ----------------------------
	//
	// Value-encoding reconciliation: SetString stores a String value as a
	// DynamoDB binary (B) attribute to keep Redis strings binary-safe, and
	// GetString reads that B attribute back. DynamoDB's native atomic ADD, by
	// contrast, operates only on a numeric (N) attribute — it cannot ADD to a B
	// attribute, and cannot even distinguish a B value that happens to parse as a
	// number ("5") from a non-numeric one ("hello"), so it can neither honour
	// `SET x 5; INCR x` nor produce Redis' not-an-integer error on `SET x hello;
	// INCR x`. A native-N ADD would also leave the value as N, which GetString
	// (reading B) would then surface as absent, breaking `INCR x; GET x`.
	//
	// IncrBy/IncrByFloat therefore reconcile the two representations at this seam:
	// they read the current value bytes, parse them as Redis would, apply the
	// delta, and write the decimal result straight back as the same B attribute
	// SET/GET use. GET then reads back exactly the decimal string Redis returns,
	// and `SET x 5; INCR x; GET x`, `INCR x; GET x` and the non-integer error all
	// behave per Redis. The read-modify-write is not a single DynamoDB operation, so
	// IncrBy/IncrByFloat write the decimal result back with a compare-and-set
	// conditional on the value the read observed and retry on conflict (bounded by
	// MaxRMWRetries), so concurrent counter updates on the same key cannot lose an
	// update (requirements 16.3, 16.4).

	// IncrBy adds delta to the integer String value at pk and returns the new
	// value, initialising a missing value to 0 first. It returns ErrNotInteger
	// when the existing value is not a base-10 signed 64-bit integer, and
	// ErrIncrOverflow when the result would leave the signed 64-bit range. The new
	// value is stored as its decimal-string bytes so GetString reads it back
	// verbatim. It backs INCR/DECR/INCRBY/DECRBY.
	IncrBy(ctx context.Context, pk string, delta int64) (newVal int64, err error)

	// IncrByFloat adds delta to the floating-point String value at pk and returns
	// the new value formatted the way Redis formats INCRBYFLOAT (shortest decimal,
	// no exponent, trailing zeros trimmed), initialising a missing value to 0
	// first. It returns ErrNotFloat when the existing value is not a valid float
	// and ErrIncrNaNOrInfinity when the result would be NaN or infinite. The
	// returned bytes are exactly what is stored, so GetString reads them back
	// verbatim. It backs INCRBYFLOAT.
	IncrByFloat(ctx context.Context, pk string, delta float64) (newVal []byte, err error)

	// --- Hash data operations (task 13.1) ----------------------------------
	//
	// A Hash key stores each field as an independent item under the key's pk with
	// the field name as the sort key (sk = field), so field reads/writes are
	// per-item and concurrency-safe (requirement 6.1). These primitives touch
	// only the field items and never the key's meta item (sk = "#meta"): type
	// checking, key creation and the O(1) field counter live in the meta layer
	// (EnsureType with a cntDelta), so a Hash command composes meta + field writes
	// the same way the String family composes meta + value writes. The redimo-
	// backed implementation filters the reserved meta item out of whole-partition
	// reads (HGetAll/HKeys/HVals) so it is never surfaced as a field.
	//
	// Values are stored and read back as opaque binary, matching the String
	// family, so HINCRBY/HINCRBYFLOAT reconcile the numeric representation with a
	// read-modify-write (parse the stored bytes, apply the delta, write the
	// decimal result back) rather than a native DynamoDB ADD — see the String
	// INCR-family note above for why the two representations must be reconciled.

	// HSet writes each field/value pair at pk, creating or overwriting the field
	// item. It returns how many of the fields were newly created (did not exist
	// before the write); that count is the net cnt delta the caller applies to the
	// meta item so HLEN stays equal to the field count (requirement 6.4). It backs
	// HSET/HMSET. Existence and type are decided by the caller via the meta layer
	// before this write.
	HSet(ctx context.Context, pk string, fields []HField) (added int, err error)

	// HSetNX sets field at pk to val only if the field does not already exist,
	// returning set=true when the field was created (a net cnt delta of 1) and
	// set=false when the field already existed (no write, no count change). It
	// backs HSETNX.
	HSetNX(ctx context.Context, pk, field string, val []byte) (set bool, err error)

	// HGet reads the value of field at pk. found is false when the field item does
	// not exist. It does not consult the meta item, so callers enforce
	// existence/expiry and type via the meta layer first. It backs HGET.
	HGet(ctx context.Context, pk, field string) (val []byte, found bool, err error)

	// HMGet reads the values for the given fields at pk, returning a map from field
	// to value containing only the fields that exist; a missing field is simply
	// absent from the map (the caller renders it as a null bulk string in request
	// order). It backs HMGET.
	HMGet(ctx context.Context, pk string, fields []string) (vals map[string][]byte, err error)

	// HGetAll returns every field/value pair at pk (the reserved meta item is
	// excluded). The order is unspecified, matching Redis HGETALL. It backs
	// HGETALL, and — via the caller — HKEYS/HVALS.
	HGetAll(ctx context.Context, pk string) (fields []HField, err error)

	// HKeys returns every field name at pk (meta item excluded), in unspecified
	// order. It backs HKEYS.
	HKeys(ctx context.Context, pk string) (fields []string, err error)

	// HVals returns every field value at pk (meta item excluded), in unspecified
	// order. It backs HVALS.
	HVals(ctx context.Context, pk string) (vals [][]byte, err error)

	// HDel removes the given fields at pk and returns how many actually existed and
	// were removed; that count (negated) is the net cnt delta the caller applies to
	// the meta item. A field listed more than once is removed (and counted) once.
	// It backs HDEL.
	HDel(ctx context.Context, pk string, fields []string) (removed int, err error)

	// HExists reports whether field exists at pk. It backs HEXISTS.
	HExists(ctx context.Context, pk, field string) (exists bool, err error)

	// HStrlen returns the byte length of the value of field at pk, or 0 when the
	// field does not exist. It backs HSTRLEN.
	HStrlen(ctx context.Context, pk, field string) (length int, err error)

	// HIncrBy adds delta to the integer value of field at pk and returns the new
	// value, initialising a missing field to 0 first. isNew reports whether the
	// field was created by this call (a net cnt delta of 1). It returns
	// ErrHashNotInteger when the existing field value is not a base-10 signed
	// 64-bit integer and ErrIncrOverflow when the result would leave the signed
	// 64-bit range. The new value is stored as its decimal-string bytes so HGet
	// reads it back verbatim. It backs HINCRBY.
	HIncrBy(ctx context.Context, pk, field string, delta int64) (newVal int64, isNew bool, err error)

	// HIncrByFloat adds delta to the floating-point value of field at pk and
	// returns the new value formatted the way Redis formats HINCRBYFLOAT,
	// initialising a missing field to 0 first. isNew reports whether the field was
	// created by this call (a net cnt delta of 1). It returns ErrHashNotFloat when
	// the existing field value is not a valid float and ErrIncrNaNOrInfinity when
	// the result would be NaN or infinite. It backs HINCRBYFLOAT.
	HIncrByFloat(ctx context.Context, pk, field string, delta float64) (newVal []byte, isNew bool, err error)

	// --- Set data operations (task 14.1) -----------------------------------
	//
	// A Set key stores each member as an independent item under the key's pk with
	// the member value as the sort key (sk = member), so member reads/writes are
	// per-item and concurrency-safe (requirement 8.1). Like the Hash family these
	// primitives touch only the member items and never the key's meta item (sk =
	// "#meta"): type checking, key creation and the O(1) cardinality counter live
	// in the meta layer (EnsureType with a cntDelta), so a Set command composes
	// meta + member writes exactly the way the Hash family composes meta + field
	// writes, and SCARD reads meta.cnt for O(1) (requirements 8.2, 8.5). Members
	// are DynamoDB sort keys and are therefore string-typed; whole-partition reads
	// (SMembers / SPop / SRandMember) filter the reserved meta item out so it is
	// never surfaced as a member.

	// SAdd adds the given members to the set at pk, creating the member items, and
	// returns how many members were newly added (members already present are not
	// counted); that count is the net cnt delta the caller applies to the meta
	// item so SCARD stays equal to the cardinality (requirement 8.5). A member
	// listed more than once is added (and counted) once. It backs SADD. Existence
	// and type are decided by the caller via the meta layer before this write.
	SAdd(ctx context.Context, pk string, members []string) (added int, err error)

	// SRem removes the given members from the set at pk and returns how many
	// actually existed and were removed; that count (negated) is the net cnt delta
	// the caller applies to the meta item. A member listed more than once is
	// removed (and counted) once. It backs SREM.
	SRem(ctx context.Context, pk string, members []string) (removed int, err error)

	// SIsMember reports whether member is present in the set at pk. It backs
	// SISMEMBER.
	SIsMember(ctx context.Context, pk, member string) (isMember bool, err error)

	// SMembers returns every member of the set at pk (the reserved meta item is
	// excluded), in unspecified order. It backs SMEMBERS.
	SMembers(ctx context.Context, pk string) (members []string, err error)

	// SPop removes up to count DISTINCT random members from the set at pk and
	// returns the members it removed (fewer than count when the set is smaller).
	// The number returned (negated) is the net cnt delta the caller applies to the
	// meta item. count <= 0 removes nothing. It backs SPOP.
	SPop(ctx context.Context, pk string, count int) (members []string, err error)

	// SRandMember returns random members of the set at pk WITHOUT removing any,
	// implementing Redis' count semantics: a non-negative count returns up to that
	// many DISTINCT members (fewer when the set is smaller); a negative count
	// returns exactly -count members WITH possible repeats. It backs SRANDMEMBER.
	SRandMember(ctx context.Context, pk string, count int) (members []string, err error)

	// SScan is the storage primitive behind the proxy's SSCAN command (task
	// 14.2). Where ScanKeys pages the WHOLE table for SCAN, SScan pages WITHIN a
	// single partition key — the members of one set — via a Query, so SSCAN reuses
	// SCAN's cursor machinery (the internal/scan registry and the uint64<->token
	// bridge) but iterates a key's members instead of the keyspace, exactly as
	// HScan does for a hash's fields. It returns one page of member names
	// (EXCLUDING the reserved meta item, so it is never surfaced as a member),
	// paging from lek (the previous page's nextLEK; nil starts a fresh page from
	// the beginning of the partition) and returning nextLEK — the token to pass
	// back on the next call, or nil when the partition has been fully paged (SSCAN
	// then reports the terminating cursor 0). limit maps Redis' COUNT hint onto the
	// underlying Query Limit (the maximum number of items EVALUATED per page,
	// applied before the meta-item filter, so a page may return fewer — even zero —
	// members while still yielding a non-nil nextLEK); a value <= 0 leaves the
	// limit unset. The MATCH filter on the member name is applied proxy-side by the
	// command layer, exactly as SCAN applies MATCH to key names.
	SScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) (members []string, nextLEK map[string]types.AttributeValue, err error)

	// --- Sorted Set data operations (task 15.1) ----------------------------
	//
	// A Sorted Set key stores each member as an independent item under the key's
	// pk with the member value as the sort key (sk = member) and the member's
	// score in the numeric sort-key attribute (skN), which the score index orders
	// on so range/rank reads come back in score order (requirement 9.1); ties on
	// equal score fall back to member order. Like the Hash/Set families these
	// primitives touch only the member items and never the key's meta item (sk ==
	// "#meta"): type checking, key creation and the O(1) member counter live in
	// the meta layer (EnsureType with a cntDelta), so a Sorted Set command
	// composes meta + member writes the same way, and ZCARD reads meta.cnt for
	// O(1) (requirements 9.2, 9.7). Scores are IEEE754 doubles (Redis' score
	// type); ZADD/ZINCRBY parse and format them consistently with the command
	// layer. The reserved meta item is never part of the score index, so ordered
	// reads never surface it.
	//
	// ZSCAN is the Sorted Set scan primitive (task 15.2, ZScan below). ZRANGEBYLEX
	// and ZUNIONSTORE/ZINTERSTORE (also task 15.2) are computed proxy-side by the
	// command layer over these existing reads (the ordered read for lex filtering,
	// ZRangeByRank/SMembers for the store operands), so they add no new seam
	// method.

	// ZAdd adds or updates the given members at pk: a member not already present
	// is created with its score, an existing member has its score overwritten. It
	// returns how many members were newly ADDED (score updates do not count); that
	// count is the net cnt delta the caller applies so ZCARD stays equal to the
	// member count (requirement 9.7). When a member is listed more than once the
	// last score wins and it is counted at most once. It backs ZADD. Existence and
	// type are decided by the caller via the meta layer before this write.
	ZAdd(ctx context.Context, pk string, members []ZMember) (added int, err error)

	// ZRem removes the given members at pk and returns how many actually existed
	// and were removed; that count (negated) is the net cnt delta the caller
	// applies. A member listed more than once is removed (and counted) once. It
	// backs ZREM.
	ZRem(ctx context.Context, pk string, members []string) (removed int, err error)

	// ZScore returns the score of member at pk. found is false when the member is
	// absent. It backs ZSCORE.
	ZScore(ctx context.Context, pk, member string) (score float64, found bool, err error)

	// ZIncrBy adds delta to the score of member at pk and returns the new score,
	// initialising a missing member to score 0 first. isNew reports whether the
	// member was created by this call so the caller bumps cnt only for a brand-new
	// member. It backs ZINCRBY.
	ZIncrBy(ctx context.Context, pk, member string, delta float64) (newScore float64, isNew bool, err error)

	// ZRangeByRank returns the members whose rank falls in the inclusive index
	// range [start, stop], ordered by score (ascending when rev is false, matching
	// ZRANGE; descending when rev is true, matching ZREVRANGE). Negative indices
	// count from the end (-1 is the last element); out-of-range indices are
	// clamped, and an empty slice is returned when the normalized range is empty.
	// It backs ZRANGE / ZREVRANGE.
	ZRangeByRank(ctx context.Context, pk string, start, stop int, rev bool) (members []ZMember, err error)

	// ZRangeByScore returns the members whose score falls within [min, max]
	// (honouring each bound's Exclusive flag), ordered by score ascending when rev
	// is false (ZRANGEBYSCORE) or descending when rev is true (ZREVRANGEBYSCORE).
	// It backs ZRANGEBYSCORE / ZREVRANGEBYSCORE.
	ZRangeByScore(ctx context.Context, pk string, min, max ScoreBound, rev bool) (members []ZMember, err error)

	// ZCount returns how many members at pk have a score within [min, max]
	// (honouring each bound's Exclusive flag). It backs ZCOUNT.
	ZCount(ctx context.Context, pk string, min, max ScoreBound) (count int, err error)

	// ZRank returns the 0-based rank of member at pk: its position in ascending
	// score order when rev is false (ZRANK) or descending score order when rev is
	// true (ZREVRANK). found is false when the member is absent. It backs ZRANK /
	// ZREVRANK.
	ZRank(ctx context.Context, pk, member string, rev bool) (rank int, found bool, err error)

	// ZRemRangeByRank removes the members whose rank falls in the inclusive
	// ascending index range [start, stop] (negative indices count from the end)
	// and returns how many were removed; that count (negated) is the net cnt delta
	// the caller applies. It backs ZREMRANGEBYRANK.
	ZRemRangeByRank(ctx context.Context, pk string, start, stop int) (removed int, err error)

	// ZRemRangeByScore removes the members whose score falls within [min, max]
	// (honouring each bound's Exclusive flag) and returns how many were removed;
	// that count (negated) is the net cnt delta the caller applies. It backs
	// ZREMRANGEBYSCORE.
	ZRemRangeByScore(ctx context.Context, pk string, min, max ScoreBound) (removed int, err error)

	// ZScan is the storage primitive behind the proxy's ZSCAN command (task
	// 15.2), the Sorted Set analogue of SScan/HScan. Where ScanKeys pages the
	// WHOLE table for SCAN, ZScan pages WITHIN a single partition key — the members
	// of one sorted set — via a Query, so ZSCAN reuses SCAN's cursor machinery (the
	// internal/scan registry and the uint64<->token bridge) but iterates a key's
	// members instead of the keyspace. Unlike SScan it returns each member together
	// with its score (the ZMember pair) so the ZSCAN reply can interleave member
	// and formatted score, matching Redis. It returns one page of members
	// (EXCLUDING the reserved meta item, so it is never surfaced as a member),
	// paging from lek (the previous page's nextLEK; nil starts a fresh page from
	// the beginning of the partition) and returning nextLEK — the token to pass
	// back on the next call, or nil when the partition has been fully paged (ZSCAN
	// then reports the terminating cursor 0). The page is iterated in base-table
	// (member) order, not score order — ZSCAN makes no ordering guarantee. limit
	// maps Redis' COUNT hint onto the underlying Query Limit (the maximum number of
	// items EVALUATED per page, applied before the meta-item filter, so a page may
	// return fewer — even zero — members while still yielding a non-nil nextLEK); a
	// value <= 0 leaves the limit unset. The MATCH filter on the member name is
	// applied proxy-side by the command layer, exactly as SCAN applies MATCH to key
	// names.
	ZScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) (members []ZMember, nextLEK map[string]types.AttributeValue, err error)

	// --- List data operations (task 16.1) ----------------------------------
	//
	// A List key stores each element as an independent item under the key's pk,
	// ordered by an integer index the fork assigns (decrementing for head pushes,
	// incrementing for tail pushes) and held in the numeric sort-key attribute
	// (skN), which the score index orders on so range/index reads come back in
	// list order (requirement 7.1). Like the other collection families these
	// primitives touch only the element items and never the key's meta item (sk ==
	// "#meta"): type checking, key creation and the O(1) length counter live in the
	// meta layer (EnsureType with a cntDelta), so a List command composes meta +
	// element writes the same way, and LLEN reads meta.cnt for O(1) (requirements
	// 7.2, 7.7).
	//
	// Element-type note: list elements are passed as binary-safe redimo.BytesValue
	// (same as the String/Hash/Set families), matching redimo v2.1's binary-tolerant
	// list handling (valueBytes), and read back via ReturnValue.String() — Go strings
	// are byte-safe so a list element round-trips its exact bytes. LPUSHX/RPUSHX are not part of this
	// seam: the "only if the key exists" gate is enforced by the command layer via
	// the meta read before it calls LPush/RPush.
	//
	// RPOPLPUSH (task 16.2) needs no new seam method: it is composed from RPop
	// (tail of source) + LPush (head of destination) by the command layer, which
	// also maintains both keys' meta counters. LSET/LTRIM/LREM/LINSERT (also task
	// 16.2) are a read-modify-write COMBINED implementation over LRangeAll +
	// LReplaceAll below — the fork's in-place list mutators are unstable/incomplete
	// (no LINSERT; LREM returns a different value than Redis), so the command layer
	// reads the whole ordered element list, computes the new sequence in process
	// and rewrites it.

	// LPush prepends elements to the head of the list at pk in the order given
	// (LPUSH semantics: after LPUSH key a b c the head-to-tail order is c, b, a)
	// and returns how many elements were pushed — always len(elements), since a
	// list admits duplicates — which is the net cnt delta the caller applies so
	// LLEN stays equal to the length. It backs LPUSH (and, gated by the caller,
	// LPUSHX). Existence and type are decided by the caller via the meta layer
	// before this write.
	LPush(ctx context.Context, pk string, elements [][]byte) (pushed int, err error)

	// RPush appends elements to the tail of the list at pk in the order given
	// (RPUSH semantics: after RPUSH key a b c the head-to-tail order is a, b, c)
	// and returns how many elements were pushed (len(elements)), the net cnt delta
	// the caller applies. It backs RPUSH (and, gated by the caller, RPUSHX).
	RPush(ctx context.Context, pk string, elements [][]byte) (pushed int, err error)

	// LPop removes and returns the head element of the list at pk. found is false
	// when the list is empty/absent (mapping LPOP's reply to the null bulk string).
	// A removal is a net cnt delta of -1 the caller applies, deleting the key when
	// its last element is popped. It backs LPOP.
	LPop(ctx context.Context, pk string) (val []byte, found bool, err error)

	// RPop removes and returns the tail element of the list at pk. found is false
	// when the list is empty/absent. Like LPop a removal is a -1 cnt delta. It
	// backs RPOP.
	RPop(ctx context.Context, pk string) (val []byte, found bool, err error)

	// LRange returns the elements of the list at pk whose index falls in the
	// inclusive range [start, stop] in head-to-tail order, applying Redis'
	// negative-index semantics (-1 is the last element), clamping out-of-range
	// bounds and returning the empty slice for an empty normalized range. It backs
	// LRANGE.
	LRange(ctx context.Context, pk string, start, stop int) (vals [][]byte, err error)

	// LIndex returns the element at index in the list at pk, applying Redis'
	// negative-index semantics (-1 is the last element). found is false when the
	// index is out of range (mapping LINDEX's reply to the null bulk string). It
	// backs LINDEX.
	LIndex(ctx context.Context, pk string, index int) (val []byte, found bool, err error)

	// LRangeAll returns every element of the list at pk in head-to-tail order (an
	// absent/empty list returns the empty slice). It is the READ half of the
	// read-modify-write combined implementation of LSET/LTRIM/LREM/LINSERT (task
	// 16.2): the command layer reads the whole list, computes the new element
	// sequence in process and rewrites it via LReplaceAll. It backs
	// LSET/LTRIM/LREM/LINSERT.
	LRangeAll(ctx context.Context, pk string) (vals [][]byte, err error)

	// LReplaceAll rewrites the list at pk to exactly elements in head-to-tail
	// order, discarding whatever elements were there before, and returns the new
	// length (len(elements)). It is the WRITE half of the LSET/LTRIM/LREM/LINSERT
	// combined implementation: the command layer computes the new sequence in
	// process and calls this to persist it, then applies the net cnt delta
	// (newLen - oldCnt) via the meta layer so LLEN stays exact. Passing an empty
	// slice clears every element item (the caller then drives cnt to 0, deleting
	// the key, matching Redis where an empty list does not exist). Elements are
	// passed as redimo.BytesValue, consistent with LPush/RPush.
	//
	// The rewrite is NOT a single atomic DynamoDB operation — it clears the
	// element items and re-writes them — so it is not atomic across concurrent
	// connections. Unlike the single-item String read-modify-write commands
	// (APPEND/SETRANGE), which task 20.1 made safe with a compare-and-set + retry,
	// this multi-item rebuild cannot be made lost-update-safe by a single
	// conditional write; true cross-connection atomicity would need a DynamoDB
	// transaction spanning all the element items. P0 serves each connection
	// serially, so a single connection's own LSET/LTRIM/LREM/LINSERT are
	// consistent, and cross-connection atomicity for these multi-item commands
	// remains best-effort in P0. It backs LSET/LTRIM/LREM/LINSERT.
	LReplaceAll(ctx context.Context, pk string, elements [][]byte) (count int, err error)

	// --- Keyspace scan (task 17.2) -----------------------------------------
	//
	// ScanKeys is the storage primitive behind the proxy's SCAN command. It pages
	// through the table returning the partition keys of LIVE meta items — items
	// with the reserved meta sort key whose exp attribute is absent or still in
	// the future relative to now (epoch seconds). The expiry predicate is applied
	// server-side in the scan's FilterExpression so a physically-present but
	// logically-expired key (whose native-TTL sweep has not run) is never
	// surfaced, matching the read path's filtering contract.
	//
	// Only the partition key is returned — SCAN reports key NAMES, not values —
	// and the pk is returned VERBATIM (still carrying its "{db}:" prefix). The
	// command layer owns the pk encoding convention (encodePK/decodePK), so it
	// decodes each pk back to the logical key and filters to the connection's
	// selected database; keeping that convention out of the storage seam leaves
	// the store database-agnostic.
	//
	// A single call returns one page. lek is the DynamoDB pagination token from
	// the previous page's nextLEK (nil starts a fresh scan from the beginning);
	// nextLEK is the token to pass back on the next call, or nil when the scan has
	// reached the end of the table (SCAN then reports the terminating cursor 0).
	// limit maps Redis' COUNT hint onto the underlying scan Limit (the maximum
	// number of items EVALUATED per page, applied before the filter, so a page may
	// return fewer — even zero — keys while still yielding a non-nil nextLEK); a
	// value <= 0 leaves the limit unset. It backs SCAN, and the shared cursor
	// mechanism HSCAN/SSCAN/ZSCAN reuse.
	ScanKeys(ctx context.Context, lek map[string]types.AttributeValue, limit int32, now int64) (keys []string, nextLEK map[string]types.AttributeValue, err error)

	// --- Hash field scan (task 13.2) ---------------------------------------
	//
	// HScan is the storage primitive behind the proxy's HSCAN command. Where
	// ScanKeys pages the WHOLE table for SCAN, HScan pages WITHIN a single
	// partition key — the fields of one hash — via a Query, so HSCAN reuses SCAN's
	// cursor machinery (the internal/scan registry and the uint64<->token bridge)
	// but iterates a key's fields instead of the keyspace. It returns the field
	// items under pk EXCLUDING the reserved meta item (sk == "#meta"), so the meta
	// item is never surfaced as a field (matching HGetAll's filtering).
	//
	// A single call returns one page. lek is the DynamoDB pagination token from
	// the previous page's nextLEK (nil starts a fresh page from the beginning of
	// the partition); nextLEK is the token to pass back on the next call, or nil
	// when the partition has been fully paged (HSCAN then reports the terminating
	// cursor 0). limit maps Redis' COUNT hint onto the underlying Query Limit (the
	// maximum number of items EVALUATED per page, applied before the meta-item
	// filter, so a page may return fewer — even zero — fields while still yielding
	// a non-nil nextLEK); a value <= 0 leaves the limit unset. The MATCH filter on
	// the field name is applied proxy-side by the command layer, exactly as SCAN
	// applies MATCH to key names.
	HScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) (fields []HField, nextLEK map[string]types.AttributeValue, err error)
}

// Options configures the redimo-backed Store.
type Options struct {
	// TableName is the DynamoDB single-table name (e.g. "redis-data"). When empty
	// the redimo default is used.
	TableName string

	// EventuallyConsistent opts the Store OUT of the P0 default of strongly
	// consistent reads. The zero value (false) selects strongly consistent reads
	// (DynamoDB ConsistentRead=true), so a Store built with a bare Options{} — the
	// P0 build — reads its own writes, matching Redis semantics (requirement 15.1).
	// Setting it true downgrades every read on this Store to eventually consistent,
	// trading read-your-writes for lower cost/latency.
	//
	// Command-granularity consistency (requirement 15.3) — grey-listing individual
	// commands onto eventually consistent reads — is a future seam and is
	// deliberately NOT a per-Store flag: because a redimo.Client's consistency is
	// fixed at construction, the intended extension is to hold two Stores (one
	// strong, one eventual, sharing the same DynamoDB client) and have the command
	// router pick the eventual Store for the specific read commands that have been
	// opted in. This Store therefore stays single-consistency; the per-command
	// switch is layered above it without changing this seam. Read-modify-write
	// commands never rely on read consistency at all: they use SetStringIfEquals'
	// conditional write plus retry (requirement 15.2), so they are correct under
	// either setting.
	EventuallyConsistent bool

	// DeleteBatchSize bounds how many members the lazy deleter removes per
	// BatchWriteItem call when reclaiming a key's data items. It is clamped to
	// [1, 25] (the DynamoDB per-call hard limit); a value <= 0 selects the default
	// of 25. Lowering it trades throughput for a gentler, more granular write rate.
	DeleteBatchSize int

	// OnThrottle, when non-nil, is invoked whenever the Store observes a DynamoDB
	// throttling error (a ProvisionedThroughputExceededException, or a throttling
	// APIError) on an operation — after the AWS SDK client's own bounded
	// retry/backoff has been exhausted. It is the alerting seam for requirement
	// 18.8: the storage package stays decoupled from metrics/command by invoking
	// this injected callback rather than importing them.
	//
	// Wiring (task 23.1): the assembly step passes a callback that bumps a
	// throttle alert counter / emits a log line (e.g. a metrics counter feeding
	// the DynamoDB ThrottledRequests alert), so a sustained throttle is visible
	// operationally. The callback runs on the request goroutine and MUST NOT block
	// the request path (do cheap, non-blocking work — increment a counter, fire a
	// buffered event). A nil OnThrottle disables alerting; throttles are still
	// classified and surfaced as ErrThrottled so the command layer replies with
	// the retryable "-ERR backend throttled, retry later".
	OnThrottle func()

	// Breaker, when non-nil, is a CircuitBreaker fed every operation outcome so it
	// can shed load during a sustained throttle storm. The command layer consults its
	// Open() state to fail fast. A nil Breaker disables load shedding (the default).
	Breaker *CircuitBreaker
}

// redimoStore is the redimo-backed Store implementation.
type redimoStore struct {
	client          redimo.Client
	deleteBatchSize int
}

// compile-time assertion that redimoStore satisfies Store.
var _ Store = (*redimoStore)(nil)

// rmwExhausted counts casRetry loops that ran out of retries under contention. It is a
// process-wide operational signal for hot-key CAS pressure, surfaced via RMWExhausted.
var rmwExhausted atomic.Uint64

// RMWExhausted returns the number of read-modify-write (CAS) loops that exhausted their
// bounded retry budget and surfaced ErrRMWMaxRetries. It backs the
// rmw_max_retries_exhausted_total metric.
func RMWExhausted() uint64 { return rmwExhausted.Load() }
