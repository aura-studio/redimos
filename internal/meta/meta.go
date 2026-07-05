// Package meta implements the MetaStore: per-key meta item semantics
// (type/exp/cnt), conditional writes for type checking and O(1) counters,
// TTL/expiry evaluation, and the lazy-delete queue seam.
//
// The MetaStore sits between the command handlers and the storage seam
// (internal/storage). Command handlers use this typed API and never touch redimo
// directly. The write path is a single conditional meta update (EnsureType) that
// atomically creates the key, checks its type and maintains the O(1) counter; the
// read path evaluates expiry against meta.exp via IsExpired.
//
// ctx note: the storage seam accepts a context, but the redimo fork v1.7 meta
// methods still call context.TODO() internally, so the context is not yet threaded
// all the way down to DynamoDB. The MetaStore API takes a ctx today so callers are
// context-aware from the start and no signature change is needed once the fork
// propagates the context.
//
// The background deleter (Query pk + BatchWriteItem) lands in task 11.1. Here we
// only provide the enqueue seam: DeleteMeta removes the meta item (making the key
// immediately logically absent) and hands the pk to an injectable DeletionEnqueuer.
package meta

import (
	"context"
	"errors"

	"github.com/aura-studio/redimos/v2/internal/storage"
)

// KeyType is the logical Redis type recorded in the meta item's `t` attribute.
type KeyType string

const (
	TypeString KeyType = "str"
	TypeHash   KeyType = "hash"
	TypeList   KeyType = "list"
	TypeSet    KeyType = "set"
	TypeZSet   KeyType = "zset"
)

// Meta is the decoded representation of a key's meta item.
type Meta struct {
	Type  KeyType // attribute t
	Exp   int64   // attribute exp, epoch seconds; 0 = never expires
	Count int64   // attribute cnt
}

// ErrWrongType is the meta-layer sentinel for a type conflict on a conditional
// meta write. Command handlers translate it to the RESP reply
// "-WRONGTYPE Operation against a key holding the wrong kind of value"
// (see resp.ErrWrongType).
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// DeletionEnqueuer is the seam for the lazy-delete queue. DeleteMeta hands a pk to
// it after the meta item is removed so the background deleter (task 11.1) can
// reclaim the key's data items. Keeping it an interface lets task 11.1 inject the
// real queue and lets unit tests inject a spy without a live DynamoDB.
type DeletionEnqueuer interface {
	// Enqueue schedules pk's data items for asynchronous deletion. It must not
	// block the calling command path.
	Enqueue(pk string)
}

// EnqueueFunc adapts a plain function to the DeletionEnqueuer interface.
type EnqueueFunc func(pk string)

// Enqueue calls the underlying function.
func (f EnqueueFunc) Enqueue(pk string) { f(pk) }

// noopEnqueuer drops enqueued pks. It is the default until task 11.1 wires the
// real deleter, so DeleteMeta still removes meta correctly with no queue attached.
type noopEnqueuer struct{}

func (noopEnqueuer) Enqueue(string) {}

// MetaStore exposes the design's meta API on top of the storage seam.
type MetaStore struct {
	store   storage.Store
	enqueue DeletionEnqueuer
}

// NewMetaStore builds a MetaStore over the given storage seam. A nil enqueue is
// replaced with a no-op, so callers that don't yet have the deleter (task 11.1)
// can still delete meta safely.
func NewMetaStore(store storage.Store, enqueue DeletionEnqueuer) *MetaStore {
	if enqueue == nil {
		enqueue = noopEnqueuer{}
	}

	return &MetaStore{store: store, enqueue: enqueue}
}

// EnsureType performs the meta conditional write that underpins every write
// command: it creates the key + records its type + applies the count delta in a
// single atomic conditional UpdateItem. A zero cntDelta still establishes/verifies
// the type (e.g. String writes that keep no member count). On a type conflict it
// returns ErrWrongType and no item is modified.
// It returns newCount, the member count after the delta was applied (read from the same
// atomic write); callers that keep no count (cntDelta 0) can ignore it, while a
// count-adjusting caller uses it to decide emptiness without a second read (see
// DeleteMetaIfEmpty / adjustCount).
func (m *MetaStore) EnsureType(ctx context.Context, pk string, expected KeyType, cntDelta int64) (newCount int64, err error) {
	newCount, err = m.store.EnsureType(ctx, pk, string(expected), cntDelta)
	if errors.Is(err, storage.ErrWrongType) {
		return 0, ErrWrongType
	}

	return newCount, err
}

// CreateTypeIfAbsent atomically claims a logically-absent key (no meta item, or one
// already expired relative to nowEpoch) with the given type, in a single conditional
// meta write. It is the concurrency-safe gate for SETNX / SET NX: created is true
// only for the one caller that wins the race; a live key of any type yields
// created=false (not ErrWrongType — SETNX never reports a type error). On a claim the
// count is reset to cntDelta and any stale expiry cleared.
func (m *MetaStore) CreateTypeIfAbsent(ctx context.Context, pk string, expected KeyType, cntDelta, nowEpoch int64) (created bool, err error) {
	return m.store.CreateTypeIfAbsent(ctx, pk, string(expected), cntDelta, nowEpoch)
}

// Load reads the meta item for pk. found is false when the key is logically
// absent. Callers combine this with IsExpired to enforce expiry on the read path.
func (m *MetaStore) Load(ctx context.Context, pk string) (Meta, bool, error) {
	sm, found, err := m.store.LoadMeta(ctx, pk)
	if err != nil {
		return Meta{}, false, err
	}
	if !found {
		return Meta{}, false, nil
	}

	return Meta{Type: KeyType(sm.Type), Exp: sm.Exp, Count: sm.Count}, true, nil
}

// SetExpire writes exp (epoch seconds) on an existing key's meta item (O(1)).
// found is false when the key has no meta item, mapping to EXPIRE returning :0.
func (m *MetaStore) SetExpire(ctx context.Context, pk string, expEpoch int64) (found bool, err error) {
	return m.store.SetExpire(ctx, pk, expEpoch)
}

// Persist removes the exp attribute, making the key never-expiring. found is false
// when the key has no meta item.
func (m *MetaStore) Persist(ctx context.Context, pk string) (found bool, err error) {
	return m.store.Persist(ctx, pk)
}

// DeleteMeta removes the meta item so the key is immediately logically absent, then
// enqueues the pk for asynchronous data-item deletion. existed reports whether a
// meta item was present before deletion (lets DEL distinguish a real delete from a
// no-op). The pk is enqueued only when a meta item was actually removed; orphan
// members left behind by a missing meta are handled by the weekly sweeper.
func (m *MetaStore) DeleteMeta(ctx context.Context, pk string) (existed bool, err error) {
	existed, err = m.store.DeleteMeta(ctx, pk)
	if err != nil {
		return existed, err
	}

	if existed {
		m.enqueue.Enqueue(pk)
	}

	return existed, nil
}

// DeleteMetaIfEmpty removes the meta item ONLY IF its member count is still <= 0. It is
// the concurrency-safe deletion used when a count-adjusting write empties a collection: a
// concurrent write that raised the count makes the conditional fail, so the meta survives
// and the freshly-added member is not stranded under a removed meta. deleted reports
// whether a meta item was actually removed.
//
// Unlike DeleteMeta it does NOT enqueue the pk for asynchronous member reclamation: an
// emptied collection has no members left (the mutation that drove the count to zero
// already removed the last one), so there is nothing to reclaim. Crucially, enqueuing here
// would race a recreate — if the key is re-populated before the lazy deleter runs,
// DeleteMembers would wipe the fresh members. Any genuine orphan (a count that drifted to
// zero with members still present) is the weekly sweeper's backstop.
func (m *MetaStore) DeleteMetaIfEmpty(ctx context.Context, pk string) (deleted bool, err error) {
	return m.store.DeleteMetaIfEmpty(ctx, pk)
}

// IsExpired reports whether m is expired relative to nowEpoch (epoch seconds): a
// key is expired when exp > 0 and exp <= now. The judgement depends only on
// meta.exp and the supplied clock, independent of when DynamoDB's native TTL
// physically removes the item.
func IsExpired(m Meta, nowEpoch int64) bool {
	return m.Exp > 0 && m.Exp <= nowEpoch
}
