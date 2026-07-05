package meta

// Integration-style lifecycle test for lazy deletion and orphan sweeping
// (redimos task 11.3). Where deleter_test.go / sweeper_test.go exercise the
// Deleter and Sweeper components in isolation with scripted doubles, this file
// wires the whole seam together end-to-end against a single stateful in-memory
// store and asserts the observable lazy-delete lifecycle:
//
//   - MetaStore.DeleteMeta removes the meta item so the key is IMMEDIATELY
//     logically absent (a subsequent Load / ReadPath reports found=false) and the
//     pk is enqueued (requirements 12.1, 12.2).
//   - The background Deleter consumes the enqueued pk and invokes the member
//     deletion primitive (Query pk + BatchWriteItem), reclaiming the key's data
//     members (requirement 12.3).
//   - The read path enqueues an EXPIRED key for the same lazy-delete pipeline
//     (requirements 12.1, 12.2, 12.3).
//   - The Sweeper drives SweepOrphans and reclaims orphan members left behind
//     (e.g. by a dropped enqueue or a failed member delete) whose owning key has
//     no meta item (requirement 12.4).
//
// Helpers are prefixed `lazy` to avoid colliding with fakeStore (meta_test.go),
// fakeMemberDeleter/waitFor (deleter_test.go), fakeOrphanSweeper/waitSignal
// (sweeper_test.go), spyEnqueuer/fixedClock (read_test.go) and the prop* doubles
// in the property test files.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aura-studio/redimos/v2/internal/storage"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// lazyStore is a stateful in-memory storage.Store double that models BOTH the meta
// items and the data members of every key, so the full lazy-delete lifecycle can be
// exercised without a live DynamoDB. Unlike the scripted fakeStore, deletes here
// actually mutate state: DeleteMeta removes the meta item (the key becomes
// logically absent) while leaving its members behind, and DeleteMembers / SweepOrphans
// reclaim those members. It is safe for concurrent use because the background
// Deleter goroutine calls DeleteMembers while the test goroutine reads state.
type lazyStore struct {
	mu      sync.Mutex
	metas   map[string]storage.Meta // pk -> meta item; presence == key logically exists
	members map[string]int          // pk -> number of data members still in the backend
	now     int64                   // clock for the fenced reclaim's expiry check (0 = keys never expire)

	// memberCalls, when non-nil, receives one send per DeleteMembers call so a test
	// can wait for the background deleter to consume the queue without sleeping.
	memberCalls chan string
	// sweepCalls, when non-nil, receives one send per SweepOrphans call.
	sweepCalls chan struct{}
}

func newLazyStore() *lazyStore {
	return &lazyStore{
		metas:   make(map[string]storage.Meta),
		members: make(map[string]int),
	}
}

// seed establishes a key with a meta item of the given type and a number of data
// members, mirroring a key that has been written by earlier command handlers.
func (s *lazyStore) seed(pk string, typ KeyType, exp int64, memberCount int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metas[pk] = storage.Meta{Type: string(typ), Exp: exp, Count: int64(memberCount)}
	s.members[pk] = memberCount
}

// seedOrphanMembers establishes data members with NO owning meta item, modelling
// orphans left behind by a dropped enqueue or a failed member delete.
func (s *lazyStore) seedOrphanMembers(pk string, memberCount int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.members[pk] = memberCount
}

// memberCount reports how many data members remain for pk.
func (s *lazyStore) memberCount(pk string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.members[pk]
}

func (s *lazyStore) EnsureType(_ context.Context, pk, expected string, cntDelta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, ok := s.metas[pk]
	if ok && m.Type != expected {
		return 0, storage.ErrWrongType
	}

	m.Type = expected
	m.Count += cntDelta
	s.metas[pk] = m

	return m.Count, nil
}

func (s *lazyStore) DeleteMetaIfEmpty(_ context.Context, pk string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, ok := s.metas[pk]
	if !ok || m.Count > 0 {
		return false, nil
	}

	delete(s.metas, pk)

	return true, nil
}

func (s *lazyStore) CreateTypeIfAbsent(_ context.Context, pk, expected string, cntDelta, nowEpoch int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, ok := s.metas[pk]
	live := ok && !(m.Exp > 0 && m.Exp <= nowEpoch)
	if live {
		return false, nil
	}
	s.metas[pk] = storage.Meta{Type: expected, Count: cntDelta}
	return true, nil
}

func (s *lazyStore) LoadMeta(_ context.Context, pk string) (storage.Meta, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, ok := s.metas[pk]

	return m, ok, nil
}

func (s *lazyStore) SetExpire(_ context.Context, pk string, expEpoch int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, ok := s.metas[pk]
	if !ok {
		return false, nil
	}

	m.Exp = expEpoch
	s.metas[pk] = m

	return true, nil
}

func (s *lazyStore) Persist(_ context.Context, pk string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	m, ok := s.metas[pk]
	if !ok {
		return false, nil
	}

	m.Exp = 0
	s.metas[pk] = m

	return true, nil
}

// DeleteMeta removes ONLY the meta item, making the key immediately logically
// absent. The data members are intentionally left behind for the background
// deleter (or, as a backstop, the sweeper) to reclaim.
func (s *lazyStore) DeleteMeta(_ context.Context, pk string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.metas[pk]
	if !ok {
		return false, nil
	}

	delete(s.metas, pk)

	return true, nil
}

// DeleteMembers reclaims all of a key's data members (the deleter's Query pk +
// BatchWriteItem primitive) and returns how many were removed.
func (s *lazyStore) DeleteMembers(_ context.Context, pk string) (int, error) {
	s.mu.Lock()
	n := s.members[pk]
	delete(s.members, pk)
	s.mu.Unlock()

	if s.memberCalls != nil {
		s.memberCalls <- pk
	}

	return n, nil
}

// DeleteMembersIfDead models the fenced reclaim used by the async deleter: it reclaims a
// key's members only while the key is dead (no meta item) and aborts, deleting nothing, when
// the key is live again (a DEL-then-recreate). It mirrors redimo's atomic transactional
// fence in-memory so the lazy-delete tests exercise the same seam the deleter now calls.
func (s *lazyStore) DeleteMembersIfDead(_ context.Context, pk string) (int, bool, error) {
	s.mu.Lock()
	meta, present := s.metas[pk]
	// A key is dead when its meta is absent (already DEL'd) OR expired (exp <= now); it is
	// live only when its meta is present and unexpired — a genuine recreate, which aborts.
	live := present && !(meta.Exp > 0 && meta.Exp <= s.now)
	if live {
		s.mu.Unlock()

		if s.memberCalls != nil {
			s.memberCalls <- pk
		}

		return 0, true, nil // recreated: abort, leave members intact
	}
	n := s.members[pk]
	delete(s.members, pk)
	s.mu.Unlock()

	if s.memberCalls != nil {
		s.memberCalls <- pk
	}

	return n, false, nil
}

// SweepOrphans reclaims every data member whose owning pk has no meta item.
func (s *lazyStore) SweepOrphans(_ context.Context) (int, error) {
	s.mu.Lock()

	reclaimed := 0

	for pk, n := range s.members {
		if _, hasMeta := s.metas[pk]; !hasMeta {
			reclaimed += n
			delete(s.members, pk)
		}
	}

	s.mu.Unlock()

	if s.sweepCalls != nil {
		s.sweepCalls <- struct{}{}
	}

	return reclaimed, nil
}

func (s *lazyStore) GetString(_ context.Context, _ string) ([]byte, bool, error) {
	return nil, false, nil
}

func (s *lazyStore) MGetStrings(_ context.Context, _ []string) (map[string][]byte, error) {
	return nil, nil
}

func (s *lazyStore) SetString(_ context.Context, _ string, _ []byte) error { return nil }

func (s *lazyStore) SetStringIfEquals(_ context.Context, _ string, _, _ []byte, _ bool) (bool, error) {
	return true, nil
}

func (s *lazyStore) GetSetString(_ context.Context, _ string, _ []byte) ([]byte, bool, error) {
	return nil, false, nil
}

func (s *lazyStore) IncrBy(_ context.Context, _ string, _ int64) (int64, error) { return 0, nil }

func (s *lazyStore) IncrByFloat(_ context.Context, _ string, _ float64) ([]byte, error) {
	return nil, nil
}

func (s *lazyStore) HSet(_ context.Context, _ string, _ []storage.HField) (int, error) { return 0, nil }
func (s *lazyStore) HSetNX(_ context.Context, _, _ string, _ []byte) (bool, error) {
	return false, nil
}
func (s *lazyStore) HGet(_ context.Context, _, _ string) ([]byte, bool, error) {
	return nil, false, nil
}
func (s *lazyStore) HMGet(_ context.Context, _ string, _ []string) (map[string][]byte, error) {
	return nil, nil
}
func (s *lazyStore) HGetAll(_ context.Context, _ string) ([]storage.HField, error) { return nil, nil }
func (s *lazyStore) HKeys(_ context.Context, _ string) ([]string, error)           { return nil, nil }
func (s *lazyStore) HVals(_ context.Context, _ string) ([][]byte, error)           { return nil, nil }
func (s *lazyStore) HDel(_ context.Context, _ string, _ []string) (int, error)     { return 0, nil }
func (s *lazyStore) HExists(_ context.Context, _, _ string) (bool, error)          { return false, nil }
func (s *lazyStore) HStrlen(_ context.Context, _, _ string) (int, error)           { return 0, nil }
func (s *lazyStore) HIncrBy(_ context.Context, _, _ string, _ int64) (int64, bool, error) {
	return 0, false, nil
}
func (s *lazyStore) HIncrByFloat(_ context.Context, _, _ string, _ float64) ([]byte, bool, error) {
	return nil, false, nil
}

func (s *lazyStore) SAdd(_ context.Context, _ string, _ []string) (int, error) { return 0, nil }
func (s *lazyStore) SRem(_ context.Context, _ string, _ []string) (int, error) { return 0, nil }
func (s *lazyStore) SIsMember(_ context.Context, _, _ string) (bool, error)    { return false, nil }
func (s *lazyStore) SMembers(_ context.Context, _ string) ([]string, error)    { return nil, nil }
func (s *lazyStore) SPop(_ context.Context, _ string, _ int) ([]string, error) { return nil, nil }
func (s *lazyStore) SRandMember(_ context.Context, _ string, _ int) ([]string, error) {
	return nil, nil
}

func (s *lazyStore) ZAdd(_ context.Context, _ string, _ []storage.ZMember) (int, error) {
	return 0, nil
}
func (s *lazyStore) ZRem(_ context.Context, _ string, _ []string) (int, error) { return 0, nil }
func (s *lazyStore) ZScore(_ context.Context, _, _ string) (float64, bool, error) {
	return 0, false, nil
}
func (s *lazyStore) ZIncrBy(_ context.Context, _, _ string, _ float64) (float64, bool, error) {
	return 0, false, nil
}
func (s *lazyStore) ZRangeByRank(_ context.Context, _ string, _, _ int, _ bool) ([]storage.ZMember, error) {
	return nil, nil
}
func (s *lazyStore) ZRangeByScore(_ context.Context, _ string, _, _ storage.ScoreBound, _ bool) ([]storage.ZMember, error) {
	return nil, nil
}
func (s *lazyStore) ZCount(_ context.Context, _ string, _, _ storage.ScoreBound) (int, error) {
	return 0, nil
}
func (s *lazyStore) ZRank(_ context.Context, _, _ string, _ bool) (int, bool, error) {
	return 0, false, nil
}
func (s *lazyStore) ZRemRangeByRank(_ context.Context, _ string, _, _ int) (int, error) {
	return 0, nil
}
func (s *lazyStore) ZRemRangeByScore(_ context.Context, _ string, _, _ storage.ScoreBound) (int, error) {
	return 0, nil
}

func (s *lazyStore) LPush(_ context.Context, _ string, _ [][]byte) (int, error) { return 0, nil }
func (s *lazyStore) RPush(_ context.Context, _ string, _ [][]byte) (int, error) { return 0, nil }
func (s *lazyStore) LPop(_ context.Context, _ string) ([]byte, bool, error)     { return nil, false, nil }
func (s *lazyStore) RPop(_ context.Context, _ string) ([]byte, bool, error)     { return nil, false, nil }
func (s *lazyStore) LRange(_ context.Context, _ string, _, _ int) ([][]byte, error) {
	return nil, nil
}
func (s *lazyStore) LIndex(_ context.Context, _ string, _ int) ([]byte, bool, error) {
	return nil, false, nil
}
func (s *lazyStore) LRangeAll(_ context.Context, _ string) ([][]byte, error) { return nil, nil }
func (s *lazyStore) LReplaceAll(_ context.Context, _ string, _ [][]byte) (int, error) {
	return 0, nil
}
func (s *lazyStore) ScanKeys(_ context.Context, _ map[string]types.AttributeValue, _ int32, _ int64) ([]string, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

func (s *lazyStore) SScan(_ context.Context, _ string, _ map[string]types.AttributeValue, _ int32) ([]string, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

func (s *lazyStore) HScan(_ context.Context, _ string, _ map[string]types.AttributeValue, _ int32) ([]storage.HField, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

func (s *lazyStore) ZScan(_ context.Context, _ string, _ map[string]types.AttributeValue, _ int32) ([]storage.ZMember, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

var _ storage.Store = (*lazyStore)(nil)

// lazyAwaitMember reads one pk from ch or fails the test after a deadline, keeping
// the lifecycle tests fast on success and bounded on failure.
func lazyAwaitMember(t *testing.T, ch <-chan string) string {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the background deleter to reclaim a pk")
		return ""
	}
}

// TestLazyDelete_DeleteMetaLifecycle exercises the DEL path end-to-end: DeleteMeta
// makes the key immediately logically absent AND enqueues the pk (12.1, 12.2), and
// the background deleter then consumes the queue and reclaims the key's members via
// the storage primitive (12.3).
//
// Requirements: 12.1, 12.2, 12.3
func TestLazyDelete_DeleteMetaLifecycle(t *testing.T) {
	store := newLazyStore()
	store.memberCalls = make(chan string, 1)
	store.seed("0:del", TypeHash, 0, 3)

	// Wire the real seam: MetaStore -> Deleter (as DeletionEnqueuer) -> store.
	deleter := NewDeleter(store, DeleterConfig{})
	deleter.Start(context.Background())
	defer deleter.Stop()

	ms := NewMetaStore(store, deleter)
	ctx := context.Background()

	// Precondition: the key is live before deletion.
	if _, found, err := ms.Load(ctx, "0:del"); err != nil || !found {
		t.Fatalf("precondition Load = (_, %v, %v), want (_, true, nil)", found, err)
	}

	// DEL: remove the meta item and enqueue the pk.
	existed, err := ms.DeleteMeta(ctx, "0:del")
	if err != nil || !existed {
		t.Fatalf("DeleteMeta = (%v, %v), want (true, nil)", existed, err)
	}

	// 12.1: the key is IMMEDIATELY logically absent — synchronously, before the
	// background deleter has had a chance to reclaim any members.
	if _, found, err := ms.Load(ctx, "0:del"); err != nil || found {
		t.Fatalf("post-DeleteMeta Load = (_, %v, %v), want (_, false, nil) — key must be logically absent immediately", found, err)
	}

	// 12.2 + 12.3: the pk was enqueued and the background deleter consumed it,
	// invoking the member-deletion primitive for exactly that pk.
	if got := lazyAwaitMember(t, store.memberCalls); got != "0:del" {
		t.Fatalf("deleter reclaimed pk %q, want 0:del", got)
	}

	// Stop drains and guarantees the worker finished recording its metrics.
	deleter.Stop()

	if got := store.memberCount("0:del"); got != 0 {
		t.Fatalf("member count after deletion = %d, want 0 (members reclaimed)", got)
	}
	if got := deleter.Deleted(); got != 3 {
		t.Fatalf("Deleted() = %d, want 3 (seeded members reclaimed)", got)
	}
	if got := deleter.Failures(); got != 0 {
		t.Fatalf("Failures() = %d, want 0", got)
	}
}

// TestLazyDelete_ReadPathExpiredEnqueuesAndReclaims exercises the read-path branch
// of the same pipeline: a read that finds an expired key surfaces it as absent and
// enqueues the pk (12.1, 12.2), and the background deleter reclaims its members
// (12.3). Expiry is judged from meta.exp against an injected clock, independent of
// whether the backend data is still present.
//
// Requirements: 12.1, 12.2, 12.3
func TestLazyDelete_ReadPathExpiredEnqueuesAndReclaims(t *testing.T) {
	store := newLazyStore()
	store.memberCalls = make(chan string, 1)
	store.now = 150 // match the reader clock so the fenced reclaim sees exp=100 as expired
	store.seed("0:exp", TypeSet, 100, 4) // exp=100

	deleter := NewDeleter(store, DeleterConfig{})
	deleter.Start(context.Background())
	defer deleter.Stop()

	ms := NewMetaStore(store, deleter)
	// Clock pinned at now=150 > exp=100 → the key is expired.
	reader := NewReader(ms, func() int64 { return 150 })

	// The backend data is still present (native TTL has not swept it); the read
	// path must still report the key as absent because meta.exp says it is expired.
	readData := func(context.Context) (string, error) { return "stale-but-present", nil }

	val, found, err := ReadPath(context.Background(), reader, "0:exp", readData)
	if err != nil {
		t.Fatalf("ReadPath returned error: %v", err)
	}
	if found || val != "" {
		t.Fatalf("ReadPath = (%q, %v), want (\"\", false) for an expired key", val, found)
	}

	// 12.2 + 12.3: the expired key's pk was enqueued and the deleter reclaimed it.
	if got := lazyAwaitMember(t, store.memberCalls); got != "0:exp" {
		t.Fatalf("deleter reclaimed pk %q, want 0:exp", got)
	}

	deleter.Stop()

	if got := store.memberCount("0:exp"); got != 0 {
		t.Fatalf("member count after expiry reclaim = %d, want 0", got)
	}
	if got := deleter.Deleted(); got != 4 {
		t.Fatalf("Deleted() = %d, want 4 (expired key members reclaimed)", got)
	}
}

// TestLazyDelete_SweeperReclaimsOrphans exercises the backstop: members whose
// owning key has no meta item (left behind when the deleter dropped a full-queue
// pk or failed a reclaim) are cleaned up by the weekly sweeper's SweepOrphans scan.
// A live key's members are untouched.
//
// Requirement: 12.4
func TestLazyDelete_SweeperReclaimsOrphans(t *testing.T) {
	store := newLazyStore()
	store.sweepCalls = make(chan struct{}, 1)

	// Two orphan keys (members but no meta) plus one live key (meta + members).
	store.seedOrphanMembers("0:orphan-a", 2)
	store.seedOrphanMembers("0:orphan-b", 5)
	store.seed("0:live", TypeList, 0, 3)

	// SweepOnStart runs one sweep immediately so the test observes it without
	// waiting for a real weekly tick.
	sweeper := NewSweeper(store, SweeperConfig{Interval: time.Hour, SweepOnStart: true})
	sweeper.Start(context.Background())
	defer sweeper.Stop()

	select {
	case <-store.sweepCalls:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the sweeper to run SweepOrphans")
	}

	sweeper.Stop() // guarantees the worker finished recording metrics.

	// Orphans reclaimed; live key's members untouched.
	if got := store.memberCount("0:orphan-a"); got != 0 {
		t.Fatalf("orphan-a members after sweep = %d, want 0", got)
	}
	if got := store.memberCount("0:orphan-b"); got != 0 {
		t.Fatalf("orphan-b members after sweep = %d, want 0", got)
	}
	if got := store.memberCount("0:live"); got != 3 {
		t.Fatalf("live key members after sweep = %d, want 3 (must not be swept)", got)
	}

	if got := sweeper.Runs(); got != 1 {
		t.Fatalf("Runs() = %d, want 1", got)
	}
	if got := sweeper.Reclaimed(); got != 7 {
		t.Fatalf("Reclaimed() = %d, want 7 (2 + 5 orphan members)", got)
	}
	if got := sweeper.Failures(); got != 0 {
		t.Fatalf("Failures() = %d, want 0", got)
	}
}

// TestLazyDelete_FullPipeline ties everything into one flow over a single store:
// DeleteMeta reclaims a key's members via the deleter, while a separate orphan set
// (modelling a dropped/failed reclaim) is later cleaned up by the sweeper — showing
// the deleter as the primary path and the sweeper as the backstop.
//
// Requirements: 12.1, 12.2, 12.3, 12.4
func TestLazyDelete_FullPipeline(t *testing.T) {
	store := newLazyStore()
	store.memberCalls = make(chan string, 1)
	store.sweepCalls = make(chan struct{}, 1)

	store.seed("0:key", TypeZSet, 0, 6)
	store.seedOrphanMembers("0:leaked", 4) // dropped/failed earlier: no meta, members remain

	deleter := NewDeleter(store, DeleterConfig{})
	deleter.Start(context.Background())
	defer deleter.Stop()

	ms := NewMetaStore(store, deleter)
	ctx := context.Background()

	// Primary path: DEL → meta gone (logically absent) → enqueue → deleter reclaims.
	existed, err := ms.DeleteMeta(ctx, "0:key")
	if err != nil || !existed {
		t.Fatalf("DeleteMeta = (%v, %v), want (true, nil)", existed, err)
	}
	if _, found, _ := ms.Load(ctx, "0:key"); found {
		t.Fatalf("0:key still logically present after DeleteMeta, want absent")
	}
	if got := lazyAwaitMember(t, store.memberCalls); got != "0:key" {
		t.Fatalf("deleter reclaimed pk %q, want 0:key", got)
	}
	deleter.Stop()

	if got := store.memberCount("0:key"); got != 0 {
		t.Fatalf("0:key members after deleter = %d, want 0", got)
	}
	// The leaked orphan is NOT reclaimed by the deleter (it was never enqueued).
	if got := store.memberCount("0:leaked"); got != 4 {
		t.Fatalf("0:leaked members before sweep = %d, want 4 (deleter must not touch it)", got)
	}

	// Backstop path: the sweeper reclaims the leaked orphan.
	sweeper := NewSweeper(store, SweeperConfig{Interval: time.Hour, SweepOnStart: true})
	sweeper.Start(ctx)
	defer sweeper.Stop()

	select {
	case <-store.sweepCalls:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the sweeper to run")
	}
	sweeper.Stop()

	if got := store.memberCount("0:leaked"); got != 0 {
		t.Fatalf("0:leaked members after sweep = %d, want 0 (sweeper backstop)", got)
	}
	if got := sweeper.Reclaimed(); got != 4 {
		t.Fatalf("Reclaimed() = %d, want 4", got)
	}
}
