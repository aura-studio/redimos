package meta

// Property-based test for Property 1 (类型一致性 / type consistency) from the
// design document.
//
// Property 1: 对任意写命令，若 key 已存在且类型 ≠ 命令期望类型，则返回
// -WRONGTYPE 且不修改任何 item（由 meta 条件写保证）。
// (For any write command, if the key already exists and its type differs from
// the command's expected type, return -WRONGTYPE and modify no item — guaranteed
// by the meta conditional write.)
//
// Every write command funnels through MetaStore.EnsureType, whose single
// conditional UpdateItem (attribute_not_exists(t) OR t = :expected) atomically
// creates the key, verifies its type and applies the count delta. This test
// pins the meta conditional-write contract that guarantees Property 1 by driving
// randomized sequences of EnsureType calls (varying key types over a small key
// pool) against a stateful in-memory storage.Store that enforces the
// conditional-write semantics, and asserting:
//
//	(a) any write whose expected type differs from the key's established type
//	    returns meta.ErrWrongType;
//	(b) on such a conflict neither the recorded type nor the count changes
//	    (no item is mutated);
//	(c) matching-type writes succeed and cnt reflects the accumulated deltas,
//	    while the first write to an absent key establishes its type and count.
//
// **Validates: Requirements 3.6, 11.1, 11.2**

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/aura-studio/redimos/v2/internal/storage"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// prop1Entry is the in-memory state of a single key's meta item in the fake
// store: its recorded type and accumulated count. exists is false until the
// first successful EnsureType establishes the key.
type prop1Entry struct {
	exists bool
	typ    string
	cnt    int64
}

// prop1Store is a stateful in-memory storage.Store double that models the meta
// conditional-write semantics that guarantee Property 1:
//
//   - the first EnsureType on an absent key establishes its type and sets cnt to
//     the delta;
//   - a later EnsureType with a different type returns storage.ErrWrongType and
//     mutates nothing (the conditional write fails, so no item changes);
//   - a same-type EnsureType accumulates the delta into cnt.
//
// It is named with the prop1 prefix to avoid colliding with fakeStore
// (meta_test.go) and the spy helpers (read_test.go) in the same package.
type prop1Store struct {
	items map[string]*prop1Entry
}

func newProp1Store() *prop1Store {
	return &prop1Store{items: make(map[string]*prop1Entry)}
}

// EnsureType enforces the conditional-write contract. On a type conflict it
// returns storage.ErrWrongType before touching any state, so the key's type and
// count are left exactly as they were.
func (s *prop1Store) EnsureType(_ context.Context, pk, expected string, cntDelta int64) error {
	e := s.items[pk]
	if e == nil || !e.exists {
		s.items[pk] = &prop1Entry{exists: true, typ: expected, cnt: cntDelta}
		return nil
	}

	if e.typ != expected {
		// Conditional write fails: no item is modified.
		return storage.ErrWrongType
	}

	// Same type: the ADD accumulates the delta atomically.
	e.cnt += cntDelta

	return nil
}

func (s *prop1Store) LoadMeta(_ context.Context, pk string) (storage.Meta, bool, error) {
	e := s.items[pk]
	if e == nil || !e.exists {
		return storage.Meta{}, false, nil
	}

	return storage.Meta{Type: e.typ, Count: e.cnt}, true, nil
}

func (s *prop1Store) SetExpire(_ context.Context, _ string, _ int64) (bool, error) {
	return false, nil
}

func (s *prop1Store) Persist(_ context.Context, _ string) (bool, error) {
	return false, nil
}

func (s *prop1Store) DeleteMeta(_ context.Context, pk string) (bool, error) {
	e := s.items[pk]
	if e == nil || !e.exists {
		return false, nil
	}

	delete(s.items, pk)

	return true, nil
}

func (s *prop1Store) DeleteMembers(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (s *prop1Store) SweepOrphans(_ context.Context) (int, error) {
	return 0, nil
}

func (s *prop1Store) GetString(_ context.Context, _ string) ([]byte, bool, error) {
	return nil, false, nil
}

func (s *prop1Store) MGetStrings(_ context.Context, _ []string) (map[string][]byte, error) {
	return nil, nil
}

func (s *prop1Store) SetString(_ context.Context, _ string, _ []byte) error { return nil }

func (s *prop1Store) SetStringIfEquals(_ context.Context, _ string, _, _ []byte, _ bool) (bool, error) {
	return true, nil
}

func (s *prop1Store) GetSetString(_ context.Context, _ string, _ []byte) ([]byte, bool, error) {
	return nil, false, nil
}

func (s *prop1Store) IncrBy(_ context.Context, _ string, _ int64) (int64, error) { return 0, nil }

func (s *prop1Store) IncrByFloat(_ context.Context, _ string, _ float64) ([]byte, error) {
	return nil, nil
}

func (s *prop1Store) HSet(_ context.Context, _ string, _ []storage.HField) (int, error) {
	return 0, nil
}
func (s *prop1Store) HSetNX(_ context.Context, _, _ string, _ []byte) (bool, error) {
	return false, nil
}
func (s *prop1Store) HGet(_ context.Context, _, _ string) ([]byte, bool, error) {
	return nil, false, nil
}
func (s *prop1Store) HMGet(_ context.Context, _ string, _ []string) (map[string][]byte, error) {
	return nil, nil
}
func (s *prop1Store) HGetAll(_ context.Context, _ string) ([]storage.HField, error) { return nil, nil }
func (s *prop1Store) HKeys(_ context.Context, _ string) ([]string, error)           { return nil, nil }
func (s *prop1Store) HVals(_ context.Context, _ string) ([][]byte, error)           { return nil, nil }
func (s *prop1Store) HDel(_ context.Context, _ string, _ []string) (int, error)     { return 0, nil }
func (s *prop1Store) HExists(_ context.Context, _, _ string) (bool, error)          { return false, nil }
func (s *prop1Store) HStrlen(_ context.Context, _, _ string) (int, error)           { return 0, nil }
func (s *prop1Store) HIncrBy(_ context.Context, _, _ string, _ int64) (int64, bool, error) {
	return 0, false, nil
}
func (s *prop1Store) HIncrByFloat(_ context.Context, _, _ string, _ float64) ([]byte, bool, error) {
	return nil, false, nil
}

func (s *prop1Store) SAdd(_ context.Context, _ string, _ []string) (int, error) { return 0, nil }
func (s *prop1Store) SRem(_ context.Context, _ string, _ []string) (int, error) { return 0, nil }
func (s *prop1Store) SIsMember(_ context.Context, _, _ string) (bool, error)    { return false, nil }
func (s *prop1Store) SMembers(_ context.Context, _ string) ([]string, error)    { return nil, nil }
func (s *prop1Store) SPop(_ context.Context, _ string, _ int) ([]string, error) { return nil, nil }
func (s *prop1Store) SRandMember(_ context.Context, _ string, _ int) ([]string, error) {
	return nil, nil
}

func (s *prop1Store) ZAdd(_ context.Context, _ string, _ []storage.ZMember) (int, error) {
	return 0, nil
}
func (s *prop1Store) ZRem(_ context.Context, _ string, _ []string) (int, error) { return 0, nil }
func (s *prop1Store) ZScore(_ context.Context, _, _ string) (float64, bool, error) {
	return 0, false, nil
}
func (s *prop1Store) ZIncrBy(_ context.Context, _, _ string, _ float64) (float64, bool, error) {
	return 0, false, nil
}
func (s *prop1Store) ZRangeByRank(_ context.Context, _ string, _, _ int, _ bool) ([]storage.ZMember, error) {
	return nil, nil
}
func (s *prop1Store) ZRangeByScore(_ context.Context, _ string, _, _ storage.ScoreBound, _ bool) ([]storage.ZMember, error) {
	return nil, nil
}
func (s *prop1Store) ZCount(_ context.Context, _ string, _, _ storage.ScoreBound) (int, error) {
	return 0, nil
}
func (s *prop1Store) ZRank(_ context.Context, _, _ string, _ bool) (int, bool, error) {
	return 0, false, nil
}
func (s *prop1Store) ZRemRangeByRank(_ context.Context, _ string, _, _ int) (int, error) {
	return 0, nil
}
func (s *prop1Store) ZRemRangeByScore(_ context.Context, _ string, _, _ storage.ScoreBound) (int, error) {
	return 0, nil
}

func (s *prop1Store) LPush(_ context.Context, _ string, _ [][]byte) (int, error) { return 0, nil }
func (s *prop1Store) RPush(_ context.Context, _ string, _ [][]byte) (int, error) { return 0, nil }
func (s *prop1Store) LPop(_ context.Context, _ string) ([]byte, bool, error)     { return nil, false, nil }
func (s *prop1Store) RPop(_ context.Context, _ string) ([]byte, bool, error)     { return nil, false, nil }
func (s *prop1Store) LRange(_ context.Context, _ string, _, _ int) ([][]byte, error) {
	return nil, nil
}
func (s *prop1Store) LIndex(_ context.Context, _ string, _ int) ([]byte, bool, error) {
	return nil, false, nil
}
func (s *prop1Store) LRangeAll(_ context.Context, _ string) ([][]byte, error) { return nil, nil }
func (s *prop1Store) LReplaceAll(_ context.Context, _ string, _ [][]byte) (int, error) {
	return 0, nil
}
func (s *prop1Store) ScanKeys(_ context.Context, _ map[string]types.AttributeValue, _ int32, _ int64) ([]string, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

func (s *prop1Store) SScan(_ context.Context, _ string, _ map[string]types.AttributeValue, _ int32) ([]string, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

func (s *prop1Store) HScan(_ context.Context, _ string, _ map[string]types.AttributeValue, _ int32) ([]storage.HField, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

func (s *prop1Store) ZScan(_ context.Context, _ string, _ map[string]types.AttributeValue, _ int32) ([]storage.ZMember, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

var _ storage.Store = (*prop1Store)(nil)

// prop1ModelEntry is the independent oracle the test compares the MetaStore's
// observed behaviour against. It mirrors the intended semantics without reusing
// the fake store's code path.
type prop1ModelEntry struct {
	exists bool
	typ    KeyType
	cnt    int64
}

// TestProperty1TypeConsistency is the property-based test for Property 1.
func TestProperty1TypeConsistency(t *testing.T) {
	// The full set of logical Redis types a write command may expect, plus a
	// small key pool so randomized sequences repeatedly revisit the same keys and
	// exercise both matching-type accumulation and cross-type conflicts.
	types := []KeyType{TypeString, TypeHash, TypeList, TypeSet, TypeZSet}
	keys := []string{"0:k0", "0:k1", "0:k2"}

	prop := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		store := newProp1Store()
		ms := NewMetaStore(store, nil)
		ctx := context.Background()

		// Independent oracle of each key's established type and accumulated count.
		model := make(map[string]*prop1ModelEntry, len(keys))

		nOps := rng.Intn(40) + 1
		for i := 0; i < nOps; i++ {
			pk := keys[rng.Intn(len(keys))]
			expected := types[rng.Intn(len(types))]
			delta := int64(rng.Intn(7) - 3) // -3..3, including zero-count writes

			// Snapshot the stored item before the write so a conflict can be
			// checked against "no item mutated".
			beforeMeta, beforeFound, _ := store.LoadMeta(ctx, pk)

			err := ms.EnsureType(ctx, pk, expected, delta)

			m := model[pk]
			switch {
			case m == nil || !m.exists:
				// First write to an absent key establishes type + count.
				if err != nil {
					t.Logf("op %d: first EnsureType(%s, %s, %d) errored: %v", i, pk, expected, delta, err)
					return false
				}
				model[pk] = &prop1ModelEntry{exists: true, typ: expected, cnt: delta}

			case m.typ != expected:
				// (a) type conflict must surface as meta.ErrWrongType.
				if !errors.Is(err, ErrWrongType) {
					t.Logf("op %d: EnsureType(%s, want %s, have %s) = %v, want ErrWrongType",
						i, pk, expected, m.typ, err)
					return false
				}
				// (b) no item mutated: the stored meta is byte-identical to the
				// pre-write snapshot.
				afterMeta, afterFound, _ := store.LoadMeta(ctx, pk)
				if afterFound != beforeFound || afterMeta != beforeMeta {
					t.Logf("op %d: conflict on %s mutated item: before=(%v,%+v) after=(%v,%+v)",
						i, pk, beforeFound, beforeMeta, afterFound, afterMeta)
					return false
				}

			default:
				// (c) matching-type write succeeds and accumulates the delta.
				if err != nil {
					t.Logf("op %d: matching EnsureType(%s, %s, %d) errored: %v", i, pk, expected, delta, err)
					return false
				}
				m.cnt += delta
			}
		}

		// Final cross-check: the store's state must equal the oracle for every
		// key — established type and accumulated count both match.
		for _, pk := range keys {
			sm, found, _ := store.LoadMeta(ctx, pk)
			me := model[pk]

			if me == nil || !me.exists {
				if found {
					t.Logf("final: %s present in store but never established in model", pk)
					return false
				}
				continue
			}

			if !found {
				t.Logf("final: %s established in model but absent from store", pk)
				return false
			}
			if KeyType(sm.Type) != me.typ {
				t.Logf("final: %s type = %s, want %s", pk, sm.Type, me.typ)
				return false
			}
			if sm.Count != me.cnt {
				t.Logf("final: %s cnt = %d, want %d", pk, sm.Count, me.cnt)
				return false
			}
		}

		return true
	}

	if err := quick.Check(prop, &quick.Config{MaxCount: 2000}); err != nil {
		t.Errorf("Property 1 (type consistency) failed: %v", err)
	}
}
