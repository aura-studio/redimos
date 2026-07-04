package meta

import (
	"context"
	"errors"
	"testing"

	"github.com/aura-studio/redimos/v2/internal/storage"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// fakeStore is an in-memory storage.Store double for testing the meta layer
// without a live DynamoDB. Each field lets a test script the primitive's result.
type fakeStore struct {
	ensureErr error

	loadMeta  storage.Meta
	loadFound bool
	loadErr   error

	setExpireFound bool
	setExpireErr   error

	persistFound bool
	persistErr   error

	deleteExisted bool
	deleteErr     error

	deleteMembersCount int
	deleteMembersErr   error

	sweepReclaimed int
	sweepErr       error

	// captured call arguments for assertions.
	ensureCalls []ensureCall
	deleteCalls []string
}

type ensureCall struct {
	pk       string
	expected string
	cntDelta int64
}

func (f *fakeStore) EnsureType(_ context.Context, pk, expected string, cntDelta int64) error {
	f.ensureCalls = append(f.ensureCalls, ensureCall{pk: pk, expected: expected, cntDelta: cntDelta})
	return f.ensureErr
}

func (f *fakeStore) CreateTypeIfAbsent(_ context.Context, pk, expected string, cntDelta, nowEpoch int64) (bool, error) {
	return true, f.ensureErr
}

func (f *fakeStore) LoadMeta(_ context.Context, _ string) (storage.Meta, bool, error) {
	return f.loadMeta, f.loadFound, f.loadErr
}

func (f *fakeStore) SetExpire(_ context.Context, _ string, _ int64) (bool, error) {
	return f.setExpireFound, f.setExpireErr
}

func (f *fakeStore) Persist(_ context.Context, _ string) (bool, error) {
	return f.persistFound, f.persistErr
}

func (f *fakeStore) DeleteMeta(_ context.Context, pk string) (bool, error) {
	f.deleteCalls = append(f.deleteCalls, pk)
	return f.deleteExisted, f.deleteErr
}

func (f *fakeStore) DeleteMembers(_ context.Context, _ string) (int, error) {
	return f.deleteMembersCount, f.deleteMembersErr
}

func (f *fakeStore) SweepOrphans(_ context.Context) (int, error) {
	return f.sweepReclaimed, f.sweepErr
}

func (f *fakeStore) GetString(_ context.Context, _ string) ([]byte, bool, error) {
	return nil, false, nil
}

func (f *fakeStore) MGetStrings(_ context.Context, _ []string) (map[string][]byte, error) {
	return nil, nil
}

func (f *fakeStore) SetString(_ context.Context, _ string, _ []byte) error { return nil }

func (f *fakeStore) SetStringIfEquals(_ context.Context, _ string, _, _ []byte, _ bool) (bool, error) {
	return true, nil
}

func (f *fakeStore) GetSetString(_ context.Context, _ string, _ []byte) ([]byte, bool, error) {
	return nil, false, nil
}

func (f *fakeStore) IncrBy(_ context.Context, _ string, _ int64) (int64, error) { return 0, nil }

func (f *fakeStore) IncrByFloat(_ context.Context, _ string, _ float64) ([]byte, error) {
	return nil, nil
}

func (f *fakeStore) HSet(_ context.Context, _ string, _ []storage.HField) (int, error) { return 0, nil }
func (f *fakeStore) HSetNX(_ context.Context, _, _ string, _ []byte) (bool, error)     { return false, nil }
func (f *fakeStore) HGet(_ context.Context, _, _ string) ([]byte, bool, error) {
	return nil, false, nil
}
func (f *fakeStore) HMGet(_ context.Context, _ string, _ []string) (map[string][]byte, error) {
	return nil, nil
}
func (f *fakeStore) HGetAll(_ context.Context, _ string) ([]storage.HField, error) { return nil, nil }
func (f *fakeStore) HKeys(_ context.Context, _ string) ([]string, error)           { return nil, nil }
func (f *fakeStore) HVals(_ context.Context, _ string) ([][]byte, error)           { return nil, nil }
func (f *fakeStore) HDel(_ context.Context, _ string, _ []string) (int, error)     { return 0, nil }
func (f *fakeStore) HExists(_ context.Context, _, _ string) (bool, error)          { return false, nil }
func (f *fakeStore) HStrlen(_ context.Context, _, _ string) (int, error)           { return 0, nil }
func (f *fakeStore) HIncrBy(_ context.Context, _, _ string, _ int64) (int64, bool, error) {
	return 0, false, nil
}
func (f *fakeStore) HIncrByFloat(_ context.Context, _, _ string, _ float64) ([]byte, bool, error) {
	return nil, false, nil
}

func (f *fakeStore) SAdd(_ context.Context, _ string, _ []string) (int, error) { return 0, nil }
func (f *fakeStore) SRem(_ context.Context, _ string, _ []string) (int, error) { return 0, nil }
func (f *fakeStore) SIsMember(_ context.Context, _, _ string) (bool, error)    { return false, nil }
func (f *fakeStore) SMembers(_ context.Context, _ string) ([]string, error)    { return nil, nil }
func (f *fakeStore) SPop(_ context.Context, _ string, _ int) ([]string, error) { return nil, nil }
func (f *fakeStore) SRandMember(_ context.Context, _ string, _ int) ([]string, error) {
	return nil, nil
}

func (f *fakeStore) ZAdd(_ context.Context, _ string, _ []storage.ZMember) (int, error) {
	return 0, nil
}
func (f *fakeStore) ZRem(_ context.Context, _ string, _ []string) (int, error) { return 0, nil }
func (f *fakeStore) ZScore(_ context.Context, _, _ string) (float64, bool, error) {
	return 0, false, nil
}
func (f *fakeStore) ZIncrBy(_ context.Context, _, _ string, _ float64) (float64, bool, error) {
	return 0, false, nil
}
func (f *fakeStore) ZRangeByRank(_ context.Context, _ string, _, _ int, _ bool) ([]storage.ZMember, error) {
	return nil, nil
}
func (f *fakeStore) ZRangeByScore(_ context.Context, _ string, _, _ storage.ScoreBound, _ bool) ([]storage.ZMember, error) {
	return nil, nil
}
func (f *fakeStore) ZCount(_ context.Context, _ string, _, _ storage.ScoreBound) (int, error) {
	return 0, nil
}
func (f *fakeStore) ZRank(_ context.Context, _, _ string, _ bool) (int, bool, error) {
	return 0, false, nil
}
func (f *fakeStore) ZRemRangeByRank(_ context.Context, _ string, _, _ int) (int, error) {
	return 0, nil
}
func (f *fakeStore) ZRemRangeByScore(_ context.Context, _ string, _, _ storage.ScoreBound) (int, error) {
	return 0, nil
}

func (f *fakeStore) LPush(_ context.Context, _ string, _ [][]byte) (int, error) { return 0, nil }
func (f *fakeStore) RPush(_ context.Context, _ string, _ [][]byte) (int, error) { return 0, nil }
func (f *fakeStore) LPop(_ context.Context, _ string) ([]byte, bool, error)     { return nil, false, nil }
func (f *fakeStore) RPop(_ context.Context, _ string) ([]byte, bool, error)     { return nil, false, nil }
func (f *fakeStore) LRange(_ context.Context, _ string, _, _ int) ([][]byte, error) {
	return nil, nil
}
func (f *fakeStore) LIndex(_ context.Context, _ string, _ int) ([]byte, bool, error) {
	return nil, false, nil
}
func (f *fakeStore) LRangeAll(_ context.Context, _ string) ([][]byte, error) { return nil, nil }
func (f *fakeStore) LReplaceAll(_ context.Context, _ string, _ [][]byte) (int, error) {
	return 0, nil
}
func (f *fakeStore) ScanKeys(_ context.Context, _ map[string]types.AttributeValue, _ int32, _ int64) ([]string, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

func (f *fakeStore) SScan(_ context.Context, _ string, _ map[string]types.AttributeValue, _ int32) ([]string, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

func (f *fakeStore) HScan(_ context.Context, _ string, _ map[string]types.AttributeValue, _ int32) ([]storage.HField, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

func (f *fakeStore) ZScan(_ context.Context, _ string, _ map[string]types.AttributeValue, _ int32) ([]storage.ZMember, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

var _ storage.Store = (*fakeStore)(nil)

func TestIsExpired(t *testing.T) {
	tests := []struct {
		name string
		meta Meta
		now  int64
		want bool
	}{
		{name: "no expiry set (exp=0) never expires", meta: Meta{Exp: 0}, now: 100, want: false},
		{name: "exp in the future is not expired", meta: Meta{Exp: 200}, now: 100, want: false},
		{name: "exp exactly now is expired", meta: Meta{Exp: 100}, now: 100, want: true},
		{name: "exp in the past is expired", meta: Meta{Exp: 50}, now: 100, want: true},
		{name: "negative exp is not expired (exp>0 guard)", meta: Meta{Exp: -1}, now: 100, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsExpired(tt.meta, tt.now); got != tt.want {
				t.Fatalf("IsExpired(%+v, %d) = %v, want %v", tt.meta, tt.now, got, tt.want)
			}
		})
	}
}

func TestEnsureType_MapsWrongType(t *testing.T) {
	store := &fakeStore{ensureErr: storage.ErrWrongType}
	ms := NewMetaStore(store, nil)

	err := ms.EnsureType(context.Background(), "0:k", TypeHash, 1)
	if !errors.Is(err, ErrWrongType) {
		t.Fatalf("EnsureType error = %v, want meta.ErrWrongType", err)
	}

	// The expected type must be forwarded to the store as its raw string.
	if len(store.ensureCalls) != 1 {
		t.Fatalf("EnsureType made %d store calls, want 1", len(store.ensureCalls))
	}
	if got := store.ensureCalls[0]; got.expected != "hash" || got.cntDelta != 1 || got.pk != "0:k" {
		t.Fatalf("forwarded call = %+v, want {pk:0:k expected:hash cntDelta:1}", got)
	}
}

func TestEnsureType_PassesThroughOtherErrors(t *testing.T) {
	sentinel := errors.New("boom")
	ms := NewMetaStore(&fakeStore{ensureErr: sentinel}, nil)

	err := ms.EnsureType(context.Background(), "0:k", TypeString, 0)
	if !errors.Is(err, sentinel) {
		t.Fatalf("EnsureType error = %v, want the underlying store error", err)
	}
	if errors.Is(err, ErrWrongType) {
		t.Fatalf("non-conflict error was misclassified as ErrWrongType")
	}
}

func TestEnsureType_Success(t *testing.T) {
	ms := NewMetaStore(&fakeStore{}, nil)
	if err := ms.EnsureType(context.Background(), "0:k", TypeSet, -2); err != nil {
		t.Fatalf("EnsureType returned unexpected error: %v", err)
	}
}

func TestLoad_MapsMetaAndAbsence(t *testing.T) {
	// present key: storage.Meta is mapped to the typed meta.Meta.
	present := &fakeStore{
		loadMeta:  storage.Meta{Type: "zset", Exp: 42, Count: 7},
		loadFound: true,
	}
	ms := NewMetaStore(present, nil)

	got, found, err := ms.Load(context.Background(), "0:k")
	if err != nil || !found {
		t.Fatalf("Load(present) = (_, %v, %v), want (_, true, nil)", found, err)
	}
	want := Meta{Type: TypeZSet, Exp: 42, Count: 7}
	if got != want {
		t.Fatalf("Load(present) meta = %+v, want %+v", got, want)
	}

	// absent key: zero meta, found=false.
	ms = NewMetaStore(&fakeStore{loadFound: false}, nil)
	got, found, err = ms.Load(context.Background(), "0:missing")
	if err != nil || found {
		t.Fatalf("Load(absent) = (_, %v, %v), want (_, false, nil)", found, err)
	}
	if got != (Meta{}) {
		t.Fatalf("Load(absent) meta = %+v, want zero Meta", got)
	}
}

func TestLoad_PropagatesError(t *testing.T) {
	sentinel := errors.New("read failed")
	ms := NewMetaStore(&fakeStore{loadErr: sentinel, loadFound: true}, nil)

	_, found, err := ms.Load(context.Background(), "0:k")
	if !errors.Is(err, sentinel) {
		t.Fatalf("Load error = %v, want the underlying store error", err)
	}
	if found {
		t.Fatalf("Load found = true on error, want false")
	}
}

func TestDeleteMeta_EnqueuesWhenMetaExisted(t *testing.T) {
	store := &fakeStore{deleteExisted: true}

	var enqueued []string
	ms := NewMetaStore(store, EnqueueFunc(func(pk string) {
		enqueued = append(enqueued, pk)
	}))

	existed, err := ms.DeleteMeta(context.Background(), "0:k")
	if err != nil || !existed {
		t.Fatalf("DeleteMeta = (%v, %v), want (true, nil)", existed, err)
	}
	if len(enqueued) != 1 || enqueued[0] != "0:k" {
		t.Fatalf("enqueued = %v, want [0:k]", enqueued)
	}
	if len(store.deleteCalls) != 1 || store.deleteCalls[0] != "0:k" {
		t.Fatalf("store DeleteMeta calls = %v, want [0:k]", store.deleteCalls)
	}
}

func TestDeleteMeta_DoesNotEnqueueWhenMetaAbsent(t *testing.T) {
	store := &fakeStore{deleteExisted: false}

	var enqueued []string
	ms := NewMetaStore(store, EnqueueFunc(func(pk string) {
		enqueued = append(enqueued, pk)
	}))

	existed, err := ms.DeleteMeta(context.Background(), "0:missing")
	if err != nil || existed {
		t.Fatalf("DeleteMeta = (%v, %v), want (false, nil)", existed, err)
	}
	if len(enqueued) != 0 {
		t.Fatalf("enqueued = %v, want empty (no meta was removed)", enqueued)
	}
}

func TestDeleteMeta_DoesNotEnqueueOnError(t *testing.T) {
	sentinel := errors.New("delete failed")
	// existed=true but an error occurred: the enqueue must be skipped.
	store := &fakeStore{deleteExisted: true, deleteErr: sentinel}

	var enqueued []string
	ms := NewMetaStore(store, EnqueueFunc(func(pk string) {
		enqueued = append(enqueued, pk)
	}))

	_, err := ms.DeleteMeta(context.Background(), "0:k")
	if !errors.Is(err, sentinel) {
		t.Fatalf("DeleteMeta error = %v, want the underlying store error", err)
	}
	if len(enqueued) != 0 {
		t.Fatalf("enqueued = %v, want empty on error", enqueued)
	}
}

func TestNewMetaStore_NilEnqueueIsSafe(t *testing.T) {
	// A nil enqueue must be replaced with a no-op so DeleteMeta does not panic.
	ms := NewMetaStore(&fakeStore{deleteExisted: true}, nil)

	existed, err := ms.DeleteMeta(context.Background(), "0:k")
	if err != nil || !existed {
		t.Fatalf("DeleteMeta with nil enqueue = (%v, %v), want (true, nil)", existed, err)
	}
}

func TestSetExpireAndPersist_PassThrough(t *testing.T) {
	ms := NewMetaStore(&fakeStore{setExpireFound: true, persistFound: false}, nil)

	if found, err := ms.SetExpire(context.Background(), "0:k", 123); err != nil || !found {
		t.Fatalf("SetExpire = (%v, %v), want (true, nil)", found, err)
	}
	if found, err := ms.Persist(context.Background(), "0:k"); err != nil || found {
		t.Fatalf("Persist = (%v, %v), want (false, nil)", found, err)
	}
}
