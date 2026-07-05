package meta

// Property 2: 过期正确性 (expiry correctness).
//
// **Validates: Requirements 11.4, 11.5**
//
// For any key, if exp > 0 AND exp <= now, then every read command behaves as if
// the key does not exist, INDEPENDENT of whether DynamoDB's native TTL has yet
// physically removed the underlying data item. Conversely, when exp == 0 (never
// expires) or exp > now (expires in the future), a present key is returned as live
// with its data.
//
// The property test drives ReadPath over randomized (exp, now) pairs and a
// randomized "data still present in the backend" flag (which simulates whether the
// native TTL sweep has run yet). It asserts:
//   - expired  ⇒ found == false and the value is the zero value, REGARDLESS of the
//     backend-data-present flag, and the pk is enqueued for lazy deletion exactly
//     once.
//   - not expired ⇒ found == true, the backend data is surfaced unchanged, and the
//     pk is never enqueued.
//
// Helpers are prefixed prop2 to avoid colliding with fakeStore/spyEnqueuer/
// fixedClock already defined in meta_test.go and read_test.go.

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/aura-studio/redimos/v2/internal/storage"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// prop2Store is a minimal storage.Store double whose meta item always exists with
// a scripted exp. It lets the property test pin a key's expiry while the read path
// evaluates it against an injected clock. The non-meta primitives are inert: the
// property exercises only the read path.
type prop2Store struct {
	exp int64
}

var _ storage.Store = prop2Store{}

func (prop2Store) EnsureType(context.Context, string, string, int64) (int64, error) {
	return 0, nil
}
func (prop2Store) DeleteMetaIfEmpty(context.Context, string) (bool, error) { return true, nil }

func (s prop2Store) CreateTypeIfAbsent(_ context.Context, pk, expected string, cntDelta, nowEpoch int64) (bool, error) {
	// The key's meta always exists; it is claimable only when already expired.
	return s.exp > 0 && s.exp <= nowEpoch, nil
}

func (s prop2Store) LoadMeta(context.Context, string) (storage.Meta, bool, error) {
	// The key's meta always exists; only exp varies. Type/Count are fixed since the
	// property is about expiry, not type or counting.
	return storage.Meta{Type: "str", Exp: s.exp, Count: 1}, true, nil
}

func (prop2Store) SetExpire(context.Context, string, int64) (bool, error) { return true, nil }
func (prop2Store) Persist(context.Context, string) (bool, error)          { return true, nil }
func (prop2Store) DeleteMeta(context.Context, string) (bool, error)       { return true, nil }
func (prop2Store) DeleteMembers(context.Context, string) (int, error)     { return 0, nil }
func (prop2Store) SweepOrphans(context.Context) (int, error)              { return 0, nil }

func (prop2Store) GetString(context.Context, string) ([]byte, bool, error) { return nil, false, nil }
func (prop2Store) SetString(context.Context, string, []byte) error         { return nil }
func (prop2Store) SetStringIfEquals(context.Context, string, []byte, []byte, bool) (bool, error) {
	return true, nil
}
func (prop2Store) MGetStrings(context.Context, []string) (map[string][]byte, error) {
	return nil, nil
}
func (prop2Store) GetSetString(context.Context, string, []byte) ([]byte, bool, error) {
	return nil, false, nil
}
func (prop2Store) IncrBy(context.Context, string, int64) (int64, error)         { return 0, nil }
func (prop2Store) IncrByFloat(context.Context, string, float64) ([]byte, error) { return nil, nil }

func (prop2Store) HSet(context.Context, string, []storage.HField) (int, error) { return 0, nil }
func (prop2Store) HSetNX(context.Context, string, string, []byte) (bool, error) {
	return false, nil
}
func (prop2Store) HGet(context.Context, string, string) ([]byte, bool, error) {
	return nil, false, nil
}
func (prop2Store) HMGet(context.Context, string, []string) (map[string][]byte, error) {
	return nil, nil
}
func (prop2Store) HGetAll(context.Context, string) ([]storage.HField, error) { return nil, nil }
func (prop2Store) HKeys(context.Context, string) ([]string, error)           { return nil, nil }
func (prop2Store) HVals(context.Context, string) ([][]byte, error)           { return nil, nil }
func (prop2Store) HDel(context.Context, string, []string) (int, error)       { return 0, nil }
func (prop2Store) HExists(context.Context, string, string) (bool, error)     { return false, nil }
func (prop2Store) HStrlen(context.Context, string, string) (int, error)      { return 0, nil }
func (prop2Store) HIncrBy(context.Context, string, string, int64) (int64, bool, error) {
	return 0, false, nil
}
func (prop2Store) HIncrByFloat(context.Context, string, string, float64) ([]byte, bool, error) {
	return nil, false, nil
}

func (prop2Store) SAdd(context.Context, string, []string) (int, error) { return 0, nil }
func (prop2Store) SRem(context.Context, string, []string) (int, error) { return 0, nil }
func (prop2Store) SIsMember(context.Context, string, string) (bool, error) {
	return false, nil
}
func (prop2Store) SMembers(context.Context, string) ([]string, error)  { return nil, nil }
func (prop2Store) SPop(context.Context, string, int) ([]string, error) { return nil, nil }
func (prop2Store) SRandMember(context.Context, string, int) ([]string, error) {
	return nil, nil
}

func (prop2Store) ZAdd(context.Context, string, []storage.ZMember) (int, error) { return 0, nil }
func (prop2Store) ZRem(context.Context, string, []string) (int, error)          { return 0, nil }
func (prop2Store) ZScore(context.Context, string, string) (float64, bool, error) {
	return 0, false, nil
}
func (prop2Store) ZIncrBy(context.Context, string, string, float64) (float64, bool, error) {
	return 0, false, nil
}
func (prop2Store) ZRangeByRank(context.Context, string, int, int, bool) ([]storage.ZMember, error) {
	return nil, nil
}
func (prop2Store) ZRangeByScore(context.Context, string, storage.ScoreBound, storage.ScoreBound, bool) ([]storage.ZMember, error) {
	return nil, nil
}
func (prop2Store) ZCount(context.Context, string, storage.ScoreBound, storage.ScoreBound) (int, error) {
	return 0, nil
}
func (prop2Store) ZRank(context.Context, string, string, bool) (int, bool, error) {
	return 0, false, nil
}
func (prop2Store) ZRemRangeByRank(context.Context, string, int, int) (int, error) {
	return 0, nil
}
func (prop2Store) ZRemRangeByScore(context.Context, string, storage.ScoreBound, storage.ScoreBound) (int, error) {
	return 0, nil
}

func (prop2Store) LPush(context.Context, string, [][]byte) (int, error) { return 0, nil }
func (prop2Store) RPush(context.Context, string, [][]byte) (int, error) { return 0, nil }
func (prop2Store) LPop(context.Context, string) ([]byte, bool, error)   { return nil, false, nil }
func (prop2Store) RPop(context.Context, string) ([]byte, bool, error)   { return nil, false, nil }
func (prop2Store) LRange(context.Context, string, int, int) ([][]byte, error) {
	return nil, nil
}
func (prop2Store) LIndex(context.Context, string, int) ([]byte, bool, error) {
	return nil, false, nil
}
func (prop2Store) LRangeAll(context.Context, string) ([][]byte, error) { return nil, nil }
func (prop2Store) LReplaceAll(context.Context, string, [][]byte) (int, error) {
	return 0, nil
}
func (prop2Store) ScanKeys(context.Context, map[string]types.AttributeValue, int32, int64) ([]string, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

func (prop2Store) SScan(context.Context, string, map[string]types.AttributeValue, int32) ([]string, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

func (prop2Store) HScan(context.Context, string, map[string]types.AttributeValue, int32) ([]storage.HField, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

func (prop2Store) ZScan(context.Context, string, map[string]types.AttributeValue, int32) ([]storage.ZMember, map[string]types.AttributeValue, error) {
	return nil, nil, nil
}

// prop2Enqueuer is a spy DeletionEnqueuer that records every pk handed to it so the

// property can assert an expired key is enqueued exactly once and a live key never.
type prop2Enqueuer struct {
	pks []string
}

func (e *prop2Enqueuer) Enqueue(pk string) { e.pks = append(e.pks, pk) }

// prop2Clock returns a clock pinned to now (epoch seconds) for deterministic expiry
// evaluation.
func prop2Clock(now int64) func() int64 { return func() int64 { return now } }

// prop2Input is the generated input space for the property: an (exp, now) pair plus
// a flag simulating whether the backend data item is still present (i.e. whether
// the native TTL sweep has run). Its Generate method constrains generation to the
// interesting regions of the space rather than uniformly random int64s.
type prop2Input struct {
	exp         int64
	now         int64
	dataPresent bool // true: native TTL has NOT yet cleaned the data item.
}

// Generate produces (exp, now, dataPresent) triples biased toward the boundaries
// that matter for IsExpired (exp <= now): never-expires (exp=0), future expiry,
// the exact exp==now boundary, past expiry, and negative exp (guarded out by the
// exp>0 clause).
func (prop2Input) Generate(rng *rand.Rand, _ int) reflect.Value {
	now := rng.Int63n(1 << 32) // a plausible epoch-seconds value.

	var exp int64
	switch rng.Intn(5) {
	case 0:
		exp = 0 // never expires.
	case 1:
		exp = now + 1 + rng.Int63n(1<<20) // strictly future → live.
	case 2:
		exp = now // exact boundary → expired (exp <= now).
	case 3:
		exp = rng.Int63n(now + 1) // in [0, now] → expired unless it lands on 0.
	case 4:
		exp = -(rng.Int63n(1<<20) + 1) // negative → not expired (exp>0 guard).
	}

	return reflect.ValueOf(prop2Input{
		exp:         exp,
		now:         now,
		dataPresent: rng.Intn(2) == 0,
	})
}

func TestProperty2_ExpiryCorrectness(t *testing.T) {
	const pk = "0:prop2-key"

	property := func(in prop2Input) bool {
		enq := &prop2Enqueuer{}
		r := NewReader(NewMetaStore(prop2Store{exp: in.exp}, enq), prop2Clock(in.now))

		// dataVal models the backend read. When dataPresent is true the native TTL
		// sweep has NOT run, so the data item is still readable; when false it has
		// been cleaned and the read yields the zero value. ReadPath's correctness
		// must not depend on which of these holds for an expired key.
		var dataVal string
		if in.dataPresent {
			dataVal = fmt.Sprintf("backend-data-%d", in.exp)
		}
		readData := func(context.Context) (string, error) { return dataVal, nil }

		val, found, err := ReadPath(context.Background(), r, pk, readData)
		if err != nil {
			return false
		}

		expired := in.exp > 0 && in.exp <= in.now

		if expired {
			// Expired: key appears absent regardless of dataPresent, and is
			// enqueued for lazy deletion exactly once.
			if found {
				return false
			}
			if val != "" {
				return false
			}
			if len(enq.pks) != 1 || enq.pks[0] != pk {
				return false
			}
			return true
		}

		// Live (exp==0 or exp>now): the present key returns found=true with its
		// data, and is never enqueued for deletion.
		if !found {
			return false
		}
		if val != dataVal {
			return false
		}
		if len(enq.pks) != 0 {
			return false
		}
		return true
	}

	if err := quick.Check(property, &quick.Config{MaxCount: 1000}); err != nil {
		t.Fatalf("Property 2 (expiry correctness) failed: %v", err)
	}
}

// TestProperty2_ExpiredIndependentOfNativeTTL pins the core of Property 2 with a
// deterministic example: an expired key whose backend data item is STILL present
// (native TTL has not yet swept it) must still read as absent. This guards the
// property's most important claim explicitly, independent of the randomized run.
func TestProperty2_ExpiredIndependentOfNativeTTL(t *testing.T) {
	const pk = "0:prop2-key"

	for _, dataPresent := range []bool{true, false} {
		enq := &prop2Enqueuer{}
		// exp=100, now=100 → exp>0 && exp<=now → expired.
		r := NewReader(NewMetaStore(prop2Store{exp: 100}, enq), prop2Clock(100))

		readData := func(context.Context) (string, error) {
			if dataPresent {
				return "stale-but-still-in-backend", nil
			}
			return "", nil
		}

		val, found, err := ReadPath(context.Background(), r, pk, readData)
		if err != nil {
			t.Fatalf("dataPresent=%v: ReadPath error: %v", dataPresent, err)
		}
		if found || val != "" {
			t.Fatalf("dataPresent=%v: ReadPath = (%q, %v), want (\"\", false) for an expired key",
				dataPresent, val, found)
		}
		if len(enq.pks) != 1 || enq.pks[0] != pk {
			t.Fatalf("dataPresent=%v: enqueued = %v, want exactly [%s]", dataPresent, enq.pks, pk)
		}
	}
}
