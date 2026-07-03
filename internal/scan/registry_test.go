package scan

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// lek builds a minimal LastEvaluatedKey with a single string partition key so
// tests can assert round-trips by value.
func lek(pk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: pk},
	}
}

func lekPK(m map[string]types.AttributeValue) string {
	return m["pk"].(*types.AttributeValueMemberS).Value
}

func TestSaveReturnsNonZeroDistinctCursors(t *testing.T) {
	r := New(Config{InstID: "inst-1"})

	seen := make(map[uint64]bool)
	for i := 0; i < 1000; i++ {
		c := r.Save(lek("k" + strconv.Itoa(i)))
		if c == 0 {
			t.Fatalf("Save returned zero cursor at iteration %d", i)
		}
		if seen[c] {
			t.Fatalf("Save returned duplicate cursor %d at iteration %d", c, i)
		}
		seen[c] = true
	}
}

func TestSaveNeverReturnsZeroEvenWhenRandYieldsZero(t *testing.T) {
	// A rand source that yields 0 first must be skipped so the cursor is
	// non-zero (requirement 13.1).
	vals := []uint64{0, 0, 42}
	i := 0
	r := New(Config{
		InstID: "inst-1",
		Rand: func() uint64 {
			v := vals[i]
			i++
			return v
		},
	})
	if got := r.Save(lek("k")); got != 42 {
		t.Fatalf("Save = %d, want 42 (zeros must be skipped)", got)
	}
}

func TestLoadRoundTripsLEK(t *testing.T) {
	r := New(Config{InstID: "inst-1"})
	c := r.Save(lek("user:42"))

	got, ok := r.Load(c)
	if !ok {
		t.Fatalf("Load(%d) ok = false, want true", c)
	}
	if pk := lekPK(got); pk != "user:42" {
		t.Fatalf("Load returned pk %q, want %q", pk, "user:42")
	}
}

func TestLoadUnknownCursor(t *testing.T) {
	r := New(Config{InstID: "inst-1"})
	if _, ok := r.Load(12345); ok {
		t.Fatal("Load of unknown cursor should return ok=false")
	}
}

func TestLoadZeroCursor(t *testing.T) {
	r := New(Config{InstID: "inst-1"})
	if _, ok := r.Load(0); ok {
		t.Fatal("Load(0) should return ok=false")
	}
}

func TestLoadTTLExpiry(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	r := New(Config{
		InstID: "inst-1",
		TTL:    time.Minute,
		Now:    clock,
	})

	c := r.Save(lek("k"))

	// Just before expiry the cursor is still valid.
	now = now.Add(time.Minute - time.Nanosecond)
	if _, ok := r.Load(c); !ok {
		t.Fatal("cursor should still be valid just before TTL")
	}

	// At/after the TTL boundary the cursor expires -> ok=false.
	now = now.Add(time.Nanosecond)
	if _, ok := r.Load(c); ok {
		t.Fatal("cursor should be expired at TTL boundary")
	}
	// Expired entry must be dropped from the registry.
	if r.Len() != 0 {
		t.Fatalf("expired entry not removed, Len = %d", r.Len())
	}
}

func TestLRUEvictionBeyondCapacity(t *testing.T) {
	r := New(Config{InstID: "inst-1", Capacity: 3})

	c1 := r.Save(lek("k1"))
	c2 := r.Save(lek("k2"))
	c3 := r.Save(lek("k3"))

	// Touch c1 so it becomes most recently used; c2 is now the LRU victim.
	if _, ok := r.Load(c1); !ok {
		t.Fatal("c1 should be present before eviction")
	}

	// Inserting a 4th entry evicts the least recently used (c2).
	c4 := r.Save(lek("k4"))

	if _, ok := r.Load(c2); ok {
		t.Fatal("c2 should have been evicted as least recently used")
	}
	for _, c := range []uint64{c1, c3, c4} {
		if _, ok := r.Load(c); !ok {
			t.Fatalf("cursor %d should still be present", c)
		}
	}
	if r.Len() != 3 {
		t.Fatalf("Len = %d, want 3 (capacity)", r.Len())
	}
}

func TestLoadOwnedRejectsForeignInstance(t *testing.T) {
	r := New(Config{InstID: "inst-1"})
	c := r.Save(lek("k"))

	// Owning instance accepts.
	if _, ok := r.LoadOwned(c, "inst-1"); !ok {
		t.Fatal("LoadOwned with owning instID should succeed")
	}
	// A different instance is rejected (requirement 13.6).
	if _, ok := r.LoadOwned(c, "inst-2"); ok {
		t.Fatal("LoadOwned with foreign instID should return ok=false")
	}
}

func TestConcurrentSaveLoad(t *testing.T) {
	r := New(Config{InstID: "inst-1", Capacity: 5000})

	const workers = 16
	const perWorker = 500

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				c := r.Save(lek("w" + strconv.Itoa(w) + ":" + strconv.Itoa(i)))
				// Immediately load back; may be evicted under contention but
				// must never panic or corrupt state.
				_, _ = r.Load(c)
			}
		}(w)
	}
	wg.Wait()

	// Registry must respect the capacity bound after concurrent churn.
	if r.Len() > 5000 {
		t.Fatalf("Len = %d exceeds capacity 5000", r.Len())
	}
}
