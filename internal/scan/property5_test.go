package scan

import (
	"math/rand"
	"strconv"
	"testing"
	"testing/quick"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Property 5: 游标安全 (cursor safety).
//
// SCAN 允许重复但不遗漏未删除的 key；失效游标必返回明确错误而非静默错误结果。
//
// At the registry level this decomposes into two testable invariants:
//
//   - No-omission / round-trip (需求 13.7): every cursor that has NOT been
//     evicted (capacity) or expired (TTL) must Load back its exact
//     LastEvaluatedKey. A scan paging through still-live cursors therefore never
//     loses a page, and Load never hands back a different entry's LEK.
//
//   - Invalid cursor is explicit (需求 13.5): a cursor that was never saved, was
//     evicted, expired past the TTL, or belongs to a different instance must
//     return ok=false — the command layer maps ok=false to
//     "-ERR invalid cursor, restart scan". Load never returns a silent
//     wrong/stale LEK for these cases.
//
// Helpers here carry a p5 prefix to avoid collisions with registry_test.go.

// p5IDLEK builds a LastEvaluatedKey whose "id" attribute uniquely identifies the
// saved entry, so a round-trip can be asserted by value and a wrong entry is
// distinguishable from the correct one.
func p5IDLEK(id int) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"id": &types.AttributeValueMemberN{Value: strconv.Itoa(id)},
	}
}

// p5LEKID extracts the identifying id from a LEK produced by p5IDLEK.
func p5LEKID(m map[string]types.AttributeValue) (int, bool) {
	av, ok := m["id"]
	if !ok {
		return 0, false
	}
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, false
	}
	id, err := strconv.Atoi(n.Value)
	if err != nil {
		return 0, false
	}
	return id, true
}

// p5Clock is a deterministic, injectable clock advanced explicitly by tests.
type p5Clock struct{ t time.Time }

func newP5Clock() *p5Clock { return &p5Clock{t: time.Unix(1_600_000_000, 0)} }

func (c *p5Clock) now() time.Time          { return c.t }
func (c *p5Clock) advance(d time.Duration) { c.t = c.t.Add(d) }

// p5DetRand returns a deterministic uint64 source seeded by seed, so cursor
// generation is reproducible across a property run.
func p5DetRand(seed int64) func() uint64 {
	rng := rand.New(rand.NewSource(seed))
	return func() uint64 { return rng.Uint64() }
}

// TestProperty5NoOmissionForLiveCursors checks the round-trip / no-omission
// invariant: across randomized Save sequences, every cursor whose entry is still
// resident (not evicted, not expired) Loads back its exact LEK, and the returned
// LEK belongs to that cursor and no other.
//
// **Validates: Requirements 13.7**
func TestProperty5NoOmissionForLiveCursors(t *testing.T) {
	// property models one randomized run. ops is the number of Save operations,
	// capSeed drives a small capacity, and randSeed drives cursor generation.
	property := func(opsRaw uint16, capRaw uint8, randSeed int64) bool {
		ops := int(opsRaw%256) + 1     // 1..256 saves
		capacity := int(capRaw%32) + 1 // 1..32 capacity (deliberately small)

		clock := newP5Clock()
		r := New(Config{
			InstID:   "inst-live",
			Capacity: capacity,
			TTL:      time.Hour, // large TTL: nothing expires within this run
			Now:      clock.now,
			Rand:     p5DetRand(randSeed),
		})

		// cursorToID records the id we stored under each cursor. Cursor
		// collisions cannot occur (newCursorLocked skips in-use values), so a
		// cursor maps to exactly one id.
		cursorToID := make(map[uint64]int, ops)
		// order preserves save order so we can compute which cursors survive LRU
		// eviction (the most-recently-saved `capacity` cursors, since we never
		// Load between saves here).
		order := make([]uint64, 0, ops)

		for i := 0; i < ops; i++ {
			c := r.Save(p5IDLEK(i))
			if c == 0 {
				t.Logf("Save returned zero cursor at op %d", i)
				return false
			}
			cursorToID[c] = i
			order = append(order, c)
		}

		// The registry must never exceed its capacity bound.
		liveCount := ops
		if liveCount > capacity {
			liveCount = capacity
		}
		if r.Len() != liveCount {
			t.Logf("Len=%d, want %d (ops=%d cap=%d)", r.Len(), liveCount, ops, capacity)
			return false
		}

		// The last `liveCount` saved cursors are the survivors; every one of
		// them must Load back its exact LEK (no omission, correct value).
		survivors := order[len(order)-liveCount:]
		for _, c := range survivors {
			got, ok := r.Load(c)
			if !ok {
				t.Logf("live cursor %d failed to Load (ops=%d cap=%d)", c, ops, capacity)
				return false
			}
			id, valid := p5LEKID(got)
			if !valid {
				t.Logf("cursor %d returned malformed LEK", c)
				return false
			}
			if id != cursorToID[c] {
				t.Logf("cursor %d returned id %d, want %d (wrong entry!)", c, id, cursorToID[c])
				return false
			}
		}
		return true
	}

	if err := quick.Check(property, &quick.Config{MaxCount: 500}); err != nil {
		t.Fatalf("Property 5 no-omission violated: %v", err)
	}
}

// TestProperty5InvalidCursorIsExplicit checks that every class of invalid cursor
// is rejected with ok=false (never a silent wrong/stale LEK):
//   - never-saved cursor
//   - evicted cursor (exceeded capacity)
//   - expired cursor (past TTL via injected clock)
//   - foreign-instance cursor (LoadOwned with a different instID)
//
// **Validates: Requirements 13.5**
func TestProperty5InvalidCursorIsExplicit(t *testing.T) {
	property := func(nRaw uint8, randSeed int64) bool {
		n := int(nRaw%24) + 2 // 2..25 saves
		capacity := n - 1     // one less than saves -> forces >=1 eviction

		clock := newP5Clock()
		ttl := 5 * time.Minute
		const ownerInst = "inst-owner"
		r := New(Config{
			InstID:   ownerInst,
			Capacity: capacity,
			TTL:      ttl,
			Now:      clock.now,
			Rand:     p5DetRand(randSeed),
		})

		order := make([]uint64, 0, n)
		used := make(map[uint64]bool, n)
		for i := 0; i < n; i++ {
			c := r.Save(p5IDLEK(i))
			order = append(order, c)
			used[c] = true
		}

		// 1) Evicted cursor: the first-saved cursor is the LRU victim (no Loads
		//    happened between saves), so it must be gone.
		evicted := order[0]
		if _, ok := r.Load(evicted); ok {
			t.Logf("evicted cursor %d returned ok=true (silent stale)", evicted)
			return false
		}

		// 2) Never-saved cursor: pick a non-zero value never handed out.
		var never uint64 = 0xDEADBEEFCAFEF00D
		for used[never] {
			never++
		}
		if never == 0 {
			never = 1
		}
		if _, ok := r.Load(never); ok {
			t.Logf("never-saved cursor %d returned ok=true", never)
			return false
		}

		// 3) Zero cursor is always invalid (SCAN 0 means "start fresh", never a
		//    stored token).
		if _, ok := r.Load(0); ok {
			t.Log("zero cursor returned ok=true")
			return false
		}

		// A surviving, still-live cursor: last-saved is guaranteed resident.
		live := order[len(order)-1]
		if _, ok := r.Load(live); !ok {
			t.Logf("expected live cursor %d to be valid before expiry", live)
			return false
		}

		// 4) Foreign-instance cursor: LoadOwned with a different instID must be
		//    rejected even though the cursor is otherwise live.
		if _, ok := r.LoadOwned(live, "inst-stranger"); ok {
			t.Logf("foreign-instance LoadOwned on cursor %d returned ok=true", live)
			return false
		}
		// The owning instance still accepts it (sanity: rejection is about
		// ownership, not corruption).
		if _, ok := r.LoadOwned(live, ownerInst); !ok {
			t.Logf("owning-instance LoadOwned on cursor %d returned ok=false", live)
			return false
		}

		// 5) Expired cursor: advance the clock past the TTL; every remaining
		//    cursor must now be rejected.
		clock.advance(ttl)
		for _, c := range order {
			if _, ok := r.Load(c); ok {
				t.Logf("cursor %d survived TTL expiry (silent stale)", c)
				return false
			}
		}
		return true
	}

	if err := quick.Check(property, &quick.Config{MaxCount: 500}); err != nil {
		t.Fatalf("Property 5 invalid-cursor-explicit violated: %v", err)
	}
}
