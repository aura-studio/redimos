package guard

// Property-based test for Property 4 (大小守卫 / size guard) from the design
// document.
//
// Property 4: 对任意写入，若成员/value 超限则整体拒绝且不产生部分写入。
// (For any write, if a member/value exceeds its limit the whole request is
// rejected and no partial write is produced.)
//
// The guard sits in front of every backend write and never mutates storage, so
// "no partial write" is expressed at the guard boundary as an all-or-nothing
// decision: CheckWrite either accepts the entire request (nil) or rejects the
// entire request (ErrSizeExceeded). There is no intermediate/partial signal.
//
// This test uses the standard library testing/quick to generate arbitrary
// key/member/value sizes that straddle the MaxNameSize (1KB) and MaxValueSize
// (390KB) boundaries, and asserts the three facets of Property 4:
//
//	(a) any input exceeding a limit is rejected with ErrSizeExceeded
//	    (whole-request rejection, never partial);
//	(b) any input within limits passes;
//	(c) the interception counter increments exactly once per rejected write
//	    via CheckWrite (a single rejection event, not once per breached field).
//
// **Validates: Requirements 14.1, 14.2, 14.3**

import (
	"errors"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
)

// maxOverflow bounds how far a generated size may exceed its limit. Keeping the
// overflow small (a few KB) makes the generators memory-light while still
// exercising the "just over the boundary" cases that matter most, alongside the
// exactly-at-limit and well-under cases.
const maxOverflow = 4 * 1024

// genSize returns a byte length for a field with the given inclusive limit.
// With ~50% probability it returns an over-limit size in (limit, limit+maxOverflow];
// otherwise it returns an at-or-under size in [0, limit] biased toward the
// boundary so that limit-1, limit, and limit+1 are all reachable. The bool
// reports whether the returned size exceeds the limit.
func genSize(rng *rand.Rand, limit int) (int, bool) {
	if rng.Intn(2) == 0 {
		// Over the limit: limit+1 .. limit+maxOverflow.
		return limit + 1 + rng.Intn(maxOverflow), true
	}
	// At or under the limit. Bias toward the boundary: half the time land
	// within [limit-2, limit], the rest spread across [0, limit].
	if rng.Intn(2) == 0 {
		return limit - rng.Intn(3), false // limit-2 .. limit (min 0 since limit>=1KB)
	}
	return rng.Intn(limit + 1), false
}

// writeCase is a generated CheckWrite input together with the oracle expectation
// (whether any field exceeds its limit).
type writeCase struct {
	key     []byte
	members [][]byte
	values  [][]byte
	anyOver bool
}

// Generate implements quick.Generator, producing a smart, boundary-focused
// CheckWrite input. It never depends on byte contents (the guard inspects only
// len), so slices are zero-filled to keep allocation cheap.
func (writeCase) Generate(rng *rand.Rand, size int) reflect.Value {
	wc := writeCase{}

	keySize, keyOver := genSize(rng, MaxNameSize)
	wc.key = make([]byte, keySize)
	wc.anyOver = wc.anyOver || keyOver

	nMembers := rng.Intn(4) // 0..3 members
	wc.members = make([][]byte, nMembers)
	for i := range wc.members {
		mSize, mOver := genSize(rng, MaxMemberNameSize)
		wc.members[i] = make([]byte, mSize)
		wc.anyOver = wc.anyOver || mOver
	}

	nValues := rng.Intn(4) // 0..3 values
	wc.values = make([][]byte, nValues)
	for i := range wc.values {
		vSize, vOver := genSize(rng, MaxValueSize)
		wc.values[i] = make([]byte, vSize)
		wc.anyOver = wc.anyOver || vOver
	}

	return reflect.ValueOf(wc)
}

// TestProperty4SizeGuard is the property-based test for Property 4.
func TestProperty4SizeGuard(t *testing.T) {
	// (a)+(b)+(c): whole-request accept/reject decision, correct sentinel, and
	// exactly-once interception counting per rejected write.
	prop := func(wc writeCase) bool {
		ResetInterceptions()
		err := CheckWrite(wc.key, wc.members, wc.values)

		if wc.anyOver {
			// (a) any over-limit input => whole-request rejection with the
			// sentinel error. There is no partial-success return value.
			if !errors.Is(err, ErrSizeExceeded) {
				t.Logf("expected ErrSizeExceeded for over-limit write, got %v", err)
				return false
			}
			// (c) exactly one interception counted for the rejected write,
			// regardless of how many fields breached their limit.
			if got := Interceptions(); got != 1 {
				t.Logf("expected exactly 1 interception for rejected write, got %d", got)
				return false
			}
			return true
		}

		// (b) every field within limits => the whole request passes and no
		// interception is counted.
		if err != nil {
			t.Logf("expected nil error for within-limit write, got %v", err)
			return false
		}
		if got := Interceptions(); got != 0 {
			t.Logf("expected 0 interceptions for accepted write, got %d", got)
			return false
		}
		return true
	}

	if err := quick.Check(prop, &quick.Config{MaxCount: 2000}); err != nil {
		t.Errorf("Property 4 (size guard) failed: %v", err)
	}
}

// TestProperty4SingleFieldChecks reinforces Property 4 for the single-field
// Check* helpers: an over-limit input is rejected with the sentinel and counted
// exactly once; an at-or-under input passes without counting. This nails down
// facets (a)/(b)/(c) at the granularity the command layer uses when it checks a
// key, member, or value in isolation.
func TestProperty4SingleFieldChecks(t *testing.T) {
	// checkFn pairs a Check* function with the limit governing its input.
	type checkFn struct {
		name  string
		limit int
		fn    func([]byte) error
	}
	fns := []checkFn{
		{"CheckKey", MaxNameSize, CheckKey},
		{"CheckMember", MaxMemberNameSize, CheckMember},
		{"CheckValue", MaxValueSize, CheckValue},
	}

	for _, c := range fns {
		c := c
		t.Run(c.name, func(t *testing.T) {
			prop := func(seed int64) bool {
				rng := rand.New(rand.NewSource(seed))
				size, over := genSize(rng, c.limit)
				ResetInterceptions()
				err := c.fn(make([]byte, size))
				if over {
					if !errors.Is(err, ErrSizeExceeded) {
						t.Logf("%s(%d): expected ErrSizeExceeded, got %v", c.name, size, err)
						return false
					}
					if got := Interceptions(); got != 1 {
						t.Logf("%s(%d): expected 1 interception, got %d", c.name, size, got)
						return false
					}
					return true
				}
				if err != nil {
					t.Logf("%s(%d): expected nil, got %v", c.name, size, err)
					return false
				}
				if got := Interceptions(); got != 0 {
					t.Logf("%s(%d): expected 0 interceptions, got %d", c.name, size, got)
					return false
				}
				return true
			}
			if err := quick.Check(prop, &quick.Config{MaxCount: 2000}); err != nil {
				t.Errorf("Property 4 single-field check %s failed: %v", c.name, err)
			}
		})
	}
}
