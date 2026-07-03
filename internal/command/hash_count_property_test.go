package command

import (
	"math/rand"
	"strconv"
	"testing"
	"testing/quick"
)

// hash_count_property_test.go is the property-based half of task 13.3
// (Property 3: 计数一致性 / count consistency) scoped to the Hash family.
//
// Property 3 states: LLEN/HLEN/SCARD/ZCARD 返回值恒等于该 key 当前成员数（由
// meta.cnt 的原子 ADD 维护）. For Hash this specialises to the invariant:
//
//	after EVERY Hash mutation, HLEN(key) == the number of distinct fields the key
//	currently holds.
//
// The test drives the REAL in-process server + router over the stateful
// fakeStringStore (the same seam the Hash unit tests use — see hashes_test.go and
// strings_test.go), so the meta.cnt maintenance path (EnsureType's atomic ADD via
// adjustCount) is exercised end-to-end rather than mocked. Over randomized
// sequences of HSET / HSETNX / HDEL / HINCRBY on a small field pool it keeps an
// independent model of the expected field set and, after every operation, asserts
// that HLEN equals both len(model) and the actual number of stored fields
// (len(HKEYS)). The dual check ties meta.cnt to the true stored member count, not
// merely to the test's own bookkeeping.
//
// **Property 3: 计数一致性**
// **Validates: 需求 6.2, 6.4**
//
// Helper names here are prefixed p3 to avoid colliding with the shared test
// helpers (fakeStringStore, sendRead, send, readReply, startStringServer,
// fixedNow) which are reused as-is.

// p3FieldPool is the small, fixed field name pool the generated operations draw
// from. Keeping the pool small (relative to the operation count) guarantees the
// sequences repeatedly create, overwrite, delete and re-create the SAME fields,
// which is exactly the churn that would expose an off-by-one or double-count in
// meta.cnt maintenance.
var p3FieldPool = []string{"f0", "f1", "f2", "f3", "f4"}

// p3IntReply parses a RESP2 integer reply (":N") into its int64 value, failing
// the test on any non-integer/errored reply so a WRONGTYPE or error surfaces
// immediately instead of being silently miscompared.
func p3IntReply(t *testing.T, reply, context string) int64 {
	t.Helper()
	if len(reply) == 0 || reply[0] != ':' {
		t.Fatalf("%s: expected integer reply, got %q", context, reply)
	}
	n, err := strconv.ParseInt(reply[1:], 10, 64)
	if err != nil {
		t.Fatalf("%s: bad integer reply %q: %v", context, reply, err)
	}
	return n
}

// TestProperty3HashCountConsistency runs the Property 3 invariant over many
// randomized Hash mutation sequences. Each quick.Check iteration builds a fresh
// server/store, replays a generated sequence of HSET/HSETNX/HDEL/HINCRBY against
// the field pool while mirroring the effect on an in-test field set, and after
// every command asserts HLEN == len(model) == len(HKEYS).
//
// **Property 3: 计数一致性**
// **Validates: 需求 6.2, 6.4**
func TestProperty3HashCountConsistency(t *testing.T) {
	property := func(seed int64, opsRaw uint16) bool {
		conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
		rng := rand.New(rand.NewSource(seed))
		ops := int(opsRaw%200) + 1 // 1..200 operations per sequence

		const key = "h"
		// model is the authoritative expected field set: field name -> present.
		// An empty model means the key does not exist (an empty hash does not
		// exist in Redis), and HLEN must then report 0.
		model := make(map[string]struct{})

		// verify asserts the count invariant after an operation: HLEN equals the
		// model size, and equals the real stored field count (HKEYS length), so
		// meta.cnt is proven consistent with the actual members.
		verify := func(step int) bool {
			hlen := p3IntReply(t, sendRead(t, conn, r, "HLEN "+key),
				"HLEN after step "+strconv.Itoa(step))
			if hlen != int64(len(model)) {
				t.Errorf("step %d: HLEN=%d, want %d (model field set)", step, hlen, len(model))
				return false
			}
			send(t, conn, "HKEYS "+key)
			keys := readArray(t, r)
			if int64(len(keys)) != hlen {
				t.Errorf("step %d: HLEN=%d but HKEYS returned %d fields (cnt diverged from stored members)",
					step, hlen, len(keys))
				return false
			}
			return true
		}

		for step := 0; step < ops; step++ {
			switch rng.Intn(4) {
			case 0:
				// HSET with 1..3 field/value pairs; each field ends present.
				n := 1 + rng.Intn(3)
				cmd := "HSET " + key
				touched := make([]string, 0, n)
				for i := 0; i < n; i++ {
					f := p3FieldPool[rng.Intn(len(p3FieldPool))]
					v := strconv.Itoa(rng.Intn(1000)) // integer value keeps HINCRBY valid later
					cmd += " " + f + " " + v
					touched = append(touched, f)
				}
				sendRead(t, conn, r, cmd)
				for _, f := range touched {
					model[f] = struct{}{}
				}
			case 1:
				// HSETNX: the field ends present whether it was created or already
				// existed, so the model gains it either way.
				f := p3FieldPool[rng.Intn(len(p3FieldPool))]
				v := strconv.Itoa(rng.Intn(1000))
				sendRead(t, conn, r, "HSETNX "+key+" "+f+" "+v)
				model[f] = struct{}{}
			case 2:
				// HDEL of 1..3 fields; each named field is removed from the model.
				n := 1 + rng.Intn(3)
				cmd := "HDEL " + key
				removed := make([]string, 0, n)
				for i := 0; i < n; i++ {
					f := p3FieldPool[rng.Intn(len(p3FieldPool))]
					cmd += " " + f
					removed = append(removed, f)
				}
				sendRead(t, conn, r, cmd)
				for _, f := range removed {
					delete(model, f)
				}
			case 3:
				// HINCRBY creates the field at 0 when absent (so it ends present).
				// Values written by HSET/HSETNX/HINCRBY are always integers here,
				// so HINCRBY never hits the not-an-integer path and always applies.
				f := p3FieldPool[rng.Intn(len(p3FieldPool))]
				delta := rng.Intn(21) - 10 // -10..10, small to avoid overflow
				sendRead(t, conn, r, "HINCRBY "+key+" "+f+" "+strconv.Itoa(delta))
				model[f] = struct{}{}
			}

			if !verify(step) {
				return false
			}
		}
		return true
	}

	if err := quick.Check(property, &quick.Config{MaxCount: 300}); err != nil {
		t.Fatalf("Property 3 (Hash count consistency) violated: %v", err)
	}
}
