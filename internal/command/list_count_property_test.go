package command

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"testing/quick"

	"github.com/aura-studio/redimos/v2/internal/storage"
)

// list_count_property_test.go is the List-scoped half of task 16.3
// (Property 3: 计数一致性). It is the property-based companion to the byte-level
// List differential gate in test/difftest/lists_diff.go.
//
// Property 3 (计数一致性): LLEN 返回值恒等于该 key 当前成员数. Scoped to List, the
// invariant checked here is:
//
//	after every mutating List operation, LLEN(key) == the key's true element
//	count.
//
// It is asserted two independent ways so a failure localizes cleanly:
//
//  1. against an in-process MODEL of each list (the value LLEN "should" be), and
//  2. against the number of elements LRANGE key 0 -1 actually enumerates (the
//     O(1) meta.cnt counter vs. the real member items).
//
// If the O(1) counter (meta.cnt) ever drifts from the real membership, either or
// both comparisons fail and testing/quick reports the shrunk operation sequence
// that first broke it.
//
// The sequences are randomized draws from LPUSH / RPUSH / LPOP / RPOP / LREM /
// LTRIM / LINSERT / RPOPLPUSH over a small value pool and a two-key key pool
// (so RPOPLPUSH exercises both the two-key move and the single-key rotation).
// The whole thing runs over the real in-process server + router + the stateful
// fakeStringStore (the same seam the List unit tests use), so the handlers, the
// meta counting and the RESP encoding are exercised end-to-end without any live
// DynamoDB. The clock is pinned so no expiry interferes.
//
// **Validates: Requirements 7.2, 7.7**

// lcpKeyPool is the two-key pool the generated List operations act on. Two keys
// let RPOPLPUSH cover both the two-key move (src != dst) and the single-key
// rotation (src == dst). Names are unique to this file to avoid collisions with
// other tests in the package.
var lcpKeyPool = []string{"lcp:l1", "lcp:l2"}

// lcpValuePool is the small value/pivot pool the operations draw from. Keeping it
// tiny makes duplicates (and therefore LREM/LINSERT hits) common.
var lcpValuePool = []string{"a", "b", "c"}

// lcpOpKind enumerates the mutating List operations the property exercises.
type lcpOpKind int

const (
	lcpLPush lcpOpKind = iota
	lcpRPush
	lcpLPop
	lcpRPop
	lcpLRem
	lcpLTrim
	lcpLInsert
	lcpRPopLPush
	lcpOpKindCount // sentinel: number of op kinds
)

// lcpOp is a single generated List operation with all operands it may need.
type lcpOp struct {
	kind   lcpOpKind
	key    string // primary key (source for RPOPLPUSH)
	dst    string // destination key (RPOPLPUSH only)
	vals   []string
	count  int    // LREM count (may be negative or zero)
	start  int    // LTRIM start
	stop   int    // LTRIM stop
	before bool   // LINSERT: true=BEFORE, false=AFTER
	pivot  string // LINSERT pivot
	val    string // LREM / LINSERT value
}

// lcpSequence is a generated sequence of List operations. It implements
// quick.Generator so testing/quick can drive it.
type lcpSequence struct {
	ops []lcpOp
}

// Generate implements quick.Generator: it builds a random, well-formed List
// operation sequence drawn from the key and value pools. The command shapes are
// always valid (correct arity, integer arguments) so the property exercises the
// counting logic rather than error paths.
func (lcpSequence) Generate(rnd *rand.Rand, size int) reflect.Value {
	n := 1 + rnd.Intn(size+1)
	ops := make([]lcpOp, n)
	for i := range ops {
		ops[i] = generateLcpOp(rnd)
	}
	return reflect.ValueOf(lcpSequence{ops: ops})
}

func generateLcpOp(rnd *rand.Rand) lcpOp {
	op := lcpOp{
		kind: lcpOpKind(rnd.Intn(int(lcpOpKindCount))),
		key:  lcpKeyPool[rnd.Intn(len(lcpKeyPool))],
	}
	switch op.kind {
	case lcpLPush, lcpRPush:
		m := 1 + rnd.Intn(3)
		op.vals = make([]string, m)
		for j := range op.vals {
			op.vals[j] = lcpValuePool[rnd.Intn(len(lcpValuePool))]
		}
	case lcpLRem:
		op.count = rnd.Intn(5) - 2 // -2..2
		op.val = lcpValuePool[rnd.Intn(len(lcpValuePool))]
	case lcpLTrim:
		op.start = rnd.Intn(7) - 3 // -3..3
		op.stop = rnd.Intn(7) - 3
	case lcpLInsert:
		op.before = rnd.Intn(2) == 0
		op.pivot = lcpValuePool[rnd.Intn(len(lcpValuePool))]
		op.val = lcpValuePool[rnd.Intn(len(lcpValuePool))]
	case lcpRPopLPush:
		op.dst = lcpKeyPool[rnd.Intn(len(lcpKeyPool))]
	}
	return op
}

// command renders the op as an inline RESP2 command line (the value pool is
// space-free so inline encoding is safe).
func (op lcpOp) command() string {
	switch op.kind {
	case lcpLPush:
		return "LPUSH " + op.key + " " + strings.Join(op.vals, " ")
	case lcpRPush:
		return "RPUSH " + op.key + " " + strings.Join(op.vals, " ")
	case lcpLPop:
		return "LPOP " + op.key
	case lcpRPop:
		return "RPOP " + op.key
	case lcpLRem:
		return fmt.Sprintf("LREM %s %d %s", op.key, op.count, op.val)
	case lcpLTrim:
		return fmt.Sprintf("LTRIM %s %d %d", op.key, op.start, op.stop)
	case lcpLInsert:
		where := "AFTER"
		if op.before {
			where = "BEFORE"
		}
		return fmt.Sprintf("LINSERT %s %s %s %s", op.key, where, op.pivot, op.val)
	case lcpRPopLPush:
		return "RPOPLPUSH " + op.key + " " + op.dst
	default:
		return ""
	}
}

// applyModel updates the in-process list model to mirror the op's Redis
// semantics. The model is the source of truth LLEN is compared against; it is
// kept deliberately in lock-step with the handler semantics (LPUSH prepends in
// argument order, LTRIM reuses the same rank-normalization helper the store
// uses, LREM matches the head/tail/all count rules, etc.).
func (op lcpOp) applyModel(model map[string][]string) {
	switch op.kind {
	case lcpLPush:
		l := model[op.key]
		for _, v := range op.vals {
			l = append([]string{v}, l...)
		}
		model[op.key] = l
	case lcpRPush:
		model[op.key] = append(model[op.key], op.vals...)
	case lcpLPop:
		if l := model[op.key]; len(l) > 0 {
			model[op.key] = l[1:]
		}
	case lcpRPop:
		if l := model[op.key]; len(l) > 0 {
			model[op.key] = l[:len(l)-1]
		}
	case lcpLRem:
		model[op.key] = modelLRem(model[op.key], op.count, op.val)
	case lcpLTrim:
		l := model[op.key]
		if len(l) == 0 {
			break
		}
		lo, hi, ok := storage.ZNormalizeRankRange(len(l), op.start, op.stop)
		if !ok {
			model[op.key] = nil
			break
		}
		model[op.key] = append([]string(nil), l[lo:hi+1]...)
	case lcpLInsert:
		l := model[op.key]
		if len(l) == 0 {
			break // absent key: LINSERT is a no-op
		}
		idx := -1
		for i, e := range l {
			if e == op.pivot {
				idx = i
				break
			}
		}
		if idx < 0 {
			break // pivot not found: no change
		}
		at := idx
		if !op.before {
			at = idx + 1
		}
		out := make([]string, 0, len(l)+1)
		out = append(out, l[:at]...)
		out = append(out, op.val)
		out = append(out, l[at:]...)
		model[op.key] = out
	case lcpRPopLPush:
		src := model[op.key]
		if len(src) == 0 {
			break
		}
		tail := src[len(src)-1]
		if op.key == op.dst {
			// Single-key rotation: tail moves to the head, length unchanged.
			rotated := append([]string{tail}, src[:len(src)-1]...)
			model[op.key] = rotated
			break
		}
		model[op.key] = src[:len(src)-1]
		model[op.dst] = append([]string{tail}, model[op.dst]...)
	}
}

// modelLRem removes elements equal to val per Redis' LREM count semantics:
// count>0 removes up to count occurrences scanning head->tail, count<0 up to
// -count scanning tail->head, count==0 removes every occurrence. It never
// mutates the input slice.
func modelLRem(list []string, count int, val string) []string {
	n := len(list)
	drop := make([]bool, n)
	switch {
	case count > 0:
		for i, left := 0, count; i < n && left > 0; i++ {
			if list[i] == val {
				drop[i] = true
				left--
			}
		}
	case count < 0:
		for i, left := n-1, -count; i >= 0 && left > 0; i-- {
			if list[i] == val {
				drop[i] = true
				left--
			}
		}
	default:
		for i := 0; i < n; i++ {
			if list[i] == val {
				drop[i] = true
			}
		}
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if !drop[i] {
			out = append(out, list[i])
		}
	}
	return out
}

// TestProperty3ListCountConsistency is the List-scoped Property 3 check. Over
// randomized LPUSH/RPUSH/LPOP/RPOP/LREM/LTRIM/LINSERT/RPOPLPUSH sequences it
// asserts, after every operation, that LLEN(key) equals both the modelled list
// length and the number of elements LRANGE key 0 -1 actually returns, for every
// key in the pool.
//
// **Validates: Requirements 7.2, 7.7**
func TestProperty3ListCountConsistency(t *testing.T) {
	var failure string

	property := func(seq lcpSequence) bool {
		// A fresh store + server per generated sequence gives each check a clean
		// keyspace with no cross-iteration state leaking through the store.
		conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
		model := make(map[string][]string, len(lcpKeyPool))

		for i, op := range seq.ops {
			sendRead(t, conn, r, op.command())
			op.applyModel(model)

			for _, k := range lcpKeyPool {
				want := len(model[k])

				llen := parseIntReply(t, sendRead(t, conn, r, "LLEN "+k))

				send(t, conn, "LRANGE "+k+" 0 -1")
				actual := len(readArrayPayloads(t, r))

				if llen != want || actual != want {
					failure = fmt.Sprintf(
						"after op %d %q on key %q: LLEN=%d, LRANGE-count=%d, model-len=%d\nsequence: %s",
						i, op.command(), k, llen, actual, want, describeLcpSequence(seq))
					return false
				}
			}
		}
		return true
	}

	if err := quick.Check(property, &quick.Config{MaxCount: 300}); err != nil {
		if failure != "" {
			t.Fatalf("Property 3 (List count consistency) failed: %v\n%s", err, failure)
		}
		t.Fatalf("Property 3 (List count consistency) failed: %v", err)
	}
}

// parseIntReply extracts the integer from a ":N" RESP2 reply string, failing the
// test on any other shape (the count commands must never error in these
// well-formed sequences).
func parseIntReply(t *testing.T, reply string) int {
	t.Helper()
	if len(reply) == 0 || reply[0] != ':' {
		t.Fatalf("expected integer reply, got %q", reply)
	}
	n, err := strconv.Atoi(reply[1:])
	if err != nil {
		t.Fatalf("bad integer reply %q: %v", reply, err)
	}
	return n
}

// describeLcpSequence renders a sequence as its command lines, for readable
// counterexamples.
func describeLcpSequence(seq lcpSequence) string {
	lines := make([]string, len(seq.ops))
	for i, op := range seq.ops {
		lines[i] = op.command()
	}
	return strings.Join(lines, " | ")
}
