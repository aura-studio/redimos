package command

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"testing/quick"
)

// set_count_property_test.go is the property-based half of task 14.3.
//
// Property 3: 计数一致性 (count consistency), scoped to the Set family.
// Validates: 需求 8.2, 8.5.
//
// The design guarantees SCARD returns meta.cnt in O(1) (需求 8.2) and that the
// counter is maintained by the meta conditional write's atomic ADD so it stays
// exactly equal to the current cardinality across every mutation (需求 8.5).
// This test asserts that invariant directly: it replays randomized sequences of
// the count-affecting Set commands (SADD / SREM / SPOP / SMOVE) over the real
// in-process server + router (the same fakeStringStore + startStringServer seam
// the unit tests use), maintaining a plain Go model of each set alongside, and
// after EVERY operation checks that SCARD equals the model's cardinality.
//
// The generator constrains inputs to a small member pool and two keys so SMOVE
// exercises genuine cross-key moves and members recur often enough to drive the
// add-existing (no-op) and remove-absent (no-op) paths that must NOT perturb the
// count. Because SPOP removes RANDOM members, the model is reconciled from SPOP's
// own reply rather than predicted, keeping the model authoritative without
// depending on which member the server happened to pop.

// The fixed input space the property ranges over: two keys (so SMOVE moves
// between distinct sets) and a small member pool (so members collide and the
// no-op add/remove paths are hit frequently). All values are ASCII and
// space-free so they survive the inline-command send helper unambiguously.
var (
	setCountKeys    = []string{"cntsetA", "cntsetB"}
	setCountMembers = []string{"m0", "m1", "m2", "m3", "m4"}
)

// setCountOpKind enumerates the count-affecting Set mutations the property drives.
type setCountOpKind uint8

const (
	setCountSAdd setCountOpKind = iota
	setCountSRem
	setCountSPop
	setCountSMove
)

// setCountOp is one generated Set mutation. Which fields are meaningful depends
// on kind: members for SADD/SREM, popCount for SPOP (0 selects the no-count
// scalar form, >0 the array form), and member+srcIsB for SMOVE.
type setCountOp struct {
	kind     setCountOpKind
	keyIdx   int      // index into setCountKeys for the primary key (SADD/SREM/SPOP)
	members  []string // SADD / SREM members
	popCount int      // SPOP: 0 => "SPOP key"; >0 => "SPOP key n"
	member   string   // SMOVE member
	srcIsB   bool     // SMOVE: false => move A->B, true => move B->A
}

// setCountProgram is a generated sequence of Set mutations. It implements
// quick.Generator so testing/quick can range the property over many programs.
type setCountProgram struct {
	ops []setCountOp
}

// Generate builds a random program: 1..40 ops drawn from the four count-affecting
// commands, all bounded to the two-key / five-member input space.
func (setCountProgram) Generate(r *rand.Rand, _ int) reflect.Value {
	n := r.Intn(40) + 1
	ops := make([]setCountOp, n)
	for i := range ops {
		op := setCountOp{
			kind:     setCountOpKind(r.Intn(4)),
			keyIdx:   r.Intn(len(setCountKeys)),
			popCount: r.Intn(4), // 0 => scalar SPOP; 1..3 => SPOP with count
			member:   setCountMembers[r.Intn(len(setCountMembers))],
			srcIsB:   r.Intn(2) == 1,
		}
		// 1..3 members (with possible repeats) for SADD/SREM.
		for k := r.Intn(3) + 1; k > 0; k-- {
			op.members = append(op.members, setCountMembers[r.Intn(len(setCountMembers))])
		}
		ops[i] = op
	}
	return reflect.ValueOf(setCountProgram{ops: ops})
}

// TestSetCountConsistencyProperty is the Property 3 (Set) count-consistency test.
// It drives generated programs over one shared in-process server, resetting the
// two keys (and the parallel model) at the start of each program, and asserts
// SCARD equals the modelled cardinality after every operation.
//
// Validates: 需求 8.2 (SCARD O(1) from meta.cnt) and 需求 8.5 (cnt maintained so
// SCARD == current cardinality).
func TestSetCountConsistencyProperty(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))

	property := func(p setCountProgram) bool {
		// Independent starting state per program. A DEL alone is not enough
		// against the in-process fake: the proxy's DEL is a lazy delete (it drops
		// the meta and enqueues member reclamation for the background deleter,
		// which is not running here), so member items would leak across programs
		// while cnt resets. We therefore purge the fake's member data for both
		// keys directly, giving each program a genuinely empty keyspace — exactly
		// the clean-slate state the count invariant is asserted against.
		model := []map[string]struct{}{{}, {}}
		for _, key := range setCountKeys {
			pk := encodePK(0, []byte(key))
			delete(store.sets, pk)
			delete(store.metas, pk)
			delete(store.live, pk)
		}

		for i, op := range p.ops {
			if !applySetCountOp(t, conn, r, model, op) {
				t.Errorf("op %d %+v: reply diverged from model", i, op)
				return false
			}
			// The invariant: after each op, SCARD == |model| for both keys.
			for k, key := range setCountKeys {
				if !checkSetCard(t, conn, r, key, len(model[k])) {
					t.Errorf("after op %d %+v: SCARD %s != model cardinality %d",
						i, op, key, len(model[k]))
					return false
				}
			}
		}
		return true
	}

	cfg := &quick.Config{MaxCount: 400, Rand: rand.New(rand.NewSource(0x5e7c0117))}
	if err := quick.Check(property, cfg); err != nil {
		t.Fatalf("Set count-consistency property (Property 3) failed: %v", err)
	}
}

// applySetCountOp executes one op against the server and mirrors its effect on
// the model, keeping the two in lockstep. It returns false when the server's
// reply contradicts the model's prediction for the deterministic commands
// (SADD/SREM/SMOVE integer replies), which would itself be a count bug. SPOP's
// removed members are read back from its reply and applied to the model, so the
// model follows the server's random choice rather than guessing it.
func applySetCountOp(t *testing.T, conn net.Conn, br *bufio.Reader, model []map[string]struct{}, op setCountOp) bool {
	t.Helper()
	switch op.kind {
	case setCountSAdd:
		key := setCountKeys[op.keyIdx]
		set := model[op.keyIdx]
		want := 0
		for _, m := range op.members {
			if _, ok := set[m]; !ok {
				want++
				set[m] = struct{}{}
			}
		}
		got := sendRead(t, conn, br, "SADD "+key+" "+strings.Join(op.members, " "))
		return got == fmt.Sprintf(":%d", want)

	case setCountSRem:
		key := setCountKeys[op.keyIdx]
		set := model[op.keyIdx]
		want := 0
		for _, m := range op.members {
			if _, ok := set[m]; ok {
				want++
				delete(set, m)
			}
		}
		got := sendRead(t, conn, br, "SREM "+key+" "+strings.Join(op.members, " "))
		return got == fmt.Sprintf(":%d", want)

	case setCountSPop:
		key := setCountKeys[op.keyIdx]
		set := model[op.keyIdx]
		if op.popCount == 0 {
			// Scalar SPOP: "$member" removes one member; "$-1" only when empty.
			got := sendRead(t, conn, br, "SPOP "+key)
			if got == "$-1" {
				return len(set) == 0
			}
			m := stripBulk(got)
			if _, ok := set[m]; !ok {
				return false // popped a member the model never had
			}
			delete(set, m)
			return true
		}
		// SPOP with count: an array of the removed members. Reconcile the model
		// from the reply (the server chooses which members at random).
		send(t, conn, fmt.Sprintf("SPOP %s %d", key, op.popCount))
		popped := readArray(t, br)
		expected := op.popCount
		if expected > len(set) {
			expected = len(set)
		}
		if len(popped) != expected {
			return false // popped a different count than the cardinality allows
		}
		for _, e := range popped {
			m := stripBulk(e)
			if _, ok := set[m]; !ok {
				return false
			}
			delete(set, m)
		}
		return true

	case setCountSMove:
		srcIdx, dstIdx := 0, 1
		if op.srcIsB {
			srcIdx, dstIdx = 1, 0
		}
		srcKey, dstKey := setCountKeys[srcIdx], setCountKeys[dstIdx]
		src, dst := model[srcIdx], model[dstIdx]
		_, inSrc := src[op.member]
		want := 0
		if inSrc {
			want = 1
			delete(src, op.member)
			dst[op.member] = struct{}{}
		}
		got := sendRead(t, conn, br, "SMOVE "+srcKey+" "+dstKey+" "+op.member)
		return got == fmt.Sprintf(":%d", want)
	}
	return false
}

// checkSetCard sends SCARD key and reports whether the integer reply equals want.
func checkSetCard(t *testing.T, conn net.Conn, br *bufio.Reader, key string, want int) bool {
	t.Helper()
	got := sendRead(t, conn, br, "SCARD "+key)
	return got == ":"+strconv.Itoa(want)
}
