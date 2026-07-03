package command

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"testing/quick"

	"github.com/aura-studio/redimos/v2/internal/storage"
)

// zset_count_property_test.go is the Sorted Set half of task 15.3.
//
// Property 3 (计数一致性): LLEN/HLEN/SCARD/ZCARD return values are ALWAYS equal to
// the key's current member count, maintained by the atomic cnt ADD on every
// member add/remove. This file pins that invariant for the Sorted Set family
// (ZCARD) with a property-based test: over randomized sequences of
// ZADD/ZREM/ZINCRBY/ZREMRANGEBYRANK/ZREMRANGEBYSCORE drawn from a small member
// pool (so members collide, keys empty out and are recreated, and the range
// removals actually bite), after EVERY operation
//
//	ZCARD z  ==  the independently-modelled member count
//	ZCARD z  ==  the number of member items ZRANGE z 0 -1 actually returns
//
// The first equality checks cnt against a model kept entirely separately from
// the production delta/adjustCount code path (the model reuses only the shared
// ordering/range helpers from the storage package, never the command handlers'
// counting). The second equality checks cnt (meta.cnt, read by ZCARD) against
// the real stored member items (read by ZRANGE, which never consults cnt), so a
// drifted counter is caught even if the model and handler happened to agree.
//
// Validates: 需求 9.2 (ZCARD is O(1) from meta.cnt), 9.7 (cnt atomically
// maintained so ZCARD == current member count). Property 3.
//
// Score-precision extreme-value coverage (需求 9.6, double vs 38-digit decimal)
// lives in the differential test test/difftest/zsets_diff.go; here scores are
// kept to a small finite pool so no member score ever becomes NaN/±Inf and the
// property isolates counting behaviour.

// zPropMemberPool is the small member universe the generator draws from. Keeping
// it tiny (4 members) makes collisions, re-adds, full emptying and key
// recreation frequent, which is exactly where a miscounted cnt shows up.
var zPropMemberPool = []string{"a", "b", "c", "d"}

// zPropScorePool is the finite score universe for ZADD / ZINCRBY. All values are
// finite so cur+delta never yields NaN or ±Inf (which would drag in error-reply
// semantics unrelated to counting). Repeated scores deliberately create ties.
var zPropScorePool = []float64{-2, -1, 0, 1, 2, 3}

// zPropBoundPool is the score-bound universe for ZREMRANGEBYSCORE. It includes
// the infinities so "-inf +inf" style full-range removals are exercised.
var zPropBoundPool = []float64{
	math.Inf(-1), -3, -1, 0, 1, 3, math.Inf(1),
}

// zPropKind enumerates the generated operation kinds.
type zPropKind int

const (
	zKindAdd zPropKind = iota
	zKindRem
	zKindIncrBy
	zKindRemRangeByRank
	zKindRemRangeByScore
)

// zPropOp is a single generated Sorted Set operation. Only the fields relevant
// to Kind are populated.
type zPropOp struct {
	Kind zPropKind

	// ZADD: paired members/scores. ZREM: members. Both non-empty for their kind.
	Members []string
	Scores  []float64

	// ZINCRBY: member + delta.
	Member string
	Delta  float64

	// ZREMRANGEBYRANK: rank interval [Start, Stop] (may be negative).
	Start int
	Stop  int

	// ZREMRANGEBYSCORE: score interval [Min, Max] with per-end exclusivity.
	Min storage.ScoreBound
	Max storage.ScoreBound
}

// zPropSeq is a generated sequence of operations. It implements quick.Generator
// so testing/quick can shrink-search over whole sequences.
type zPropSeq struct {
	Ops []zPropOp
}

// Generate builds a random, well-formed operation sequence for testing/quick.
func (zPropSeq) Generate(rnd *rand.Rand, size int) reflect.Value {
	// A handful up to a few dozen ops per sequence: long enough to grow and
	// empty the key repeatedly, short enough to stay fast across 100+ trials.
	n := rnd.Intn(size%40+1) + 1
	ops := make([]zPropOp, n)
	for i := range ops {
		ops[i] = randZPropOp(rnd)
	}
	return reflect.ValueOf(zPropSeq{Ops: ops})
}

// randZPropOp draws a single random operation.
func randZPropOp(rnd *rand.Rand) zPropOp {
	switch zPropKind(rnd.Intn(5)) {
	case zKindAdd:
		pairs := rnd.Intn(2) + 1 // 1 or 2 score/member pairs
		op := zPropOp{Kind: zKindAdd}
		for j := 0; j < pairs; j++ {
			op.Members = append(op.Members, zPropMemberPool[rnd.Intn(len(zPropMemberPool))])
			op.Scores = append(op.Scores, zPropScorePool[rnd.Intn(len(zPropScorePool))])
		}
		return op
	case zKindRem:
		count := rnd.Intn(2) + 1 // 1 or 2 members
		op := zPropOp{Kind: zKindRem}
		for j := 0; j < count; j++ {
			op.Members = append(op.Members, zPropMemberPool[rnd.Intn(len(zPropMemberPool))])
		}
		return op
	case zKindIncrBy:
		return zPropOp{
			Kind:   zKindIncrBy,
			Member: zPropMemberPool[rnd.Intn(len(zPropMemberPool))],
			Delta:  zPropScorePool[rnd.Intn(len(zPropScorePool))],
		}
	case zKindRemRangeByRank:
		return zPropOp{
			Kind:  zKindRemRangeByRank,
			Start: rnd.Intn(11) - 5, // [-5, 5]
			Stop:  rnd.Intn(11) - 5,
		}
	default: // zKindRemRangeByScore
		return zPropOp{
			Kind: zKindRemRangeByScore,
			Min:  randZPropBound(rnd),
			Max:  randZPropBound(rnd),
		}
	}
}

// randZPropBound draws a random score bound (value + exclusivity).
func randZPropBound(rnd *rand.Rand) storage.ScoreBound {
	return storage.ScoreBound{
		Value:     zPropBoundPool[rnd.Intn(len(zPropBoundPool))],
		Exclusive: rnd.Intn(2) == 0,
	}
}

// zPropFormatScore renders a finite score as a ZADD/ZINCRBY argument, matching
// the parseScore the handlers use on the way in.
func zPropFormatScore(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// zPropFormatBound renders a score bound as a ZREMRANGEBYSCORE argument: a
// leading '(' for the exclusive form and inf/-inf for the infinities, matching
// parseScoreBound.
func zPropFormatBound(b storage.ScoreBound) string {
	var v string
	switch {
	case math.IsInf(b.Value, 1):
		v = "+inf"
	case math.IsInf(b.Value, -1):
		v = "-inf"
	default:
		v = strconv.FormatFloat(b.Value, 'g', -1, 64)
	}
	if b.Exclusive {
		return "(" + v
	}
	return v
}

// zPropApplyToModel applies an operation to the independent membership model
// (member -> score), reproducing the count-affecting semantics. It reuses only
// the shared ordering/range helpers from the storage package — never the command
// handlers' counting — so it is an independent oracle for the member count.
func zPropApplyToModel(model map[string]float64, op zPropOp) {
	switch op.Kind {
	case zKindAdd:
		for i, m := range op.Members {
			model[m] = op.Scores[i]
		}
	case zKindRem:
		for _, m := range op.Members {
			delete(model, m)
		}
	case zKindIncrBy:
		model[op.Member] += op.Delta // missing member reads as 0, matching ZINCRBY
	case zKindRemRangeByRank:
		ordered := zPropSorted(model)
		lo, hi, ok := storage.ZNormalizeRankRange(len(ordered), op.Start, op.Stop)
		if !ok {
			return
		}
		for _, zm := range ordered[lo : hi+1] {
			delete(model, zm.Member)
		}
	case zKindRemRangeByScore:
		for m, score := range model {
			if storage.ZScoreInRange(score, op.Min, op.Max) {
				delete(model, m)
			}
		}
	}
}

// zPropSorted returns the model's members in the store's (score asc, member asc)
// order, so rank-range removal picks exactly the members the store would.
func zPropSorted(model map[string]float64) []storage.ZMember {
	out := make([]storage.ZMember, 0, len(model))
	for m, s := range model {
		out = append(out, storage.ZMember{Member: m, Score: s})
	}
	storage.SortZMembers(out)
	return out
}

// zPropCommand renders an operation as an inline RESP command against key "z".
// Members are single lowercase letters and scores are space-free, so inline
// tokenisation is unambiguous.
func zPropCommand(op zPropOp) string {
	switch op.Kind {
	case zKindAdd:
		parts := []string{"ZADD", "z"}
		for i, m := range op.Members {
			parts = append(parts, zPropFormatScore(op.Scores[i]), m)
		}
		return strings.Join(parts, " ")
	case zKindRem:
		return "ZREM z " + strings.Join(op.Members, " ")
	case zKindIncrBy:
		return fmt.Sprintf("ZINCRBY z %s %s", zPropFormatScore(op.Delta), op.Member)
	case zKindRemRangeByRank:
		return fmt.Sprintf("ZREMRANGEBYRANK z %d %d", op.Start, op.Stop)
	default:
		return fmt.Sprintf("ZREMRANGEBYSCORE z %s %s",
			zPropFormatBound(op.Min), zPropFormatBound(op.Max))
	}
}

// TestZSetCountConsistencyProperty is the Property 3 (计数一致性) property test
// for the Sorted Set family. Validates: 需求 9.2, 9.7.
func TestZSetCountConsistencyProperty(t *testing.T) {
	property := func(seq zPropSeq) bool {
		store := newFakeStringStore()
		conn, r := startStringServer(t, store, fixedNow(1000))
		model := make(map[string]float64)

		for i, op := range seq.Ops {
			cmd := zPropCommand(op)
			// A mutating reply is a single line (integer, bulk score, or error);
			// sendRead consumes exactly one reply.
			if reply := sendRead(t, conn, r, cmd); strings.HasPrefix(reply, "-") {
				t.Logf("op %d %q unexpectedly errored: %s", i, cmd, reply)
				return false
			}
			zPropApplyToModel(model, op)

			// (1) ZCARD must equal the independently-modelled member count.
			cardReply := sendRead(t, conn, r, "ZCARD z")
			card, cerr := strconv.Atoi(strings.TrimPrefix(cardReply, ":"))
			if cerr != nil {
				t.Logf("after op %d %q: ZCARD reply %q not an integer", i, cmd, cardReply)
				return false
			}
			if card != len(model) {
				t.Logf("after op %d %q: ZCARD=%d, model count=%d", i, cmd, card, len(model))
				return false
			}

			// (2) ZCARD must equal the number of member items actually stored,
			// read via ZRANGE (which never consults meta.cnt).
			send(t, conn, "ZRANGE z 0 -1")
			members := readArray(t, r)
			if card != len(members) {
				t.Logf("after op %d %q: ZCARD=%d, ZRANGE returned %d members",
					i, cmd, card, len(members))
				return false
			}
		}
		return true
	}

	if err := quick.Check(property, &quick.Config{MaxCount: 300}); err != nil {
		t.Fatalf("ZSet count-consistency property failed: %v", err)
	}
}
