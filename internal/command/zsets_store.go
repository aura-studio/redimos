package command

import (
	"context"
	"math"
	"strings"

	"github.com/aura-studio/redimos/internal/guard"
	"github.com/aura-studio/redimos/internal/meta"
	"github.com/aura-studio/redimos/internal/resp"
	"github.com/aura-studio/redimos/internal/server"
	"github.com/aura-studio/redimos/internal/storage"
)

// errAtLeastOneKey is the Redis reply for a ZUNIONSTORE / ZINTERSTORE whose
// numkeys is not a positive integer.
const errAtLeastOneKey = "ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE"

// handleZUnionStore implements ZUNIONSTORE dest numkeys key [key ...] [WEIGHTS
// w1 ..] [AGGREGATE SUM|MIN|MAX] (requirement 9.5). See handleZStore.
func (r *Router) handleZUnionStore(ctx context.Context, c *server.Conn, args [][]byte) {
	r.handleZStore(ctx, c, args, false)
}

// handleZInterStore implements ZINTERSTORE dest numkeys key [key ...] [WEIGHTS
// w1 ..] [AGGREGATE SUM|MIN|MAX] (requirement 9.5). See handleZStore.
func (r *Router) handleZInterStore(ctx context.Context, c *server.Conn, args [][]byte) {
	r.handleZStore(ctx, c, args, true)
}

// zAggregate selects how ZUNIONSTORE / ZINTERSTORE combine the scores of a member
// that appears in more than one operand.
type zAggregate int

const (
	zAggSum zAggregate = iota // sum the weighted scores (Redis default)
	zAggMin                   // keep the minimum weighted score
	zAggMax                   // keep the maximum weighted score
)

// aggregateScore combines an accumulated score with the next weighted operand
// score under the selected aggregate. For SUM, a NaN result (e.g. +inf + -inf) is
// flushed to 0, matching Redis' zunionInterAggregate.
func aggregateScore(agg zAggregate, acc, next float64) float64 {
	switch agg {
	case zAggMin:
		if next < acc {
			return next
		}
		return acc
	case zAggMax:
		if next > acc {
			return next
		}
		return acc
	default: // zAggSum
		v := acc + next
		if math.IsNaN(v) {
			return 0
		}
		return v
	}
}

// weightScore multiplies a member's score by its operand weight, flushing a NaN
// product (Redis treats 0 * ±inf as 0) to 0.
func weightScore(score, weight float64) float64 {
	v := score * weight
	if math.IsNaN(v) {
		return 0
	}
	return v
}

// handleZStore is the shared body of ZUNIONSTORE / ZINTERSTORE (requirement 9.5).
// It reads the numkeys operand keys into proxy memory and combines their scores —
// a UNION (every member of any operand) or INTERSECTION (only members present in
// every operand) — applying optional per-operand WEIGHTS multipliers and the
// AGGREGATE function (SUM default, or MIN/MAX), then STORES the result into dest
// as a sorted set and replies the resulting cardinality.
//
// NON-ATOMIC SNAPSHOT (requirement 9.5): the operand sets are read one after
// another on P0's serial per-connection path, so the result reflects a
// point-in-time snapshot per key rather than one atomic view across all keys, and
// the store into dest is a further separate write. A concurrent writer touching an
// operand mid-computation can therefore be partially reflected.
//
// Operand types: a Sorted Set operand contributes each member's score; a plain Set
// operand contributes each member with score 1 (matching Redis, which treats a set
// as a zset of score-1 members). Any other live type (String/Hash/List) replies
// WRONGTYPE and leaves dest untouched. An absent/expired operand contributes
// nothing (the empty set), which makes an INTERSECTION empty.
//
// Store/overwrite semantics match Redis and the S*STORE family: the result is
// computed from the operands FIRST (so a dest that is also an operand is read
// pre-overwrite), then dest is REPLACED entirely — its meta (clearing any prior
// type) and members are removed — before the fresh sorted set is written. An empty
// result leaves dest deleted and replies 0. dest's meta.cnt is maintained to the
// result cardinality via adjustCount so ZCARD stays O(1) and exact.
func (r *Router) handleZStore(ctx context.Context, c *server.Conn, args [][]byte, inter bool) {
	w := resp.NewWriter(c.Redcon())

	destKey := args[1]
	destPK := r.encodePK(c.DB(), destKey)

	numKeys, err := ParseInt(args[2])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}
	if numKeys <= 0 {
		w.Error(errAtLeastOneKey)
		return
	}

	// The numkeys operand keys must be exactly present; the remaining tokens are
	// the optional WEIGHTS / AGGREGATE clauses.
	rest := args[3:]
	if int64(len(rest)) < numKeys {
		w.Error(resp.ErrSyntax)
		return
	}
	keyArgs := rest[:numKeys]
	tail := rest[numKeys:]

	// Load + type-check each operand BEFORE parsing WEIGHTS/AGGREGATE: Redis
	// zunionInterGenericCommand looks the source keys up and rejects a wrong-type
	// source (WRONGTYPE) before it ever parses the optional clauses, so a wrong-type
	// source with a bad WEIGHTS value replies WRONGTYPE, not "weight value is not a
	// float". (This load is a non-atomic snapshot either way.)
	operands := make([]map[string]float64, numKeys)
	for i, k := range keyArgs {
		opPK := r.encodePK(c.DB(), k)
		scores, wrongType, lerr := r.loadZStoreOperand(ctx, opPK)
		if lerr != nil {
			r.writeStoreError(c, lerr)
			return
		}
		if wrongType {
			w.Error(resp.ErrWrongType)
			return
		}
		operands[i] = scores
	}

	weights, agg, errText := parseZStoreOptions(tail, int(numKeys))
	if errText != "" {
		w.Error(errText)
		return
	}

	result := combineZStores(operands, weights, agg, inter)

	// Validate every RESULT score against the DynamoDB Number domain BEFORE touching
	// dest. A WEIGHTS multiplier or SUM aggregate can push a result to ±inf or past
	// the domain (e.g. WEIGHTS inf, or summing near-max scores); those are unstorable.
	// Rejecting here — ahead of the DeleteMeta/DeleteMembers below — keeps *STORE
	// all-or-nothing instead of wiping dest and then failing the backend write. Redis
	// stores such scores; redimos cannot (documented §4.1 platform limit).
	for _, score := range result {
		if e := checkScoreDomain(score); e != "" {
			w.Error(e)
			return
		}
	}

	// Guard the members that will be stored (dest key name + each result member).
	members := make([]storage.ZMember, 0, len(result))
	memberBytes := make([][]byte, 0, len(result))
	for member, score := range result {
		members = append(members, storage.ZMember{Member: member, Score: score})
		memberBytes = append(memberBytes, []byte(member))
	}
	if err := guard.CheckWrite(destKey, memberBytes, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}

	// Replace dest entirely: drop its meta (clears any prior type) and reclaim any
	// existing members, matching the *STORE overwrite-regardless-of-type semantics.
	if _, err := r.Storage.Meta.DeleteMeta(ctx, destPK); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if _, err := r.Storage.Store.DeleteMembers(ctx, destPK); err != nil {
		r.writeStoreError(c, err)
		return
	}

	// An empty result leaves dest deleted (an empty sorted set does not exist) and
	// replies 0.
	if len(members) == 0 {
		w.Int(0)
		return
	}

	// Create dest as a fresh Sorted Set and add the result members, maintaining cnt.
	if err := r.ensureTypeExpiring(ctx, destPK, meta.TypeZSet); err != nil {
		r.writeStoreError(c, err)
		return
	}
	added, err := r.Storage.Store.ZAdd(ctx, destPK, members)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, destPK, meta.TypeZSet, int64(added)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(added))
}

// errZWeightNotFloat is Redis' getDoubleFromObjectOrReply message for a
// non-numeric WEIGHTS value (zunionInterGenericCommand).
const errZWeightNotFloat = "ERR weight value is not a float"

// parseZStoreOptions parses the optional trailing tokens of ZUNIONSTORE /
// ZINTERSTORE: a "WEIGHTS w1 .. wN" clause (exactly numKeys floats, defaulting to
// all 1.0 when absent) and an "AGGREGATE SUM|MIN|MAX" clause (defaulting to SUM).
// errText is "" on success; otherwise it is the exact Redis error body — a
// structural problem (too few weight tokens, a bad AGGREGATE value, an unexpected
// token) is a syntax error, but a present-but-non-numeric weight is the specific
// "weight value is not a float" (getDoubleFromObjectOrReply).
func parseZStoreOptions(tail [][]byte, numKeys int) (weights []float64, agg zAggregate, errText string) {
	weights = make([]float64, numKeys)
	for i := range weights {
		weights[i] = 1
	}
	agg = zAggSum

	i := 0
	for i < len(tail) {
		switch strings.ToUpper(string(tail[i])) {
		case "WEIGHTS":
			// The WEIGHTS token only matches when at least numKeys values follow;
			// otherwise Redis falls through to a syntax error.
			if len(tail)-(i+1) < numKeys {
				return nil, agg, resp.ErrSyntax
			}
			for j := 0; j < numKeys; j++ {
				f, valid := parseScore(tail[i+1+j])
				if !valid {
					return nil, agg, errZWeightNotFloat
				}
				weights[j] = f
			}
			i += 1 + numKeys
		case "AGGREGATE":
			if i+1 >= len(tail) {
				return nil, agg, resp.ErrSyntax
			}
			switch strings.ToUpper(string(tail[i+1])) {
			case "SUM":
				agg = zAggSum
			case "MIN":
				agg = zAggMin
			case "MAX":
				agg = zAggMax
			default:
				return nil, agg, resp.ErrSyntax
			}
			i += 2
		default:
			return nil, agg, resp.ErrSyntax
		}
	}

	return weights, agg, ""
}

// loadZStoreOperand reads an operand key of ZUNIONSTORE / ZINTERSTORE into a
// member->score map. A Sorted Set contributes its members' scores; a plain Set
// contributes each member with score 1; an absent/expired key contributes the
// empty map. wrongType is true (and the caller replies WRONGTYPE) when the key is
// live but neither a Sorted Set nor a Set.
func (r *Router) loadZStoreOperand(ctx context.Context, pk string) (scores map[string]float64, wrongType bool, err error) {
	m, found, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		return nil, false, err
	}
	if !found || meta.IsExpired(m, r.now()) {
		return map[string]float64{}, false, nil
	}

	// v1 line: redimo v1.6.1 has no type tag (LoadMeta returns an empty Type), so we
	// cannot dispatch on m.Type to tell a Sorted Set operand from a plain Set. Instead
	// we probe: a Sorted Set stores each member with a numeric score in the score
	// index, so ZRangeByRank (an ordered read over that index) returns its members
	// with scores; a plain Set's members carry no score and are absent from that index,
	// so ZRangeByRank comes back empty and we re-read the raw members via SMembers,
	// contributing each with the Redis Set-as-zset score of 1. WRONGTYPE on a
	// non-collection operand is UNENFORCEABLE on the v1 line, so wrongType is never
	// set; a live String key simply contributes its single value item as a member.
	members, rerr := r.Storage.Store.ZRangeByRank(ctx, pk, 0, -1, false)
	if rerr != nil {
		return nil, false, rerr
	}
	if len(members) > 0 {
		out := make(map[string]float64, len(members))
		for _, zm := range members {
			out[zm.Member] = zm.Score
		}
		return out, false, nil
	}

	setMembers, rerr := r.Storage.Store.SMembers(ctx, pk)
	if rerr != nil {
		return nil, false, rerr
	}
	out := make(map[string]float64, len(setMembers))
	for _, member := range setMembers {
		out[member] = 1
	}
	return out, false, nil
}

// combineZStores computes the ZUNIONSTORE (inter=false) or ZINTERSTORE
// (inter=true) result over the operand score maps, applying each operand's weight
// and the aggregate. For a union every member of any operand is included; for an
// intersection only members present in EVERY operand survive.
func combineZStores(operands []map[string]float64, weights []float64, agg zAggregate, inter bool) map[string]float64 {
	result := make(map[string]float64)
	if len(operands) == 0 {
		return result
	}

	if inter {
		// Seed from the first operand, then require every later operand to contain
		// the member, aggregating its weighted score.
		for member, score := range operands[0] {
			acc := weightScore(score, weights[0])
			inAll := true
			for i := 1; i < len(operands); i++ {
				s, present := operands[i][member]
				if !present {
					inAll = false
					break
				}
				acc = aggregateScore(agg, acc, weightScore(s, weights[i]))
			}
			if inAll {
				result[member] = acc
			}
		}
		return result
	}

	// Union: fold every operand's weighted scores into the accumulator.
	for i, op := range operands {
		for member, score := range op {
			weighted := weightScore(score, weights[i])
			if acc, seen := result[member]; seen {
				result[member] = aggregateScore(agg, acc, weighted)
			} else {
				result[member] = weighted
			}
		}
	}
	return result
}
