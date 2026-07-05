package command

import (
	"context"
	"math"
	"strconv"
	"strings"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This file implements the Sorted Set command family: ZADD/ZREM/ZSCORE/ZINCRBY/
// ZCOUNT/ZRANGE/ZREVRANGE/ZRANGEBYSCORE/ZREVRANGEBYSCORE/ZRANK/ZREVRANK/
// ZREMRANGEBYRANK/ZREMRANGEBYSCORE and the O(1) ZCARD counter (requirements 9.1,
// 9.2, 9.7). It follows the collection pattern the Hash (task 13.1) and Set (task
// 14.1) families established.
//
// Data model: each member is an independent item under the key's pk with the
// member value as the sort key (sk = member) and the member's score in the
// numeric sort-key attribute (skN), which the score index orders on so the
// range/rank reads come back in score order (requirement 9.1); ties on equal
// score fall back to member order. The key's member count lives in the meta
// item's cnt attribute, maintained by the meta conditional write's atomic ADD, so
// ZCARD is O(1) (requirement 9.2) and always equals the current member count
// (requirement 9.7).
//
// Collection write-path pattern (shared with the Hash/Set families via
// datacmd.go):
//
//	guard.CheckWrite(key, members, nil)          // size limits, no partial write
//	  -> Meta.EnsureType(TypeZSet, 0)            // create key + type check (WRONGTYPE)
//	  -> Store.Z<op>(...)                         // per-member mutation; returns net member delta
//	  -> r.adjustCount(pk, TypeZSet, delta)       // atomic cnt maintenance, deletes key when empty
//
// EnsureType runs with a zero cnt delta FIRST, purely for the type check and key
// creation, so a wrong-type key is rejected with WRONGTYPE before any member item
// is written (requirement 3.6, 11.2). The count is then adjusted by the NET
// number of members the mutation actually created or removed, keeping meta.cnt
// exactly equal to the member count regardless of how many supplied members
// already existed. adjustCount deletes the key when a removal empties it, matching
// Redis (an empty sorted set does not exist).
//
// Score precision: Redis scores are IEEE754 doubles while DynamoDB Number is a
// 38-digit decimal, so extreme values can differ; that differential coverage is
// task 15.3. Here scores are parsed with parseScore (accepting inf/-inf/+inf) and
// formatted with formatScore consistently on the way in and out.
//
// ZSCAN, ZRANGEBYLEX and ZUNIONSTORE/ZINTERSTORE are task 15.2 and are
// registered by registerZSets alongside the base family. ZSCAN pages a single pk
// (requirement 9.3), the ZRANGEBYLEX family filters lexicographically (requirement
// 9.4), and ZUNIONSTORE/ZINTERSTORE combine operands in proxy memory as an
// explicitly NON-ATOMIC snapshot (requirement 9.5).

// registerZSets installs the Sorted Set command family on the router's table. It
// is invoked from registerDataCommands (router_storage.go). Arity counts include
// the command name; the mutating commands are marked Write.
func (r *Router) registerZSets() {
	r.reg("ZADD", -4, true, r.handleZAdd)
	r.reg("ZREM", -3, true, r.handleZRem)
	r.reg("ZSCORE", 3, false, r.handleZScore)
	r.reg("ZINCRBY", 4, true, r.handleZIncrBy)
	r.reg("ZCARD", 2, false, r.handleZCard)
	r.reg("ZCOUNT", 4, false, r.handleZCount)
	r.reg("ZRANGE", -4, false, r.handleZRange)
	r.reg("ZREVRANGE", -4, false, r.handleZRevRange)
	r.reg("ZRANGEBYSCORE", -4, false, r.handleZRangeByScore)
	r.reg("ZREVRANGEBYSCORE", -4, false, r.handleZRevRangeByScore)
	r.reg("ZRANK", 3, false, r.handleZRank)
	r.reg("ZREVRANK", 3, false, r.handleZRevRank)
	r.reg("ZREMRANGEBYRANK", 4, true, r.handleZRemRangeByRank)
	r.reg("ZREMRANGEBYSCORE", 4, true, r.handleZRemRangeByScore)
	// Task 15.2: single-pk scan, lexicographic range, and the in-memory store ops.
	r.reg("ZSCAN", -3, false, r.handleZScan)
	// ZRANGEBYLEX/ZREVRANGEBYLEX take an optional "LIMIT offset count" clause, so
	// their arity is variadic (-4), matching Redis 3.2.
	r.reg("ZRANGEBYLEX", -4, false, r.handleZRangeByLex)
	r.reg("ZREVRANGEBYLEX", -4, false, r.handleZRevRangeByLex)
	r.reg("ZLEXCOUNT", 4, false, r.handleZLexCount)
	r.reg("ZREMRANGEBYLEX", 4, true, r.handleZRemRangeByLex)
	r.reg("ZUNIONSTORE", -4, true, r.handleZUnionStore)
	r.reg("ZINTERSTORE", -4, true, r.handleZInterStore)
}

// errNotValidFloat is the Redis reply for a ZADD score / ZINCRBY increment that is
// not a valid float.
const errNotValidFloat = "ERR value is not a valid float"

// ZADD flag errors, matching Redis 3.2 verbatim.
const (
	errZaddNxXx = "ERR XX and NX options at the same time are not compatible"
	errZaddIncr = "ERR INCR option supports a single increment-element pair"
)

// errMinOrMaxNotFloat is the Redis reply when a ZRANGEBYSCORE / ZCOUNT min or max
// bound does not parse as a float.
const errMinOrMaxNotFloat = "ERR min or max is not a float"

// zsetState is the outcome of loading a key's meta for a Sorted Set command:
// whether it is a live Sorted Set, whether it is live but a different type
// (WRONGTYPE), and the loaded meta (valid only when the key is a live Sorted Set).
// An absent or expired key reports live=false, wrongType=false — a Sorted Set read
// then behaves as if the key were an empty sorted set.
func (r *Router) zsetState(ctx context.Context, pk string) (m meta.Meta, live, wrongType bool, err error) {
	m, found, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		return meta.Meta{}, false, false, err
	}
	if !found || meta.IsExpired(m, r.now()) {
		return meta.Meta{}, false, false, nil
	}
	if m.Type != meta.TypeZSet {
		return m, false, true, nil
	}

	return m, true, false, nil
}

// parseScore parses a ZADD score / ZINCRBY increment as an IEEE754 double,
// accepting Redis' inf/-inf/+inf spellings and rejecting NaN. ok=false signals the
// not-a-valid-float reply.
func parseScore(arg []byte) (float64, bool) {
	switch string(arg) {
	case "inf", "+inf", "Inf", "+Inf", "INF", "+INF":
		return math.Inf(1), true
	case "-inf", "-Inf", "-INF":
		return math.Inf(-1), true
	}

	// The finite case shares the canonical ParseFloat (NaN-rejecting, whole-string).
	f, err := ParseFloat(arg)
	return f, err == nil
}

// parseScoreBound parses a ZRANGEBYSCORE / ZCOUNT bound: a leading '(' selects the
// exclusive (open) interval, and the remainder is a float accepting inf/-inf/+inf.
// ok=false signals the min-or-max-not-a-float reply.
func parseScoreBound(arg []byte) (storage.ScoreBound, bool) {
	exclusive := false
	if len(arg) > 0 && arg[0] == '(' {
		exclusive = true
		arg = arg[1:]
	}

	f, ok := parseScore(arg)
	if !ok {
		return storage.ScoreBound{}, false
	}

	return storage.ScoreBound{Value: f, Exclusive: exclusive}, true
}

// formatScore renders a score the way Redis formats ZSCORE / ZINCRBY / WITHSCORES
// replies: "inf"/"-inf" for the infinities, otherwise up to 17 significant digits
// with trailing zeros trimmed (so an integral score like 3.0 renders "3"). This
// matches the fork's 17-digit score encoding.
func formatScore(score float64) []byte {
	switch {
	case math.IsInf(score, 1):
		return []byte("inf")
	case math.IsInf(score, -1):
		return []byte("-inf")
	}

	return []byte(strconv.FormatFloat(score, 'g', 17, 64))
}

// zMembersReply renders a list of members as a bulk-string array; when withScores
// is set each member is followed by its formatted score (the ZRANGE WITHSCORES
// wire shape). An empty list renders the empty array.
func zMembersReply(w *resp.Writer, members []storage.ZMember, withScores bool) {
	if !withScores {
		out := make([][]byte, len(members))
		for i, m := range members {
			out[i] = []byte(m.Member)
		}
		w.BulkArray(out)
		return
	}

	out := make([][]byte, 0, len(members)*2)
	for _, m := range members {
		out = append(out, []byte(m.Member), formatScore(m.Score))
	}
	w.BulkArray(out)
}

// handleZAdd implements ZADD key score member [score member ...] (requirements
// 9.1, 9.7): add/update the score/member pairs and reply the integer number of
// NEWLY added members (score updates do not count). cnt is bumped by that same
// number so ZCARD stays equal to the member count. A live non-ZSet key replies
// WRONGTYPE; a bad score replies the not-a-valid-float error before any write.
func (r *Router) handleZAdd(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key := args[1]
	rest := args[2:]

	// Parse the optional leading flags: [NX|XX] [CH] [INCR] (Redis 3.2 set; GT/LT are 6.2+).
	var nx, xx, ch, incr bool
	i := 0
flags:
	for ; i < len(rest); i++ {
		switch strings.ToUpper(string(rest[i])) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "CH":
			ch = true
		case "INCR":
			incr = true
		default:
			break flags
		}
	}
	if nx && xx {
		w.Error(errZaddNxXx)
		return
	}
	pairs := rest[i:]
	// score/member pairs => the argument count after the flags must be even and non-empty.
	if len(pairs) == 0 || len(pairs)%2 != 0 {
		w.Error(resp.ErrSyntax)
		return
	}
	if incr && len(pairs) != 2 {
		w.Error(errZaddIncr)
		return
	}

	pk := encodePK(c.DB(), key)

	if incr {
		r.zaddIncr(ctx, c, w, key, pk, nx, xx, pairs)
		return
	}

	// No flags: the original fast path (parse all pairs, bulk add, return #added).
	if !nx && !xx && !ch {
		members := make([]storage.ZMember, 0, len(pairs)/2)
		memberBytes := make([][]byte, 0, len(pairs)/2)
		for j := 0; j < len(pairs); j += 2 {
			score, ok := parseScore(pairs[j])
			if !ok {
				w.Error(errNotValidFloat)
				return
			}
			members = append(members, storage.ZMember{Member: string(pairs[j+1]), Score: score})
			memberBytes = append(memberBytes, pairs[j+1])
		}
		if err := guard.CheckWrite(key, memberBytes, nil); err != nil {
			r.writeStoreError(c, err)
			return
		}
		if _, err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeZSet, 0); err != nil {
			r.writeStoreError(c, err)
			return
		}
		added, err := r.Storage.Store.ZAdd(ctx, pk, members)
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if err := r.adjustCount(ctx, pk, meta.TypeZSet, int64(added)); err != nil {
			r.writeStoreError(c, err)
			return
		}
		w.Int(int64(added))
		return
	}

	// Flag path (NX/XX/CH): process pairs LEFT-TO-RIGHT against an in-command view of scores,
	// exactly like Redis. A member repeated in one call is ADDED by its first applied pair and
	// then UPDATED by later ones, so CH counts each change (a later duplicate can bump CH even
	// though the member is only added once). Each member's score is read from the store at most
	// once, then tracked locally as prior pairs mutate it.
	scoreOf := make(map[string]float64, len(pairs)/2)
	hasMember := make(map[string]bool, len(pairs)/2)
	loaded := make(map[string]bool, len(pairs)/2)
	final := make(map[string]float64, len(pairs)/2)
	writeOrder := make([]string, 0, len(pairs)/2)
	memberBytes := make([][]byte, 0, len(pairs)/2)
	added, changed := 0, 0

	for j := 0; j < len(pairs); j += 2 {
		score, ok := parseScore(pairs[j])
		if !ok {
			w.Error(errNotValidFloat)
			return
		}
		mb := pairs[j+1]
		m := string(mb)
		if !loaded[m] {
			cur, found, err := r.Storage.Store.ZScore(ctx, pk, m)
			if err != nil {
				r.writeStoreError(c, err)
				return
			}
			scoreOf[m], hasMember[m], loaded[m] = cur, found, true
			memberBytes = append(memberBytes, mb)
		}
		if (nx && hasMember[m]) || (xx && !hasMember[m]) {
			continue // gated out; the in-command view is unchanged
		}
		if !hasMember[m] {
			added++
			changed++
			hasMember[m] = true
		} else if score != scoreOf[m] {
			changed++
		}
		scoreOf[m] = score
		if _, seen := final[m]; !seen {
			writeOrder = append(writeOrder, m)
		}
		final[m] = score
	}

	if err := guard.CheckWrite(key, memberBytes, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}

	if len(writeOrder) > 0 {
		if _, err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeZSet, 0); err != nil {
			r.writeStoreError(c, err)
			return
		}
		toWrite := make([]storage.ZMember, len(writeOrder))
		for idx, m := range writeOrder {
			toWrite[idx] = storage.ZMember{Member: m, Score: final[m]}
		}
		if _, err := r.Storage.Store.ZAdd(ctx, pk, toWrite); err != nil {
			r.writeStoreError(c, err)
			return
		}
		if added > 0 {
			if err := r.adjustCount(ctx, pk, meta.TypeZSet, int64(added)); err != nil {
				r.writeStoreError(c, err)
				return
			}
		}
	}

	if ch {
		w.Int(int64(changed))
	} else {
		w.Int(int64(added))
	}
}

// zaddIncr implements ZADD ... INCR (a single score/member pair): it behaves like ZINCRBY but
// honours NX/XX and returns the new score, or a nil bulk when the NX/XX condition blocks the
// update.
func (r *Router) zaddIncr(ctx context.Context, c *server.Conn, w *resp.Writer, key []byte, pk string, nx, xx bool, pairs [][]byte) {
	delta, ok := parseScore(pairs[0])
	if !ok {
		w.Error(errNotValidFloat)
		return
	}
	member := pairs[1]
	if err := guard.CheckWrite(key, [][]byte{member}, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if nx || xx {
		_, found, err := r.Storage.Store.ZScore(ctx, pk, string(member))
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if (nx && found) || (xx && !found) {
			w.NullBulk()
			return
		}
	}
	if _, err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeZSet, 0); err != nil {
		r.writeStoreError(c, err)
		return
	}
	newScore, isNew, err := r.Storage.Store.ZIncrBy(ctx, pk, string(member), delta)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if isNew {
		if err := r.adjustCount(ctx, pk, meta.TypeZSet, 1); err != nil {
			r.writeStoreError(c, err)
			return
		}
	}
	w.BulkString(formatScore(newScore))
}

// handleZRem implements ZREM key member [member ...] (requirements 9.1, 9.7):
// remove the given members and reply the integer count that actually existed and
// were removed. cnt is decremented by that count, and the key is deleted when its
// last member is removed. An absent/expired key replies ":0"; a live non-ZSet key
// replies WRONGTYPE.
func (r *Router) handleZRem(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])
	members := bytesToStrings(args[2:])

	_, live, wrongType, err := r.zsetState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.Int(0)
		return
	}

	removed, err := r.Storage.Store.ZRem(ctx, pk, members)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeZSet, -int64(removed)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(removed))
}

// handleZScore implements ZSCORE key member (requirement 9.1): reply the member's
// score as a bulk string, or the null bulk string when the member (or key) is
// absent. A live non-ZSet key replies WRONGTYPE.
func (r *Router) handleZScore(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	_, live, wrongType, err := r.zsetState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.NullBulk()
		return
	}

	score, found, err := r.Storage.Store.ZScore(ctx, pk, string(args[2]))
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !found {
		w.NullBulk()
		return
	}

	w.BulkString(formatScore(score))
}

// handleZIncrBy implements ZINCRBY key increment member (requirement 9.1, 9.7):
// add the increment to the member's score (initialising a missing member to 0),
// reply the new score as a bulk string, and bump cnt when the member is new. A bad
// increment replies the not-a-valid-float error before any write; a live non-ZSet
// key replies WRONGTYPE.
func (r *Router) handleZIncrBy(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key := args[1]

	delta, ok := parseScore(args[2])
	if !ok {
		w.Error(errNotValidFloat)
		return
	}
	member := args[3]

	pk := encodePK(c.DB(), key)
	if err := guard.CheckWrite(key, [][]byte{member}, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if _, err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeZSet, 0); err != nil {
		r.writeStoreError(c, err)
		return
	}

	newScore, isNew, err := r.Storage.Store.ZIncrBy(ctx, pk, string(member), delta)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if isNew {
		if err := r.adjustCount(ctx, pk, meta.TypeZSet, 1); err != nil {
			r.writeStoreError(c, err)
			return
		}
	}

	w.BulkString(formatScore(newScore))
}

// handleZCard implements ZCARD key (requirements 9.2, 9.7): reply the member count
// in O(1) from meta.cnt. An absent/expired key replies ":0"; a live non-ZSet key
// replies WRONGTYPE.
func (r *Router) handleZCard(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	m, live, wrongType, err := r.zsetState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.Int(0)
		return
	}

	w.Int(m.Count)
}

// handleZCount implements ZCOUNT key min max (requirement 9.1): reply the integer
// number of members whose score falls within [min, max] (each bound may be
// exclusive via '(' or an infinity). An absent/expired key replies ":0"; a live
// non-ZSet key replies WRONGTYPE; a bad bound replies the min-or-max error.
func (r *Router) handleZCount(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	min, ok := parseScoreBound(args[2])
	if !ok {
		w.Error(errMinOrMaxNotFloat)
		return
	}
	max, ok := parseScoreBound(args[3])
	if !ok {
		w.Error(errMinOrMaxNotFloat)
		return
	}

	_, live, wrongType, err := r.zsetState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.Int(0)
		return
	}

	count, err := r.Storage.Store.ZCount(ctx, pk, min, max)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(count))
}

// handleZRange implements ZRANGE key start stop [WITHSCORES] (requirement 9.1):
// reply the members in the inclusive rank range, ascending by score. See
// handleZRangeByRank.
func (r *Router) handleZRange(ctx context.Context, c *server.Conn, args [][]byte) {
	r.handleZRangeByRank(ctx, c, args, false)
}

// handleZRevRange implements ZREVRANGE key start stop [WITHSCORES] (requirement
// 9.1): reply the members in the inclusive rank range, descending by score.
func (r *Router) handleZRevRange(ctx context.Context, c *server.Conn, args [][]byte) {
	r.handleZRangeByRank(ctx, c, args, true)
}

// handleZRangeByRank is the shared implementation of ZRANGE / ZREVRANGE. It parses
// the start/stop rank indices and an optional WITHSCORES flag, then replies the
// members in the requested direction. An absent/expired key replies the empty
// array; a live non-ZSet key replies WRONGTYPE.
func (r *Router) handleZRangeByRank(ctx context.Context, c *server.Conn, args [][]byte, rev bool) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	start, err := ParseInt(args[2])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}
	stop, err := ParseInt(args[3])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}

	withScores, ok := parseWithScores(args[4:])
	if !ok {
		w.Error(resp.ErrSyntax)
		return
	}

	_, live, wrongType, serr := r.zsetState(ctx, pk)
	if serr != nil {
		r.writeStoreError(c, serr)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.EmptyArray()
		return
	}

	members, merr := r.Storage.Store.ZRangeByRank(ctx, pk, int(start), int(stop), rev)
	if merr != nil {
		r.writeStoreError(c, merr)
		return
	}

	zMembersReply(w, members, withScores)
}

// handleZRangeByScore implements ZRANGEBYSCORE key min max [WITHSCORES]
// (requirement 9.1): reply the members whose score falls within [min, max],
// ascending by score.
func (r *Router) handleZRangeByScore(ctx context.Context, c *server.Conn, args [][]byte) {
	r.zRangeByScore(ctx, c, args, false)
}

// handleZRevRangeByScore implements ZREVRANGEBYSCORE key max min [WITHSCORES]
// (requirement 9.1): reply the members whose score falls within [min, max],
// descending by score. Note the reversed max/min argument order.
func (r *Router) handleZRevRangeByScore(ctx context.Context, c *server.Conn, args [][]byte) {
	r.zRangeByScore(ctx, c, args, true)
}

// zRangeByScore is the shared implementation of ZRANGEBYSCORE / ZREVRANGEBYSCORE.
// For the forward form the bounds are (min, max); for the reverse form Redis takes
// them as (max, min), so they are swapped before the range read. An optional
// WITHSCORES flag interleaves scores. An absent/expired key replies the empty
// array; a live non-ZSet key replies WRONGTYPE; a bad bound replies the
// min-or-max error.
func (r *Router) zRangeByScore(ctx context.Context, c *server.Conn, args [][]byte, rev bool) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	first, ok := parseScoreBound(args[2])
	if !ok {
		w.Error(errMinOrMaxNotFloat)
		return
	}
	second, ok := parseScoreBound(args[3])
	if !ok {
		w.Error(errMinOrMaxNotFloat)
		return
	}

	min, max := first, second
	if rev {
		// ZREVRANGEBYSCORE takes its bounds as (max, min).
		min, max = second, first
	}

	withScores, ok := parseWithScores(args[4:])
	if !ok {
		w.Error(resp.ErrSyntax)
		return
	}

	_, live, wrongType, serr := r.zsetState(ctx, pk)
	if serr != nil {
		r.writeStoreError(c, serr)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.EmptyArray()
		return
	}

	members, merr := r.Storage.Store.ZRangeByScore(ctx, pk, min, max, rev)
	if merr != nil {
		r.writeStoreError(c, merr)
		return
	}

	zMembersReply(w, members, withScores)
}

// parseWithScores interprets the optional trailing tokens of a ZRANGE-family
// command: an empty tail means no scores; a single "WITHSCORES" (any case) sets
// the flag; anything else is a syntax error (ok=false). LIMIT is task 15.2.
func parseWithScores(tail [][]byte) (withScores, ok bool) {
	if len(tail) == 0 {
		return false, true
	}
	if len(tail) == 1 && equalFold(tail[0], "WITHSCORES") {
		return true, true
	}

	return false, false
}

// equalFold reports whether b equals s ignoring ASCII case. It avoids allocating
// for the small option tokens the ZSet handlers compare.
func equalFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		bc := b[i]
		if bc >= 'A' && bc <= 'Z' {
			bc += 'a' - 'A'
		}
		sc := s[i]
		if sc >= 'A' && sc <= 'Z' {
			sc += 'a' - 'A'
		}
		if bc != sc {
			return false
		}
	}

	return true
}

// handleZRank implements ZRANK key member (requirement 9.1): reply the member's
// 0-based ascending rank as an integer, or the null bulk string when the member
// (or key) is absent. A live non-ZSet key replies WRONGTYPE.
func (r *Router) handleZRank(ctx context.Context, c *server.Conn, args [][]byte) {
	r.handleZRankDir(ctx, c, args, false)
}

// handleZRevRank implements ZREVRANK key member (requirement 9.1): reply the
// member's 0-based descending rank, or the null bulk string when absent.
func (r *Router) handleZRevRank(ctx context.Context, c *server.Conn, args [][]byte) {
	r.handleZRankDir(ctx, c, args, true)
}

// handleZRankDir is the shared implementation of ZRANK / ZREVRANK.
func (r *Router) handleZRankDir(ctx context.Context, c *server.Conn, args [][]byte, rev bool) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	_, live, wrongType, err := r.zsetState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.NullBulk()
		return
	}

	rank, found, rerr := r.Storage.Store.ZRank(ctx, pk, string(args[2]), rev)
	if rerr != nil {
		r.writeStoreError(c, rerr)
		return
	}
	if !found {
		w.NullBulk()
		return
	}

	w.Int(int64(rank))
}

// handleZRemRangeByRank implements ZREMRANGEBYRANK key start stop (requirements
// 9.1, 9.7): remove the members in the inclusive rank range and reply the count
// removed. cnt is decremented accordingly and the key deleted when emptied. An
// absent/expired key replies ":0"; a live non-ZSet key replies WRONGTYPE.
func (r *Router) handleZRemRangeByRank(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	start, err := ParseInt(args[2])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}
	stop, err := ParseInt(args[3])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}

	_, live, wrongType, serr := r.zsetState(ctx, pk)
	if serr != nil {
		r.writeStoreError(c, serr)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.Int(0)
		return
	}

	removed, rerr := r.Storage.Store.ZRemRangeByRank(ctx, pk, int(start), int(stop))
	if rerr != nil {
		r.writeStoreError(c, rerr)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeZSet, -int64(removed)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(removed))
}

// handleZRemRangeByScore implements ZREMRANGEBYSCORE key min max (requirements
// 9.1, 9.7): remove the members whose score falls within [min, max] and reply the
// count removed. cnt is decremented accordingly and the key deleted when emptied.
// An absent/expired key replies ":0"; a live non-ZSet key replies WRONGTYPE; a bad
// bound replies the min-or-max error.
func (r *Router) handleZRemRangeByScore(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	min, ok := parseScoreBound(args[2])
	if !ok {
		w.Error(errMinOrMaxNotFloat)
		return
	}
	max, ok := parseScoreBound(args[3])
	if !ok {
		w.Error(errMinOrMaxNotFloat)
		return
	}

	_, live, wrongType, err := r.zsetState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.Int(0)
		return
	}

	removed, rerr := r.Storage.Store.ZRemRangeByScore(ctx, pk, min, max)
	if rerr != nil {
		r.writeStoreError(c, rerr)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeZSet, -int64(removed)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(removed))
}

// errNotValidStringRange is the Redis reply for a ZRANGEBYLEX / ZREVRANGEBYLEX
// min or max bound that is not a valid lexicographic range item (i.e. does not
// start with '[' or '(' and is not the bare '-' / '+').
const errNotValidStringRange = "ERR min or max not valid string range item"

// errAtLeastOneKey is the Redis reply for a ZUNIONSTORE / ZINTERSTORE whose
// numkeys is not a positive integer.
const errAtLeastOneKey = "ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE"

// handleZScan implements ZSCAN key cursor [MATCH pattern] [COUNT n] (requirement
// 9.3). It is the Sorted Set-scoped analogue of SCAN (see scan.go): where SCAN
// pages the WHOLE keyspace, ZSCAN pages WITHIN a single pk — the members of one
// sorted set — via Store.ZScan (a partition Query), REUSING the exact same cursor
// machinery HSCAN/SSCAN reuse. The uint64 cursor bridges to the backend's opaque
// pagination token through the per-instance SCAN registry (r.Storage.Scan), MATCH
// is applied proxy-side to the MEMBER name (glob.go), and COUNT maps to the Query
// page limit.
//
// Cursor lifecycle mirrors SCAN: "ZSCAN key 0" starts a fresh page; a non-zero
// cursor must be an own-instance registry entry, and a miss (LRU eviction,
// instance restart, or a cursor minted elsewhere) replies "-ERR invalid cursor,
// restart scan"; the terminating page carries cursor "0", otherwise the next
// page's token is registered under a fresh cursor.
//
// The reply is the two-element array [cursor, [member1, score1, member2, score2,
// ...]] — the inner array interleaves each member with its formatted score,
// matching Redis/Pika. A wrong-type key replies WRONGTYPE; an absent or expired
// key replies the terminating ["0", []] (an empty, non-null inner array), exactly
// as ZRANGE treats an absent key as an empty sorted set.
func (r *Router) handleZScan(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	// The cursor is a Redis uint64. A value that does not parse is treated as an
	// invalid cursor (the "restart scan" contract), not a syntax error, matching
	// SCAN.
	cursor, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil {
		w.Error(resp.ErrInvalidCursor)
		return
	}

	// Optional [MATCH pattern] [COUNT n] pairs, in any order.
	var (
		pattern  []byte
		hasMatch bool
		limit    int32
	)
	opts := args[3:]
	if len(opts)%2 != 0 {
		w.Error(resp.ErrSyntax)
		return
	}
	for i := 0; i+1 < len(opts); i += 2 {
		switch strings.ToUpper(string(opts[i])) {
		case "MATCH":
			pattern = opts[i+1]
			hasMatch = true
		case "COUNT":
			n, err := strconv.Atoi(string(opts[i+1]))
			if err != nil {
				w.Error(resp.ErrNotInteger)
				return
			}
			if n < 1 {
				w.Error(resp.ErrSyntax)
				return
			}
			limit = int32(n)
		default:
			w.Error(resp.ErrSyntax)
			return
		}
	}

	// Type / existence check via meta: a live non-ZSet key is WRONGTYPE; an
	// absent/expired key behaves as an empty sorted set and replies the terminating
	// ["0", []].
	_, live, wrongType, err := r.zsetState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		writeZScanReply(c, "0", nil)
		return
	}

	// Resolve the pagination token. Cursor 0 starts a fresh page (nil token); any
	// other cursor must be a live, own-instance entry in the registry.
	var lek map[string]types.AttributeValue
	if cursor != 0 {
		l, ok := r.Storage.Scan.LoadOwned(cursor, c.InstID())
		if !ok {
			w.Error(resp.ErrInvalidCursor)
			return
		}
		lek = l
	}

	members, nextLEK, err := r.Storage.Store.ZScan(ctx, pk, lek, limit)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	// Flatten the page into interleaved member/score pairs, applying the MATCH
	// filter proxy-side on the member name. out is a non-nil slice, so an empty
	// filtered page still encodes as an empty array, never the null array.
	out := make([][]byte, 0, len(members)*2)
	for _, m := range members {
		if hasMatch && !globMatch(pattern, []byte(m.Member)) {
			continue
		}
		out = append(out, []byte(m.Member), formatScore(m.Score))
	}

	// A nil next token means the partition has been fully paged → terminating
	// cursor "0". Otherwise register the token under a fresh cursor.
	cursorOut := "0"
	if nextLEK != nil {
		cursorOut = strconv.FormatUint(r.Storage.Scan.Save(nextLEK), 10)
	}

	writeZScanReply(c, cursorOut, out)
}

// writeZScanReply writes the two-element ZSCAN array [cursor, [pairs...]]. A nil
// pairs slice is normalized to a non-nil empty slice so the inner array always
// encodes as "*0" (empty array), never the null array "*-1", matching Redis/Pika.
func writeZScanReply(c *server.Conn, cursor string, pairs [][]byte) {
	if pairs == nil {
		pairs = [][]byte{}
	}
	buf := resp.AppendArrayHeader(nil, 2)
	buf = resp.AppendBulkString(buf, []byte(cursor))
	buf = resp.AppendBulkArray(buf, pairs)
	c.Redcon().WriteRaw(buf)
}

// lexBound is one end of a ZRANGEBYLEX / ZREVRANGEBYLEX lexicographic interval.
// negInf marks the '-' (smallest possible string) end and posInf the '+' (largest
// possible string) end; otherwise value is the comparison string and inclusive
// selects the '[' (closed) vs '(' (open) semantics. Redis' lex commands assume all
// members share the same score and order purely by member value, so the bounds
// compare member strings directly.
type lexBound struct {
	value     string
	inclusive bool
	negInf    bool
	posInf    bool
}

// parseLexBound parses a ZRANGEBYLEX / ZREVRANGEBYLEX bound: the bare "-" is
// negative infinity, "+" is positive infinity, a "[value" prefix is an inclusive
// bound and "(value" an exclusive bound. Any other spelling (including a bare
// value without a marker) is invalid (ok=false → the not-valid-string-range
// reply), matching Redis.
func parseLexBound(arg []byte) (lexBound, bool) {
	if len(arg) == 0 {
		return lexBound{}, false
	}
	switch arg[0] {
	case '-':
		if len(arg) == 1 {
			return lexBound{negInf: true}, true
		}
		return lexBound{}, false
	case '+':
		if len(arg) == 1 {
			return lexBound{posInf: true}, true
		}
		return lexBound{}, false
	case '[':
		return lexBound{value: string(arg[1:]), inclusive: true}, true
	case '(':
		return lexBound{value: string(arg[1:]), inclusive: false}, true
	default:
		return lexBound{}, false
	}
}

// lexInRange reports whether member falls within the lexicographic interval
// [min, max], honouring each bound's inclusive flag and the -/+ infinities. It is
// the proxy-side lex filter ZRANGEBYLEX / ZREVRANGEBYLEX apply to the score-ordered
// member list (which, for equal scores, is member-ordered).
func lexInRange(member string, min, max lexBound) bool {
	// Lower bound: '-' is unbounded below; '+' as a lower bound admits nothing.
	if min.posInf {
		return false
	}
	if !min.negInf {
		if min.inclusive {
			if member < min.value {
				return false
			}
		} else if member <= min.value {
			return false
		}
	}

	// Upper bound: '+' is unbounded above; '-' as an upper bound admits nothing.
	if max.negInf {
		return false
	}
	if !max.posInf {
		if max.inclusive {
			if member > max.value {
				return false
			}
		} else if member >= max.value {
			return false
		}
	}

	return true
}

// handleZRangeByLex implements ZRANGEBYLEX key min max (requirement 9.4): reply the
// members whose value falls within the lexicographic range [min, max], in
// ascending member order. Redis' lex semantics assume every member shares the same
// score; the members are read in the score index's (score, member) order and the
// lex bounds are applied proxy-side to the member name.
func (r *Router) handleZRangeByLex(ctx context.Context, c *server.Conn, args [][]byte) {
	r.zRangeByLex(ctx, c, args, false)
}

// handleZRevRangeByLex implements ZREVRANGEBYLEX key max min (requirement 9.4):
// reply the members within the lexicographic range in DESCENDING member order.
// Note the reversed max/min argument order, matching Redis.
func (r *Router) handleZRevRangeByLex(ctx context.Context, c *server.Conn, args [][]byte) {
	r.zRangeByLex(ctx, c, args, true)
}

// zRangeByLex is the shared implementation of ZRANGEBYLEX / ZREVRANGEBYLEX. For the
// forward form the bounds are (min, max); for the reverse form Redis takes them as
// (max, min), so they are swapped before filtering and the result is emitted in
// reverse. An absent/expired key replies the empty array; a live non-ZSet key
// replies WRONGTYPE; a malformed bound replies the not-valid-string-range error.
func (r *Router) zRangeByLex(ctx context.Context, c *server.Conn, args [][]byte, rev bool) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	first, ok := parseLexBound(args[2])
	if !ok {
		w.Error(errNotValidStringRange)
		return
	}
	second, ok := parseLexBound(args[3])
	if !ok {
		w.Error(errNotValidStringRange)
		return
	}

	min, max := first, second
	if rev {
		// ZREVRANGEBYLEX takes its bounds as (max, min).
		min, max = second, first
	}

	// Optional "LIMIT offset count" trailer (Redis 3.2).
	offset, count, ok := parseLexLimit(args[4:])
	if !ok {
		w.Error(resp.ErrSyntax)
		return
	}

	_, live, wrongType, serr := r.zsetState(ctx, pk)
	if serr != nil {
		r.writeStoreError(c, serr)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.EmptyArray()
		return
	}

	// Read every member in (score, member) order; the lex filter compares the
	// member name. Using the full-range ascending rank read keeps the ordering
	// identical to the store's score index.
	all, merr := r.Storage.Store.ZRangeByRank(ctx, pk, 0, -1, false)
	if merr != nil {
		r.writeStoreError(c, merr)
		return
	}

	filtered := make([]storage.ZMember, 0, len(all))
	for _, m := range all {
		if lexInRange(m.Member, min, max) {
			filtered = append(filtered, m)
		}
	}
	if rev {
		filtered = storage.ZReverse(filtered)
	}
	filtered = applyZLimit(filtered, offset, count)

	zMembersReply(w, filtered, false)
}

// parseLexLimit parses the optional "LIMIT offset count" trailer of the
// ZRANGEBYLEX family. No trailer means (0, -1) = "all"; any other shape is a
// syntax error.
func parseLexLimit(rest [][]byte) (offset, count int, ok bool) {
	if len(rest) == 0 {
		return 0, -1, true
	}
	if len(rest) != 3 || !strings.EqualFold(string(rest[0]), "LIMIT") {
		return 0, 0, false
	}
	o, err := ParseInt(rest[1])
	if err != nil {
		return 0, 0, false
	}
	n, err := ParseInt(rest[2])
	if err != nil {
		return 0, 0, false
	}
	return int(o), int(n), true
}

// applyZLimit applies a Redis LIMIT offset/count window to an ordered member
// slice. A negative offset (or one past the end) yields nothing; a negative count
// means "all remaining from offset".
func applyZLimit(members []storage.ZMember, offset, count int) []storage.ZMember {
	if offset < 0 || offset >= len(members) {
		return members[:0]
	}
	members = members[offset:]
	if count >= 0 && count < len(members) {
		members = members[:count]
	}
	return members
}

// handleZLexCount implements ZLEXCOUNT key min max (Redis 3.2): reply the number
// of members whose lexical value falls in [min, max] using Redis' '[' inclusive /
// '(' exclusive / '-' / '+' bound syntax. An absent/expired key replies ":0"; a
// live non-ZSet key replies WRONGTYPE.
func (r *Router) handleZLexCount(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	min, ok := parseLexBound(args[2])
	if !ok {
		w.Error(errNotValidStringRange)
		return
	}
	max, ok := parseLexBound(args[3])
	if !ok {
		w.Error(errNotValidStringRange)
		return
	}

	_, live, wrongType, serr := r.zsetState(ctx, pk)
	if serr != nil {
		r.writeStoreError(c, serr)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.Int(0)
		return
	}

	all, merr := r.Storage.Store.ZRangeByRank(ctx, pk, 0, -1, false)
	if merr != nil {
		r.writeStoreError(c, merr)
		return
	}
	var n int64
	for _, m := range all {
		if lexInRange(m.Member, min, max) {
			n++
		}
	}

	w.Int(n)
}

// handleZRemRangeByLex implements ZREMRANGEBYLEX key min max (Redis 3.2): remove
// every member whose lexical value falls in [min, max] and reply the number
// removed. An absent/expired key replies ":0"; a live non-ZSet key replies
// WRONGTYPE. A removal that empties the set deletes the key (via adjustCount).
func (r *Router) handleZRemRangeByLex(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	min, ok := parseLexBound(args[2])
	if !ok {
		w.Error(errNotValidStringRange)
		return
	}
	max, ok := parseLexBound(args[3])
	if !ok {
		w.Error(errNotValidStringRange)
		return
	}

	_, live, wrongType, serr := r.zsetState(ctx, pk)
	if serr != nil {
		r.writeStoreError(c, serr)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.Int(0)
		return
	}

	all, merr := r.Storage.Store.ZRangeByRank(ctx, pk, 0, -1, false)
	if merr != nil {
		r.writeStoreError(c, merr)
		return
	}
	var members []string
	for _, m := range all {
		if lexInRange(m.Member, min, max) {
			members = append(members, m.Member)
		}
	}
	if len(members) == 0 {
		w.Int(0)
		return
	}

	removed, rerr := r.Storage.Store.ZRem(ctx, pk, members)
	if rerr != nil {
		r.writeStoreError(c, rerr)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeZSet, -int64(removed)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(removed))
}

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
	destPK := encodePK(c.DB(), destKey)

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

	weights, agg, ok := parseZStoreOptions(tail, int(numKeys))
	if !ok {
		w.Error(resp.ErrSyntax)
		return
	}

	// Load each operand into an in-memory member->score map (non-atomic snapshot).
	operands := make([]map[string]float64, numKeys)
	for i, k := range keyArgs {
		opPK := encodePK(c.DB(), k)
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

	result := combineZStores(operands, weights, agg, inter)

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
	if _, err := r.Storage.Meta.EnsureType(ctx, destPK, meta.TypeZSet, 0); err != nil {
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

// parseZStoreOptions parses the optional trailing tokens of ZUNIONSTORE /
// ZINTERSTORE: a "WEIGHTS w1 .. wN" clause (exactly numKeys floats, defaulting to
// all 1.0 when absent) and an "AGGREGATE SUM|MIN|MAX" clause (defaulting to SUM).
// ok=false signals a syntax error (a malformed clause, a bad weight/aggregate
// value, or an unexpected token).
func parseZStoreOptions(tail [][]byte, numKeys int) (weights []float64, agg zAggregate, ok bool) {
	weights = make([]float64, numKeys)
	for i := range weights {
		weights[i] = 1
	}
	agg = zAggSum

	i := 0
	for i < len(tail) {
		switch strings.ToUpper(string(tail[i])) {
		case "WEIGHTS":
			// Exactly numKeys weight values must follow.
			if len(tail)-(i+1) < numKeys {
				return nil, agg, false
			}
			for j := 0; j < numKeys; j++ {
				f, valid := parseScore(tail[i+1+j])
				if !valid {
					return nil, agg, false
				}
				weights[j] = f
			}
			i += 1 + numKeys
		case "AGGREGATE":
			if i+1 >= len(tail) {
				return nil, agg, false
			}
			switch strings.ToUpper(string(tail[i+1])) {
			case "SUM":
				agg = zAggSum
			case "MIN":
				agg = zAggMin
			case "MAX":
				agg = zAggMax
			default:
				return nil, agg, false
			}
			i += 2
		default:
			return nil, agg, false
		}
	}

	return weights, agg, true
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

	switch m.Type {
	case meta.TypeZSet:
		members, rerr := r.Storage.Store.ZRangeByRank(ctx, pk, 0, -1, false)
		if rerr != nil {
			return nil, false, rerr
		}
		out := make(map[string]float64, len(members))
		for _, zm := range members {
			out[zm.Member] = zm.Score
		}
		return out, false, nil
	case meta.TypeSet:
		members, rerr := r.Storage.Store.SMembers(ctx, pk)
		if rerr != nil {
			return nil, false, rerr
		}
		out := make(map[string]float64, len(members))
		for _, member := range members {
			out[member] = 1
		}
		return out, false, nil
	default:
		return nil, true, nil
	}
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
