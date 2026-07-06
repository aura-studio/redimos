package command

import (
	"context"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

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

	delta, errText := storeScore(args[2])
	if errText != "" {
		w.Error(errText)
		return
	}
	member := args[3]

	pk := encodePK(c.DB(), key)
	if err := guard.CheckWrite(key, [][]byte{member}, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.ensureTypeExpiring(ctx, pk, meta.TypeZSet); err != nil {
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

	// "[WITHSCORES] [LIMIT offset count]" (either order). Option errors take
	// precedence over the absent-key empty reply, matching Redis' parse-then-lookup.
	withScores, offset, count, errMsg := parseScoreTail(args[4:])
	if errMsg != "" {
		w.Error(errMsg)
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

	// Apply the LIMIT window to the already-ordered range (asc for ZRANGEBYSCORE,
	// desc for ZREVRANGEBYSCORE). The default (0, -1) is a no-op.
	members = applyZLimit(members, offset, count)

	zMembersReply(w, members, withScores)
}
