package command

import (
	"context"

	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// handleZCard implements ZCARD key (requirements 9.2, 9.7): reply the member count
// in O(1) from meta.cnt. An absent/expired key replies ":0"; a live non-ZSet key
// replies WRONGTYPE.
func (r *Router) handleZCard(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := r.encodePK(c.DB(), args[1])

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
	pk := r.encodePK(c.DB(), args[1])

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

	m, live, wrongType, serr := r.zsetState(ctx, pk)
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

	// Cap the reply on the members this rank range actually selects (not the whole
	// set), matching LRANGE: ZRANGE/ZREVRANGE 0 -1 on an over-cap key is rejected
	// before materializing, a small bounded range still succeeds.
	if r.resultCapExceeded(w, rangeResultCount(m.Count, start, stop)) {
		return
	}

	members, merr := r.Storage.Store.ZRangeByRank(ctx, pk, int(start), int(stop), rev)
	if merr != nil {
		r.writeStoreError(c, merr)
		return
	}

	zMembersReply(w, members, withScores)
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
	pk := r.encodePK(c.DB(), args[1])

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
	if !memberStorable(args[2]) { // oversized member can never exist
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
