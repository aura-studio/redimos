package command

import (
	"context"

	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

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
