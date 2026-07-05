package command

import (
	"context"
	"strings"

	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

// errNotValidStringRange is the Redis reply for a ZRANGEBYLEX / ZREVRANGEBYLEX
// min or max bound that is not a valid lexicographic range item (i.e. does not
// start with '[' or '(' and is not the bare '-' / '+').
const errNotValidStringRange = "ERR min or max not valid string range item"

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
