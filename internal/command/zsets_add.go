package command

import (
	"context"
	"strings"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

// zaddFlags holds the parsed leading option flags of a ZADD command
// (Redis 3.2 set: [NX|XX] [CH] [INCR]; GT/LT are 6.2+).
type zaddFlags struct {
	nx, xx, ch, incr bool
}

// parseZAddFlags scans the arguments after the key (rest) left-to-right for the
// optional leading [NX|XX] [CH] [INCR] flags, stopping at the first token that is
// not a flag. It returns the parsed flags, the index in rest of the first
// score/member argument (firstPair), and a non-empty errText naming the exact
// wire error to reply, or "" when the flag combination and pair shape are valid.
//
// The validation order and every error string are identical to the original
// inline implementation: mutually-exclusive NX/XX first (errZaddNxXx), then the
// even-and-non-empty pair-count check (resp.ErrSyntax), then the INCR
// single-pair constraint (errZaddIncr).
func parseZAddFlags(rest [][]byte) (flags zaddFlags, firstPair int, errText string) {
	i := 0
flags:
	for ; i < len(rest); i++ {
		switch strings.ToUpper(string(rest[i])) {
		case "NX":
			flags.nx = true
		case "XX":
			flags.xx = true
		case "CH":
			flags.ch = true
		case "INCR":
			flags.incr = true
		default:
			break flags
		}
	}
	if flags.nx && flags.xx {
		return flags, i, errZaddNxXx
	}
	pairs := rest[i:]
	// score/member pairs => the argument count after the flags must be even and non-empty.
	if len(pairs) == 0 || len(pairs)%2 != 0 {
		return flags, i, resp.ErrSyntax
	}
	if flags.incr && len(pairs) != 2 {
		return flags, i, errZaddIncr
	}
	return flags, i, ""
}

// handleZAdd implements ZADD key score member [score member ...] (requirements
// 9.1, 9.7): add/update the score/member pairs and reply the integer number of
// NEWLY added members (score updates do not count). cnt is bumped by that same
// number so ZCARD stays equal to the member count. A live non-ZSet key replies
// WRONGTYPE; a bad score replies the not-a-valid-float error before any write.
//
// It reads as parse-flags -> dispatch: parseZAddFlags validates the option flags
// and locates the first score/member pair, then the command routes to the INCR
// path (zaddIncr), the no-flag bulk fast path (zaddFastPath), or the NX/XX/CH
// flag path (zaddFlagPath).
func (r *Router) handleZAdd(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key := args[1]
	rest := args[2:]

	flags, firstPair, errText := parseZAddFlags(rest)
	if errText != "" {
		w.Error(errText)
		return
	}
	pairs := rest[firstPair:]

	pk := encodePK(c.DB(), key)

	switch {
	case flags.incr:
		r.zaddIncr(ctx, c, w, key, pk, flags.nx, flags.xx, pairs)
	case !flags.nx && !flags.xx && !flags.ch:
		r.zaddFastPath(ctx, c, w, key, pk, pairs)
	default:
		r.zaddFlagPath(ctx, c, w, key, pk, flags, pairs)
	}
}

// zaddFastPath is the no-flag ZADD path: parse ALL pairs up front, bulk-add them,
// and reply the number of newly added members. A bad score replies the
// not-a-valid-float error before any write. This is the original fast path,
// unchanged.
func (r *Router) zaddFastPath(ctx context.Context, c *server.Conn, w *resp.Writer, key []byte, pk string, pairs [][]byte) {
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
}

// zaddFlagPath is the NX/XX/CH ZADD path: it processes pairs LEFT-TO-RIGHT against
// an in-command view of scores, exactly like Redis. A member repeated in one call
// is ADDED by its first applied pair and then UPDATED by later ones, so CH counts
// each change (a later duplicate can bump CH even though the member is only added
// once). Each member's score is read from the store at most once, then tracked
// locally as prior pairs mutate it. It replies the changed count when CH is set,
// otherwise the added count. This is the original flag path, unchanged.
func (r *Router) zaddFlagPath(ctx context.Context, c *server.Conn, w *resp.Writer, key []byte, pk string, flags zaddFlags, pairs [][]byte) {
	nx, xx, ch := flags.nx, flags.xx, flags.ch

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
