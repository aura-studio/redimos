package command

import (
	"context"
	"math"
	"strconv"
	"strings"

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
// score fall back to member order. The key's member count lives in the meta item's
// cnt attribute, maintained by the meta conditional write's atomic ADD, so
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
	return r.loadMetaState(ctx, pk, meta.TypeZSet)
}

// parseScore parses a ZADD score / ZINCRBY increment / ZRANGEBYSCORE bound as an
// IEEE754 double, accepting Redis' inf/-inf/+inf spellings and rejecting NaN.
// ok=false signals the not-a-valid-float reply. It does NOT reject ±inf, because a
// RANGE bound (ZRANGEBYSCORE/ZCOUNT) legitimately uses +inf/-inf and is never
// persisted; the store paths use storeScore instead.
func parseScore(arg []byte) (float64, bool) {
	switch string(arg) {
	case "inf", "+inf", "Inf", "+Inf", "INF", "+INF":
		return math.Inf(1), true
	case "-inf", "-Inf", "-INF":
		return math.Inf(-1), true
	}

	// Redis' getDoubleFromObject is strtod-based and parses an EMPTY string as 0.0
	// (strtod("") returns 0 with eptr left at the start, which the surrounding checks
	// accept). Go's strconv.ParseFloat instead rejects "", so special-case it to match
	// the live oracle: `ZADD key "" m` stores score 0, and `ZCOUNT/ZRANGEBYSCORE key ""
	// <hi>` treats the empty bound as 0. (An exclusive "(" with nothing after it reaches
	// here as "" via parseScoreBound, so "(" behaves as exclusive-0 too, again like Redis.)
	if len(arg) == 0 {
		return 0, true
	}

	// The finite case shares the canonical ParseFloat (NaN-rejecting, whole-string).
	f, err := ParseFloat(arg)
	return f, err == nil
}

// errScoreNotFinite is redimos' reply when a score that would be PERSISTED is ±inf.
// Redis accepts infinite sorted-set scores, but DynamoDB's Number type has no
// representation for infinity (its magnitude tops out around 1e125), so redimos
// cannot store one. This is a documented, accepted architectural divergence: rather
// than issue a doomed backend write — which surfaced as a misleading "backend error,
// retry later" and could stall the connection — the score is rejected up front with a
// clear, deterministic message.
const errScoreNotFinite = "ERR score must be a finite number"

// storeScore parses a score/increment that will be WRITTEN (ZADD, ZADD INCR,
// ZINCRBY). It rejects an unparseable float exactly as Redis does (errNotValidFloat)
// and, unlike parseScore, also rejects a non-finite ±inf value with errScoreNotFinite
// (see above). errText is "" on success.
// dynamoNumberMaxMagnitude / dynamoNumberMinMagnitude bound the magnitudes DynamoDB's
// Number type can hold (its domain is 1e-130 .. 9.9999…e+125, 38 significant digits,
// no inf/NaN). A finite score OUTSIDE this band — too large (1e130) OR too small in
// magnitude but non-zero (1e-200) — is a perfectly valid IEEE-754 double that Redis
// accepts but redimos cannot persist as the item's numeric sort attribute.
const (
	dynamoNumberMaxMagnitude = 9.9999999999999999999999999999999999999e+125
	dynamoNumberMinMagnitude = 1e-130
)

// errScoreOutOfRange is redimos' reply for a FINITE score whose magnitude falls
// outside the DynamoDB Number domain (e.g. 1e130 above, or 1e-200 below the floor).
// Redis stores these as ordinary doubles; redimos cannot, so — like the ±inf case —
// it rejects them deterministically up front rather than letting the write reach the
// backend, which returned a misleading "backend error, retry later" AND, in a
// multi-member ZADD or a *STORE, left a torn/half-wiped result behind. See doc §4.1.
const errScoreOutOfRange = "ERR value is out of range"

// checkScoreDomain reports the deterministic rejection message for a score redimos
// cannot persist (non-finite, or a finite magnitude outside the DynamoDB Number
// domain), or "" when the score is storable. Used both for a directly-supplied score
// (storeScore) and for a computed result score (ZUNIONSTORE/ZINTERSTORE), so an
// out-of-domain result is rejected BEFORE any destructive write rather than tearing.
func checkScoreDomain(f float64) string {
	if math.IsInf(f, 0) {
		return errScoreNotFinite
	}
	if f != 0 && math.Abs(f) < dynamoNumberMinMagnitude {
		return errScoreOutOfRange
	}
	if math.Abs(f) > dynamoNumberMaxMagnitude {
		return errScoreOutOfRange
	}
	return ""
}

func storeScore(arg []byte) (score float64, errText string) {
	f, ok := parseScore(arg)
	if !ok {
		return 0, errNotValidFloat
	}
	if e := checkScoreDomain(f); e != "" {
		return 0, e
	}
	return f, ""
}

// parseScoreBound parses a ZRANGEBYSCORE / ZCOUNT bound: a leading '(' selects the
// exclusive (open) interval, and the remainder is a float accepting inf/-inf/+inf.
// ok=false signals the min-or-max-not-a-float reply.
//
// Redis parses a range bound with zslParseRange, which uses RAW strtod — this SKIPS
// leading ASCII whitespace ("ZRANGEBYSCORE k \" 1\" 5" treats " 1" as 1.0), unlike
// getDoubleFromObject (used for ZADD scores) which rejects a leading space. It still
// requires the number to consume the rest of the string, so trailing junk ("1 ") is
// rejected. An all-whitespace bound is left unconsumed by strtod and rejected — it
// must NOT collapse into parseScore's empty-string-is-0 special case.
func parseScoreBound(arg []byte) (storage.ScoreBound, bool) {
	exclusive := false
	if len(arg) > 0 && arg[0] == '(' {
		exclusive = true
		arg = arg[1:]
	}

	trimmed := trimLeadingSpace(arg)
	if len(trimmed) == 0 && len(arg) != 0 {
		// arg was all whitespace: strtod consumes nothing, eptr != end -> error.
		return storage.ScoreBound{}, false
	}

	f, ok := parseScore(trimmed)
	if !ok {
		return storage.ScoreBound{}, false
	}

	return storage.ScoreBound{Value: f, Exclusive: exclusive}, true
}

// trimLeadingSpace drops the leading ASCII whitespace strtod skips (space, tab, and
// the vertical-tab/form-feed/newline/carriage-return family).
func trimLeadingSpace(b []byte) []byte {
	i := 0
	for i < len(b) {
		switch b[i] {
		case ' ', '\t', '\n', '\v', '\f', '\r':
			i++
		default:
			return b[i:]
		}
	}
	return b[i:]
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

// parseWithScores interprets the optional trailing tokens of the rank-based
// ZRANGE / ZREVRANGE: an empty tail means no scores; a single "WITHSCORES" (any
// case) sets the flag; anything else is a syntax error (ok=false). These commands
// take no LIMIT clause — that is the ...BYSCORE family's parseScoreTail.
func parseWithScores(tail [][]byte) (withScores, ok bool) {
	if len(tail) == 0 {
		return false, true
	}
	if len(tail) == 1 && equalFold(tail[0], "WITHSCORES") {
		return true, true
	}

	return false, false
}

// parseScoreTail parses the optional trailer of ZRANGEBYSCORE / ZREVRANGEBYSCORE:
// "[WITHSCORES] [LIMIT offset count]", where the two clauses may appear in EITHER
// order (Redis 3.2's genericZrangebyscoreCommand walks the tail token-by-token).
// offset/count default to the identity window (0, -1) = "all". errMsg is "" on
// success; otherwise it is the exact RESP body to reply — a non-integer LIMIT
// offset/count is the not-an-integer error (getLongFromObjectOrReply), while any
// other malformed token, or a LIMIT without its two values, is a syntax error.
func parseScoreTail(tail [][]byte) (withScores bool, offset, count int, errMsg string) {
	offset, count = 0, -1
	for i := 0; i < len(tail); {
		switch {
		case equalFold(tail[i], "WITHSCORES"):
			withScores = true
			i++
		case equalFold(tail[i], "LIMIT") && i+2 < len(tail):
			o, err := ParseInt(tail[i+1])
			if err != nil {
				return false, 0, 0, resp.ErrNotInteger
			}
			n, err := ParseInt(tail[i+2])
			if err != nil {
				return false, 0, 0, resp.ErrNotInteger
			}
			offset, count = int(o), int(n)
			i += 3
		default:
			return false, 0, 0, resp.ErrSyntax
		}
	}
	return withScores, offset, count, ""
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

	// Type / existence check BEFORE the option parse: Redis' zscanCommand does the
	// lookup + checkType before scanGenericCommand parses MATCH/COUNT, so a live
	// non-ZSet key is WRONGTYPE and an absent/expired key replies the terminating
	// ["0", []] — both regardless of a malformed MATCH/COUNT option.
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
			// string2ll semantics (reject leading '+'/zeros), not strconv.Atoi.
			n, err := ParseInt(opts[i+1])
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
