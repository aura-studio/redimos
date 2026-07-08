package integration

import (
	"strconv"
	"testing"
)

// Dimension D (deepened): range/index boundary semantics beyond boundary_test.go.
// These push the exact edge values the gap file and dimension scope name: SETRANGE
// type-checks BEFORE the empty-value short-circuit; ZRANGEBYLEX rejects malformed
// bounds like "(+"; LTRIM / LREM / LINSERT negative-index and delete-on-empty paths;
// INT64-min/max and huge indices that must not overflow; and WRONGTYPE on every
// range/index op. Everything is compared byte-for-byte against a live Redis 3.2.

// ---------------------------------------------------------------------------
// GAP 1: SETRANGE type-check precedence (empty value on a non-string key).
// Redis type-checks in setrangeCommand BEFORE the empty-value short-circuit, so
// SETRANGE on a live list/set/hash/zset key must reply WRONGTYPE even when the
// value is empty (which on a missing key would otherwise be a no-op returning 0).
// ---------------------------------------------------------------------------

func TestDiffSetRangeTypeCheckPrecedence(t *testing.T) {
	d := newDiffer(t)

	// Empty value against each non-string type: WRONGTYPE must win over the
	// "empty value -> return current length (0)" fast path.
	lk := d.k("sr_list")
	d.eq("seed list", bs("RPUSH"), lk, bs("a"))
	d.eq("SETRANGE list 0 '' -> WRONGTYPE", bs("SETRANGE"), lk, bs("0"), bs(""))
	d.eq("SETRANGE list 5 '' -> WRONGTYPE (nonzero offset)", bs("SETRANGE"), lk, bs("5"), bs(""))
	d.eq("SETRANGE list 0 nonempty -> WRONGTYPE", bs("SETRANGE"), lk, bs("0"), bs("XY"))
	d.eq("list intact after failed SETRANGE", bs("LRANGE"), lk, bs("0"), bs("-1"))

	hk := d.k("sr_hash")
	d.eq("seed hash", bs("HSET"), hk, bs("f"), bs("v"))
	d.eq("SETRANGE hash 0 '' -> WRONGTYPE", bs("SETRANGE"), hk, bs("0"), bs(""))
	d.eq("SETRANGE hash 3 nonempty -> WRONGTYPE", bs("SETRANGE"), hk, bs("3"), bs("Z"))

	sk := d.k("sr_set")
	d.eq("seed set", bs("SADD"), sk, bs("m"))
	d.eq("SETRANGE set 0 '' -> WRONGTYPE", bs("SETRANGE"), sk, bs("0"), bs(""))

	zk := d.k("sr_zset")
	d.eq("seed zset", bs("ZADD"), zk, bs("1"), bs("m"))
	d.eq("SETRANGE zset 0 '' -> WRONGTYPE", bs("SETRANGE"), zk, bs("0"), bs(""))

	// Contrast: SETRANGE with empty value on a MISSING key is a no-op returning 0
	// (no type error), and must NOT create the key.
	mk := d.k("sr_missing")
	d.eq("SETRANGE missing 0 '' -> 0", bs("SETRANGE"), mk, bs("0"), bs(""))
	d.eq("SETRANGE missing 10 '' -> 0 (nonzero offset)", bs("SETRANGE"), mk, bs("10"), bs(""))
	d.eq("EXISTS after empty SETRANGE on missing", bs("EXISTS"), mk)
	d.eq("TYPE after empty SETRANGE on missing", bs("TYPE"), mk)

	// Contrast: empty value on a live STRING key returns its current length and
	// leaves it unchanged (no type error, no truncation).
	stk := d.k("sr_str")
	d.eq("seed string", bs("SET"), stk, bs("hello"))
	d.eq("SETRANGE string 0 '' -> STRLEN", bs("SETRANGE"), stk, bs("0"), bs(""))
	d.eq("SETRANGE string 99 '' -> STRLEN (past end, still no-op)", bs("SETRANGE"), stk, bs("99"), bs(""))
	d.eq("GET after empty SETRANGE on string", bs("GET"), stk)
}

// ---------------------------------------------------------------------------
// GAP 2: ZRANGEBYLEX bound parsing edge cases. A bare '+' / '-' are the only
// infinities; '(' or '[' must be followed by an actual member. Malformed bounds
// like "(+", "[+", "(-", "+x", trailing junk, or an empty bound must reply
// "min or max not valid string range item".
// ---------------------------------------------------------------------------

func TestDiffZRangeByLexBadBounds(t *testing.T) {
	d := newDiffer(t)

	lk := d.k("lexbad")
	for _, m := range []string{"a", "b", "c", "d"} {
		d.eq("ZADD lex "+m, bs("ZADD"), lk, bs("0"), bs(m))
	}

	// Malformed min bound with a valid max.
	bad := []string{"(+", "[+", "(-", "[-", "+a", "-a", "(", "[", "", "a", "]a", "((a"}
	for _, b := range bad {
		d.eq("ZRANGEBYLEX min="+b+" max=+", bs("ZRANGEBYLEX"), lk, bs(b), bs("+"))
		d.eq("ZRANGEBYLEX min=- max="+b, bs("ZRANGEBYLEX"), lk, bs("-"), bs(b))
		d.eq("ZREVRANGEBYLEX max="+b+" min=-", bs("ZREVRANGEBYLEX"), lk, bs(b), bs("-"))
		d.eq("ZLEXCOUNT min="+b+" max=+", bs("ZLEXCOUNT"), lk, bs(b), bs("+"))
		d.eq("ZREMRANGEBYLEX min="+b+" max=+ (no mutate on parse err)", bs("ZREMRANGEBYLEX"), lk, bs(b), bs("+"))
	}
	// The parse errors above must not have removed anything.
	d.eq("set intact after bad ZREMRANGEBYLEX", bs("ZRANGEBYLEX"), lk, bs("-"), bs("+"))

	// Valid infinity + valid bracketed/paren bounds still work (sanity: parser
	// accepts the good forms, so the errors above are specific to the malformed ones).
	d.eq("ZRANGEBYLEX - + (valid full)", bs("ZRANGEBYLEX"), lk, bs("-"), bs("+"))
	d.eq("ZRANGEBYLEX + - (reversed empty)", bs("ZRANGEBYLEX"), lk, bs("+"), bs("-"))
	d.eq("ZRANGEBYLEX [a [c (inclusive)", bs("ZRANGEBYLEX"), lk, bs("[a"), bs("[c"))
	d.eq("ZRANGEBYLEX (a (c (exclusive)", bs("ZRANGEBYLEX"), lk, bs("(a"), bs("(c"))
	// A member literally named "+" / "-" / "[" must be addressable via the bracket form.
	pk := d.k("lexbrk")
	for _, m := range []string{"+", "-", "[", "("} {
		d.eq("ZADD literal "+m, bs("ZADD"), pk, bs("0"), bs(m))
	}
	d.eq("ZRANGEBYLEX [+ [+ (literal plus member)", bs("ZRANGEBYLEX"), pk, bs("[+"), bs("[+"))
	d.eq("ZRANGEBYLEX (- + (exclusive of literal minus)", bs("ZRANGEBYLEX"), pk, bs("(-"), bs("+"))
	d.eq("ZLEXCOUNT - + over literal members", bs("ZLEXCOUNT"), pk, bs("-"), bs("+"))
}

// ---------------------------------------------------------------------------
// LTRIM negative-index and delete-on-empty semantics (dimension scope).
// A trim whose range excludes everything deletes the key; negative indices count
// from the tail; start>stop and fully out-of-range both empty the list.
// ---------------------------------------------------------------------------

func TestDiffListTrimBoundaries(t *testing.T) {
	seed := func(d *differ) []byte {
		lk := d.k("ltrim")
		for _, v := range []string{"a", "b", "c", "d", "e"} {
			d.eq("RPUSH "+v, bs("RPUSH"), lk, bs(v))
		}
		return lk
	}

	cases := [][2]string{
		{"1", "3"},     // interior keep b,c,d
		{"0", "-1"},    // keep all
		{"-3", "-1"},   // keep last three
		{"-100", "100"},// clamp both ends -> keep all
		{"2", "1"},     // start>stop -> empties (key deleted)
		{"5", "10"},    // fully past end -> empties (key deleted)
		{"-100", "-6"}, // fully before start -> empties (key deleted)
		{"3", "3"},     // single element
		{"0", "0"},     // first only
		{"-1", "-1"},   // last only
	}
	for _, r := range cases {
		d := newDiffer(t)
		lk := seed(d)
		d.eq("LTRIM "+r[0]+" "+r[1], bs("LTRIM"), lk, bs(r[0]), bs(r[1]))
		d.eq("LRANGE after LTRIM "+r[0]+" "+r[1], bs("LRANGE"), lk, bs("0"), bs("-1"))
		d.eq("EXISTS after LTRIM "+r[0]+" "+r[1], bs("EXISTS"), lk)
		d.eq("LLEN after LTRIM "+r[0]+" "+r[1], bs("LLEN"), lk)
		d.eq("TYPE after LTRIM "+r[0]+" "+r[1], bs("TYPE"), lk)
	}
}

// ---------------------------------------------------------------------------
// INT64 min/max and overflow-scale indices to LRANGE / LINDEX / LSET / LTRIM.
// These must clamp/reject without integer overflow, matching Redis exactly.
// ---------------------------------------------------------------------------

func TestDiffListIndexIntLimits(t *testing.T) {
	d := newDiffer(t)

	lk := d.k("intlim")
	for _, v := range []string{"a", "b", "c"} {
		d.eq("RPUSH "+v, bs("RPUSH"), lk, bs(v))
	}

	const (
		i64max  = "9223372036854775807"
		i64min  = "-9223372036854775808"
		over    = "9223372036854775808"  // int64max+1
		under   = "-9223372036854775809" // int64min-1
		wayover = "99999999999999999999999999999"
	)

	// LINDEX at the extremes -> out of range (nil), and non-integer -> error.
	for _, idx := range []string{i64max, i64min, over, under, wayover, "notanint", "1.5", "", "+1", " 1"} {
		d.eq("LINDEX "+idx, bs("LINDEX"), lk, bs(idx))
	}

	// LRANGE with extreme starts/stops must clamp, never overflow.
	for _, r := range [][2]string{
		{i64min, i64max}, {i64max, i64min}, {"0", i64max}, {i64min, "-1"},
		{over, wayover}, {under, i64max}, {"0", over}, {under, under},
	} {
		d.eq("LRANGE "+r[0]+" "+r[1], bs("LRANGE"), lk, bs(r[0]), bs(r[1]))
	}

	// LSET at the extremes -> "index out of range"; non-integer -> "not an integer".
	for _, idx := range []string{i64max, i64min, over, under, wayover, "1.5", "x"} {
		d.eq("LSET "+idx, bs("LSET"), lk, bs(idx), bs("Z"))
	}
	d.eq("list intact after failed LSETs", bs("LRANGE"), lk, bs("0"), bs("-1"))

	// LTRIM at the extremes: [min,max] keeps all; [max,min] empties (delete).
	tk := d.k("intlim_trim")
	for _, v := range []string{"a", "b", "c"} {
		d.eq("RPUSH trim "+v, bs("RPUSH"), tk, bs(v))
	}
	d.eq("LTRIM min max -> keep all", bs("LTRIM"), tk, bs(i64min), bs(i64max))
	d.eq("LRANGE after keep-all trim", bs("LRANGE"), tk, bs("0"), bs("-1"))
	d.eq("LTRIM max min -> empty", bs("LTRIM"), tk, bs(i64max), bs(i64min))
	d.eq("EXISTS after empty trim", bs("EXISTS"), tk)
}

// ---------------------------------------------------------------------------
// GETRANGE / SETRANGE integer-limit and offset boundaries on strings.
// Redis caps the string at 512MB (proto-max-bulk-len); a huge SETRANGE offset
// must reply "string exceeds maximum allowed size", and GETRANGE with extreme
// indices must clamp without overflow.
// ---------------------------------------------------------------------------

func TestDiffStringRangeIntLimits(t *testing.T) {
	d := newDiffer(t)

	sk := d.k("srlim")
	d.eq("SET base", bs("SET"), sk, bs("Hello"))

	const (
		i64max  = "9223372036854775807"
		i64min  = "-9223372036854775808"
		over    = "9223372036854775808"
		wayover = "99999999999999999999999999999"
	)

	// GETRANGE with extreme signed indices must clamp to the 5-byte string.
	for _, r := range [][2]string{
		{i64min, i64max}, {"0", i64max}, {i64min, "-1"}, {i64max, i64min},
		{over, wayover}, {"-1000000000000", "-1"}, {"0", "-1000000000000"},
	} {
		d.eq("GETRANGE "+r[0]+" "+r[1], bs("GETRANGE"), sk, bs(r[0]), bs(r[1]))
	}

	// SETRANGE with an out-of-int64-range offset replies the not-an-integer error,
	// and a negative offset replies "offset is out of range" — identical on both
	// endpoints, with no mutation. (Offsets that only exceed redimos' ~390KB value
	// cap — i64max and 536870911 — are omitted: redimos rejects them with its
	// backend-limit error text while Redis 3.2 allows up to 512MB and errors with
	// different text / attempts a huge allocation, so they can never byte-compare and
	// are oracle-hostile.)
	for _, off := range []string{over, wayover, "-1", i64min} {
		d.eq("SETRANGE offset="+off, bs("SETRANGE"), sk, bs(off), bs("X"))
	}
	d.eq("string intact after oversized SETRANGE", bs("GET"), sk)

	// SETRANGE with a negative offset is an "offset is out of range" error even
	// with an empty value (empty short-circuit does not skip the offset check for
	// negatives in Redis).
	d.eq("SETRANGE -1 '' -> error", bs("SETRANGE"), sk, bs("-1"), bs(""))
}

// ---------------------------------------------------------------------------
// LREM count-sign / magnitude boundaries and LINSERT before/after at edges.
// count>0 head->tail, count<0 tail->head, count==0 all; INT64 counts must not
// overflow. LINSERT with a missing pivot returns -1; on a missing key returns 0.
// ---------------------------------------------------------------------------

func TestDiffListRemAndInsertBoundaries(t *testing.T) {
	seed := func(d *differ) []byte {
		lk := d.k("lrem")
		for _, v := range []string{"x", "a", "x", "b", "x", "c", "x"} {
			d.eq("RPUSH "+v, bs("RPUSH"), lk, bs(v))
		}
		return lk
	}

	const (
		i64max = "9223372036854775807"
		i64min = "-9223372036854775808"
	)

	for _, cnt := range []string{"0", "1", "2", "-1", "-2", "100", "-100", i64max, i64min} {
		d := newDiffer(t)
		lk := seed(d)
		d.eq("LREM count="+cnt+" x", bs("LREM"), lk, bs(cnt), bs("x"))
		d.eq("LRANGE after LREM "+cnt, bs("LRANGE"), lk, bs("0"), bs("-1"))
		d.eq("EXISTS after LREM "+cnt, bs("EXISTS"), lk)
		d.eq("LLEN after LREM "+cnt, bs("LLEN"), lk)
	}

	// Non-integer count -> "value is not an integer or out of range".
	dc := newDiffer(t)
	lc := seed(dc)
	dc.eq("LREM non-int count", bs("LREM"), lc, bs("1.5"), bs("x"))
	dc.eq("LREM empty count", bs("LREM"), lc, bs(""), bs("x"))

	// LINSERT edge cases: pivot present at head/tail, pivot absent (-1), missing
	// key (0), and the delete-does-not-apply case (LINSERT never deletes).
	di := newDiffer(t)
	li := di.k("linsert")
	for _, v := range []string{"a", "b", "c"} {
		di.eq("RPUSH ins "+v, bs("RPUSH"), li, bs(v))
	}
	di.eq("LINSERT BEFORE a", bs("LINSERT"), li, bs("BEFORE"), bs("a"), bs("Z1"))
	di.eq("LINSERT AFTER c", bs("LINSERT"), li, bs("AFTER"), bs("c"), bs("Z2"))
	di.eq("LINSERT BEFORE missing -> -1", bs("LINSERT"), li, bs("BEFORE"), bs("nope"), bs("Q"))
	di.eq("LRANGE after inserts", bs("LRANGE"), li, bs("0"), bs("-1"))
	// Missing key -> 0, and must not create the key.
	mi := di.k("linsert_missing")
	di.eq("LINSERT on missing key -> 0", bs("LINSERT"), mi, bs("BEFORE"), bs("x"), bs("y"))
	di.eq("EXISTS after LINSERT on missing", bs("EXISTS"), mi)
	// Invalid where token -> syntax error.
	di.eq("LINSERT bad where -> error", bs("LINSERT"), li, bs("SIDEWAYS"), bs("a"), bs("q"))
}

// ---------------------------------------------------------------------------
// ZSET rank/score boundary ops: ZREMRANGEBYRANK negative indices and empty
// ranges (delete-on-empty), ZREMRANGEBYSCORE ±inf and exclusive bounds, and
// INT64-scale rank inputs. Delete-when-empty must remove the key.
// ---------------------------------------------------------------------------

func TestDiffZSetRemRangeBoundaries(t *testing.T) {
	seed := func(d *differ) []byte {
		zk := d.k("zremr")
		for i, m := range []string{"a", "b", "c", "d", "e"} {
			d.eq("ZADD "+m, bs("ZADD"), zk, bs(strconv.Itoa(i)), bs(m))
		}
		return zk
	}

	const (
		i64max = "9223372036854775807"
		i64min = "-9223372036854775808"
	)

	// ZREMRANGEBYRANK with negative / out-of-range / start>stop / whole-set ranks.
	rankCases := [][2]string{
		{"0", "-1"},     // remove all -> key deleted
		{"1", "3"},      // interior
		{"-2", "-1"},    // last two
		{"5", "10"},     // out of range -> nothing removed
		{"2", "1"},      // start>stop -> nothing removed
		{"-100", "100"}, // clamp -> remove all -> deleted
		{i64min, i64max},// extremes -> remove all -> deleted
		{i64max, i64min},// reversed extremes -> nothing
		{"0", "0"},      // first only
		{"-1", "-1"},    // last only
	}
	for _, r := range rankCases {
		d := newDiffer(t)
		zk := seed(d)
		d.eq("ZREMRANGEBYRANK "+r[0]+" "+r[1], bs("ZREMRANGEBYRANK"), zk, bs(r[0]), bs(r[1]))
		d.eq("ZRANGE after byrank "+r[0]+" "+r[1], bs("ZRANGE"), zk, bs("0"), bs("-1"))
		d.eq("EXISTS after byrank "+r[0]+" "+r[1], bs("EXISTS"), zk)
		d.eq("ZCARD after byrank "+r[0]+" "+r[1], bs("ZCARD"), zk)
	}

	// ZREMRANGEBYSCORE ±inf, exclusive bounds, empty ranges.
	scoreCases := [][2]string{
		{"-inf", "+inf"}, // remove all -> deleted
		{"(0", "(4"},     // exclusive interior
		{"(4", "(0"},     // reversed exclusive -> nothing
		{"2", "2"},       // single score
		{"-inf", "(0"},   // exclusive of 0 -> nothing below 0
		{"(4", "+inf"},   // above 4 exclusive -> nothing
	}
	for _, r := range scoreCases {
		d := newDiffer(t)
		zk := seed(d)
		d.eq("ZREMRANGEBYSCORE "+r[0]+" "+r[1], bs("ZREMRANGEBYSCORE"), zk, bs(r[0]), bs(r[1]))
		d.eq("ZRANGE after byscore "+r[0]+" "+r[1], bs("ZRANGE"), zk, bs("0"), bs("-1"))
		d.eq("EXISTS after byscore "+r[0]+" "+r[1], bs("EXISTS"), zk)
	}

	// Malformed score bound -> "min or max is not a float", no mutation.
	dbad := newDiffer(t)
	zbad := seed(dbad)
	dbad.eq("ZREMRANGEBYSCORE bad min", bs("ZREMRANGEBYSCORE"), zbad, bs("(("), bs("+inf"))
	dbad.eq("ZREMRANGEBYSCORE bad max", bs("ZREMRANGEBYSCORE"), zbad, bs("-inf"), bs("notafloat"))
	dbad.eq("set intact after bad byscore", bs("ZRANGE"), zbad, bs("0"), bs("-1"))
	// Non-integer rank -> "value is not an integer or out of range".
	dbad.eq("ZREMRANGEBYRANK non-int", bs("ZREMRANGEBYRANK"), zbad, bs("1.5"), bs("2"))
}

// NOTE: A ZRANGEBYSCORE/ZREVRANGEBYSCORE "LIMIT offset count" differential was
// removed here: redimos does not implement the LIMIT trailer on the by-score
// range commands (parseWithScores accepts only an optional lone WITHSCORES and
// replies "syntax error" for a LIMIT tail), while Redis 3.2 supports it. Every
// LIMIT case therefore diverges (syntax error vs. a real windowed result) and
// can never byte-compare. This is a redimos feature gap flagged for the oracle
// run; LIMIT is supported only on the ZRANGEBYLEX family (see zsets_lex.go),
// which the ZLEXCOUNT/ZRANGEBYLEX coverage above already exercises.

// ---------------------------------------------------------------------------
// WRONGTYPE on every range/index op against a wrong-typed key, and binary-byte
// keys/members in index positions. A range/index command on a key of the wrong
// type must reply WRONGTYPE before doing any bound parsing.
// ---------------------------------------------------------------------------

func TestDiffRangeIndexWrongType(t *testing.T) {
	d := newDiffer(t)

	// A string key: list/zset range ops must all WRONGTYPE.
	sk := d.k("wt_str")
	d.eq("SET wt_str", bs("SET"), sk, bs("v"))
	d.eq("LRANGE on string -> WRONGTYPE", bs("LRANGE"), sk, bs("0"), bs("-1"))
	d.eq("LINDEX on string -> WRONGTYPE", bs("LINDEX"), sk, bs("0"))
	d.eq("LSET on string -> WRONGTYPE", bs("LSET"), sk, bs("0"), bs("x"))
	d.eq("LTRIM on string -> WRONGTYPE", bs("LTRIM"), sk, bs("0"), bs("-1"))
	d.eq("LREM on string -> WRONGTYPE", bs("LREM"), sk, bs("0"), bs("x"))
	d.eq("LINSERT on string -> WRONGTYPE", bs("LINSERT"), sk, bs("BEFORE"), bs("a"), bs("b"))
	d.eq("ZRANGE on string -> WRONGTYPE", bs("ZRANGE"), sk, bs("0"), bs("-1"))
	d.eq("ZRANGEBYSCORE on string -> WRONGTYPE", bs("ZRANGEBYSCORE"), sk, bs("-inf"), bs("+inf"))
	d.eq("ZRANGEBYLEX on string -> WRONGTYPE", bs("ZRANGEBYLEX"), sk, bs("-"), bs("+"))
	d.eq("ZREMRANGEBYRANK on string -> WRONGTYPE", bs("ZREMRANGEBYRANK"), sk, bs("0"), bs("-1"))

	// A list key: zset/string range ops must WRONGTYPE; GETRANGE too.
	lk := d.k("wt_list")
	d.eq("RPUSH wt_list", bs("RPUSH"), lk, bs("a"))
	d.eq("GETRANGE on list -> WRONGTYPE", bs("GETRANGE"), lk, bs("0"), bs("-1"))
	d.eq("ZRANGE on list -> WRONGTYPE", bs("ZRANGE"), lk, bs("0"), bs("-1"))
	d.eq("ZRANGEBYLEX on list -> WRONGTYPE", bs("ZRANGEBYLEX"), lk, bs("-"), bs("+"))

	// A zset key: list range ops must WRONGTYPE.
	zk := d.k("wt_zset")
	d.eq("ZADD wt_zset", bs("ZADD"), zk, bs("1"), bs("m"))
	d.eq("LRANGE on zset -> WRONGTYPE", bs("LRANGE"), zk, bs("0"), bs("-1"))
	d.eq("LINDEX on zset -> WRONGTYPE", bs("LINDEX"), zk, bs("0"))
	d.eq("GETRANGE on zset -> WRONGTYPE", bs("GETRANGE"), zk, bs("0"), bs("-1"))
}

// ---------------------------------------------------------------------------
// Binary-byte members/values in range/index positions: NUL bytes and high
// bytes must round-trip through index ops and lex bounds. Lex ordering is
// byte-wise (memcmp), so a NUL-containing member sorts before its bare prefix.
// ---------------------------------------------------------------------------

func TestDiffRangeIndexBinaryBytes(t *testing.T) {
	d := newDiffer(t)

	// List with NUL and high-byte values, addressed by negative index.
	lk := d.k("bin_list")
	vals := []string{"a\x00b", string([]byte{0x00}), string([]byte{0xff, 0xfe}), "z"}
	for _, v := range vals {
		d.eq("RPUSH bin", bs("RPUSH"), lk, bs(v))
	}
	for _, i := range []string{"0", "1", "-1", "-2"} {
		d.eq("LINDEX bin "+i, bs("LINDEX"), lk, bs(i))
	}
	d.eq("LRANGE bin all", bs("LRANGE"), lk, bs("0"), bs("-1"))
	d.eq("LSET bin -1 with NUL", bs("LSET"), lk, bs("-1"), bs("q\x00\xffr"))
	d.eq("LRANGE bin after LSET", bs("LRANGE"), lk, bs("0"), bs("-1"))

	// GETRANGE across a NUL-containing string.
	sk := d.k("bin_str")
	d.eq("SET bin str", bs("SET"), sk, bs("a\x00b\xffc"))
	for _, r := range [][2]string{{"0", "-1"}, {"1", "3"}, {"-2", "-1"}, {"0", "0"}} {
		d.eq("GETRANGE bin "+r[0]+" "+r[1], bs("GETRANGE"), sk, bs(r[0]), bs(r[1]))
	}

	// Lex range over binary members (byte-wise ordering): "m\x00" < "m".
	zk := d.k("bin_lex")
	members := []string{string([]byte{0x00}), "m", "m\x00", string([]byte{0xff}), "ma"}
	for _, m := range members {
		d.eq("ZADD bin lex", bs("ZADD"), zk, bs("0"), bs(m))
	}
	d.eq("ZRANGEBYLEX - + binary", bs("ZRANGEBYLEX"), zk, bs("-"), bs("+"))
	d.eq("ZRANGEBYLEX [m\\x00 + binary", bs("ZRANGEBYLEX"), zk, bs("[m\x00"), bs("+"))
	d.eq("ZRANGEBYLEX [\\x00 (m binary", bs("ZRANGEBYLEX"), zk, bs("[\x00"), bs("(m"))
	d.eq("ZLEXCOUNT [m [ma binary", bs("ZLEXCOUNT"), zk, bs("[m"), bs("[ma"))
}
