package integration

// Dimension B (deepened): numeric / float / overflow parity against a live Redis 3.2.
//
// numeric_test.go establishes the baseline (score formatting, INCR overflow). This file
// pushes on the exact edge values the baseline skipped — non-finite float parsing, int64
// boundary offsets/indices, and the integer overflow guards — each sent to BOTH endpoints
// and byte-compared so any divergence in the redimos proxy (parse-time rejection, index
// clamping, overflow validation, memory guard) surfaces as a failing case rather than a
// silent difference.
//
// Calibration note: cases that hit redimos' KNOWN, intentional divergences from Redis 3.2
// are deliberately omitted so this file byte-compares only where the two genuinely agree:
//   - value-size cap: redimos rejects a derived length > 390KB with its own error text,
//     while Redis 3.2 allows up to 512MB and errors with a different string (and huge
//     offsets segfault/OOM the old oracle). Only small, in-range offsets are exercised.
//   - float magnitude / DynamoDB numeric domain: DynamoDB Number cannot hold ±inf, values
//     outside its ~1e-130..1e125 range, or the long-double magnitudes (1e400, 1e309, 9e999)
//     Redis' strtold accepts. Scores are kept to small, DynamoDB-representable, exactly
//     round-tripping decimals; the accepted-vs-rejected boundary of those extremes is a
//     separate oracle-verified probe (see suspected divergences in the audit).
//   - accumulating float precision: INCRBYFLOAT accumulates in float64, not long double, so
//     accumulating results are compared with eqFloatClose; only direct, exactly-formatted
//     values are byte-compared.
//
// Only commands the proxy actually registers are exercised: INCRBYFLOAT, SETRANGE,
// GETRANGE/SUBSTR, ZADD, ZINCRBY, ZSCORE, INCR/INCRBY/DECR/DECRBY, plus GET/STRLEN for
// state readback.

import "testing"

// TestDiffIncrByFloatNonFinite covers GAP 1: INCRBYFLOAT must reject a non-finite
// increment at parse time with "value is not a valid float", exactly like Redis'
// string2ld (which rejects inf/nan). The proxy's parseFloatArg rejects both the literal
// ±inf spellings and an exponent that overflows float64 to ±Inf (1e400). A rejected
// command must ALSO leave the key untouched, so each rejection is followed by a GET.
func TestDiffIncrByFloatNonFinite(t *testing.T) {
	d := newDiffer(t)

	// Fresh key: a literal ±inf increment is ACCEPTED at parse by both (Redis' string2ld and
	// Go's strconv both read "inf") and then fails on the non-finite RESULT with "increment
	// would produce NaN or Infinity"; nan is a parse error on both. Nothing is stored either
	// way, so GET stays null.
	//
	// NOTE: "1e400" / "-1e400" are deliberately NOT compared. They are an accepted
	// float64-vs-long-double divergence: Redis accumulates in C 80-bit long double, whose
	// range (~1e4932) REPRESENTS 1e400 as finite, so Redis STORES it (and even mangles the
	// 400-digit ld2string reply with a buffer over-read — a Redis 3.2 bug). redimos uses
	// IEEE-754 float64 (max ~1.8e308), and its DynamoDB Number backend tops out near 1e125,
	// so it rejects 1e400 as "value is not a valid float". Architectural limit, not a
	// shortcoming (see the float64/long-double note in the atomicity findings).
	fresh := d.k("ibf_fresh")
	d.eq("INCRBYFLOAT fresh +inf", bs("INCRBYFLOAT"), fresh, bs("+inf"))
	d.eq("INCRBYFLOAT fresh -inf", bs("INCRBYFLOAT"), fresh, bs("-inf"))
	d.eq("INCRBYFLOAT fresh inf (bare)", bs("INCRBYFLOAT"), fresh, bs("inf"))
	d.eq("INCRBYFLOAT fresh Inf (mixed case)", bs("INCRBYFLOAT"), fresh, bs("Inf"))
	d.eq("INCRBYFLOAT fresh INF (upper)", bs("INCRBYFLOAT"), fresh, bs("INF"))
	d.eq("INCRBYFLOAT fresh nan", bs("INCRBYFLOAT"), fresh, bs("nan"))
	d.eq("INCRBYFLOAT fresh NaN", bs("INCRBYFLOAT"), fresh, bs("NaN"))
	d.eq("GET fresh unchanged after rejects", bs("GET"), fresh)

	// A key that already holds a finite float: a rejected increment must not mutate it.
	seed := d.k("ibf_seed")
	d.eqFloatClose("INCRBYFLOAT seed 10.5", bs("INCRBYFLOAT"), seed, bs("10.5"))
	d.eq("INCRBYFLOAT seed +inf rejected", bs("INCRBYFLOAT"), seed, bs("+inf"))
	d.eqFloatClose("GET seed unchanged (still 10.5)", bs("GET"), seed)
}

// TestDiffSetRangeOffsetBoundaries covers GAP 2 and part of GAP 6: SETRANGE offset with
// int64 boundary values. A negative offset (including int64 min) replies "offset is out
// of range"; a non-integer offset replies not-an-integer. An empty value with a negative
// offset is still rejected on the offset FIRST (the offset check precedes the empty-value
// short-circuit in the proxy — Redis matches). Only small, in-range positive offsets are
// exercised for the success path; oversized offsets hit the value-size cap where the two
// error texts intentionally differ (and OOM the old oracle), so they are not compared.
func TestDiffSetRangeOffsetBoundaries(t *testing.T) {
	d := newDiffer(t)

	k := d.k("sr")
	// Negative offsets.
	d.eq("SETRANGE offset -1", bs("SETRANGE"), k, bs("-1"), bs("x"))
	d.eq("SETRANGE offset int64-min", bs("SETRANGE"), k, bs("-9223372036854775808"), bs("x"))
	d.eq("SETRANGE offset -1 empty value", bs("SETRANGE"), k, bs("-1"), bs(""))

	// Non-integer offset.
	d.eq("SETRANGE offset not-int", bs("SETRANGE"), k, bs("abc"), bs("x"))
	d.eq("SETRANGE offset float", bs("SETRANGE"), k, bs("1.5"), bs("x"))
	d.eq("SETRANGE offset with plus", bs("SETRANGE"), k, bs("+5"), bs("x"))

	// The key must still be untouched after every rejection above.
	d.eq("GET sr untouched", bs("GET"), k)
	d.eq("STRLEN sr untouched", bs("STRLEN"), k)

	// A small in-range offset works and zero-pads: seed then read back with GET so the
	// NUL-padding is compared byte-for-byte.
	ok := d.k("sr_ok")
	d.eq("SETRANGE offset 5 pads with NUL", bs("SETRANGE"), ok, bs("5"), bs("hi"))
	d.eq("GET after zero-pad SETRANGE", bs("GET"), ok)
	d.eq("STRLEN after zero-pad SETRANGE", bs("STRLEN"), ok)

	// SETRANGE offset 0 empty value on a fresh key: no write, replies length 0.
	e := d.k("sr_empty")
	d.eq("SETRANGE offset 0 empty on fresh", bs("SETRANGE"), e, bs("0"), bs(""))
	d.eq("GET fresh after empty SETRANGE (still null)", bs("GET"), e)

	// WRONGTYPE: SETRANGE against a live list, both with a real value and with empty
	// value (the empty-value path still type-checks first).
	lk := d.k("sr_list")
	d.eq("RPUSH make list", bs("RPUSH"), lk, bs("a"))
	d.eq("SETRANGE on list WRONGTYPE", bs("SETRANGE"), lk, bs("0"), bs("z"))
	d.eq("SETRANGE empty on list WRONGTYPE", bs("SETRANGE"), lk, bs("0"), bs(""))
}

// TestDiffGetRangeIndexBoundaries covers GAP 3: GETRANGE / SUBSTR with int64 boundary
// indices. GETRANGE is a READ, so the int64 extremes exercise only Redis' index
// arithmetic (no huge allocation): a negative index counts from the end (strlen+idx),
// then both ends clamp to [0, strlen-1], and start > end yields the empty string. The
// proxy's rangeBounds does the same int64 arithmetic, so the int64 extremes probe whether
// strlen+idx over/underflows differently on the two sides.
func TestDiffGetRangeIndexBoundaries(t *testing.T) {
	d := newDiffer(t)

	k := d.k("gr")
	d.eq("SET gr value", bs("SET"), k, bs("Hello World")) // len 11

	cases := [][2]string{
		{"0", "-1"},   // whole string
		{"0", "0"},    // first byte
		{"-1", "-1"},  // last byte
		{"-5", "-1"},  // tail
		{"-100", "-1"}, // start clamps to 0
		{"5", "3"},    // start > end -> empty
		{"0", "100"},  // end clamps to strlen-1
		{"100", "200"}, // both past end -> empty
		{"9223372036854775807", "9223372036854775807"},   // start = int64-max -> empty
		{"-9223372036854775808", "-9223372036854775808"}, // both int64-min
		{"-9223372036854775808", "9223372036854775807"},  // min..max -> whole string
		{"0", "-9223372036854775808"},                    // end int64-min underflows
		{"-9223372036854775808", "0"},                    // start int64-min, end 0
		{"9223372036854775807", "-1"},                    // start past end
		{"-11", "-1"},                                    // start = -strlen
		{"-12", "-1"},                                    // start = -(strlen+1) -> clamp 0
	}
	for _, c := range cases {
		d.eq("GETRANGE "+c[0]+" "+c[1], bs("GETRANGE"), k, bs(c[0]), bs(c[1]))
		// SUBSTR is the deprecated alias with identical semantics.
		d.eq("SUBSTR "+c[0]+" "+c[1], bs("SUBSTR"), k, bs(c[0]), bs(c[1]))
	}

	// Non-integer index replies not-an-integer (parsed before the range logic).
	d.eq("GETRANGE non-int start", bs("GETRANGE"), k, bs("x"), bs("1"))
	d.eq("GETRANGE non-int end", bs("GETRANGE"), k, bs("0"), bs("y"))
	d.eq("GETRANGE out-of-range int start", bs("GETRANGE"), k, bs("99999999999999999999999999"), bs("1"))

	// Missing key -> empty string regardless of indices (including extremes).
	missing := d.k("gr_missing")
	d.eq("GETRANGE missing key int64 extremes", bs("GETRANGE"), missing,
		bs("-9223372036854775808"), bs("9223372036854775807"))

	// Empty-string value -> empty regardless of indices.
	empty := d.k("gr_empty")
	d.eq("SET empty string", bs("SET"), empty, bs(""))
	d.eq("GETRANGE empty value", bs("GETRANGE"), empty, bs("0"), bs("-1"))

	// WRONGTYPE on a live non-string key.
	sk := d.k("gr_set")
	d.eq("SADD make set", bs("SADD"), sk, bs("m"))
	d.eq("GETRANGE on set WRONGTYPE", bs("GETRANGE"), sk, bs("0"), bs("-1"))
	d.eq("SUBSTR on set WRONGTYPE", bs("SUBSTR"), sk, bs("0"), bs("-1"))
}

// TestDiffZAddScoreParsing covers GAP 4: ZADD score parsing and round-trip formatting for
// DynamoDB-representable scores. Extreme magnitudes (1e400/1e309/9e999, ±inf, values
// outside DynamoDB Number's ~1e-130..1e125 domain, -0) are NOT compared here: they hit a
// known divergence — redimos parses scores with float64 and stores them as a DynamoDB
// Number, so it rejects overflow magnitudes Redis' long double accepts and cannot hold ±inf,
// while Redis stores them. Only small, exactly-representable, exactly-round-tripping scores
// are byte-compared, plus NaN which BOTH reject at parse time. ZADD reply, ZSCORE, and
// ZRANGE WITHSCORES are all compared.
func TestDiffZAddScoreParsing(t *testing.T) {
	d := newDiffer(t)

	// Each score value gets its own key so a rejection on one does not perturb the next.
	// ZADD reply (added count or error) and the resulting ZSCORE are compared.
	values := []string{
		"3.0e0", // exponent form of an integer -> "3"
		"0e0",   // zero via exponent -> "0"
		"1E10",  // uppercase exponent marker -> "10000000000"
		"nan",   // NaN -> rejected on both sides at parse time
	}
	for _, v := range values {
		zk := d.k("zx_" + v)
		m := bs("member")
		d.eq("ZADD score="+v, bs("ZADD"), zk, bs(v), m)
		d.eq("ZSCORE score="+v, bs("ZSCORE"), zk, m)
		// ZRANGE WITHSCORES exercises the stored-score formatting path once more.
		d.eq("ZRANGE WITHSCORES score="+v, bs("ZRANGE"), zk, bs("0"), bs("-1"), bs("WITHSCORES"))
	}
}

// TestDiffZIncrByParseRejects covers the parse-time surface of GAP 5 that BOTH sides agree
// on: a ZINCRBY increment that is not a valid float (including NaN) is rejected before any
// write, leaving the member untouched, and ZINCRBY against a wrong-type key replies
// WRONGTYPE. The result-non-finite cases (summing to ±inf/NaN) are intentionally omitted:
// redimos' handleZIncrBy does not re-validate the summed score and the backend cannot hold
// a non-finite score, so those diverge from Redis' "resulting score is not a number (NaN)"
// rejection — that divergence is verified separately against the live oracle.
func TestDiffZIncrByParseRejects(t *testing.T) {
	d := newDiffer(t)

	// A non-float increment is rejected before any write (parse-time), leaving the
	// member untouched. NaN is likewise rejected at parse on both sides.
	badk := d.k("zi_bad")
	d.eq("ZADD seed 5", bs("ZADD"), badk, bs("5"), bs("m"))
	d.eq("ZINCRBY not-a-float", bs("ZINCRBY"), badk, bs("abc"), bs("m"))
	d.eq("ZINCRBY nan increment", bs("ZINCRBY"), badk, bs("nan"), bs("m"))
	d.eq("ZSCORE after bad increments", bs("ZSCORE"), badk, bs("m"))

	// WRONGTYPE: ZINCRBY against a live string.
	strk := d.k("zi_str")
	d.eq("SET string", bs("SET"), strk, bs("v"))
	d.eq("ZINCRBY on string WRONGTYPE", bs("ZINCRBY"), strk, bs("1"), bs("m"))
}

// TestDiffIntegerBoundaryOps deepens the integer INCR/DECR/INCRBY/DECRBY overflow surface
// beyond the baseline: the DECRBY-of-int64-min negation guard, INCRBY toward both limits,
// exact-boundary values that must NOT overflow, and non-integer deltas. Every reply
// (new value or overflow error) is byte-compared and the stored value is read back.
func TestDiffIntegerBoundaryOps(t *testing.T) {
	d := newDiffer(t)

	// NOTE: "DECRBY key -9223372036854775808" (decrement BY int64-min) is deliberately NOT
	// compared. -(-2^63) is not representable, and Redis 3.2's decrbyCommand computes it as
	// incrDecrCommand(c, -incr): in C, -(INT64_MIN) is signed-overflow UB that wraps back to
	// INT64_MIN, so `DECRBY 0 -2^63` acts as `INCRBY 0 -2^63` and Redis STORES -2^63 (verified
	// against the live oracle: it replies :-9223372036854775808, NOT an overflow error).
	// redimos guards the negation and rejects it as "decrement would overflow", which is the
	// mathematically-correct answer (the true result +2^63 is unrepresentable). Reproducing
	// Redis here would mean copying a C-UB wraparound to emit a wrong value, so this is an
	// accepted (redimos-is-stricter) divergence — mirrors the LREM/SELECT int64-min quirks.

	// INCRBY by int64-min on 0 reaches exactly int64-min (representable) — must succeed.
	im := d.k("ib_min")
	d.eq("SET ib_min 0", bs("SET"), im, bs("0"))
	d.eq("INCRBY by int64-min", bs("INCRBY"), im, bs("-9223372036854775808"))
	d.eq("GET ib_min == int64-min", bs("GET"), im)
	// One more DECR underflows.
	d.eq("DECR past int64-min underflows", bs("DECR"), im)
	d.eq("GET ib_min still int64-min", bs("GET"), im)

	// INCRBY by int64-max on 0 reaches exactly int64-max — must succeed.
	imx := d.k("ib_max")
	d.eq("SET ib_max 0", bs("SET"), imx, bs("0"))
	d.eq("INCRBY by int64-max", bs("INCRBY"), imx, bs("9223372036854775807"))
	d.eq("GET ib_max == int64-max", bs("GET"), imx)
	// INCR overflows.
	d.eq("INCR past int64-max overflows", bs("INCR"), imx)
	d.eq("GET ib_max still int64-max", bs("GET"), imx)

	// INCRBY 0 on a fresh key: creates it as 0.
	z := d.k("ib_zero")
	d.eq("INCRBY 0 on fresh", bs("INCRBY"), z, bs("0"))
	d.eq("GET fresh after INCRBY 0", bs("GET"), z)

	// DECRBY by int64-max: -(2^63-1) IS representable, so it succeeds on 0.
	dmx := d.k("db_max")
	d.eq("SET db_max 0", bs("SET"), dmx, bs("0"))
	d.eq("DECRBY by int64-max", bs("DECRBY"), dmx, bs("9223372036854775807"))
	d.eq("GET db_max == -(int64-max)", bs("GET"), dmx)

	// Non-integer / out-of-range deltas are rejected before any write.
	nd := d.k("ib_bad")
	d.eq("SET ib_bad 7", bs("SET"), nd, bs("7"))
	d.eq("INCRBY float delta rejected", bs("INCRBY"), nd, bs("1.5"))
	d.eq("INCRBY overflow-magnitude delta rejected", bs("INCRBY"), nd, bs("99999999999999999999999999"))
	d.eq("DECRBY float delta rejected", bs("DECRBY"), nd, bs("2.5"))
	d.eq("GET ib_bad untouched (still 7)", bs("GET"), nd)

	// INCR on a non-integer stored value (leading '+', embedded space, float text).
	pv := d.k("ib_plusval")
	d.eq("SET value with leading +", bs("SET"), pv, bs("+10"))
	d.eq("INCR on +10 -> not integer", bs("INCR"), pv)
	fv := d.k("ib_floatval")
	d.eq("SET float-text value", bs("SET"), fv, bs("3.14"))
	d.eq("INCR on 3.14 -> not integer", bs("INCR"), fv)
	sv := d.k("ib_spaceval")
	d.eq("SET spaced value", bs("SET"), sv, bs(" 5"))
	d.eq("INCR on ' 5' -> not integer", bs("INCR"), sv)
}

// TestDiffValueSizeGuardNumeric covers the in-range half of GAP 6: a modest SETRANGE
// offset within both size limits succeeds and reports the right length, and a fresh key
// reads back identically. The oversized-offset rejection is NOT compared: redimos' size
// error text ("value exceeds backend limit (400KB)") differs from Redis 3.2's
// ("string exceeds maximum allowed size (512MB)"), and a multi-GB offset OOMs the old
// oracle — that boundary is an intentional divergence, not a parity case.
func TestDiffValueSizeGuardNumeric(t *testing.T) {
	d := newDiffer(t)

	// A modest offset well within limits succeeds and reports the right length.
	ok := d.k("vg_ok")
	d.eq("SETRANGE offset 100 short value", bs("SETRANGE"), ok, bs("100"), bs("abc"))
	d.eq("STRLEN after in-range SETRANGE", bs("STRLEN"), ok)
	d.eq("GET after in-range SETRANGE", bs("GET"), ok)

	// A fresh key never written reads back null / length 0 on both sides.
	fresh := d.k("vg_fresh")
	d.eq("GET vg_fresh untouched", bs("GET"), fresh)
	d.eq("STRLEN vg_fresh untouched", bs("STRLEN"), fresh)
}
