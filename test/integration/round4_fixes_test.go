package integration

import (
	"strconv"
	"testing"
)

// Regression tests for the 2026-07-06 round-4 adversarial alignment pass. Each case
// byte-diffs redimos against the live redis:3.2 oracle. The divergences here were all
// fixable (not platform-bound): integer/flag parse strictness, arity, error-text
// wording, and the Redis 3.2 HLL estimator. Platform-bound divergences found in the
// same pass (out-of-domain zset scores) are asserted separately as atomicity unit
// tests, since they legitimately differ from the oracle.

// TestFixIntParseStrictness: SCAN-family COUNT and BITCOUNT/BITPOS indices parse via
// string2ll, which rejects a leading '+' and leading zeros (strconv.Atoi accepts them).
func TestFixIntParseStrictness(t *testing.T) {
	d := newDiffer(t)

	for _, v := range []string{"+5", "007", "-0"} {
		d.eq("SCAN COUNT "+v, bs("SCAN"), bs("0"), bs("COUNT"), bs(v))
	}
	kh := d.k("h")
	d.eq("HSET", bs("HSET"), kh, bs("f"), bs("v"))
	d.eq("HSCAN COUNT +5", bs("HSCAN"), kh, bs("0"), bs("COUNT"), bs("+5"))
	ks := d.k("s")
	d.eq("SADD", bs("SADD"), ks, bs("m"))
	d.eq("SSCAN COUNT 007", bs("SSCAN"), ks, bs("0"), bs("COUNT"), bs("007"))
	kz := d.k("z")
	d.eq("ZADD", bs("ZADD"), kz, bs("1"), bs("m"))
	d.eq("ZSCAN COUNT +1", bs("ZSCAN"), kz, bs("0"), bs("COUNT"), bs("+1"))

	kb := d.k("b")
	d.eq("SET foobar", bs("SET"), kb, bs("foobar"))
	d.eq("BITCOUNT +0 -1", bs("BITCOUNT"), kb, bs("+0"), bs("-1"))
	d.eq("BITCOUNT 007 -1", bs("BITCOUNT"), kb, bs("007"), bs("-1"))
	d.eq("BITPOS 1 +0", bs("BITPOS"), kb, bs("1"), bs("+0"))
	d.eq("BITCOUNT 0 -1 (valid, still works)", bs("BITCOUNT"), kb, bs("0"), bs("-1"))

	t.Logf("compared %d integer-parse replies vs Redis 3.2", d.n)
}

// TestFixPushXArity: Redis 3.2 LPUSHX/RPUSHX take exactly one value (arity 3); a
// multi-value form is a wrong-number-of-arguments error, not a multi-push.
func TestFixPushXArity(t *testing.T) {
	d := newDiffer(t)
	k := d.k("l")
	d.eq("RPUSH seed", bs("RPUSH"), k, bs("x"))
	d.eq("LPUSHX multi -> arity error", bs("LPUSHX"), k, bs("a"), bs("b"))
	d.eq("RPUSHX multi -> arity error", bs("RPUSHX"), k, bs("a"), bs("b"))
	d.eq("LPUSHX single (still works)", bs("LPUSHX"), k, bs("a"))
	d.eq("RPUSHX single (still works)", bs("RPUSHX"), k, bs("z"))
	t.Logf("compared %d pushx-arity replies vs Redis 3.2", d.n)
}

// TestFixSetRepeatedFlags: Redis' setCommand loose loop accepts a repeated NX/XX
// (idempotent) and a repeated EX/PX (last value wins), validates the surviving EX/PX
// value only once, and rejects a genuine EX<->PX conflict as a syntax error.
func TestFixSetRepeatedFlags(t *testing.T) {
	d := newDiffer(t)
	k := d.k("s")
	d.eq("SET NX NX -> OK", bs("SET"), k, bs("v"), bs("NX"), bs("NX"))
	d.eq("SET EX 10 EX 20 -> OK (last wins)", bs("SET"), k, bs("v"), bs("EX"), bs("10"), bs("EX"), bs("20"))
	d.eq("SET EX abc EX 10 -> OK (shadowed bad value)", bs("SET"), k, bs("v"), bs("EX"), bs("abc"), bs("EX"), bs("10"))
	d.eq("SET EX 10 PX 20000 -> syntax error", bs("SET"), k, bs("v"), bs("EX"), bs("10"), bs("PX"), bs("20000"))
	d.eq("SET NX XX -> syntax error", bs("SET"), k, bs("v"), bs("NX"), bs("XX"))
	d.eq("SET EX xyz -> not an integer", bs("SET"), k, bs("v"), bs("EX"), bs("xyz"))
	t.Logf("compared %d SET-flag replies vs Redis 3.2", d.n)
}

// TestFixZAddXXWrongType: ZADD ... XX (and XX INCR) on a live wrong-type key replies
// WRONGTYPE — Redis checks the key type right after lookup, before the XX-on-missing
// short-circuit. An XX on a genuinely missing key still replies :0 / nil.
func TestFixZAddXXWrongType(t *testing.T) {
	d := newDiffer(t)
	k := d.k("wt")
	d.eq("SET string", bs("SET"), k, bs("hello"))
	d.eq("ZADD XX on wrong-type -> WRONGTYPE", bs("ZADD"), k, bs("XX"), bs("1"), bs("a"))
	d.eq("ZADD XX INCR on wrong-type -> WRONGTYPE", bs("ZADD"), k, bs("XX"), bs("INCR"), bs("1"), bs("a"))

	miss := d.k("miss")
	d.eq("ZADD XX missing -> :0", bs("ZADD"), miss, bs("XX"), bs("1"), bs("a"))
	miss2 := d.k("miss2")
	d.eq("ZADD XX INCR missing -> nil", bs("ZADD"), miss2, bs("XX"), bs("INCR"), bs("1"), bs("a"))
	t.Logf("compared %d ZADD-XX-type replies vs Redis 3.2", d.n)
}

// TestFixScoreBoundWhitespace: a ZRANGEBYSCORE bound is parsed with raw strtod, which
// SKIPS leading whitespace (" 1" == 1.0) but rejects trailing junk and an all-space
// bound. A ZADD score, parsed via getDoubleFromObject, still rejects a leading space.
func TestFixScoreBoundWhitespace(t *testing.T) {
	d := newDiffer(t)
	k := d.k("z")
	d.eq("ZADD", bs("ZADD"), k, bs("1"), bs("a"), bs("2"), bs("b"), bs("3"), bs("c"))
	d.eq("ZRANGEBYSCORE ' 1' 5 (leading space ok)", bs("ZRANGEBYSCORE"), k, bs(" 1"), bs("5"))
	d.eq("ZRANGEBYSCORE '1 ' 5 (trailing space reject)", bs("ZRANGEBYSCORE"), k, bs("1 "), bs("5"))
	d.eq("ZRANGEBYSCORE '  ' 5 (all space reject)", bs("ZRANGEBYSCORE"), k, bs("  "), bs("5"))
	d.eq("ZRANGEBYSCORE '' 0 (empty bound is 0)", bs("ZRANGEBYSCORE"), k, bs(""), bs("0"))
	d.eq("ZCOUNT '( 1' 5 (exclusive + leading space)", bs("ZCOUNT"), k, bs("( 1"), bs("5"))

	zz := d.k("zz")
	d.eq("ZADD ' 1' m (score rejects leading space)", bs("ZADD"), zz, bs(" 1"), bs("m"))
	t.Logf("compared %d score-bound replies vs Redis 3.2", d.n)
}

// TestFixZSetErrorWording: ZRANGEBYLEX LIMIT non-integer offset/count and
// ZUNIONSTORE/ZINTERSTORE WEIGHTS non-float use Redis' specific error strings.
func TestFixZSetErrorWording(t *testing.T) {
	d := newDiffer(t)
	k := d.k("z")
	d.eq("ZADD", bs("ZADD"), k, bs("0"), bs("a"), bs("0"), bs("b"))
	d.eq("ZRANGEBYLEX LIMIT foo 5 -> not an integer", bs("ZRANGEBYLEX"), k, bs("-"), bs("+"), bs("LIMIT"), bs("foo"), bs("5"))

	src := d.k("src")
	dst := d.k("dst")
	d.eq("ZADD src", bs("ZADD"), src, bs("1"), bs("a"))
	d.eq("ZUNIONSTORE WEIGHTS foo -> weight not a float", bs("ZUNIONSTORE"), dst, bs("1"), src, bs("WEIGHTS"), bs("foo"))
	d.eq("ZINTERSTORE WEIGHTS foo -> weight not a float", bs("ZINTERSTORE"), dst, bs("1"), src, bs("WEIGHTS"), bs("foo"))
	t.Logf("compared %d zset-error-wording replies vs Redis 3.2", d.n)
}

// TestFixGeoErrors: GEO parse order (lookup first: missing -> *0, wrong type ->
// WRONGTYPE) and the exact geo.c error strings for radius/COUNT/coordinate failures.
func TestFixGeoErrors(t *testing.T) {
	d := newDiffer(t)
	g := d.k("g")
	d.eq("GEOADD seed", bs("GEOADD"), g, bs("13.361389"), bs("38.115556"), bs("Palermo"))

	d.eq("GEORADIUS neg radius", bs("GEORADIUS"), g, bs("15"), bs("37"), bs("-5"), bs("km"))
	d.eq("GEORADIUS non-numeric radius", bs("GEORADIUS"), g, bs("15"), bs("37"), bs("x"), bs("km"))
	d.eq("GEORADIUS COUNT 0", bs("GEORADIUS"), g, bs("15"), bs("37"), bs("200"), bs("km"), bs("COUNT"), bs("0"))
	d.eq("GEORADIUS COUNT abc", bs("GEORADIUS"), g, bs("15"), bs("37"), bs("200"), bs("km"), bs("COUNT"), bs("abc"))
	d.eq("GEORADIUS COUNT +5", bs("GEORADIUS"), g, bs("15"), bs("37"), bs("200"), bs("km"), bs("COUNT"), bs("+5"))
	d.eq("GEORADIUS center out of range", bs("GEORADIUS"), g, bs("999"), bs("37"), bs("5"), bs("km"))

	// Lookup-first order: a missing key replies *0 even with otherwise-bad args.
	miss := d.k("miss")
	d.eq("GEORADIUS missing -> *0", bs("GEORADIUS"), miss, bs("15"), bs("37"), bs("x"), bs("km"))
	d.eq("GEORADIUSBYMEMBER missing -> *0", bs("GEORADIUSBYMEMBER"), miss, bs("M"), bs("5"), bs("km"))

	// A live wrong-type key is WRONGTYPE, before any radius parse.
	wt := d.k("wt")
	d.eq("SET string", bs("SET"), wt, bs("v"))
	d.eq("GEORADIUS wrong-type -> WRONGTYPE", bs("GEORADIUS"), wt, bs("15"), bs("37"), bs("-5"), bs("km"))

	// GEOADD odd triplet and a non-finite coordinate.
	g2 := d.k("g2")
	d.eq("GEOADD odd triplet", bs("GEOADD"), g2, bs("13.0"), bs("38.0"), bs("P"), bs("extra"))
	d.eq("GEOADD inf coordinate", bs("GEOADD"), g2, bs("inf"), bs("38.0"), bs("X"))
	t.Logf("compared %d geo-error replies vs Redis 3.2", d.n)
}

// TestFixConfigAndPFDebug: CONFIG rejects an unknown subcommand (only GET/SET/
// RESETSTAT/REWRITE are valid) while keeping the CONFIG GET probe stub; PFDEBUG splits
// its unknown-subcommand and wrong-arity errors.
func TestFixConfigAndPFDebug(t *testing.T) {
	d := newDiffer(t)
	d.eq("CONFIG NOSUCH -> subcommand error", bs("CONFIG"), bs("NOSUCH"))
	d.eq("CONFIG GET maxmemory (stub still works)", bs("CONFIG"), bs("GET"), bs("maxmemory"))

	k := d.k("hll")
	d.eq("PFADD", bs("PFADD"), k, bs("a"))
	d.eq("PFDEBUG FOO -> unknown subcommand", bs("PFDEBUG"), bs("FOO"), k)
	d.eq("PFDEBUG GETREG extra -> arity error", bs("PFDEBUG"), bs("GETREG"), k, bs("extra"))
	t.Logf("compared %d config/pfdebug replies vs Redis 3.2", d.n)
}

// TestFixPFCountEstimator: with the Redis 3.2 LEGACY estimator (harmonic mean +
// LINEARCOUNTING + bias polynomial), PFCOUNT matches redis:3.2 exactly for the same
// registers across the small/mid/large cardinality regimes.
func TestFixPFCountEstimator(t *testing.T) {
	d := newDiffer(t)
	for _, n := range []int{5, 200, 1000, 3000} {
		k := d.k("pf" + strconv.Itoa(n))
		args := make([][]byte, 0, n+2)
		args = append(args, bs("PFADD"), k)
		for i := 0; i < n; i++ {
			args = append(args, bs("e:"+strconv.Itoa(i)))
		}
		d.eq("PFADD "+strconv.Itoa(n), args...)
		d.eq("PFCOUNT n="+strconv.Itoa(n), bs("PFCOUNT"), k)
	}
	t.Logf("compared %d PFCOUNT replies vs Redis 3.2", d.n)
}
