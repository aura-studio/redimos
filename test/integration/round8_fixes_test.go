package integration

import "testing"

// Regression tests for the 2026-07-07 round-8 adversarial pass — a regression audit of
// the round-6/7 fixes plus a command-table-completeness sweep. Each case byte-diffs
// against the live redis:3.2 oracle. (The accepted platform floors surfaced this round
// — INCRBYFLOAT 5e-18 / accumulation drift / >17-significant-digit long-double noise,
// and the OBJECT/DEBUG "unsupported" and CLIENT-KILL "No such client" residuals — are
// NOT asserted here; see doc §4.1 / §4.5.)

// TestFixHSetExactArity: Redis 3.2's HSET is arity EXACTLY 4 (single field/value pair);
// the multi-field form is a 4.0+ extension. redimos had registered it -4 and silently
// wrote multiple pairs, replying a count where 3.2 rejects with an arity error.
func TestFixHSetExactArity(t *testing.T) {
	d := newDiffer(t)
	k := d.k("h2")
	d.eq("HSET new field -> :1", bs("HSET"), k, bs("f"), bs("v"))
	d.eq("HSET update field -> :0", bs("HSET"), k, bs("f"), bs("v2"))
	d.eq("HSET two pairs -> arity error", bs("HSET"), d.k("h3"), bs("f1"), bs("v1"), bs("f2"), bs("v2"))
	d.eq("HSET three pairs -> arity error", bs("HSET"), d.k("h4"), bs("a"), bs("1"), bs("b"), bs("2"), bs("c"), bs("3"))
	d.eq("HSET missing value (3 args) -> arity error", bs("HSET"), d.k("h5"), bs("f"))
	d.eq("HSET odd 5 args -> arity error", bs("HSET"), d.k("h6"), bs("f"), bs("v"), bs("x"))
	t.Logf("compared %d HSET-arity replies vs Redis 3.2", d.n)
}

// TestFixDecrByMostNegativeValueBased: DECRBY key -9223372036854775808 is not rejected
// outright — Redis negates the decrement (in C, -MinInt64 wraps to MinInt64), making it
// INCRBY by MinInt64, decided by the current value. redimos had a self-invented
// "-ERR decrement would overflow" special-case that fired regardless of the value.
func TestFixDecrByMostNegativeValueBased(t *testing.T) {
	d := newDiffer(t)
	const minI64 = "-9223372036854775808"

	k0 := d.k("d0")
	d.eq("SET 0", bs("SET"), k0, bs("0"))
	d.eq("DECRBY MinInt64 on 0 -> MinInt64", bs("DECRBY"), k0, bs(minI64))

	k5 := d.k("d5")
	d.eq("SET 5", bs("SET"), k5, bs("5"))
	d.eq("DECRBY MinInt64 on 5 -> ...803", bs("DECRBY"), k5, bs(minI64))

	kn := d.k("dn")
	d.eq("SET -1", bs("SET"), kn, bs("-1"))
	d.eq("DECRBY MinInt64 on -1 -> overflow error", bs("DECRBY"), kn, bs(minI64))
	t.Logf("compared %d DECRBY-MinInt64 replies vs Redis 3.2", d.n)
}

// TestFixClientPauseAndKill: CLIENT PAUSE rejects a negative millisecond timeout with
// "ERR timeout is negative" (round-7 only checked integer-ness); and CLIENT KILL with no
// addr/filter (argc 2) is a plain "ERR syntax error", not the CLIENT-usage help text
// (a round-7 arity-gate side effect).
func TestFixClientPauseAndKill(t *testing.T) {
	d := newDiffer(t)
	d.eq("CLIENT PAUSE -5 -> timeout is negative", bs("CLIENT"), bs("PAUSE"), bs("-5"))
	d.eq("CLIENT PAUSE 0 -> OK", bs("CLIENT"), bs("PAUSE"), bs("0"))
	d.eq("CLIENT PAUSE 1.5 -> not an integer", bs("CLIENT"), bs("PAUSE"), bs("1.5"))
	d.eq("CLIENT KILL (no addr) -> syntax error", bs("CLIENT"), bs("KILL"))
	t.Logf("compared %d CLIENT PAUSE/KILL replies vs Redis 3.2", d.n)
}

// TestFixIncrByFloatShortDecimals: INCRBYFLOAT/HINCRBYFLOAT reply the clean shortest
// decimal for ordinary human-entered values (round 6's %.17f surfaced float64's binary
// noise at the 17th place — "0.1" -> "0.10000000000000001"). Redis formats with a long
// double %.17Lf whose error lies beyond 17 decimals and trims clean; the float64
// shortest form reproduces that for every value whose two representations agree.
func TestFixIncrByFloatShortDecimals(t *testing.T) {
	d := newDiffer(t)
	for _, v := range []string{"0.1", "0.3", "3.3", "100.001", "3.14159", "-0.1", "10.5", "3.0", "5.0e3"} {
		d.eq("INCRBYFLOAT "+v, bs("INCRBYFLOAT"), d.k("f:"+v), bs(v))
	}
	for _, v := range []string{"0.1", "3.3", "100.001"} {
		d.eq("HINCRBYFLOAT "+v, bs("HINCRBYFLOAT"), d.k("hf:"+v), bs("fld"), bs(v))
	}
	// Sub-1e-17 magnitudes still round to Redis' 17 fixed decimals via the fallback path.
	d.eq("INCRBYFLOAT 9e-18 -> 17-place round", bs("INCRBYFLOAT"), d.k("f9"), bs("9e-18"))
	d.eq("INCRBYFLOAT 1e-20 -> 0", bs("INCRBYFLOAT"), d.k("f20"), bs("1e-20"))
	t.Logf("compared %d INCRBYFLOAT short-decimal replies vs Redis 3.2", d.n)
}
