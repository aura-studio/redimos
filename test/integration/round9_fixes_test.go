package integration

import "testing"

// Regression tests for the 2026-07-07 round-9 adversarial pass — a full arity-table
// audit (which came back clean after round 8's HSET fix) plus option/edge sweeps. Each
// case byte-diffs against the live redis:3.2 oracle. (The ZINCRBY/ZADD-INCR
// result-out-of-domain error-shape item and the COMMAND COUNT stub remain documented
// §4.1 / §4.5 residuals and are not asserted here.)

// TestFixHIncrByFloatInfinity: Redis 3.2's hincrbyfloatCommand — unlike
// incrbyfloatCommand — has NO isnan/isinf result guard, so HINCRBYFLOAT accepts an
// inf/-inf increment (storing "inf"/"-inf") and an inf+(-inf) NaN result (storing
// "-nan"). INCRBYFLOAT must still reject inf. A "nan" increment argument is rejected on
// both (string2ld rejects it at parse).
func TestFixHIncrByFloatInfinity(t *testing.T) {
	d := newDiffer(t)
	d.eq("HINCRBYFLOAT inf (new) -> inf", bs("HINCRBYFLOAT"), d.k("h1"), bs("f"), bs("inf"))
	d.eq("HINCRBYFLOAT +inf (new) -> inf", bs("HINCRBYFLOAT"), d.k("h2"), bs("f"), bs("+inf"))
	d.eq("HINCRBYFLOAT -inf (new) -> -inf", bs("HINCRBYFLOAT"), d.k("h3"), bs("f"), bs("-inf"))

	k4 := d.k("h4")
	d.eq("HSET f 5", bs("HSET"), k4, bs("f"), bs("5"))
	d.eq("HINCRBYFLOAT inf on 5 -> inf", bs("HINCRBYFLOAT"), k4, bs("f"), bs("inf"))
	d.eq("HGET f -> inf", bs("HGET"), k4, bs("f"))

	k5 := d.k("h5")
	d.eq("HINCRBYFLOAT inf seed", bs("HINCRBYFLOAT"), k5, bs("f"), bs("inf"))
	d.eq("HINCRBYFLOAT -inf on inf -> -nan", bs("HINCRBYFLOAT"), k5, bs("f"), bs("-inf"))

	d.eq("HINCRBYFLOAT nan -> not a valid float", bs("HINCRBYFLOAT"), d.k("h6"), bs("f"), bs("nan"))
	d.eq("INCRBYFLOAT inf still rejected", bs("INCRBYFLOAT"), d.k("s1"), bs("inf"))
	t.Logf("compared %d HINCRBYFLOAT-inf replies vs Redis 3.2", d.n)
}

// TestFixWaitArgValidation: WAIT validates numreplicas / timeout the way waitCommand
// does (a stub still returns :0, but a bad argument surfaces the real error, not :0).
func TestFixWaitArgValidation(t *testing.T) {
	d := newDiffer(t)
	d.eq("WAIT non-int numreplicas", bs("WAIT"), bs("foo"), bs("100"))
	d.eq("WAIT negative timeout", bs("WAIT"), bs("0"), bs("-5"))
	d.eq("WAIT non-int timeout", bs("WAIT"), bs("0"), bs("notanumber"))
	d.eq("WAIT 0 0 -> :0", bs("WAIT"), bs("0"), bs("0"))
	t.Logf("compared %d WAIT replies vs Redis 3.2", d.n)
}

// TestFixGetRangeDoubleNegative: GETRANGE with both indices negative and start > end
// replies "" (Redis' pre-normalization guard), while every start>=0 or start<=end case
// is unchanged — most importantly 0 <MinInt64> -> "h", which an earlier clamp-based
// attempt had broken.
func TestFixGetRangeDoubleNegative(t *testing.T) {
	d := newDiffer(t)
	k := d.k("gr")
	d.eq("SET hello", bs("SET"), k, bs("hello"))
	d.eq("GETRANGE -100 -200 -> empty", bs("GETRANGE"), k, bs("-100"), bs("-200"))
	d.eq("GETRANGE -1 -5 -> empty", bs("GETRANGE"), k, bs("-1"), bs("-5"))
	// Must-stay cases (guard is conditioned on start<0 && start>end):
	d.eq("GETRANGE 0 -200 -> h", bs("GETRANGE"), k, bs("0"), bs("-200"))
	d.eq("GETRANGE 0 MinInt64 -> h", bs("GETRANGE"), k, bs("0"), bs("-9223372036854775808"))
	d.eq("GETRANGE MinInt64 MinInt64 -> h", bs("GETRANGE"), k, bs("-9223372036854775808"), bs("-9223372036854775808"))
	d.eq("GETRANGE MinInt64 -1 -> hello", bs("GETRANGE"), k, bs("-9223372036854775808"), bs("-1"))
	d.eq("GETRANGE -3 -1 -> llo", bs("GETRANGE"), k, bs("-3"), bs("-1"))
	d.eq("GETRANGE 0 0 -> h", bs("GETRANGE"), k, bs("0"), bs("0"))
	t.Logf("compared %d GETRANGE double-negative replies vs Redis 3.2", d.n)
}
