package integration

import (
	"strings"
	"testing"
)

// Regression tests for the 2026-07-06 round-5 adversarial pass — completeness gaps in
// earlier fixes plus a few less-covered edges. Each byte-diffs against redis:3.2.
// (The auth-ordering fixes from the same round need a requirepass instance and are
// covered by unit tests in internal/command instead.)

// TestFixHexFractionalFloat: glibc strtod accepts hex floats with a fractional part and
// no binary 'p' exponent (0x1.8 = 1.5). Round 4 rescued hex INTEGERS only; the fraction
// case must be rescued too, across every float-argument entry point.
func TestFixHexFractionalFloat(t *testing.T) {
	d := newDiffer(t)
	ki := d.k("incr")
	d.eq("INCRBYFLOAT 0x1.8 -> 1.5", bs("INCRBYFLOAT"), ki, bs("0x1.8"))
	kh := d.k("hincr")
	d.eq("HINCRBYFLOAT 0x.8 -> 0.5", bs("HINCRBYFLOAT"), kh, bs("f"), bs("0x.8"))
	kz := d.k("z")
	d.eq("ZADD 0x1.8 m", bs("ZADD"), kz, bs("0x1.8"), bs("m"))
	d.eq("ZSCORE m -> 1.5", bs("ZSCORE"), kz, bs("m"))
	d.eq("ZRANGEBYSCORE bound 0x1.8", bs("ZRANGEBYSCORE"), kz, bs("0x1.8"), bs("3"))
	// The round-4 hex-integer rescue still works.
	ki2 := d.k("incr2")
	d.eq("INCRBYFLOAT 0x10 -> 16", bs("INCRBYFLOAT"), ki2, bs("0x10"))
	// A stored hex-fraction value is read back correctly by INCRBYFLOAT.
	ks := d.k("stored")
	d.eq("SET 0x1.8", bs("SET"), ks, bs("0x1.8"))
	d.eq("INCRBYFLOAT +1 -> 2.5", bs("INCRBYFLOAT"), ks, bs("1"))
	t.Logf("compared %d hex-float replies vs Redis 3.2", d.n)
}

// TestFixScoreBoundOverflow: a ZRANGEBYSCORE/ZCOUNT range bound is never persisted, so
// Redis' zslParseRange saturates an out-of-float64-range magnitude (1e400) to +/-inf and
// accepts it — while a STORE score of 1e400 stays rejected (accepted platform limit).
func TestFixScoreBoundOverflow(t *testing.T) {
	d := newDiffer(t)
	k := d.k("z")
	d.eq("ZADD", bs("ZADD"), k, bs("1"), bs("a"), bs("2"), bs("b"), bs("3"), bs("c"))
	d.eq("ZRANGEBYSCORE 1 1e400", bs("ZRANGEBYSCORE"), k, bs("1"), bs("1e400"))
	d.eq("ZCOUNT -inf 1e400 -> 3", bs("ZCOUNT"), k, bs("-inf"), bs("1e400"))
	d.eq("ZRANGEBYSCORE -1e400 3", bs("ZRANGEBYSCORE"), k, bs("-1e400"), bs("3"))
	d.eq("ZREVRANGEBYSCORE 1e400 -inf", bs("ZREVRANGEBYSCORE"), k, bs("1e400"), bs("-inf"))
	// A STORE score of 1e400 is still rejected on both (accepted DynamoDB-Number limit).
	d.eq("ZADD 1e400 rejected", bs("ZADD"), d.k("z2"), bs("1e400"), bs("m"))
	t.Logf("compared %d score-bound-overflow replies vs Redis 3.2", d.n)
}

// TestFixListElementSize: a list element is stored in the value attribute (up to ~390KB),
// not as a member name (1KB), so a 2KB element must be accepted like Redis (not rejected).
func TestFixListElementSize(t *testing.T) {
	d := newDiffer(t)
	big := bs(strings.Repeat("x", 2000))
	kr := d.k("r")
	d.eq("RPUSH 2KB element", bs("RPUSH"), kr, big)
	d.eq("LRANGE round-trips the element", bs("LRANGE"), kr, bs("0"), bs("-1"))
	kl := d.k("l")
	d.eq("LPUSH 2KB element", bs("LPUSH"), kl, big)
	d.eq("RPUSH seed", bs("RPUSH"), d.k("s"), bs("x"))
	// LSET with a 2KB value.
	ks := d.k("lset")
	d.eq("RPUSH seed2", bs("RPUSH"), ks, bs("y"))
	d.eq("LSET 0 2KB", bs("LSET"), ks, bs("0"), big)
	// LINSERT with a 2KB value.
	ki := d.k("lins")
	d.eq("RPUSH pivot", bs("RPUSH"), ki, bs("p"))
	d.eq("LINSERT before p 2KB", bs("LINSERT"), ki, bs("BEFORE"), bs("p"), big)
	t.Logf("compared %d list-element-size replies vs Redis 3.2", d.n)
}

// TestFixBitCountEdges: BITCOUNT parses both range args before the empty-string check (so
// a non-integer bound errors even on an empty value), and short-circuits to :0 for two
// negative indices with start > end (Redis' pre-clamp guard).
func TestFixBitCountEdges(t *testing.T) {
	d := newDiffer(t)
	e := d.k("empty")
	d.eq("SET empty", bs("SET"), e, bs(""))
	d.eq("BITCOUNT empty 0 abc -> not integer", bs("BITCOUNT"), e, bs("0"), bs("abc"))

	k := d.k("bc")
	d.eq("SET foobar", bs("SET"), k, bs("foobar"))
	d.eq("BITCOUNT -100 -200 -> :0", bs("BITCOUNT"), k, bs("-100"), bs("-200"))
	d.eq("BITCOUNT -10 -20 -> :0", bs("BITCOUNT"), k, bs("-10"), bs("-20"))
	d.eq("BITCOUNT -8 -7 (start<end)", bs("BITCOUNT"), k, bs("-8"), bs("-7"))
	d.eq("BITCOUNT 0 -1 (whole)", bs("BITCOUNT"), k, bs("0"), bs("-1"))
	t.Logf("compared %d BITCOUNT-edge replies vs Redis 3.2", d.n)
}
