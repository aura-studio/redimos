package integration

import "testing"

// Dimension B: numeric/float parity. Score and INCRBYFLOAT results are formatted back to
// the client as strings; a formatting difference (exponent case, trailing zeros, precision)
// is a silent divergence. Integer overflow must also produce the exact Redis error. These
// compare ZSCORE / ZRANGE WITHSCORES / INCRBYFLOAT / INCR-overflow byte-for-byte.

func TestDiffFloatFormatting(t *testing.T) {
	d := newDiffer(t)

	zk := d.k("z")
	// NOTE: negative zero ("-0") is deliberately omitted. DynamoDB's Number type normalizes
	// -0 to 0, so a stored score of -0 reads back as 0, whereas Redis preserves "-0". This
	// is a backend-normalization divergence (the local emulator hid it — it did NOT
	// normalize -0), documented in the compat doc; it is not a proxy formatting bug.
	scores := []string{
		"3.14", "2.5", "-1.5", "0", "100", "3.0", "0.1",
		"3.141592653589793", "1000000000000000", "0.0001",
		"1e10", "1.5e-5", "123456789012345",
	}
	for _, s := range scores {
		m := bs("m_" + s)
		d.eq("ZADD "+s, bs("ZADD"), zk, bs(s), m)
		d.eq("ZSCORE "+s, bs("ZSCORE"), zk, m)
	}

	// ZRANGE WITHSCORES is score-ordered (deterministic) — compare the whole reply, which
	// exercises score formatting for every member in one shot.
	d.eq("ZRANGE WITHSCORES", bs("ZRANGE"), zk, bs("0"), bs("-1"), bs("WITHSCORES"))

	// Accumulating float ops compare numerically (not byte-for-byte): Redis accumulates in
	// C long double, the proxy in float64, so results differ near the 17th digit. See
	// eqFloatClose. Direct formatting above (ZADD/ZSCORE) stays byte-for-byte and matches.
	fk := d.k("f")
	d.eqFloatClose("INCRBYFLOAT 3.0", bs("INCRBYFLOAT"), fk, bs("3.0"))
	d.eqFloatClose("INCRBYFLOAT 0.1", bs("INCRBYFLOAT"), fk, bs("0.1"))
	d.eqFloatClose("INCRBYFLOAT 5.0e3", bs("INCRBYFLOAT"), fk, bs("5.0e3"))
	d.eqFloatClose("GET after INCRBYFLOAT", bs("GET"), fk)
	d.eqFloatClose("ZINCRBY 2.5", bs("ZINCRBY"), zk, bs("2.5"), bs("m_3.14"))
}

func TestDiffIntegerOverflow(t *testing.T) {
	d := newDiffer(t)

	mk := d.k("max")
	d.eq("SET max int64", bs("SET"), mk, bs("9223372036854775807"))
	d.eq("INCR overflow", bs("INCR"), mk)
	d.eq("value unchanged after overflow", bs("GET"), mk)

	mk2 := d.k("max2")
	d.eq("SET max int64", bs("SET"), mk2, bs("9223372036854775807"))
	d.eq("INCRBY 1 overflow", bs("INCRBY"), mk2, bs("1"))

	mnk := d.k("min")
	d.eq("SET min int64", bs("SET"), mnk, bs("-9223372036854775808"))
	d.eq("DECR underflow", bs("DECR"), mnk)

	// A delta that itself exceeds int64.
	d.eq("INCRBY huge delta", bs("INCRBY"), d.k("h"), bs("99999999999999999999999999"))

	// INCR on a value with leading/trailing space is not an integer in Redis.
	sk := d.k("sp")
	d.eq("SET spaced", bs("SET"), sk, bs(" 11 "))
	d.eq("INCR spaced -> not integer", bs("INCR"), sk)
}
