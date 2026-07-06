package integration

import "testing"

// Regression tests for the 2026-07-06 round-3 adversarial alignment pass — edge-case
// divergences that were fixable (not platform-bound) and are now fixed. Each byte-diffs
// against the live redis:3.2 oracle.

// TestFixFloatStrtodCompat: redimos' float parsing must match Redis' strtod, not Go's
// strconv — reject Go-style underscore separators (Redis rejects), accept a hex integer
// without a binary 'p' exponent (Redis accepts). Covers both the argument parser and the
// separate stored-value parser (INCRBYFLOAT/HINCRBYFLOAT reading an existing value).
func TestFixFloatStrtodCompat(t *testing.T) {
	d := newDiffer(t)

	// Underscores rejected everywhere Redis rejects them.
	d.eq("ZADD underscore score -> not a valid float", bs("ZADD"), d.k("u1"), bs("1_000"), bs("m"))
	d.eq("INCRBYFLOAT underscore arg -> error", bs("INCRBYFLOAT"), d.k("u2"), bs("1_000.5"))
	d.eq("ZRANGEBYSCORE underscore bound -> error", bs("ZRANGEBYSCORE"), d.k("u3"), bs("1_0"), bs("2_0"))

	// Hex integers accepted like Redis (0x1f = 31, 0x10 = 16).
	kh := d.k("hex")
	d.eq("ZADD hex score", bs("ZADD"), kh, bs("0x1f"), bs("m"))
	d.eq("ZSCORE hex -> 31", bs("ZSCORE"), kh, bs("m"))
	ki := d.k("hexincr")
	d.eq("SET 10", bs("SET"), ki, bs("10"))
	d.eq("INCRBYFLOAT 0x10 -> 26", bs("INCRBYFLOAT"), ki, bs("0x10"))

	// Stored-value parser: an underscore-bearing stored string is not a valid float.
	ks := d.k("stored")
	d.eq("SET 1_000", bs("SET"), ks, bs("1_000"))
	d.eq("INCRBYFLOAT on underscore stored value -> error", bs("INCRBYFLOAT"), ks, bs("1"))

	t.Logf("compared %d strtod-compat float replies vs Redis 3.2", d.n)
}

// TestFixBitposMissingVsEmpty: a MISSING key is a NULL object (BITPOS 0 -> 0, BITPOS 1 ->
// -1, ignoring start/end); an EXISTING empty string has no bytes to search (always -1).
func TestFixBitposMissingVsEmpty(t *testing.T) {
	d := newDiffer(t)
	miss := d.k("bp-miss")
	d.eq("BITPOS missing 0", bs("BITPOS"), miss, bs("0"))
	d.eq("BITPOS missing 0 0 5", bs("BITPOS"), miss, bs("0"), bs("0"), bs("5"))
	d.eq("BITPOS missing 0 100 200", bs("BITPOS"), miss, bs("0"), bs("100"), bs("200"))
	d.eq("BITPOS missing 1 0 5", bs("BITPOS"), miss, bs("1"), bs("0"), bs("5"))

	empty := d.k("bp-empty")
	d.eq("APPEND empty", bs("APPEND"), empty, bs("")) // create an existing empty string
	d.eq("BITPOS empty 0 -> -1", bs("BITPOS"), empty, bs("0"))
	d.eq("BITPOS empty 0 0 5 -> -1", bs("BITPOS"), empty, bs("0"), bs("0"), bs("5"))

	t.Logf("compared %d BITPOS missing/empty replies vs Redis 3.2", d.n)
}

// TestFixMiscEdges: SPOP negative-count error text (3.2 wording), the glob trailing-bare-
// backslash class case, and EXPIRE-family overflow (deletes, matching Redis) — all vs oracle.
func TestFixMiscEdges(t *testing.T) {
	d := newDiffer(t)

	// SPOP negative count -> Redis 3.2 "index out of range".
	ks := d.k("spop")
	d.eq("SADD", bs("SADD"), ks, bs("a"), bs("b"), bs("c"))
	d.eq("SPOP -1 -> index out of range", bs("SPOP"), ks, bs("-1"))

	// glob: a trailing bare backslash in an unclosed class matches nothing.
	kh := d.k("glob")
	d.eq("HSET backslash member", bs("HSET"), kh, bs("\\"), bs("v"))
	d.scanMatchEq("HSCAN MATCH [a\\ -> no match", [][]byte{bs("HSCAN"), kh}, "[a\\")

	// EXPIRE-family overflow: seconds*1000 overflows the ms domain -> Redis deletes the key.
	for _, tc := range []struct {
		name string
		cmd  [][]byte
	}{
		{"EXPIRE", [][]byte{bs("EXPIRE"), d.k("ov-ex"), bs("9223372036854775807")}},
		{"PEXPIRE", [][]byte{bs("PEXPIRE"), d.k("ov-pe"), bs("9223372036854775807")}},
		{"EXPIREAT", [][]byte{bs("EXPIREAT"), d.k("ov-ea"), bs("9223372036854775807")}},
	} {
		key := tc.cmd[1]
		d.eq("SET "+tc.name, bs("SET"), key, bs("v"))
		d.eq(tc.name+" overflow -> :1", tc.cmd...)
		d.eq(tc.name+" overflow deletes (EXISTS 0)", bs("EXISTS"), key)
		d.eq(tc.name+" overflow deletes (GET nil)", bs("GET"), key)
	}

	t.Logf("compared %d misc-edge replies vs Redis 3.2", d.n)
}
