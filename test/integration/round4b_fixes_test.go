package integration

import (
	"strings"
	"testing"
)

// Regression tests for the 2026-07-06 round-4b adversarial alignment pass (the seven
// finder dimensions that were rate-limited in round-4a, re-run). Each case byte-diffs
// against the live redis:3.2 oracle.

// TestFixBitParseStrictness: SETBIT/GETBIT/BITFIELD offsets, the BITFIELD SET/INCRBY
// value, and the BITFIELD type width all parse via string2ll — a leading '+', a
// leading zero, or an int64-overflowing value is rejected (not silently wrapped).
func TestFixBitParseStrictness(t *testing.T) {
	d := newDiffer(t)
	k := d.k("b")
	d.eq("SET foobar", bs("SET"), k, bs("foobar"))

	// Bit offsets (error: bit offset is not an integer or out of range).
	d.eq("SETBIT +8", bs("SETBIT"), d.k("sb1"), bs("+8"), bs("1"))
	d.eq("SETBIT 008", bs("SETBIT"), d.k("sb2"), bs("008"), bs("1"))
	d.eq("GETBIT +3", bs("GETBIT"), k, bs("+3"))

	// BITFIELD offset / value / type width.
	d.eq("BITFIELD offset +8", bs("BITFIELD"), d.k("bf1"), bs("GET"), bs("u8"), bs("+8"))
	d.eq("BITFIELD value +5", bs("BITFIELD"), d.k("bf2"), bs("SET"), bs("u8"), bs("0"), bs("+5"))
	d.eq("BITFIELD value overflow", bs("BITFIELD"), d.k("bf3"), bs("SET"), bs("u8"), bs("0"), bs("999999999999999999999999"))
	d.eq("BITFIELD type u08", bs("BITFIELD"), d.k("bf4"), bs("GET"), bs("u08"), bs("0"))
	d.eq("BITFIELD type u+8", bs("BITFIELD"), d.k("bf5"), bs("GET"), bs("u+8"), bs("0"))
	// A valid BITFIELD still works.
	d.eq("BITFIELD valid", bs("BITFIELD"), d.k("bf6"), bs("SET"), bs("u8"), bs("0"), bs("255"))

	t.Logf("compared %d bit-parse replies vs Redis 3.2", d.n)
}

// TestFixBitPosBitArg: BITPOS parses its bit argument via string2ll, so a non-integer
// (incl. leading '+') is the not-an-integer error, while a valid integer other than
// 0/1 is the bit-argument error.
func TestFixBitPosBitArg(t *testing.T) {
	d := newDiffer(t)
	k := d.k("bp")
	d.eq("SET foobar", bs("SET"), k, bs("foobar"))
	d.eq("BITPOS +1 -> not integer", bs("BITPOS"), k, bs("+1"))
	d.eq("BITPOS xyz -> not integer", bs("BITPOS"), k, bs("xyz"))
	d.eq("BITPOS 2 -> bit must be 0/1", bs("BITPOS"), k, bs("2"))
	d.eq("BITPOS 1 (valid)", bs("BITPOS"), k, bs("1"))
	t.Logf("compared %d BITPOS bit-arg replies vs Redis 3.2", d.n)
}

// TestFixBitCountOrdering: BITCOUNT validates its argument count AFTER the key lookup
// and type check, so a missing key is :0 and a wrong-type key is WRONGTYPE even with a
// surplus argument (not a syntax error).
func TestFixBitCountOrdering(t *testing.T) {
	d := newDiffer(t)
	d.eq("BITCOUNT missing extra -> :0", bs("BITCOUNT"), d.k("nokey"), bs("1"), bs("2"), bs("3"))
	lk := d.k("lk")
	d.eq("RPUSH", bs("RPUSH"), lk, bs("a"))
	d.eq("BITCOUNT wrong-type extra -> WRONGTYPE", bs("BITCOUNT"), lk, bs("1"), bs("2"), bs("3"))
	sk := d.k("sk")
	d.eq("SET foobar", bs("SET"), sk, bs("foobar"))
	d.eq("BITCOUNT string extra -> syntax error", bs("BITCOUNT"), sk, bs("1"), bs("2"), bs("3"))
	t.Logf("compared %d BITCOUNT-ordering replies vs Redis 3.2", d.n)
}

// TestFixIncrByFloatWrongType: INCRBYFLOAT checks the key type BEFORE parsing the
// increment, so a live wrong-type key replies WRONGTYPE even with a bad increment.
func TestFixIncrByFloatWrongType(t *testing.T) {
	d := newDiffer(t)
	k := d.k("wt")
	d.eq("RPUSH (make list)", bs("RPUSH"), k, bs("a"), bs("b"))
	d.eq("INCRBYFLOAT wrong-type bad incr -> WRONGTYPE", bs("INCRBYFLOAT"), k, bs("notafloat"))
	d.eq("INCRBYFLOAT wrong-type good incr -> WRONGTYPE", bs("INCRBYFLOAT"), k, bs("1.5"))
	t.Logf("compared %d INCRBYFLOAT-type replies vs Redis 3.2", d.n)
}

// TestFixScanWrongTypeOrder: HSCAN/SSCAN/ZSCAN check the key type BEFORE parsing the
// MATCH/COUNT options, so a wrong-type key replies WRONGTYPE even with a malformed
// option; a missing key replies the terminating ["0", []].
func TestFixScanWrongTypeOrder(t *testing.T) {
	d := newDiffer(t)
	k := d.k("st")
	d.eq("SET string", bs("SET"), k, bs("v"))
	d.eq("HSCAN wrong-type bad COUNT -> WRONGTYPE", bs("HSCAN"), k, bs("0"), bs("COUNT"), bs("bar"))
	d.eq("SSCAN wrong-type bad COUNT -> WRONGTYPE", bs("SSCAN"), k, bs("0"), bs("COUNT"), bs("bar"))
	d.eq("ZSCAN wrong-type bad COUNT -> WRONGTYPE", bs("ZSCAN"), k, bs("0"), bs("COUNT"), bs("bar"))
	d.eq("HSCAN missing bad COUNT -> [0,[]]", bs("HSCAN"), d.k("miss"), bs("0"), bs("COUNT"), bs("bar"))
	t.Logf("compared %d SCAN-type-order replies vs Redis 3.2", d.n)
}

// TestFixZAddNxXxOrder: ZADD validates the score/member pair count before the NX/XX
// incompatibility, so "ZADD k NX XX" (no pairs) is a syntax error, not the NX/XX error.
func TestFixZAddNxXxOrder(t *testing.T) {
	d := newDiffer(t)
	k := d.k("z")
	d.eq("ZADD NX XX (no pairs) -> syntax error", bs("ZADD"), k, bs("NX"), bs("XX"))
	// With pairs, the NX/XX conflict is reported.
	d.eq("ZADD NX XX 1 a -> NX/XX conflict", bs("ZADD"), k, bs("NX"), bs("XX"), bs("1"), bs("a"))
	t.Logf("compared %d ZADD-flag-order replies vs Redis 3.2", d.n)
}

// TestFixMetaFieldMember: a hash field or set member literally named "#meta" is a
// normal field/member (it is stored under the member sort-key prefix, distinct from
// the reserved meta item's prefix) and must NOT be filtered out of iterations.
func TestFixMetaFieldMember(t *testing.T) {
	d := newDiffer(t)
	h := d.k("h")
	d.eq("HSET #meta", bs("HSET"), h, bs("#meta"), bs("v1"))
	d.eq("HSET a", bs("HSET"), h, bs("a"), bs("v2"))
	d.eqSorted("HGETALL keeps #meta", bs("HGETALL"), h) // field/value order is unspecified
	d.eqSorted("HKEYS keeps #meta", bs("HKEYS"), h)
	d.eq("HLEN counts #meta", bs("HLEN"), h)
	d.eq("HGET #meta", bs("HGET"), h, bs("#meta"))

	s := d.k("s")
	d.eq("SADD #meta", bs("SADD"), s, bs("#meta"))
	d.eq("SADD x", bs("SADD"), s, bs("x"))
	d.eqSorted("SMEMBERS keeps #meta", bs("SMEMBERS"), s)
	d.eq("SISMEMBER #meta", bs("SISMEMBER"), s, bs("#meta"))
	d.eq("SCARD counts #meta", bs("SCARD"), s)
	t.Logf("compared %d #meta-field/member replies vs Redis 3.2", d.n)
}

// TestFixOversizedMemberNoop: SISMEMBER/SREM/SMOVE of a member too large to be stored
// (its sort key would exceed DynamoDB's limit) report it absent (:0), matching Redis'
// "not present" rather than surfacing a backend error.
func TestFixOversizedMemberNoop(t *testing.T) {
	d := newDiffer(t)
	big := bs(strings.Repeat("a", 1024)) // sort key would be 1025 bytes > DynamoDB's 1024
	s := d.k("s")
	d.eq("SADD normal", bs("SADD"), s, bs("x"))
	d.eq("SISMEMBER oversized -> :0", bs("SISMEMBER"), s, big)
	d.eq("SREM oversized -> :0", bs("SREM"), s, big)
	dst := d.k("dst")
	d.eq("SMOVE oversized -> :0", bs("SMOVE"), s, dst, big)
	// The normal member is untouched.
	d.eq("SISMEMBER normal -> :1", bs("SISMEMBER"), s, bs("x"))
	t.Logf("compared %d oversized-member replies vs Redis 3.2", d.n)
}
