package integration

import "testing"

// Dimension N (depth): bit-operation edge cases that the base bitops_test.go does not reach.
// Everything here is compared byte-for-byte against a live Redis 3.2 oracle by the differ, so
// no expected outputs are hardcoded — these functions just drive the exact edge inputs named
// in the gap file (BITFIELD #idx multiplier near the 2^32 offset cap, negative SET/INCRBY on
// unsigned fields including the u1/u63 extremes, all three OVERFLOW modes, the SETBIT offset
// boundary, and multi-op BITFIELD where an early op fails but later ops still run) against
// both endpoints. Binary values are set directly so both sides start from identical bytes.

// --- GAP 1 -------------------------------------------------------------------------------
// BITFIELD #idx multiplier where idx*nbits sits at/around the 2^32-bit offset cap. For a u63
// field the guard rejects an idx whose (idx*63 + 63) would exceed 2^32 bits. Exercise the
// last-accepted index, the first-rejected index, and the overflow-into-negative index. Both
// sides must agree (Redis rejects an out-of-range offset; a huge in-range offset would try to
// grow to ~512MB, so we deliberately probe only the *rejected* side of the boundary here and
// verify a tiny valid op afterwards still matches).
func TestDiffBitfieldIdxMultiplierBoundaryU63(t *testing.T) {
	d := newDiffer(t)
	k := d.k("bfidx63")

	// 2^32 = 4294967296 bits. For u63, floor((2^32 - 63) / 63) is the largest idx that could
	// even be considered; the proxy's guard rejects when idx*63 + 63 > 2^32. These indices all
	// land at or above that cap and must be rejected identically on both sides.
	d.eq("BITFIELD GET u63 #68191066", bs("BITFIELD"), k, bs("GET"), bs("u63"), bs("#68191066")) // ~ just over 2^32
	d.eq("BITFIELD GET u63 #68191067", bs("BITFIELD"), k, bs("GET"), bs("u63"), bs("#68191067"))
	d.eq("BITFIELD SET u63 #68191067 1", bs("BITFIELD"), k, bs("SET"), bs("u63"), bs("#68191067"), bs("1"))
	// idx large enough that idx*63 overflows int64 -> historically a negative slice index panic.
	d.eq("BITFIELD GET u63 #huge overflow", bs("BITFIELD"), k, bs("GET"), bs("u63"), bs("#146402730743537575"))
	d.eq("BITFIELD SET u63 #huge overflow", bs("BITFIELD"), k, bs("SET"), bs("u63"), bs("#146402730743537575"), bs("1"))

	// A small #idx that is safely in range still works on both sides (proxy alive, math correct).
	d.eq("BITFIELD SET u63 #0 max-ish", bs("BITFIELD"), k, bs("SET"), bs("u63"), bs("#0"), bs("123456789"))
	d.eq("BITFIELD GET u63 #0", bs("BITFIELD"), k, bs("GET"), bs("u63"), bs("#0"))
	d.eq("BITFIELD SET u63 #1 wrap-neg", bs("BITFIELD"), k, bs("SET"), bs("u63"), bs("#1"), bs("-1"))
	d.eq("BITFIELD GET u63 #1", bs("BITFIELD"), k, bs("GET"), bs("u63"), bs("#1"))
	d.eq("PING after idx boundary", bs("PING"))
}

// --- GAP 2 -------------------------------------------------------------------------------
// BITFIELD SET/INCRBY with a NEGATIVE value into an UNSIGNED field. Default OVERFLOW is WRAP,
// so a negative wraps via two's-complement modulo 2^nbits (-1 -> 2^nbits-1). Cover u8, u16 and
// the widest unsigned u63, plus INCRBY going negative from 0.
func TestDiffBitfieldNegativeToUnsignedWrap(t *testing.T) {
	d := newDiffer(t)

	k8 := d.k("bfneg8")
	d.eq("SET u8 0 -1 (=>255)", bs("BITFIELD"), k8, bs("SET"), bs("u8"), bs("0"), bs("-1"))
	d.eq("GET u8 0 after -1", bs("BITFIELD"), k8, bs("GET"), bs("u8"), bs("0"))
	d.eq("GET byte0", bs("GET"), k8)
	d.eq("SET u8 0 -256 (=>0)", bs("BITFIELD"), k8, bs("SET"), bs("u8"), bs("0"), bs("-256"))
	d.eq("SET u8 0 -257 (=>255)", bs("BITFIELD"), k8, bs("SET"), bs("u8"), bs("0"), bs("-257"))

	k16 := d.k("bfneg16")
	d.eq("SET u16 0 -1 (=>65535)", bs("BITFIELD"), k16, bs("SET"), bs("u16"), bs("0"), bs("-1"))
	d.eq("GET u16 0", bs("BITFIELD"), k16, bs("GET"), bs("u16"), bs("0"))
	d.eq("SET u16 0 -32768", bs("BITFIELD"), k16, bs("SET"), bs("u16"), bs("0"), bs("-32768"))

	k63 := d.k("bfneg63")
	d.eq("SET u63 0 -1 (=>2^63-1)", bs("BITFIELD"), k63, bs("SET"), bs("u63"), bs("0"), bs("-1"))
	d.eq("GET u63 0", bs("BITFIELD"), k63, bs("GET"), bs("u63"), bs("0"))

	// INCRBY going below 0 on an unsigned field wraps too.
	ki := d.k("bfnegincr")
	d.eq("SET u8 0 0", bs("BITFIELD"), ki, bs("SET"), bs("u8"), bs("0"), bs("0"))
	d.eq("INCRBY u8 0 -1 (=>255)", bs("BITFIELD"), ki, bs("INCRBY"), bs("u8"), bs("0"), bs("-1"))
	d.eq("INCRBY u8 0 -255 (=>0)", bs("BITFIELD"), ki, bs("INCRBY"), bs("u8"), bs("0"), bs("-255"))
	d.eq("INCRBY u8 0 -1 again (=>255)", bs("BITFIELD"), ki, bs("INCRBY"), bs("u8"), bs("0"), bs("-1"))
}

// --- GAP 3 -------------------------------------------------------------------------------
// BITFIELD on the tightest fields (u1, i1) with WRAP/SAT/FAIL to pin the narrow-width modulo
// and saturation boundaries. u1 range is [0,1]; i1 range is [-1,0].
func TestDiffBitfieldNarrowWidthOverflowModes(t *testing.T) {
	d := newDiffer(t)

	// u1 WRAP: (-1) mod 2 = 1; INCRBY toggles.
	u1 := d.k("bfu1")
	d.eq("SET u1 0 0", bs("BITFIELD"), u1, bs("SET"), bs("u1"), bs("0"), bs("0"))
	d.eq("INCRBY u1 0 -1 WRAP (=>1)", bs("BITFIELD"), u1, bs("INCRBY"), bs("u1"), bs("0"), bs("-1"))
	d.eq("INCRBY u1 0 1 WRAP (=>0)", bs("BITFIELD"), u1, bs("INCRBY"), bs("u1"), bs("0"), bs("1"))
	d.eq("SET u1 0 3 WRAP (=>1)", bs("BITFIELD"), u1, bs("SET"), bs("u1"), bs("0"), bs("3"))
	d.eq("SET u1 0 2 WRAP (=>0)", bs("BITFIELD"), u1, bs("SET"), bs("u1"), bs("0"), bs("2"))

	// u1 SAT: values clamp to [0,1].
	u1s := d.k("bfu1sat")
	d.eq("OVERFLOW SAT SET u1 0 5 (=>1)", bs("BITFIELD"), u1s, bs("OVERFLOW"), bs("SAT"), bs("SET"), bs("u1"), bs("0"), bs("5"))
	d.eq("OVERFLOW SAT SET u1 0 -5 (=>0)", bs("BITFIELD"), u1s, bs("OVERFLOW"), bs("SAT"), bs("SET"), bs("u1"), bs("0"), bs("-5"))
	d.eq("OVERFLOW SAT INCRBY u1 0 -3 (=>0)", bs("BITFIELD"), u1s, bs("OVERFLOW"), bs("SAT"), bs("INCRBY"), bs("u1"), bs("0"), bs("-3"))
	d.eq("OVERFLOW SAT INCRBY u1 0 9 (=>1)", bs("BITFIELD"), u1s, bs("OVERFLOW"), bs("SAT"), bs("INCRBY"), bs("u1"), bs("0"), bs("9"))

	// u1 FAIL: out-of-range write returns nil and leaves the field unchanged.
	u1f := d.k("bfu1fail")
	d.eq("OVERFLOW FAIL SET u1 0 2 (=>nil)", bs("BITFIELD"), u1f, bs("OVERFLOW"), bs("FAIL"), bs("SET"), bs("u1"), bs("0"), bs("2"))
	d.eq("GET u1 0 unchanged", bs("BITFIELD"), u1f, bs("GET"), bs("u1"), bs("0"))

	// i1 range is [-1,0]: cover WRAP and SAT boundaries.
	i1 := d.k("bfi1")
	d.eq("SET i1 0 -1", bs("BITFIELD"), i1, bs("SET"), bs("i1"), bs("0"), bs("-1"))
	d.eq("GET i1 0 (=>-1)", bs("BITFIELD"), i1, bs("GET"), bs("i1"), bs("0"))
	d.eq("INCRBY i1 0 -1 WRAP (=>0)", bs("BITFIELD"), i1, bs("INCRBY"), bs("i1"), bs("0"), bs("-1"))
	d.eq("OVERFLOW SAT SET i1 0 5 (=>0)", bs("BITFIELD"), i1, bs("OVERFLOW"), bs("SAT"), bs("SET"), bs("i1"), bs("0"), bs("5"))
	d.eq("OVERFLOW SAT SET i1 0 -9 (=>-1)", bs("BITFIELD"), i1, bs("OVERFLOW"), bs("SAT"), bs("SET"), bs("i1"), bs("0"), bs("-9"))
}

// --- GAP 4 -------------------------------------------------------------------------------
// SETBIT/GETBIT offset boundary at the 2^32-bit cap. 2^32 = 4294967296; the last valid bit
// offset is 4294967295 (a ~512MB write), the first invalid is 4294967296. We probe the reject
// side of the boundary (cheap, no 512MB alloc) on both endpoints and a couple past it, plus the
// negative-offset and non-integer-offset errors. A GETBIT far past the end (read path, no alloc)
// on a short key returns 0 on both sides.
func TestDiffSetBitOffsetBoundary(t *testing.T) {
	d := newDiffer(t)
	k := d.k("sboff")
	d.eq("SET seed short", bs("SET"), k, bs("\xff"))

	// First-invalid offset (== 2^32) and beyond: both reject with the same error.
	d.eq("SETBIT off 2^32 (reject)", bs("SETBIT"), k, bs("4294967296"), bs("1"))
	d.eq("SETBIT off 2^32+5 (reject)", bs("SETBIT"), k, bs("4294967301"), bs("1"))
	d.eq("SETBIT off way over", bs("SETBIT"), k, bs("9999999999999"), bs("1"))
	d.eq("SETBIT negative offset", bs("SETBIT"), k, bs("-1"), bs("1"))
	d.eq("SETBIT non-int offset", bs("SETBIT"), k, bs("notanumber"), bs("1"))
	d.eq("SETBIT bad bit value 2", bs("SETBIT"), k, bs("3"), bs("2"))
	d.eq("SETBIT bad bit value -1", bs("SETBIT"), k, bs("3"), bs("-1"))

	// GETBIT read path at/over the boundary: past-end returns 0, the cap itself errors.
	d.eq("GETBIT off 2^32 (reject)", bs("GETBIT"), k, bs("4294967296"))
	d.eq("GETBIT far past end (=>0)", bs("GETBIT"), k, bs("4294967295"))
	d.eq("GETBIT negative offset", bs("GETBIT"), k, bs("-1"))

	// A valid small SETBIT after the rejected ones still works and matches (proxy alive).
	d.eq("SETBIT 0 0 old bit", bs("SETBIT"), k, bs("0"), bs("0"))
	d.eq("GET after", bs("GET"), k)
	d.eq("PING after boundary", bs("PING"))
}

// --- GAP 5 -------------------------------------------------------------------------------
// Multi-op BITFIELD where an early op fails under OVERFLOW FAIL (write skipped, nil element)
// but subsequent ops in the same command still execute and observe the UNCHANGED value. Also
// mixes modes across the op list (OVERFLOW only affects ops that follow it) and interleaves
// GET/SET/INCRBY so the returned array shape and per-element values are checked together.
func TestDiffBitfieldMultiOpFailContinues(t *testing.T) {
	d := newDiffer(t)

	k := d.k("bfmulti")
	// Seed a known byte via a plain SET so both sides start identical, then read/mutate.
	d.eq("SET seed u8=10", bs("BITFIELD"), k, bs("SET"), bs("u8"), bs("0"), bs("10"))
	// FAIL SET 300 (>255) skips -> nil; the GET after still returns the unchanged 10.
	d.eq("FAIL SET u8 300 then GET",
		bs("BITFIELD"), k,
		bs("OVERFLOW"), bs("FAIL"), bs("SET"), bs("u8"), bs("0"), bs("300"),
		bs("GET"), bs("u8"), bs("0"))
	// Verify the field is still 10 after the failed write.
	d.eq("GET u8 0 unchanged (=>10)", bs("BITFIELD"), k, bs("GET"), bs("u8"), bs("0"))

	// Mode switches mid-list: WRAP set, then FAIL incrby overflow (nil), then SAT incrby (clamp).
	k2 := d.k("bfmulti2")
	d.eq("multi mixed modes",
		bs("BITFIELD"), k2,
		bs("SET"), bs("u8"), bs("0"), bs("250"), // default WRAP, returns old 0
		bs("OVERFLOW"), bs("FAIL"), bs("INCRBY"), bs("u8"), bs("0"), bs("10"), // 260 > 255 -> nil, unchanged
		bs("OVERFLOW"), bs("SAT"), bs("INCRBY"), bs("u8"), bs("0"), bs("10"), // 250+10 -> clamp 255
		bs("GET"), bs("u8"), bs("0")) // => 255

	// FAIL on the FIRST op, valid op second: array must still have both elements.
	k3 := d.k("bfmulti3")
	d.eq("FAIL first, SET second",
		bs("BITFIELD"), k3,
		bs("OVERFLOW"), bs("FAIL"), bs("SET"), bs("i8"), bs("0"), bs("999"), // out of i8 range -> nil
		bs("SET"), bs("i8"), bs("0"), bs("-5")) // valid (mode reverts? no: FAIL stays) — value in range
	d.eq("GET i8 0 after", bs("BITFIELD"), k3, bs("GET"), bs("i8"), bs("0"))

	// Two failing FAIL ops in a row, then a valid GET: shape is [nil, nil, value].
	k4 := d.k("bfmulti4")
	d.eq("SET seed i8=1", bs("BITFIELD"), k4, bs("SET"), bs("i8"), bs("0"), bs("1"))
	d.eq("two fails then get",
		bs("BITFIELD"), k4,
		bs("OVERFLOW"), bs("FAIL"),
		bs("INCRBY"), bs("i8"), bs("0"), bs("200"), // 201 > 127 -> nil
		bs("INCRBY"), bs("i8"), bs("0"), bs("-200"), // 1-200 < -128 -> nil
		bs("GET"), bs("i8"), bs("0")) // => 1, unchanged
}

// TestDiffBitfieldWrongType checks WRONGTYPE parity for BITFIELD (both read-only and write
// forms) against a key holding a non-string type, plus the base bit commands, so the depth
// suite pins the type-guard behavior on every bitop entry point.
func TestDiffBitfieldWrongType(t *testing.T) {
	d := newDiffer(t)

	k := d.k("bfwrong")
	d.eq("LPUSH makes it a list", bs("LPUSH"), k, bs("x"))

	d.eq("BITFIELD GET on list (WRONGTYPE)", bs("BITFIELD"), k, bs("GET"), bs("u8"), bs("0"))
	d.eq("BITFIELD SET on list (WRONGTYPE)", bs("BITFIELD"), k, bs("SET"), bs("u8"), bs("0"), bs("1"))
	d.eq("BITFIELD INCRBY on list (WRONGTYPE)", bs("BITFIELD"), k, bs("INCRBY"), bs("u8"), bs("0"), bs("1"))
	d.eq("SETBIT on list (WRONGTYPE)", bs("SETBIT"), k, bs("0"), bs("1"))
	d.eq("GETBIT on list (WRONGTYPE)", bs("GETBIT"), k, bs("0"))
	d.eq("BITCOUNT on list (WRONGTYPE)", bs("BITCOUNT"), k)
	d.eq("BITPOS on list (WRONGTYPE)", bs("BITPOS"), k, bs("1"))
	d.eq("BITOP with list src (WRONGTYPE)", bs("BITOP"), bs("AND"), d.k("bfwrongdst"), k)

	// BITFIELD with an empty op list (only key) — arity/behavior parity.
	s := d.k("bfempty")
	d.eq("SET string", bs("SET"), s, bs("hello"))
	d.eq("BITFIELD with no ops", bs("BITFIELD"), s)
	d.eq("BITFIELD OVERFLOW only (syntax)", bs("BITFIELD"), s, bs("OVERFLOW"), bs("WRAP"))
	d.eq("BITFIELD bad type u64", bs("BITFIELD"), s, bs("GET"), bs("u64"), bs("0"))
	d.eq("BITFIELD bad type i0", bs("BITFIELD"), s, bs("GET"), bs("i0"), bs("0"))
	d.eq("BITFIELD bad overflow mode", bs("BITFIELD"), s, bs("OVERFLOW"), bs("NOPE"), bs("GET"), bs("u8"), bs("0"))
}
