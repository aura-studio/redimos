package integration

import "testing"

// Dimension N: bit-operation parity. SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP are pure string
// bit-twiddling with several off-by-one traps: SETBIT past the end zero-extends the string,
// GETBIT past the end is 0, BITCOUNT/BITPOS take BYTE ranges (negative allowed), and BITPOS
// for a clear bit in an all-ones string has the famous "return the bit just past the end
// unless an explicit end is given, then -1" rule. redimos implements these in the command
// layer over its string value, so they are compared byte-for-byte with Redis 3.2. Binary
// values are set directly so both endpoints start from identical bytes.

func TestDiffBitCountPos(t *testing.T) {
	d := newDiffer(t)

	// 0xff 0xf0 0x00  ->  11111111 11110000 00000000  (12 set bits, first clear bit at 12)
	k := d.k("k")
	d.eq("SET binary", bs("SET"), k, bs("\xff\xf0\x00"))

	d.eq("BITCOUNT all", bs("BITCOUNT"), k)
	d.eq("BITCOUNT 0 0", bs("BITCOUNT"), k, bs("0"), bs("0"))
	d.eq("BITCOUNT 1 1", bs("BITCOUNT"), k, bs("1"), bs("1"))
	d.eq("BITCOUNT 2 2", bs("BITCOUNT"), k, bs("2"), bs("2"))
	d.eq("BITCOUNT 0 -1", bs("BITCOUNT"), k, bs("0"), bs("-1"))
	d.eq("BITCOUNT -2 -1", bs("BITCOUNT"), k, bs("-2"), bs("-1"))

	d.eq("BITPOS 1", bs("BITPOS"), k, bs("1"))
	d.eq("BITPOS 0", bs("BITPOS"), k, bs("0"))
	d.eq("BITPOS 1 in byte2 -> -1", bs("BITPOS"), k, bs("1"), bs("2"))
	d.eq("BITPOS 0 from byte1", bs("BITPOS"), k, bs("0"), bs("1"))

	for _, off := range []string{"0", "7", "8", "11", "12", "23", "24", "100"} {
		d.eq("GETBIT "+off, bs("GETBIT"), k, bs(off))
	}
}

// TestDiffBitPosAllOnes covers the all-ones edge: BITPOS <key> 0 with no explicit end returns
// the first bit PAST the string; with an explicit end range it returns -1 when not found.
func TestDiffBitPosAllOnes(t *testing.T) {
	d := newDiffer(t)

	k := d.k("ones")
	d.eq("SET 0xffff", bs("SET"), k, bs("\xff\xff"))
	d.eq("BITPOS 0 no-range (=> 16)", bs("BITPOS"), k, bs("0"))
	d.eq("BITPOS 0 with end (=> -1)", bs("BITPOS"), k, bs("0"), bs("0"), bs("-1"))
	d.eq("BITPOS 1 (=> 0)", bs("BITPOS"), k, bs("1"))

	z := d.k("zeros")
	d.eq("SET 0x0000", bs("SET"), z, bs("\x00\x00"))
	d.eq("BITPOS 1 all-zero (=> -1)", bs("BITPOS"), z, bs("1"))
	d.eq("BITPOS 0 all-zero (=> 0)", bs("BITPOS"), z, bs("0"))
}

func TestDiffSetBit(t *testing.T) {
	d := newDiffer(t)

	k := d.k("sb")
	d.eq("SETBIT 7 1 -> old 0", bs("SETBIT"), k, bs("7"), bs("1"))
	d.eq("GET after (byte0=0x01)", bs("GET"), k)
	d.eq("SETBIT 7 0 -> old 1", bs("SETBIT"), k, bs("7"), bs("0"))
	d.eq("SETBIT 0 1", bs("SETBIT"), k, bs("0"), bs("1"))
	// Set a far bit -> zero-extends the string.
	d.eq("SETBIT 100 1 -> old 0", bs("SETBIT"), k, bs("100"), bs("1"))
	d.eq("STRLEN after extend", bs("STRLEN"), k)
	d.eq("BITCOUNT after extend", bs("BITCOUNT"), k)
	d.eq("GETBIT 100", bs("GETBIT"), k, bs("100"))
	d.eq("GETBIT 99", bs("GETBIT"), k, bs("99"))
}

func TestDiffBitOp(t *testing.T) {
	d := newDiffer(t)

	a, b, c := d.k("a"), d.k("b"), d.k("c")
	d.eq("SET a", bs("SET"), a, bs("\xff\x0f"))
	d.eq("SET b", bs("SET"), b, bs("\x0f\xff"))
	d.eq("SET c (shorter)", bs("SET"), c, bs("\xff"))

	and, or, xor, not := d.k("and"), d.k("or"), d.k("xor"), d.k("not")
	d.eq("BITOP AND len", bs("BITOP"), bs("AND"), and, a, b)
	d.eq("GET AND", bs("GET"), and)
	d.eq("BITOP OR len", bs("BITOP"), bs("OR"), or, a, b)
	d.eq("GET OR", bs("GET"), or)
	d.eq("BITOP XOR len", bs("BITOP"), bs("XOR"), xor, a, b)
	d.eq("GET XOR", bs("GET"), xor)
	d.eq("BITOP NOT len", bs("BITOP"), bs("NOT"), not, a)
	d.eq("GET NOT", bs("GET"), not)

	// Differing source lengths: the shorter operand is zero-padded to the longest.
	mixed := d.k("mixed")
	d.eq("BITOP AND mixed-len", bs("BITOP"), bs("AND"), mixed, a, c)
	d.eq("GET mixed", bs("GET"), mixed)

	// A missing source key is treated as a zero string of the longest length.
	miss := d.k("miss")
	d.eq("BITOP AND with missing src", bs("BITOP"), bs("AND"), miss, a, d.k("nonexistent"))
	d.eq("GET miss", bs("GET"), miss)
	d.eq("STRLEN miss", bs("STRLEN"), miss)
}
