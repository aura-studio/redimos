package command

import (
	"bufio"
	"net"
	"testing"
)

// Wire tests for BITFIELD, the one BIT-family command whose reply is an array (one element
// per sub-operation), covering SET/GET/INCRBY across signed/unsigned widths and the three
// overflow modes (WRAP / SAT / FAIL), plus the error paths. Backed by the shared fake
// string store (BITFIELD is string-value-backed).

func bfArray(t *testing.T, conn net.Conn, r *bufio.Reader, cmd string) []string {
	t.Helper()
	send(t, conn, cmd)
	return readArray(t, r)
}

func assertArr(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("array length = %d %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("element %d = %q, want %q (full %v)", i, got[i], want[i], got)
		}
	}
}

func TestBitfieldSetGet(t *testing.T) {
	t.Skip("v1 line: bit ops are gated on redimo v1.6.1 (no bit/HLL primitive, needs CAS)")
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	// GET on a fresh key reads 0.
	assertArr(t, bfArray(t, conn, r, "BITFIELD k GET u8 0"), []string{":0"})

	// SET returns the PREVIOUS value (0 on a fresh field).
	assertArr(t, bfArray(t, conn, r, "BITFIELD k SET u8 0 255"), []string{":0"})
	assertArr(t, bfArray(t, conn, r, "BITFIELD k GET u8 0"), []string{":255"})

	// A multi-op call: SET returns the old value (255), then GET returns the new value (100).
	assertArr(t, bfArray(t, conn, r, "BITFIELD k SET u8 0 100 GET u8 0"), []string{":255", ":100"})

	// A #-offset addresses the Nth field of the type width.
	assertArr(t, bfArray(t, conn, r, "BITFIELD k SET u8 #1 7 GET u8 #1"), []string{":0", ":7"})
}

func TestBitfieldSigned(t *testing.T) {
	t.Skip("v1 line: bit ops are gated on redimo v1.6.1 (no bit/HLL primitive, needs CAS)")
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	// Signed 8-bit: store -128, read it back.
	assertArr(t, bfArray(t, conn, r, "BITFIELD k SET i8 0 -128 GET i8 0"), []string{":0", ":-128"})
	assertArr(t, bfArray(t, conn, r, "BITFIELD k SET i8 0 -1 GET i8 0"), []string{":-128", ":-1"})
}

func TestBitfieldOverflow(t *testing.T) {
	t.Skip("v1 line: bit ops are gated on redimo v1.6.1 (no bit/HLL primitive, needs CAS)")
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	// WRAP (default): u8 255 + 10 wraps to 9.
	assertArr(t, bfArray(t, conn, r, "BITFIELD k SET u8 0 255"), []string{":0"})
	assertArr(t, bfArray(t, conn, r, "BITFIELD k INCRBY u8 0 10"), []string{":9"})

	// SAT: u8 255 + 10 saturates to 255.
	assertArr(t, bfArray(t, conn, r, "BITFIELD k SET u8 0 255"), []string{":9"})
	assertArr(t, bfArray(t, conn, r, "BITFIELD k OVERFLOW SAT INCRBY u8 0 10"), []string{":255"})

	// FAIL: u8 255 + 10 returns a nil element.
	assertArr(t, bfArray(t, conn, r, "BITFIELD k SET u8 0 255"), []string{":255"})
	assertArr(t, bfArray(t, conn, r, "BITFIELD k OVERFLOW FAIL INCRBY u8 0 10"), []string{"$-1"})
}

func TestBitfieldErrors(t *testing.T) {
	t.Skip("v1 line: bit ops are gated on redimo v1.6.1 (no bit/HLL primitive, needs CAS)")
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	// Missing offset, bad type and unknown sub-command reply a single error (not an array).
	for _, cmd := range []string{
		"BITFIELD k GET u8",
		"BITFIELD k GET x8 0",
		"BITFIELD k FOO u8 0",
		"BITFIELD k SET u8 0",
		"BITFIELD",
	} {
		send(t, conn, cmd)
		if got := readReply(t, r); len(got) == 0 || got[0] != '-' {
			t.Fatalf("%q reply = %q, want an error", cmd, got)
		}
	}
}
