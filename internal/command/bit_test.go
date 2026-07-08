package command

import "testing"

// Wire tests for the BIT family (SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD), which had
// no dedicated coverage. They run against the shared fake string store + server harness
// (startStringServer/send/readReply), asserting exact RESP2 reply shapes and the arity/
// argument-validation error paths. BIT commands are string-backed, so the fake store's
// functional GetString/SetString back them directly.

func TestSetBitGetBit(t *testing.T) {
	t.Skip("v1 line: bit ops are gated on redimo v1.6.1 (no bit/HLL primitive, needs CAS)")
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	// SETBIT returns the ORIGINAL bit at the offset.
	send(t, conn, "SETBIT k 7 1")
	if got := readReply(t, r); got != ":0" {
		t.Fatalf("SETBIT fresh = %q, want :0", got)
	}
	send(t, conn, "GETBIT k 7")
	if got := readReply(t, r); got != ":1" {
		t.Fatalf("GETBIT set = %q, want :1", got)
	}
	// An offset never written reads 0.
	send(t, conn, "GETBIT k 100")
	if got := readReply(t, r); got != ":0" {
		t.Fatalf("GETBIT unset = %q, want :0", got)
	}
	// Overwriting returns the previous bit (1).
	send(t, conn, "SETBIT k 7 0")
	if got := readReply(t, r); got != ":1" {
		t.Fatalf("SETBIT overwrite = %q, want :1", got)
	}
	send(t, conn, "GETBIT k 7")
	if got := readReply(t, r); got != ":0" {
		t.Fatalf("GETBIT cleared = %q, want :0", got)
	}
}

func TestBitCount(t *testing.T) {
	t.Skip("v1 line: bit ops are gated on redimo v1.6.1 (no bit/HLL primitive, needs CAS)")
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	send(t, conn, "BITCOUNT absent")
	if got := readReply(t, r); got != ":0" {
		t.Fatalf("BITCOUNT absent = %q, want :0", got)
	}
	for _, off := range []string{"1", "3", "7"} {
		send(t, conn, "SETBIT k "+off+" 1")
		_ = readReply(t, r)
	}
	send(t, conn, "BITCOUNT k")
	if got := readReply(t, r); got != ":3" {
		t.Fatalf("BITCOUNT = %q, want :3", got)
	}
}

func TestBitCommandArity(t *testing.T) {
	t.Skip("v1 line: bit ops are gated on redimo v1.6.1 (no bit/HLL primitive, needs CAS)")
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	// SETBIT requires exactly key, offset, value.
	send(t, conn, "SETBIT k 7")
	if got := readReply(t, r); got == "" || got[0] != '-' {
		t.Fatalf("SETBIT missing value = %q, want an error", got)
	}
	// A non-0/1 bit value is rejected.
	send(t, conn, "SETBIT k 7 2")
	if got := readReply(t, r); got == "" || got[0] != '-' {
		t.Fatalf("SETBIT bad bit = %q, want an error", got)
	}
	// A non-integer offset is rejected.
	send(t, conn, "GETBIT k notanoffset")
	if got := readReply(t, r); got == "" || got[0] != '-' {
		t.Fatalf("GETBIT bad offset = %q, want an error", got)
	}
}

func TestBitOp(t *testing.T) {
	t.Skip("v1 line: bit ops are gated on redimo v1.6.1 (no bit/HLL primitive, needs CAS)")
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	// a = bit 1 set, b = bit 2 set; AND -> empty, OR -> {1,2}.
	send(t, conn, "SETBIT a 1 1")
	_ = readReply(t, r)
	send(t, conn, "SETBIT b 2 1")
	_ = readReply(t, r)

	send(t, conn, "BITOP OR dest a b")
	_ = readReply(t, r) // reply is the dest byte length
	send(t, conn, "BITCOUNT dest")
	if got := readReply(t, r); got != ":2" {
		t.Fatalf("BITCOUNT after BITOP OR = %q, want :2", got)
	}

	send(t, conn, "BITOP AND dand a b")
	_ = readReply(t, r)
	send(t, conn, "BITCOUNT dand")
	if got := readReply(t, r); got != ":0" {
		t.Fatalf("BITCOUNT after BITOP AND = %q, want :0", got)
	}
}
