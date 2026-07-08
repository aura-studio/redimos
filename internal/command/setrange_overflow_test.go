package command

import (
	"testing"
)

// TestSetRangeIntOffsetOverflowNoPanic is the regression guard for a client-triggerable
// crash the deepened differential caught: SETRANGE with an int64-max offset made
// `offset + len(val)` overflow to a negative length, which slipped past the value-size
// guard and then panicked at `buf[offset:]` — taking down the whole proxy (redcon has no
// per-command recover). The handler now rejects the overflowing offset with the size
// error BEFORE allocating. This test drives the exact input and then confirms the proxy
// is still alive (a panic would have killed the connection goroutine).
func TestSetRangeIntOffsetOverflowNoPanic(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	if got := sendRead(t, conn, r, "SET k hello"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}

	// The offending input: int64-max offset. Must reply an error, not panic.
	got := sendRead(t, conn, r, "SETRANGE k 9223372036854775807 x")
	if len(got) == 0 || got[0] != '-' {
		t.Fatalf("SETRANGE huge offset = %q, want an error reply (not a crash)", got)
	}

	// A near-int64-max offset (whose sum with len(val) still overflows) must also be
	// rejected, not panic.
	if got := sendRead(t, conn, r, "SETRANGE k 9223372036854775800 abcdefghij"); len(got) == 0 || got[0] != '-' {
		t.Fatalf("SETRANGE near-max offset = %q, want an error reply", got)
	}

	// The proxy must still be responsive — proves no panic took down the goroutine.
	if got := sendRead(t, conn, r, "PING"); got != "+PONG" {
		t.Fatalf("PING after huge SETRANGE = %q, want +PONG (proxy must survive)", got)
	}
}
