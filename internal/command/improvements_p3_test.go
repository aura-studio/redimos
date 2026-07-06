package command

import (
	"strings"
	"testing"
)

// Regression wire tests for the P3 Redis-3.2 parity fixes from the audit (2026-07):
// EXPIRE overflow, LINSERT type-before-size, INCRBYFLOAT infinite increment, HMSET
// odd-args literal, and SINTER's key-order empty short-circuit.

// --- EXPIRE with an overflowing TTL must not delete the key (P3-14) ----------

func TestExpireHugeTTLOverflowDeletes(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	if got := sendRead(t, conn, r, "SET k v"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	// An EXPIRE so large that seconds*1000 overflows the millisecond domain matches Redis'
	// observable behaviour: Redis wraps to a past deadline and immediately deletes the key
	// (EXPIRE k 9223372036854775807 leaves TTL -2). The command still replies :1 (applied).
	if got := sendRead(t, conn, r, "EXPIRE k 9223372036854775807"); got != ":1" {
		t.Fatalf("EXPIRE overflow = %q, want :1", got)
	}
	if got := sendRead(t, conn, r, "EXISTS k"); got != ":0" {
		t.Fatalf("EXISTS after overflow EXPIRE = %q, want :0 (deleted, matching Redis 3.2)", got)
	}

	// A large TTL that does NOT overflow the ms domain survives with a far-future expiry.
	if got := sendRead(t, conn, r, "SET k2 v"); got != "+OK" {
		t.Fatalf("SET k2 = %q", got)
	}
	if got := sendRead(t, conn, r, "EXPIRE k2 1000000000000"); got != ":1" { // 1e12 s: no ms overflow
		t.Fatalf("EXPIRE large-non-overflow = %q, want :1", got)
	}
	if got := sendRead(t, conn, r, "EXISTS k2"); got != ":1" {
		t.Fatalf("EXISTS after large-non-overflow EXPIRE = %q, want :1 (survives)", got)
	}
}

// --- LINSERT type-checks before the value-size guard (P3-4) -------------------

func TestLInsertWrongTypeBeatsSizeGuard(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	// A String key (non-List).
	if got := sendRead(t, conn, r, "SET h x"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	// An oversized value (> 390KB) against a wrong-type key must reply WRONGTYPE, not
	// the value-size error — the type check runs first.
	big := strings.Repeat("z", 400*1024)
	send(t, conn, "LINSERT h BEFORE p "+big)
	got := readReply(t, r)
	if got != "-WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Fatalf("LINSERT oversized on a string = %q, want WRONGTYPE", got)
	}
}

// --- INCRBYFLOAT / HINCRBYFLOAT and an infinite increment (P3-2) --------------
//
// Redis 3.2 splits ±Inf by spelling, and so must we (verified against the live
// oracle):
//   - the LITERAL "inf"/"+inf"/"-inf" is a VALID increment; the command fails on the
//     non-finite RESULT with "increment would produce NaN or Infinity".
//   - an OVERFLOWING magnitude "1e400" is rejected at PARSE with "value is not a valid
//     float".
func TestIncrByFloatInfiniteIncrement(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	sendRead(t, conn, r, "SET k 1")
	// Literal infinities parse OK, then fail on the infinite result.
	for _, inc := range []string{"inf", "+inf", "-inf"} {
		if got := sendRead(t, conn, r, "INCRBYFLOAT k "+inc); got != "-ERR increment would produce NaN or Infinity" {
			t.Fatalf("INCRBYFLOAT k %s = %q, want -ERR increment would produce NaN or Infinity", inc, got)
		}
	}
	// An overflowing magnitude is a parse error, not a result error.
	if got := sendRead(t, conn, r, "INCRBYFLOAT k 1e400"); got != "-ERR value is not a valid float" {
		t.Fatalf("INCRBYFLOAT k 1e400 = %q, want -ERR value is not a valid float", got)
	}
	// A finite increment still works.
	if got := sendRead(t, conn, r, "INCRBYFLOAT k 2.5"); got != "$3.5" {
		t.Fatalf("INCRBYFLOAT k 2.5 = %q, want $3.5", got)
	}
	// HINCRBYFLOAT shares the same parse path and result guard.
	sendRead(t, conn, r, "HSET h f 1")
	if got := sendRead(t, conn, r, "HINCRBYFLOAT h f inf"); got != "-ERR increment would produce NaN or Infinity" {
		t.Fatalf("HINCRBYFLOAT h f inf = %q, want -ERR increment would produce NaN or Infinity", got)
	}
	if got := sendRead(t, conn, r, "HINCRBYFLOAT h f 1e400"); got != "-ERR value is not a valid float" {
		t.Fatalf("HINCRBYFLOAT h f 1e400 = %q, want -ERR value is not a valid float", got)
	}
}

// --- HMSET odd args uses Redis' HMSET-specific literal (P3-6) -----------------

func TestHMSetOddArgsLiteral(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	if got := sendRead(t, conn, r, "HMSET k f1 v1 f2"); got != "-ERR wrong number of arguments for HMSET" {
		t.Fatalf("HMSET odd args = %q, want -ERR wrong number of arguments for HMSET", got)
	}
}

// --- SINTER short-circuits on the first empty operand in key order (P3-3) -----

func TestSInterShortCircuitsBeforeWrongType(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	// A wrong-type (String) key.
	sendRead(t, conn, r, "SET wrongkey x")

	// absent BEFORE wrongkey: Redis returns the empty array without type-checking
	// wrongkey, so no WRONGTYPE.
	send(t, conn, "SINTER absentkey wrongkey")
	if got := readArray(t, r); len(got) != 0 {
		t.Fatalf("SINTER absent wrongkey = %v, want empty array", got)
	}

	// wrongkey FIRST: the type check fires before any empty short-circuit -> WRONGTYPE.
	if got := sendRead(t, conn, r, "SINTER wrongkey absentkey"); got != "-WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Fatalf("SINTER wrongkey absent = %q, want WRONGTYPE", got)
	}
}
