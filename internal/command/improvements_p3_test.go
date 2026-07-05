package command

import (
	"strings"
	"testing"
)

// Regression wire tests for the P3 Redis-3.2 parity fixes from the audit (2026-07):
// EXPIRE overflow, LINSERT type-before-size, INCRBYFLOAT infinite increment, HMSET
// odd-args literal, and SINTER's key-order empty short-circuit.

// --- EXPIRE with an overflowing TTL must not delete the key (P3-14) ----------

func TestExpireHugeTTLDoesNotDeleteKey(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	if got := sendRead(t, conn, r, "SET k v"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	// now(1000) + MaxInt64 overflows int64; the key must get a far-future TTL, not be
	// deleted (Redis stores the far-future expiry and never deletes on a large TTL).
	if got := sendRead(t, conn, r, "EXPIRE k 9223372036854775807"); got != ":1" {
		t.Fatalf("EXPIRE huge = %q, want :1", got)
	}
	if got := sendRead(t, conn, r, "EXISTS k"); got != ":1" {
		t.Fatalf("EXISTS after huge EXPIRE = %q, want :1 (key must survive)", got)
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

// --- INCRBYFLOAT / HINCRBYFLOAT reject an infinite increment (P3-2) -----------

func TestIncrByFloatRejectsInfiniteIncrement(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	sendRead(t, conn, r, "SET k 1")
	for _, inc := range []string{"inf", "+inf", "-inf", "1e400"} {
		if got := sendRead(t, conn, r, "INCRBYFLOAT k "+inc); got != "-ERR value is not a valid float" {
			t.Fatalf("INCRBYFLOAT k %s = %q, want -ERR value is not a valid float", inc, got)
		}
	}
	// A finite increment still works.
	if got := sendRead(t, conn, r, "INCRBYFLOAT k 2.5"); got != "$3.5" {
		t.Fatalf("INCRBYFLOAT k 2.5 = %q, want $3.5", got)
	}
	// HINCRBYFLOAT shares the same parse path.
	sendRead(t, conn, r, "HSET h f 1")
	if got := sendRead(t, conn, r, "HINCRBYFLOAT h f inf"); got != "-ERR value is not a valid float" {
		t.Fatalf("HINCRBYFLOAT h f inf = %q, want -ERR value is not a valid float", got)
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
