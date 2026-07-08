package command

import (
	"bufio"
	"sort"
	"testing"

	"github.com/aura-studio/redimos/internal/meta"
	"github.com/aura-studio/redimos/internal/storage"
)

// Unit tests for the Set command family (task 14.1). They run the real
// in-process server + router over the stateful fakeStringStore (which models the
// per-member item layout and the meta counter), so the handlers, meta counting
// and RESP encoding are exercised end-to-end. Order-insensitive replies
// (SMEMBERS/SPOP-with-count/SRANDMEMBER) are compared as sets because the member
// order is unspecified.

// --- SADD new/dup + SCARD (requirements 8.1, 8.2, 8.5) ----------------------

func TestSAddNewAndDupWithSCard(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	// Three new members → reply 3, SCARD 3.
	if got, want := sendRead(t, conn, r, "SADD s a b c"), ":3"; got != want {
		t.Errorf("SADD (3 new) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SCARD s"), ":3"; got != want {
		t.Errorf("SCARD = %q, want %q", got, want)
	}

	// One dup (a) + one new (d) → reply 1, SCARD 4.
	if got, want := sendRead(t, conn, r, "SADD s a d"), ":1"; got != want {
		t.Errorf("SADD (1 dup, 1 new) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SCARD s"), ":4"; got != want {
		t.Errorf("SCARD after dup = %q, want %q", got, want)
	}

	// All dups → reply 0, SCARD unchanged.
	if got, want := sendRead(t, conn, r, "SADD s a b"), ":0"; got != want {
		t.Errorf("SADD (all dup) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SCARD s"), ":4"; got != want {
		t.Errorf("SCARD unchanged = %q, want %q", got, want)
	}
}

func TestSCardAbsentIsZero(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "SCARD absent"), ":0"; got != want {
		t.Errorf("SCARD absent = %q, want %q", got, want)
	}
}

// --- SREM + count maintenance + last-member key deletion (8.1, 8.5) ---------

func TestSRemMaintainsCount(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s a b c")

	// Remove two existing + one absent → reply 2, SCARD 1.
	if got, want := sendRead(t, conn, r, "SREM s a b zzz"), ":2"; got != want {
		t.Errorf("SREM = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SCARD s"), ":1"; got != want {
		t.Errorf("SCARD after SREM = %q, want %q", got, want)
	}
}

func TestSRemAbsentKey(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "SREM absent m"), ":0"; got != want {
		t.Errorf("SREM absent = %q, want %q", got, want)
	}
}

// TestSRemLastMemberRemovesKey verifies removing the final member deletes the key
// (an empty set does not exist in Redis): EXISTS/TYPE/SCARD all report it gone.
func TestSRemLastMemberRemovesKey(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s only")

	if got, want := sendRead(t, conn, r, "SREM s only"), ":1"; got != want {
		t.Errorf("SREM last = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "EXISTS s"), ":0"; got != want {
		t.Errorf("EXISTS after emptying = %q, want %q (empty set must not exist)", got, want)
	}
	// v1 line: TYPE is gated; the empty-set-deletes-key behavior is covered by EXISTS
	// (above) and SCARD (below).
	if got, want := sendRead(t, conn, r, "SCARD s"), ":0"; got != want {
		t.Errorf("SCARD after emptying = %q, want %q", got, want)
	}
}

// --- SISMEMBER (requirement 8.1) --------------------------------------------

func TestSIsMember(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s a b")
	if got, want := sendRead(t, conn, r, "SISMEMBER s a"), ":1"; got != want {
		t.Errorf("SISMEMBER present = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SISMEMBER s nope"), ":0"; got != want {
		t.Errorf("SISMEMBER missing = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SISMEMBER absent a"), ":0"; got != want {
		t.Errorf("SISMEMBER absent key = %q, want %q", got, want)
	}
}

// --- SMEMBERS (requirement 8.1) ---------------------------------------------

func TestSMembers(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s a b c")

	send(t, conn, "SMEMBERS s")
	if got, want := readSortedBulk(t, r), []string{"a", "b", "c"}; !equalStrings(got, want) {
		t.Errorf("SMEMBERS = %v, want %v", got, want)
	}
}

func TestSMembersAbsentIsEmptyArray(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	send(t, conn, "SMEMBERS absent")
	if arr := readArray(t, r); len(arr) != 0 {
		t.Errorf("SMEMBERS absent = %v, want empty array", arr)
	}
}

// --- SPOP (requirements 8.1, 8.5) -------------------------------------------

// TestSPopSingleRemovesAndDecrements verifies SPOP without a count returns one
// member as a bulk string, removes it, and decrements the cardinality.
func TestSPopSingleRemovesAndDecrements(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "SADD s a b c")

	got := sendRead(t, conn, r, "SPOP s")
	if len(got) < 2 || got[0] != '$' {
		t.Fatalf("SPOP s = %q, want a bulk string member", got)
	}
	popped := stripBulk(got)
	if popped != "a" && popped != "b" && popped != "c" {
		t.Errorf("SPOP returned %q, want one of a/b/c", popped)
	}
	// Cardinality decremented and the popped member is gone.
	if got, want := sendRead(t, conn, r, "SCARD s"), ":2"; got != want {
		t.Errorf("SCARD after SPOP = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SISMEMBER s "+popped), ":0"; got != want {
		t.Errorf("SISMEMBER on popped %q = %q, want %q (removed)", popped, got, want)
	}
}

func TestSPopEmptyIsNullBulk(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "SPOP absent"), "$-1"; got != want {
		t.Errorf("SPOP absent = %q, want %q (null bulk)", got, want)
	}
}

// TestSPopWithCountRemovesAndEmpties verifies SPOP key count returns an array of
// distinct members, removes them all, and deletes the key when the last member is
// popped.
func TestSPopWithCountRemovesAndEmpties(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s a b c")

	// Pop more than the cardinality → returns all 3, empties the key.
	send(t, conn, "SPOP s 10")
	got := readSortedBulkPayloads(t, r)
	if want := []string{"a", "b", "c"}; !equalStrings(got, want) {
		t.Errorf("SPOP s 10 = %v, want %v (all members)", got, want)
	}
	// Emptied → the key no longer exists.
	if got, want := sendRead(t, conn, r, "EXISTS s"), ":0"; got != want {
		t.Errorf("EXISTS after SPOP-all = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SCARD s"), ":0"; got != want {
		t.Errorf("SCARD after SPOP-all = %q, want %q", got, want)
	}
}

func TestSPopWithCountEmptyKeyIsEmptyArray(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	send(t, conn, "SPOP absent 3")
	if arr := readArray(t, r); len(arr) != 0 {
		t.Errorf("SPOP absent 3 = %v, want empty array", arr)
	}
}

func TestSPopNegativeCount(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s a")
	want := "-ERR index out of range" // Redis 3.2 wording (5.0+ says "value is out of range, must be positive")
	if got := sendRead(t, conn, r, "SPOP s -1"); got != want {
		t.Errorf("SPOP s -1 = %q, want %q", got, want)
	}
}

func TestSPopNonIntegerCount(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR value is not an integer or out of range"
	if got := sendRead(t, conn, r, "SPOP s abc"); got != want {
		t.Errorf("SPOP s abc = %q, want %q", got, want)
	}
}

// --- SRANDMEMBER (requirement 8.1) — does NOT remove -------------------------

func TestSRandMemberDoesNotRemove(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s a b c")

	got := sendRead(t, conn, r, "SRANDMEMBER s")
	if len(got) < 2 || got[0] != '$' {
		t.Fatalf("SRANDMEMBER s = %q, want a bulk string member", got)
	}
	// Cardinality unchanged: SRANDMEMBER must not remove.
	if got, want := sendRead(t, conn, r, "SCARD s"), ":3"; got != want {
		t.Errorf("SCARD after SRANDMEMBER = %q, want %q (no removal)", got, want)
	}
}

func TestSRandMemberEmptyIsNullBulk(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "SRANDMEMBER absent"), "$-1"; got != want {
		t.Errorf("SRANDMEMBER absent = %q, want %q (null bulk)", got, want)
	}
}

// TestSRandMemberWithCountDistinct verifies a non-negative count returns up to
// that many distinct members without removing any.
func TestSRandMemberWithCountDistinct(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s a b c")

	send(t, conn, "SRANDMEMBER s 2")
	got := readArrayPayloads(t, r)
	if len(got) != 2 {
		t.Fatalf("SRANDMEMBER s 2 returned %d members, want 2 (%v)", len(got), got)
	}
	// Distinct.
	if got[0] == got[1] {
		t.Errorf("SRANDMEMBER s 2 returned duplicate %q, want distinct", got[0])
	}
	// Still present (no removal).
	if got, want := sendRead(t, conn, r, "SCARD s"), ":3"; got != want {
		t.Errorf("SCARD after SRANDMEMBER count = %q, want %q", got, want)
	}
}

// TestSRandMemberNegativeCountAllowsRepeats verifies a negative count returns
// exactly -count members (repeats allowed) without removing any.
func TestSRandMemberNegativeCountAllowsRepeats(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s only")

	send(t, conn, "SRANDMEMBER s -3")
	got := readArrayPayloads(t, r)
	if len(got) != 3 {
		t.Fatalf("SRANDMEMBER s -3 returned %d members, want exactly 3 (%v)", len(got), got)
	}
	for i, m := range got {
		if m != "only" {
			t.Errorf("SRANDMEMBER s -3 [%d] = %q, want %q (only member repeated)", i, m, "only")
		}
	}
	if got, want := sendRead(t, conn, r, "SCARD s"), ":1"; got != want {
		t.Errorf("SCARD after negative SRANDMEMBER = %q, want %q (no removal)", got, want)
	}
}

// --- WRONGTYPE (requirement 8.1) --------------------------------------------

func TestSetWrongType(t *testing.T) {
	t.Skip("v1 line: no WRONGTYPE on redimo v1.6.1 (no type tag)")
	store := newFakeStringStore()
	// Seed a String key; every Set command against it must reply WRONGTYPE.
	store.metas["0:k"] = storage.Meta{Type: string(meta.TypeString)}
	store.live["0:k"] = true

	conn, r := startStringServer(t, store, fixedNow(1000))
	want := "-WRONGTYPE Operation against a key holding the wrong kind of value"

	cmds := []string{
		"SADD k m",
		"SREM k m",
		"SISMEMBER k m",
		"SMEMBERS k",
		"SCARD k",
		"SPOP k",
		"SPOP k 2",
		"SRANDMEMBER k",
		"SRANDMEMBER k 2",
	}
	for _, cmd := range cmds {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// --- arity (requirement 3.2) ------------------------------------------------

func TestSetArityErrors(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	cases := map[string]string{
		"SADD s":           "-ERR wrong number of arguments for 'sadd' command",
		"SREM s":           "-ERR wrong number of arguments for 'srem' command",
		"SISMEMBER s":      "-ERR wrong number of arguments for 'sismember' command",
		"SISMEMBER s a b":  "-ERR wrong number of arguments for 'sismember' command",
		"SMEMBERS":         "-ERR wrong number of arguments for 'smembers' command",
		"SMEMBERS s extra": "-ERR wrong number of arguments for 'smembers' command",
		"SCARD":            "-ERR wrong number of arguments for 'scard' command",
		"SPOP":             "-ERR wrong number of arguments for 'spop' command",
		"SRANDMEMBER":      "-ERR wrong number of arguments for 'srandmember' command",
	}
	for cmd, want := range cases {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// TestSPopSyntaxErrorOnExtraArgs verifies more than one optional argument to SPOP
// / SRANDMEMBER replies the syntax error (matching Redis, which caps the optional
// count at one argument).
func TestSPopSyntaxErrorOnExtraArgs(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR syntax error"
	if got := sendRead(t, conn, r, "SPOP s 1 2"); got != want {
		t.Errorf("SPOP s 1 2 = %q, want %q", got, want)
	}
	if got := sendRead(t, conn, r, "SRANDMEMBER s 1 2"); got != want {
		t.Errorf("SRANDMEMBER s 1 2 = %q, want %q", got, want)
	}
}

// readArrayPayloads reads an array reply and returns the bulk-string payloads in
// wire order (no sorting), for tests that care about count or repeats.
func readArrayPayloads(t *testing.T, r *bufio.Reader) []string {
	t.Helper()
	arr := readArray(t, r)
	out := make([]string, len(arr))
	for i, e := range arr {
		out[i] = stripBulk(e)
	}
	return out
}

// readSortedBulkPayloads reads an array reply and returns the bulk-string payloads
// sorted, for order-independent comparison.
func readSortedBulkPayloads(t *testing.T, r *bufio.Reader) []string {
	t.Helper()
	out := readArrayPayloads(t, r)
	sort.Strings(out)
	return out
}
