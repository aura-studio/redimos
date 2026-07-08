package command

import (
	"testing"

	"github.com/aura-studio/redimos/internal/meta"
	"github.com/aura-studio/redimos/internal/storage"
)

// Unit tests for the high-cost combined List mutators (LSET/LTRIM/LREM/LINSERT)
// and the two-key rotation RPOPLPUSH (task 16.2, requirements 7.4, 7.5). They run
// the real in-process server + router over the stateful fakeStringStore (which
// models the ordered element slice and the meta counter), so the handlers, the
// read-modify-write combined implementation, meta counting and RESP encoding are
// exercised end-to-end. Order-sensitive replies are compared in wire order via
// readArrayPayloads.

// --- LSET (requirement 7.4) --------------------------------------------------

// TestLSetInRangeAndNegative verifies LSET replaces the element at a positive and
// a negative index, replies +OK and leaves the length unchanged.
func TestLSetInRangeAndNegative(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b c") // 0:a 1:b 2:c

	if got, want := sendRead(t, conn, r, "LSET l 1 B"), "+OK"; got != want {
		t.Errorf("LSET l 1 B = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "LSET l -1 C"), "+OK"; got != want {
		t.Errorf("LSET l -1 C = %q, want %q", got, want)
	}

	send(t, conn, "LRANGE l 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"a", "B", "C"}; !equalStrings(got, want) {
		t.Errorf("LRANGE after LSET = %v, want %v", got, want)
	}
	// Length is unchanged.
	if got, want := sendRead(t, conn, r, "LLEN l"), ":3"; got != want {
		t.Errorf("LLEN after LSET = %q, want %q", got, want)
	}
}

// TestLSetOutOfRange verifies an index outside the current bounds replies the
// index-out-of-range error for both a positive and a negative overshoot.
func TestLSetOutOfRange(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b c")

	want := "-ERR index out of range"
	if got := sendRead(t, conn, r, "LSET l 3 x"); got != want {
		t.Errorf("LSET l 3 x = %q, want %q", got, want)
	}
	if got := sendRead(t, conn, r, "LSET l -4 x"); got != want {
		t.Errorf("LSET l -4 x = %q, want %q", got, want)
	}
}

// TestLSetNoSuchKey verifies LSET on an absent key replies the no-such-key error.
func TestLSetNoSuchKey(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "LSET absent 0 x"), "-ERR no such key"; got != want {
		t.Errorf("LSET absent = %q, want %q", got, want)
	}
}

func TestLSetNonInteger(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a")
	if got, want := sendRead(t, conn, r, "LSET l x v"), "-ERR value is not an integer or out of range"; got != want {
		t.Errorf("LSET l x v = %q, want %q", got, want)
	}
}

// --- LTRIM (requirement 7.4) -------------------------------------------------

// TestLTrimMiddle keeps an inner subrange and reconciles the length.
func TestLTrimMiddle(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b c d e") // 0..4

	if got, want := sendRead(t, conn, r, "LTRIM l 1 3"), "+OK"; got != want {
		t.Errorf("LTRIM l 1 3 = %q, want %q", got, want)
	}
	send(t, conn, "LRANGE l 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"b", "c", "d"}; !equalStrings(got, want) {
		t.Errorf("LRANGE after LTRIM = %v, want %v", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN l"), ":3"; got != want {
		t.Errorf("LLEN after LTRIM = %q, want %q", got, want)
	}
}

// TestLTrimNegative uses negative bounds (keep the last two).
func TestLTrimNegative(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b c d e")

	if got, want := sendRead(t, conn, r, "LTRIM l -2 -1"), "+OK"; got != want {
		t.Errorf("LTRIM l -2 -1 = %q, want %q", got, want)
	}
	send(t, conn, "LRANGE l 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"d", "e"}; !equalStrings(got, want) {
		t.Errorf("LRANGE after negative LTRIM = %v, want %v", got, want)
	}
}

// TestLTrimEmptyDeletesKey verifies a range that selects nothing empties and
// deletes the key (an empty list does not exist in Redis).
func TestLTrimEmptyDeletesKey(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b c")

	if got, want := sendRead(t, conn, r, "LTRIM l 5 10"), "+OK"; got != want {
		t.Errorf("LTRIM l 5 10 = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "EXISTS l"), ":0"; got != want {
		t.Errorf("EXISTS after emptying LTRIM = %q, want %q (empty list must not exist)", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN l"), ":0"; got != want {
		t.Errorf("LLEN after emptying LTRIM = %q, want %q", got, want)
	}
}

// TestLTrimAbsentIsOK verifies LTRIM on an absent key is a no-op replying +OK.
func TestLTrimAbsentIsOK(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "LTRIM absent 0 -1"), "+OK"; got != want {
		t.Errorf("LTRIM absent = %q, want %q", got, want)
	}
}

// --- LREM (requirement 7.4) --------------------------------------------------

// TestLRemHeadToTail removes up to count occurrences scanning head->tail (count>0).
func TestLRemHeadToTail(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b a c a") // three a's

	// Remove the first two a's (head->tail).
	if got, want := sendRead(t, conn, r, "LREM l 2 a"), ":2"; got != want {
		t.Errorf("LREM l 2 a = %q, want %q (removed count)", got, want)
	}
	send(t, conn, "LRANGE l 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"b", "c", "a"}; !equalStrings(got, want) {
		t.Errorf("LRANGE after LREM 2 = %v, want %v", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN l"), ":3"; got != want {
		t.Errorf("LLEN after LREM = %q, want %q", got, want)
	}
}

// TestLRemTailToHead removes scanning tail->head (count<0).
func TestLRemTailToHead(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b a c a")

	// Remove the last two a's (tail->head): leaves the first a in place.
	if got, want := sendRead(t, conn, r, "LREM l -2 a"), ":2"; got != want {
		t.Errorf("LREM l -2 a = %q, want %q", got, want)
	}
	send(t, conn, "LRANGE l 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"a", "b", "c"}; !equalStrings(got, want) {
		t.Errorf("LRANGE after LREM -2 = %v, want %v", got, want)
	}
}

// TestLRemAllOccurrences removes every occurrence (count==0).
func TestLRemAllOccurrences(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b a c a")

	if got, want := sendRead(t, conn, r, "LREM l 0 a"), ":3"; got != want {
		t.Errorf("LREM l 0 a = %q, want %q", got, want)
	}
	send(t, conn, "LRANGE l 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"b", "c"}; !equalStrings(got, want) {
		t.Errorf("LRANGE after LREM 0 = %v, want %v", got, want)
	}
}

// TestLRemAllElementsDeletesKey verifies removing every element deletes the key.
func TestLRemAllElementsDeletesKey(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a a a")

	if got, want := sendRead(t, conn, r, "LREM l 0 a"), ":3"; got != want {
		t.Errorf("LREM l 0 a = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "EXISTS l"), ":0"; got != want {
		t.Errorf("EXISTS after removing all = %q, want %q", got, want)
	}
}

// TestLRemNoMatchReturnsZero verifies a value not present reports :0 and no change.
func TestLRemNoMatchReturnsZero(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b c")

	if got, want := sendRead(t, conn, r, "LREM l 0 zzz"), ":0"; got != want {
		t.Errorf("LREM l 0 zzz = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN l"), ":3"; got != want {
		t.Errorf("LLEN after no-match LREM = %q, want %q", got, want)
	}
}

// TestLRemAbsentReturnsZero verifies LREM on an absent key replies :0.
func TestLRemAbsentReturnsZero(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "LREM absent 0 a"), ":0"; got != want {
		t.Errorf("LREM absent = %q, want %q", got, want)
	}
}

// --- LINSERT (requirement 7.4) -----------------------------------------------

// TestLInsertBefore inserts before the first pivot and reports the new length.
func TestLInsertBefore(t *testing.T) {
	t.Skip("v1 line: LINSERT is gated on redimo v1.6.1 (no pivot-insert primitive)")
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b c")

	if got, want := sendRead(t, conn, r, "LINSERT l BEFORE b X"), ":4"; got != want {
		t.Errorf("LINSERT BEFORE = %q, want %q", got, want)
	}
	send(t, conn, "LRANGE l 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"a", "X", "b", "c"}; !equalStrings(got, want) {
		t.Errorf("LRANGE after LINSERT BEFORE = %v, want %v", got, want)
	}
}

// TestLInsertAfter inserts after the first pivot.
func TestLInsertAfter(t *testing.T) {
	t.Skip("v1 line: LINSERT is gated on redimo v1.6.1 (no pivot-insert primitive)")
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b c")

	if got, want := sendRead(t, conn, r, "LINSERT l AFTER b Y"), ":4"; got != want {
		t.Errorf("LINSERT AFTER = %q, want %q", got, want)
	}
	send(t, conn, "LRANGE l 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"a", "b", "Y", "c"}; !equalStrings(got, want) {
		t.Errorf("LRANGE after LINSERT AFTER = %v, want %v", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN l"), ":4"; got != want {
		t.Errorf("LLEN after LINSERT = %q, want %q", got, want)
	}
}

// TestLInsertPivotNotFound replies :-1 and leaves the list unchanged.
func TestLInsertPivotNotFound(t *testing.T) {
	t.Skip("v1 line: LINSERT is gated on redimo v1.6.1 (no pivot-insert primitive)")
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b c")

	if got, want := sendRead(t, conn, r, "LINSERT l BEFORE zzz X"), ":-1"; got != want {
		t.Errorf("LINSERT missing pivot = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN l"), ":3"; got != want {
		t.Errorf("LLEN after failed LINSERT = %q, want %q", got, want)
	}
}

// TestLInsertAbsentKey replies :0 on an absent key.
func TestLInsertAbsentKey(t *testing.T) {
	t.Skip("v1 line: LINSERT is gated on redimo v1.6.1 (no pivot-insert primitive)")
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "LINSERT absent BEFORE p v"), ":0"; got != want {
		t.Errorf("LINSERT absent = %q, want %q", got, want)
	}
}

// TestLInsertSyntaxError rejects an illegal where token.
func TestLInsertSyntaxError(t *testing.T) {
	t.Skip("v1 line: LINSERT is gated on redimo v1.6.1 (no pivot-insert primitive)")
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a")
	if got, want := sendRead(t, conn, r, "LINSERT l SIDEWAYS a v"), "-ERR syntax error"; got != want {
		t.Errorf("LINSERT bad where = %q, want %q", got, want)
	}
}

// --- RPOPLPUSH (requirement 7.5) ---------------------------------------------

// TestRPopLPushTwoKeyMove moves the source tail to the destination head and
// maintains both keys' lengths.
func TestRPopLPushTwoKeyMove(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH src a b c") // head-to-tail a, b, c
	sendRead(t, conn, r, "RPUSH dst x y")   // head-to-tail x, y

	if got, want := sendRead(t, conn, r, "RPOPLPUSH src dst"), "$c"; got != want {
		t.Errorf("RPOPLPUSH src dst = %q, want %q (moved tail)", got, want)
	}

	send(t, conn, "LRANGE src 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"a", "b"}; !equalStrings(got, want) {
		t.Errorf("src after RPOPLPUSH = %v, want %v", got, want)
	}
	send(t, conn, "LRANGE dst 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"c", "x", "y"}; !equalStrings(got, want) {
		t.Errorf("dst after RPOPLPUSH = %v, want %v", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN src"), ":2"; got != want {
		t.Errorf("LLEN src = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN dst"), ":3"; got != want {
		t.Errorf("LLEN dst = %q, want %q", got, want)
	}
}

// TestRPopLPushCreatesDestination verifies the destination is created when absent.
func TestRPopLPushCreatesDestination(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH src a b")

	if got, want := sendRead(t, conn, r, "RPOPLPUSH src dst"), "$b"; got != want {
		t.Errorf("RPOPLPUSH src dst = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN dst"), ":1"; got != want {
		t.Errorf("LLEN dst after create = %q, want %q", got, want)
	}
	send(t, conn, "LRANGE dst 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"b"}; !equalStrings(got, want) {
		t.Errorf("dst = %v, want %v", got, want)
	}
}

// TestRPopLPushEmptiesSourceDeletesKey verifies moving the last element deletes
// the source key.
func TestRPopLPushEmptiesSourceDeletesKey(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH src only")

	if got, want := sendRead(t, conn, r, "RPOPLPUSH src dst"), "$only"; got != want {
		t.Errorf("RPOPLPUSH src dst = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "EXISTS src"), ":0"; got != want {
		t.Errorf("EXISTS src after emptying = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN dst"), ":1"; got != want {
		t.Errorf("LLEN dst = %q, want %q", got, want)
	}
}

// TestRPopLPushSingleKeyRotation verifies source == destination rotates the tail
// to the head with the length unchanged.
func TestRPopLPushSingleKeyRotation(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b c") // head-to-tail a, b, c

	if got, want := sendRead(t, conn, r, "RPOPLPUSH l l"), "$c"; got != want {
		t.Errorf("RPOPLPUSH l l = %q, want %q", got, want)
	}
	send(t, conn, "LRANGE l 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"c", "a", "b"}; !equalStrings(got, want) {
		t.Errorf("rotation result = %v, want %v", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN l"), ":3"; got != want {
		t.Errorf("LLEN after rotation = %q, want %q (unchanged)", got, want)
	}
}

// TestRPopLPushEmptySourceIsNull verifies an absent/empty source replies the null
// bulk string and does not create the destination.
func TestRPopLPushEmptySourceIsNull(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "RPOPLPUSH absent dst"), "$-1"; got != want {
		t.Errorf("RPOPLPUSH absent = %q, want %q (null bulk)", got, want)
	}
	if got, want := sendRead(t, conn, r, "EXISTS dst"), ":0"; got != want {
		t.Errorf("EXISTS dst after null RPOPLPUSH = %q, want %q (must not create)", got, want)
	}
}

// --- WRONGTYPE + arity (requirements 7.4, 7.5, 3.2) --------------------------

// TestListMutatorWrongType verifies every task-16.2 command replies WRONGTYPE
// against a non-List key.
func TestListMutatorWrongType(t *testing.T) {
	t.Skip("v1 line: no WRONGTYPE on redimo v1.6.1 (no type tag)")
	store := newFakeStringStore()
	store.metas["0:k"] = storage.Meta{Type: string(meta.TypeString)}
	store.live["0:k"] = true

	conn, r := startStringServer(t, store, fixedNow(1000))
	want := "-WRONGTYPE Operation against a key holding the wrong kind of value"

	cmds := []string{
		"LSET k 0 v",
		"LTRIM k 0 -1",
		"LREM k 0 v",
		"LINSERT k BEFORE p v",
		"RPOPLPUSH k dst",
	}
	for _, cmd := range cmds {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// TestRPopLPushWrongTypeDestination verifies a wrong-type destination is rejected
// WITHOUT losing the source element (the destination type is checked before the
// pop).
func TestRPopLPushWrongTypeDestination(t *testing.T) {
	t.Skip("v1 line: no WRONGTYPE on redimo v1.6.1 (no type tag)")
	store := newFakeStringStore()
	store.metas["0:dst"] = storage.Meta{Type: string(meta.TypeString)}
	store.live["0:dst"] = true

	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "RPUSH src a b")

	want := "-WRONGTYPE Operation against a key holding the wrong kind of value"
	if got := sendRead(t, conn, r, "RPOPLPUSH src dst"); got != want {
		t.Errorf("RPOPLPUSH src dst(wrongtype) = %q, want %q", got, want)
	}
	// Source is untouched.
	if got, want := sendRead(t, conn, r, "LLEN src"), ":2"; got != want {
		t.Errorf("LLEN src after wrong-type dst = %q, want %q (must not pop)", got, want)
	}
}

func TestListMutatorArityErrors(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	cases := map[string]string{
		"LSET l 0":      "-ERR wrong number of arguments for 'lset' command",
		"LTRIM l 0":     "-ERR wrong number of arguments for 'ltrim' command",
		"LREM l 0":      "-ERR wrong number of arguments for 'lrem' command",
		"RPOPLPUSH src": "-ERR wrong number of arguments for 'rpoplpush' command",
		// v1 line: LINSERT is GATED (unregistered → "unknown command"), so its arity
		// is not checked here; that case was removed.
	}
	for cmd, want := range cases {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}
