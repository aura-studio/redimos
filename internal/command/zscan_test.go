package command

import (
	"fmt"
	"testing"
)

// Unit tests for task 15.2: ZSCAN (single-pk paging, requirement 9.3),
// ZRANGEBYLEX / ZREVRANGEBYLEX (lexicographic range, requirement 9.4), and
// ZUNIONSTORE / ZINTERSTORE (non-atomic in-memory combine + store, requirement
// 9.5). They run the real in-process server + router over the stateful
// fakeStringStore, exactly like the Hash/Set scan and store tests.
//
// ZSCAN reuses the shared SCAN cursor machinery, so its paging tests use
// startScanServer (which wires a scan.Registry sharing scanInstID with the
// server) exactly like hscan_test.go / sscan_test.go. The ZSCAN reply shape
// [cursor, [member1, score1, ...]] is parsed with readScanReply (scan_test.go) and
// folded into a member->score map with hscanPairs (hscan_test.go) for
// order-independent comparison (ZSCAN, like SCAN, does not promise order). The lex
// and store commands need no cursor, so they run over the default
// startStringServer wiring.

// --- ZSCAN (requirement 9.3) ------------------------------------------------

// TestZScanSinglePageReturnsAllMembers verifies ZSCAN <key> 0 returns every
// member/score pair of a small sorted set in a single page and reports the
// terminating cursor "0".
func TestZScanSinglePageReturnsAllMembers(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c")

	send(t, conn, "ZSCAN z 0")
	cursor, flat := readScanReply(t, r)
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\" (scan complete)", cursor)
	}
	got := hscanPairs(t, flat)
	want := map[string]string{"a": "1", "b": "2", "c": "3"}
	if len(got) != len(want) {
		t.Fatalf("ZSCAN pairs = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("ZSCAN[%s] = %q, want %q", k, got[k], v)
		}
	}
}

// TestZScanMatchFiltersMembers verifies MATCH applies a proxy-side glob filter to
// the MEMBER names, leaving each matched member paired with its score.
func TestZScanMatchFiltersMembers(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 m:1 2 m:2 3 other 4 m:10")

	send(t, conn, "ZSCAN z 0 MATCH m:*")
	cursor, flat := readScanReply(t, r)
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\"", cursor)
	}
	got := hscanPairs(t, flat)
	want := map[string]string{"m:1": "1", "m:2": "2", "m:10": "4"}
	if len(got) != len(want) {
		t.Fatalf("MATCH m:* pairs = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("ZSCAN MATCH[%s] = %q, want %q", k, got[k], v)
		}
	}

	// A pattern matching no member yields the terminating cursor and an empty
	// (non-null) inner array.
	send(t, conn, "ZSCAN z 0 MATCH nomatch*")
	cursor, flat = readScanReply(t, r)
	if cursor != "0" || len(flat) != 0 {
		t.Errorf("MATCH nomatch* = (%q, %v), want (\"0\", [])", cursor, flat)
	}
}

// TestZScanCountPagingCoversAllMembers verifies iterating ZSCAN with a small COUNT
// reassembles the ENTIRE sorted set across pages without omission and terminates
// at cursor "0" — the single-pk analogue of SCAN's COUNT paging.
func TestZScanCountPagingCoversAllMembers(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))

	const n = 20
	want := make(map[string]string, n)
	cmd := "ZADD z"
	for i := 0; i < n; i++ {
		member := fmt.Sprintf("m%02d", i)
		cmd += fmt.Sprintf(" %d %s", i, member)
		want[member] = fmt.Sprintf("%d", i)
	}
	sendRead(t, conn, r, cmd)

	got := make(map[string]string, n)
	cursor := "0"
	pages := 0
	for {
		send(t, conn, "ZSCAN z "+cursor+" COUNT 3")
		next, flat := readScanReply(t, r)
		for k, v := range hscanPairs(t, flat) {
			got[k] = v
		}
		pages++
		if next == "0" {
			break
		}
		cursor = next
		if pages > n+5 {
			t.Fatalf("ZSCAN did not terminate after %d pages", pages)
		}
	}
	if pages < 2 {
		t.Errorf("expected multiple pages with COUNT 3 over %d members, got %d", n, pages)
	}
	if len(got) != len(want) {
		t.Fatalf("ZSCAN paged pairs = %d, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("ZSCAN paged[%s] = %q, want %q", k, got[k], v)
		}
	}
}

// TestZScanWrongType verifies ZSCAN against a live key of a different type replies
// the byte-for-byte WRONGTYPE error.
func TestZScanWrongType(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET s value") // a String key

	if got, want := sendRead(t, conn, r, "ZSCAN s 0"),
		"-WRONGTYPE Operation against a key holding the wrong kind of value"; got != want {
		t.Errorf("ZSCAN s 0 = %q, want %q", got, want)
	}
}

// TestZScanAbsentKeyIsEmpty verifies ZSCAN on an absent key replies the
// terminating ["0", []] (an empty, non-null inner array), treating the missing key
// as an empty sorted set.
func TestZScanAbsentKeyIsEmpty(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))

	send(t, conn, "ZSCAN absent 0")
	cursor, flat := readScanReply(t, r)
	if cursor != "0" || len(flat) != 0 {
		t.Errorf("ZSCAN absent 0 = (%q, %v), want (\"0\", [])", cursor, flat)
	}
}

// TestZScanInvalidCursor verifies a non-numeric cursor and an unknown numeric
// cursor are both rejected with the byte-for-byte invalid-cursor error, matching
// SCAN's cursor contract (requirement 13.5).
func TestZScanInvalidCursor(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a")

	want := "-ERR invalid cursor, restart scan"
	if got := sendRead(t, conn, r, "ZSCAN z notanumber"); got != want {
		t.Errorf("ZSCAN z notanumber = %q, want %q", got, want)
	}
	// A syntactically valid but never-registered cursor is likewise invalid.
	if got := sendRead(t, conn, r, "ZSCAN z 999"); got != want {
		t.Errorf("ZSCAN z 999 = %q, want %q", got, want)
	}
}

// --- ZRANGEBYLEX / ZREVRANGEBYLEX (requirement 9.4) -------------------------

// TestZRangeByLexInclusiveExclusive verifies the [ ( - + bound syntax over a set
// of equal-score members ordered lexicographically.
func TestZRangeByLexInclusiveExclusive(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// Equal scores: lex order is pure member order.
	sendRead(t, conn, r, "ZADD z 0 a 0 b 0 c 0 d")

	// Full range with - and +.
	send(t, conn, "ZRANGEBYLEX z - +")
	assertArray(t, "ZRANGEBYLEX z - +", readArray(t, r), []string{"$a", "$b", "$c", "$d"})

	// Inclusive [b [c.
	send(t, conn, "ZRANGEBYLEX z [b [c")
	assertArray(t, "ZRANGEBYLEX z [b [c", readArray(t, r), []string{"$b", "$c"})

	// Exclusive (a (c => only b.
	send(t, conn, "ZRANGEBYLEX z (a (c")
	assertArray(t, "ZRANGEBYLEX z (a (c", readArray(t, r), []string{"$b"})

	// Mixed: from - up to (c (exclusive) => a, b.
	send(t, conn, "ZRANGEBYLEX z - (c")
	assertArray(t, "ZRANGEBYLEX z - (c", readArray(t, r), []string{"$a", "$b"})
}

// TestZRevRangeByLex verifies ZREVRANGEBYLEX takes its bounds as (max, min) and
// emits the members in descending lexicographic order.
func TestZRevRangeByLex(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 0 a 0 b 0 c 0 d")

	// Full range: max=+, min=-.
	send(t, conn, "ZREVRANGEBYLEX z + -")
	assertArray(t, "ZREVRANGEBYLEX z + -", readArray(t, r), []string{"$d", "$c", "$b", "$a"})

	// Bounded inclusive: max=[c, min=[b => c, b.
	send(t, conn, "ZREVRANGEBYLEX z [c [b")
	assertArray(t, "ZREVRANGEBYLEX z [c [b", readArray(t, r), []string{"$c", "$b"})
}

// TestZRangeByLexInvalidBound verifies a bound missing the [ / ( / - / + marker is
// rejected with the not-valid-string-range error.
func TestZRangeByLexInvalidBound(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 0 a")

	want := "-ERR min or max not valid string range item"
	if got := sendRead(t, conn, r, "ZRANGEBYLEX z a b"); got != want {
		t.Errorf("ZRANGEBYLEX z a b = %q, want %q", got, want)
	}
}

// TestZRangeByLexWrongTypeAndAbsent verifies a live non-ZSet key replies WRONGTYPE
// and an absent key replies the empty array.
func TestZRangeByLexWrongTypeAndAbsent(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	send(t, conn, "ZRANGEBYLEX absent - +")
	assertArray(t, "ZRANGEBYLEX absent - +", readArray(t, r), []string{})

	sendRead(t, conn, r, "SET s value")
	if got, want := sendRead(t, conn, r, "ZRANGEBYLEX s - +"),
		"-WRONGTYPE Operation against a key holding the wrong kind of value"; got != want {
		t.Errorf("ZRANGEBYLEX s - + = %q, want %q", got, want)
	}
}

// --- ZUNIONSTORE / ZINTERSTORE (requirement 9.5) ----------------------------

// TestZUnionStore verifies the default (SUM) union combines overlapping scores,
// stores the result into dest as a sorted set, replies the cardinality, and keeps
// dest's meta.cnt exact.
func TestZUnionStore(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "ZADD z1 1 a 2 b 3 c")
	sendRead(t, conn, r, "ZADD z2 10 b 20 c 30 d")

	// Union of 2 keys: 4 distinct members.
	if got, want := sendRead(t, conn, r, "ZUNIONSTORE dest 2 z1 z2"), ":4"; got != want {
		t.Errorf("ZUNIONSTORE dest 2 z1 z2 = %q, want %q", got, want)
	}
	// Scores SUM overlapping members: a=1, b=12, c=23, d=30 (ascending by score).
	send(t, conn, "ZRANGE dest 0 -1 WITHSCORES")
	assertArray(t, "ZRANGE dest WITHSCORES", readArray(t, r),
		[]string{"$a", "$1", "$b", "$12", "$c", "$23", "$d", "$30"})
	if got, want := store.metas["0:dest"].Count, int64(4); got != want {
		t.Errorf("dest meta.cnt = %d, want %d", got, want)
	}
}

// TestZInterStore verifies the intersection keeps only members present in every
// operand, summing their scores by default.
func TestZInterStore(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z1 1 a 2 b 3 c")
	sendRead(t, conn, r, "ZADD z2 10 b 20 c 30 d")

	if got, want := sendRead(t, conn, r, "ZINTERSTORE dest 2 z1 z2"), ":2"; got != want {
		t.Errorf("ZINTERSTORE dest 2 z1 z2 = %q, want %q", got, want)
	}
	// Only b and c are in both: b=2+10=12, c=3+20=23.
	send(t, conn, "ZRANGE dest 0 -1 WITHSCORES")
	assertArray(t, "ZRANGE dest WITHSCORES", readArray(t, r),
		[]string{"$b", "$12", "$c", "$23"})
}

// TestZUnionStoreWeights verifies per-operand WEIGHTS multiply each operand's
// scores before aggregation.
func TestZUnionStoreWeights(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z1 1 a 2 b 3 c")
	sendRead(t, conn, r, "ZADD z2 10 b 20 c 30 d")

	// WEIGHTS 1 2: a=1, b=2+20=22, c=3+40=43, d=60.
	if got, want := sendRead(t, conn, r, "ZUNIONSTORE dest 2 z1 z2 WEIGHTS 1 2"), ":4"; got != want {
		t.Errorf("ZUNIONSTORE WEIGHTS = %q, want %q", got, want)
	}
	send(t, conn, "ZRANGE dest 0 -1 WITHSCORES")
	assertArray(t, "ZRANGE dest WEIGHTS", readArray(t, r),
		[]string{"$a", "$1", "$b", "$22", "$c", "$43", "$d", "$60"})
}

// TestZUnionStoreAggregateMinMax verifies the AGGREGATE MIN / MAX functions select
// the minimum / maximum weighted score for overlapping members.
func TestZUnionStoreAggregateMinMax(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z1 1 a 2 b 3 c")
	sendRead(t, conn, r, "ZADD z2 10 b 20 c 30 d")

	// MIN: a=1, b=min(2,10)=2, c=min(3,20)=3, d=30.
	sendRead(t, conn, r, "ZUNIONSTORE dest 2 z1 z2 AGGREGATE MIN")
	send(t, conn, "ZRANGE dest 0 -1 WITHSCORES")
	assertArray(t, "ZUNIONSTORE AGGREGATE MIN", readArray(t, r),
		[]string{"$a", "$1", "$b", "$2", "$c", "$3", "$d", "$30"})

	// MAX: a=1, b=10, c=20, d=30 (ascending by resulting score).
	sendRead(t, conn, r, "ZUNIONSTORE dest 2 z1 z2 AGGREGATE MAX")
	send(t, conn, "ZRANGE dest 0 -1 WITHSCORES")
	assertArray(t, "ZUNIONSTORE AGGREGATE MAX", readArray(t, r),
		[]string{"$a", "$1", "$b", "$10", "$c", "$20", "$d", "$30"})
}

// TestZUnionStoreSetOperand verifies a plain Set operand contributes each member
// with score 1 (Redis treats a set as a zset of score-1 members).
func TestZUnionStoreSetOperand(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z1 5 x 1 a")
	sendRead(t, conn, r, "SADD st x y") // a plain Set operand

	// x = 5 (zset) + 1 (set) = 6, a = 1, y = 1. => 3 members.
	if got, want := sendRead(t, conn, r, "ZUNIONSTORE dest 2 z1 st"), ":3"; got != want {
		t.Errorf("ZUNIONSTORE with set operand = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "ZSCORE dest x"), "$6"; got != want {
		t.Errorf("ZSCORE dest x = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "ZSCORE dest y"), "$1"; got != want {
		t.Errorf("ZSCORE dest y = %q, want %q", got, want)
	}
}

// TestZStoreWrongTypeOperand verifies a String (neither zset nor set) operand
// replies WRONGTYPE and leaves dest untouched.
func TestZStoreWrongTypeOperand(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET s value")

	if got, want := sendRead(t, conn, r, "ZUNIONSTORE dest 1 s"),
		"-WRONGTYPE Operation against a key holding the wrong kind of value"; got != want {
		t.Errorf("ZUNIONSTORE dest 1 s = %q, want %q", got, want)
	}
	// dest must not exist.
	if got, want := sendRead(t, conn, r, "ZCARD dest"), ":0"; got != want {
		t.Errorf("ZCARD dest after wrong-type = %q, want %q", got, want)
	}
}

// TestZStoreBadNumkeys verifies numkeys validation: a non-positive numkeys, a
// non-integer numkeys, and a numkeys larger than the supplied key count each reply
// the appropriate error.
func TestZStoreBadNumkeys(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z1 1 a")

	if got, want := sendRead(t, conn, r, "ZUNIONSTORE dest 0 z1"),
		"-ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE"; got != want {
		t.Errorf("ZUNIONSTORE dest 0 z1 = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "ZUNIONSTORE dest notint z1"),
		"-ERR value is not an integer or out of range"; got != want {
		t.Errorf("ZUNIONSTORE dest notint z1 = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "ZUNIONSTORE dest 3 z1"), "-ERR syntax error"; got != want {
		t.Errorf("ZUNIONSTORE dest 3 z1 (too few keys) = %q, want %q", got, want)
	}
}

// TestZInterStoreEmptyResultDeletesDest verifies an empty intersection leaves dest
// deleted (an empty sorted set does not exist) and replies 0, overwriting any
// prior dest value.
func TestZInterStoreEmptyResultDeletesDest(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z1 1 a")
	sendRead(t, conn, r, "ZADD dest 9 preexisting") // dest exists before the store

	// z1 ∩ absent = empty => reply 0 and dest is removed.
	if got, want := sendRead(t, conn, r, "ZINTERSTORE dest 2 z1 absent"), ":0"; got != want {
		t.Errorf("ZINTERSTORE dest 2 z1 absent = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "ZCARD dest"), ":0"; got != want {
		t.Errorf("ZCARD dest after empty store = %q, want %q", got, want)
	}
}

// TestZUnionStoreOverwritesDest verifies dest is replaced entirely (regardless of
// its prior type/contents) by the store result.
func TestZUnionStoreOverwritesDest(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z1 1 a 2 b")
	sendRead(t, conn, r, "SET dest oldvalue") // dest is a String before the store

	if got, want := sendRead(t, conn, r, "ZUNIONSTORE dest 1 z1"), ":2"; got != want {
		t.Errorf("ZUNIONSTORE dest 1 z1 = %q, want %q", got, want)
	}
	// dest is now a sorted set with z1's members; the old String is gone.
	send(t, conn, "ZRANGE dest 0 -1 WITHSCORES")
	assertArray(t, "ZRANGE dest after overwrite", readArray(t, r),
		[]string{"$a", "$1", "$b", "$2"})
}

// TestZStoreArity verifies the minimum arity is enforced (ZUNIONSTORE needs at
// least dest + numkeys + one key).
func TestZStoreArity(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "ZUNIONSTORE dest 1"),
		"-ERR wrong number of arguments for 'zunionstore' command"; got != want {
		t.Errorf("ZUNIONSTORE dest 1 = %q, want %q", got, want)
	}
}
