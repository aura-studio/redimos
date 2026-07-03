package command

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// Unit tests for SSCAN and the set-algebra commands (task 14.2, requirements
// 8.3, 8.4). SSCAN reuses the shared SCAN cursor machinery, so its paging tests
// use startScanServer (which wires a scan.Registry sharing scanInstID with the
// server) exactly like hscan_test.go. The set-algebra commands need no cursor, so
// they run over the default startStringServer wiring.
//
// SSCAN replies the two-element array [cursor, [member...]]; readScanReply
// (scan_test.go) parses it, returning the member names directly. SUNION/SINTER/
// SDIFF reply a flat member array parsed by readArray (strings_test.go), whose
// elements render as "$member"; setOfArray folds them into a set for
// order-independent comparison since set-algebra member order is unspecified.

// setOfArray folds a readArray result (elements rendered "$member") into a
// set of member names with the "$" bulk-string marker stripped.
func setOfArray(elems []string) map[string]bool {
	out := make(map[string]bool, len(elems))
	for _, e := range elems {
		out[strings.TrimPrefix(e, "$")] = true
	}
	return out
}

// sortedKeys returns the keys of a string set in sorted order for stable
// comparison and error messages.
func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func assertMemberSet(t *testing.T, got map[string]bool, want ...string) {
	t.Helper()
	wantSet := make(map[string]bool, len(want))
	for _, w := range want {
		wantSet[w] = true
	}
	if len(got) != len(wantSet) {
		t.Fatalf("members = %v, want %v", sortedKeys(got), sortedKeys(wantSet))
	}
	for w := range wantSet {
		if !got[w] {
			t.Errorf("missing member %q (got %v, want %v)", w, sortedKeys(got), sortedKeys(wantSet))
		}
	}
}

// --- SSCAN (requirement 8.3) ------------------------------------------------

// TestSScanSinglePageReturnsAllMembers verifies SSCAN <key> 0 returns every
// member of a small set in a single page and reports the terminating cursor "0".
func TestSScanSinglePageReturnsAllMembers(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s a b c")

	send(t, conn, "SSCAN s 0")
	cursor, members := readScanReply(t, r)
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\" (scan complete)", cursor)
	}
	got := make(map[string]bool, len(members))
	for _, m := range members {
		got[m] = true
	}
	assertMemberSet(t, got, "a", "b", "c")
}

// TestSScanMatchFiltersMembers verifies MATCH applies a proxy-side glob filter to
// the member names.
func TestSScanMatchFiltersMembers(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s m:1 m:2 other m:10")

	send(t, conn, "SSCAN s 0 MATCH m:*")
	cursor, members := readScanReply(t, r)
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\"", cursor)
	}
	got := make(map[string]bool, len(members))
	for _, m := range members {
		got[m] = true
	}
	assertMemberSet(t, got, "m:1", "m:2", "m:10")

	// A single-char class: m:? matches only single-char suffixes.
	send(t, conn, "SSCAN s 0 MATCH m:?")
	_, members = readScanReply(t, r)
	got = make(map[string]bool, len(members))
	for _, m := range members {
		got[m] = true
	}
	assertMemberSet(t, got, "m:1", "m:2")

	// A pattern matching nothing yields the terminating cursor and an empty
	// (non-null) inner array.
	send(t, conn, "SSCAN s 0 MATCH nomatch*")
	cursor, members = readScanReply(t, r)
	if cursor != "0" || len(members) != 0 {
		t.Errorf("MATCH nomatch* = (%q, %v), want (\"0\", [])", cursor, members)
	}
}

// TestSScanCountPagingCoversAllMembers verifies iterating SSCAN with a small
// COUNT reassembles the ENTIRE set across pages without omission and terminates
// at cursor "0".
func TestSScanCountPagingCoversAllMembers(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))

	const n = 20
	want := make(map[string]bool, n)
	cmd := "SADD s"
	for i := 0; i < n; i++ {
		m := fmt.Sprintf("m%02d", i)
		cmd += " " + m
		want[m] = true
	}
	sendRead(t, conn, r, cmd)

	got := make(map[string]bool, n)
	cursor := "0"
	pages := 0
	for {
		send(t, conn, "SSCAN s "+cursor+" COUNT 3")
		next, members := readScanReply(t, r)
		for _, m := range members {
			got[m] = true
		}
		pages++
		if pages > n+5 {
			t.Fatalf("SSCAN did not terminate after %d pages (cursor=%q)", pages, next)
		}
		if next == "0" {
			break
		}
		cursor = next
	}

	if pages < 2 {
		t.Errorf("expected multiple pages with COUNT 3 over %d members, got %d page(s)", n, pages)
	}
	if len(got) != len(want) {
		t.Fatalf("scanned %d distinct members, want %d", len(got), len(want))
	}
	for m := range want {
		if !got[m] {
			t.Errorf("member %q was omitted by the paged SSCAN", m)
		}
	}
}

// TestSScanWrongType verifies SSCAN against a live key of a different type replies
// the byte-for-byte WRONGTYPE error.
func TestSScanWrongType(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET s value") // a String key

	want := "-WRONGTYPE Operation against a key holding the wrong kind of value"
	if got := sendRead(t, conn, r, "SSCAN s 0"); got != want {
		t.Errorf("SSCAN on String key = %q, want %q", got, want)
	}
}

// TestSScanAbsentKeyIsEmpty verifies SSCAN on an absent key replies the
// terminating ["0", []] (an empty, non-null inner array).
func TestSScanAbsentKeyIsEmpty(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))

	send(t, conn, "SSCAN absent 0")
	cursor, members := readScanReply(t, r)
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\"", cursor)
	}
	if len(members) != 0 {
		t.Errorf("SSCAN absent inner array = %v, want empty", members)
	}
}

// TestSScanUnknownCursorIsInvalid verifies a non-zero cursor that was never handed
// out (and a non-numeric cursor) is rejected with the byte-for-byte invalid-cursor
// error, matching SCAN.
func TestSScanUnknownCursorIsInvalid(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s a")

	want := "-ERR invalid cursor, restart scan"
	if got := sendRead(t, conn, r, "SSCAN s 123456789"); got != want {
		t.Errorf("SSCAN <unknown cursor> = %q, want %q", got, want)
	}
	if got := sendRead(t, conn, r, "SSCAN s notanumber"); got != want {
		t.Errorf("SSCAN <non-numeric cursor> = %q, want %q", got, want)
	}
}

// --- SUNION / SINTER / SDIFF (requirements 8.1, 8.4) ------------------------

func TestSUnion(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s1 a b c")
	sendRead(t, conn, r, "SADD s2 c d e")

	send(t, conn, "SUNION s1 s2")
	got := setOfArray(readArray(t, r))
	assertMemberSet(t, got, "a", "b", "c", "d", "e")
}

func TestSInter(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s1 a b c d")
	sendRead(t, conn, r, "SADD s2 c d e")
	sendRead(t, conn, r, "SADD s3 d c f")

	send(t, conn, "SINTER s1 s2 s3")
	got := setOfArray(readArray(t, r))
	assertMemberSet(t, got, "c", "d")
}

// TestSInterWithAbsentOperandIsEmpty verifies an absent operand (the empty set)
// makes the intersection empty.
func TestSInterWithAbsentOperandIsEmpty(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s1 a b c")

	send(t, conn, "SINTER s1 absent")
	got := setOfArray(readArray(t, r))
	if len(got) != 0 {
		t.Errorf("SINTER with absent operand = %v, want empty", sortedKeys(got))
	}
}

func TestSDiff(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s1 a b c d")
	sendRead(t, conn, r, "SADD s2 c")
	sendRead(t, conn, r, "SADD s3 d e")

	// s1 - s2 - s3 = {a, b}
	send(t, conn, "SDIFF s1 s2 s3")
	got := setOfArray(readArray(t, r))
	assertMemberSet(t, got, "a", "b")
}

func TestSetAlgebraWrongType(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s1 a b")
	sendRead(t, conn, r, "SET str value") // non-set operand

	want := "-WRONGTYPE Operation against a key holding the wrong kind of value"
	for _, cmd := range []string{"SUNION s1 str", "SINTER s1 str", "SDIFF s1 str"} {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// --- SUNIONSTORE / SINTERSTORE / SDIFFSTORE (requirements 8.1, 8.4, 8.5) -----

func TestSUnionStoreCardinalityAndDestCount(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "SADD s1 a b c")
	sendRead(t, conn, r, "SADD s2 c d e")

	// Reply is the resulting cardinality.
	if got, want := sendRead(t, conn, r, "SUNIONSTORE dest s1 s2"), ":5"; got != want {
		t.Errorf("SUNIONSTORE = %q, want %q", got, want)
	}
	// dest's meta.cnt tracks the result cardinality (SCARD is O(1) from cnt).
	if got, want := store.metas["0:dest"].Count, int64(5); got != want {
		t.Errorf("dest meta.cnt = %d, want %d", got, want)
	}
	if got, want := sendRead(t, conn, r, "SCARD dest"), ":5"; got != want {
		t.Errorf("SCARD dest = %q, want %q", got, want)
	}
	send(t, conn, "SMEMBERS dest")
	assertMemberSet(t, setOfArray(readArray(t, r)), "a", "b", "c", "d", "e")
}

func TestSInterStore(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "SADD s1 a b c d")
	sendRead(t, conn, r, "SADD s2 b c e")

	if got, want := sendRead(t, conn, r, "SINTERSTORE dest s1 s2"), ":2"; got != want {
		t.Errorf("SINTERSTORE = %q, want %q", got, want)
	}
	send(t, conn, "SMEMBERS dest")
	assertMemberSet(t, setOfArray(readArray(t, r)), "b", "c")
}

func TestSDiffStore(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "SADD s1 a b c d")
	sendRead(t, conn, r, "SADD s2 c d")

	if got, want := sendRead(t, conn, r, "SDIFFSTORE dest s1 s2"), ":2"; got != want {
		t.Errorf("SDIFFSTORE = %q, want %q", got, want)
	}
	send(t, conn, "SMEMBERS dest")
	assertMemberSet(t, setOfArray(readArray(t, r)), "a", "b")
}

// TestSStoreOverwritesExistingDest verifies a *STORE replaces the destination
// entirely, including a dest that previously held a DIFFERENT set, and drives
// meta.cnt to the new cardinality.
func TestSStoreOverwritesExistingDest(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "SADD dest old1 old2 old3 old4") // pre-existing dest
	sendRead(t, conn, r, "SADD s1 a b")
	sendRead(t, conn, r, "SADD s2 b c")

	if got, want := sendRead(t, conn, r, "SUNIONSTORE dest s1 s2"), ":3"; got != want {
		t.Errorf("SUNIONSTORE (overwrite) = %q, want %q", got, want)
	}
	if got, want := store.metas["0:dest"].Count, int64(3); got != want {
		t.Errorf("dest meta.cnt after overwrite = %d, want %d", got, want)
	}
	send(t, conn, "SMEMBERS dest")
	assertMemberSet(t, setOfArray(readArray(t, r)), "a", "b", "c")
}

// TestSStoreEmptyResultDeletesDest verifies an empty result deletes the
// destination (an empty set does not exist) and replies 0.
func TestSStoreEmptyResultDeletesDest(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "SADD dest old1 old2") // pre-existing dest
	sendRead(t, conn, r, "SADD s1 a b")
	sendRead(t, conn, r, "SADD s2 c d") // disjoint → empty intersection

	if got, want := sendRead(t, conn, r, "SINTERSTORE dest s1 s2"), ":0"; got != want {
		t.Errorf("SINTERSTORE (empty) = %q, want %q", got, want)
	}
	// dest must be gone: EXISTS-style checks via SCARD and TYPE.
	if got, want := sendRead(t, conn, r, "SCARD dest"), ":0"; got != want {
		t.Errorf("SCARD dest after empty store = %q, want %q", got, want)
	}
	if store.live["0:dest"] {
		t.Errorf("dest should be deleted after an empty *STORE result")
	}
}

// TestSStoreDestIsAlsoSource verifies a *STORE whose destination is also one of
// the sources reads the source pre-overwrite, matching Redis.
func TestSStoreDestIsAlsoSource(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s1 a b c")
	sendRead(t, conn, r, "SADD s2 c d")

	// SUNIONSTORE s1 s1 s2 → s1 becomes {a,b,c,d}.
	if got, want := sendRead(t, conn, r, "SUNIONSTORE s1 s1 s2"), ":4"; got != want {
		t.Errorf("SUNIONSTORE s1 s1 s2 = %q, want %q", got, want)
	}
	send(t, conn, "SMEMBERS s1")
	assertMemberSet(t, setOfArray(readArray(t, r)), "a", "b", "c", "d")
}

// --- SMOVE (requirements 8.1, 8.4) ------------------------------------------

func TestSMovePresent(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "SADD src a b c")
	sendRead(t, conn, r, "SADD dst x")

	if got, want := sendRead(t, conn, r, "SMOVE src dst b"), ":1"; got != want {
		t.Errorf("SMOVE (present) = %q, want %q", got, want)
	}
	// b left src, joined dst; counts maintained.
	if got, want := sendRead(t, conn, r, "SCARD src"), ":2"; got != want {
		t.Errorf("SCARD src = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SCARD dst"), ":2"; got != want {
		t.Errorf("SCARD dst = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SISMEMBER src b"), ":0"; got != want {
		t.Errorf("SISMEMBER src b = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SISMEMBER dst b"), ":1"; got != want {
		t.Errorf("SISMEMBER dst b = %q, want %q", got, want)
	}
}

// TestSMoveAbsentMember verifies moving a member not in source is a no-op
// replying :0.
func TestSMoveAbsentMember(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD src a b")
	sendRead(t, conn, r, "SADD dst x")

	if got, want := sendRead(t, conn, r, "SMOVE src dst zzz"), ":0"; got != want {
		t.Errorf("SMOVE (absent member) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SCARD src"), ":2"; got != want {
		t.Errorf("SCARD src unchanged = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SCARD dst"), ":1"; got != want {
		t.Errorf("SCARD dst unchanged = %q, want %q", got, want)
	}
}

// TestSMoveLastMemberDeletesSource verifies moving the only member of source
// deletes source (an empty set does not exist).
func TestSMoveLastMemberDeletesSource(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "SADD src only")

	if got, want := sendRead(t, conn, r, "SMOVE src dst only"), ":1"; got != want {
		t.Errorf("SMOVE (last member) = %q, want %q", got, want)
	}
	if store.live["0:src"] {
		t.Errorf("src should be deleted after its last member moved")
	}
	if got, want := sendRead(t, conn, r, "SISMEMBER dst only"), ":1"; got != want {
		t.Errorf("SISMEMBER dst only = %q, want %q", got, want)
	}
}

func TestSMoveWrongType(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD src a")
	sendRead(t, conn, r, "SET str value")

	want := "-WRONGTYPE Operation against a key holding the wrong kind of value"
	// Wrong-type source.
	if got := sendRead(t, conn, r, "SMOVE str dst a"); got != want {
		t.Errorf("SMOVE wrong-type source = %q, want %q", got, want)
	}
	// Wrong-type destination.
	if got := sendRead(t, conn, r, "SMOVE src str a"); got != want {
		t.Errorf("SMOVE wrong-type dest = %q, want %q", got, want)
	}
}

// --- arity (requirement 3.2) ------------------------------------------------

func TestSetAlgebraArityErrors(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	cases := map[string]string{
		"SSCAN s":           "-ERR wrong number of arguments for 'sscan' command",
		"SUNION":            "-ERR wrong number of arguments for 'sunion' command",
		"SINTER":            "-ERR wrong number of arguments for 'sinter' command",
		"SDIFF":             "-ERR wrong number of arguments for 'sdiff' command",
		"SUNIONSTORE dest":  "-ERR wrong number of arguments for 'sunionstore' command",
		"SINTERSTORE dest":  "-ERR wrong number of arguments for 'sinterstore' command",
		"SDIFFSTORE dest":   "-ERR wrong number of arguments for 'sdiffstore' command",
		"SMOVE src dst":     "-ERR wrong number of arguments for 'smove' command",
		"SMOVE src dst m x": "-ERR wrong number of arguments for 'smove' command",
	}
	for cmd, want := range cases {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}
