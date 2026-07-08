package command

import (
	"bufio"
	"fmt"
	"net"
	"testing"

	"github.com/aura-studio/redimos/v2/internal/scan"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// Unit tests for HSCAN (task 13.2, requirement 6.3). They run the real in-process
// server + router over the stateful fakeStringStore (whose HScan pages the
// in-memory hash model with an index-based LEK) wired to a scan.Registry that
// shares scanInstID with the server, exactly like scan_test.go wires SCAN — so the
// shared cursor machinery HSCAN reuses is exercised end-to-end.
//
// The reply shape [cursor, [field1, value1, ...]] is parsed with readScanReply
// (scan_test.go): the inner array is a flat list of alternating field and value
// payloads, which hscanPairs folds into a field->value map for order-independent
// comparison (HSCAN, like HGETALL, does not promise field order).

// hscanPairs folds a flat [field, value, field, value, ...] slice into a map so an
// HSCAN result can be compared independent of field order. It fails the test on an
// odd-length slice (a malformed field/value reply).
func hscanPairs(t *testing.T, flat []string) map[string]string {
	t.Helper()
	if len(flat)%2 != 0 {
		t.Fatalf("HSCAN inner array has odd length %d: %v", len(flat), flat)
	}
	m := make(map[string]string, len(flat)/2)
	for i := 0; i+1 < len(flat); i += 2 {
		m[flat[i]] = flat[i+1]
	}
	return m
}

// TestHScanSinglePageReturnsAllFields verifies HSCAN <key> 0 returns every
// field/value pair of a small hash in a single page and reports the terminating
// cursor "0". Requirement 6.3.
func TestHScanSinglePageReturnsAllFields(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "HMSET h a 1 b 2 c 3")

	send(t, conn, "HSCAN h 0")
	cursor, flat := readScanReply(t, r)
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\" (scan complete)", cursor)
	}
	got := hscanPairs(t, flat)
	want := map[string]string{"a": "1", "b": "2", "c": "3"}
	if len(got) != len(want) {
		t.Fatalf("HSCAN pairs = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("HSCAN[%s] = %q, want %q", k, got[k], v)
		}
	}
}

// TestHScanMatchFiltersFields verifies MATCH applies a proxy-side glob filter to
// the field names, leaving each matched field paired with its value. Requirement
// 6.3.
func TestHScanMatchFiltersFields(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "HMSET h f:1 a f:2 b other c f:10 d")

	send(t, conn, "HSCAN h 0 MATCH f:*")
	cursor, flat := readScanReply(t, r)
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\"", cursor)
	}
	got := hscanPairs(t, flat)
	want := map[string]string{"f:1": "a", "f:2": "b", "f:10": "d"}
	if len(got) != len(want) {
		t.Fatalf("MATCH f:* pairs = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("HSCAN MATCH[%s] = %q, want %q", k, got[k], v)
		}
	}

	// A single-char class: f:? matches only single-char suffixes.
	send(t, conn, "HSCAN h 0 MATCH f:?")
	_, flat = readScanReply(t, r)
	got = hscanPairs(t, flat)
	want = map[string]string{"f:1": "a", "f:2": "b"}
	if len(got) != len(want) {
		t.Fatalf("MATCH f:? pairs = %v, want %v", got, want)
	}

	// A pattern matching no field yields the terminating cursor and an empty
	// (non-null) inner array.
	send(t, conn, "HSCAN h 0 MATCH nomatch*")
	cursor, flat = readScanReply(t, r)
	if cursor != "0" || len(flat) != 0 {
		t.Errorf("MATCH nomatch* = (%q, %v), want (\"0\", [])", cursor, flat)
	}
}

// TestHScanCountPagingCoversAllFields verifies iterating HSCAN with a small COUNT
// reassembles the ENTIRE hash across pages without omission and terminates at
// cursor "0" — the single-pk analogue of SCAN's COUNT paging. Requirement 6.3.
func TestHScanCountPagingCoversAllFields(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))

	const n = 20
	want := make(map[string]string, n)
	cmd := "HMSET h"
	for i := 0; i < n; i++ {
		f := fmt.Sprintf("f%02d", i)
		v := fmt.Sprintf("v%02d", i)
		cmd += " " + f + " " + v
		want[f] = v
	}
	sendRead(t, conn, r, cmd)

	got := make(map[string]string, n)
	cursor := "0"
	pages := 0
	for {
		send(t, conn, "HSCAN h "+cursor+" COUNT 3")
		next, flat := readScanReply(t, r)
		for k, v := range hscanPairs(t, flat) {
			got[k] = v
		}
		pages++
		if pages > n+5 {
			t.Fatalf("HSCAN did not terminate after %d pages (cursor=%q)", pages, next)
		}
		if next == "0" {
			break
		}
		cursor = next
	}

	if pages < 2 {
		t.Errorf("expected multiple pages with COUNT 3 over %d fields, got %d page(s)", n, pages)
	}
	if len(got) != len(want) {
		t.Fatalf("scanned %d distinct fields, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("field %q = %q, want %q (paged HSCAN)", k, got[k], v)
		}
	}
}

// TestHScanWrongType verifies HSCAN against a live key of a different type replies
// the byte-for-byte WRONGTYPE error. Requirement 6.3.
func TestHScanWrongType(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET s value") // a String key

	want := "-WRONGTYPE Operation against a key holding the wrong kind of value"
	if got := sendRead(t, conn, r, "HSCAN s 0"); got != want {
		t.Errorf("HSCAN on String key = %q, want %q", got, want)
	}
}

// TestHScanAbsentKeyIsEmpty verifies HSCAN on an absent key replies the
// terminating ["0", []] (an empty, non-null inner array), treating the missing
// key as an empty hash. Requirement 6.3.
func TestHScanAbsentKeyIsEmpty(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))

	send(t, conn, "HSCAN absent 0")
	cursor, flat := readScanReply(t, r)
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\"", cursor)
	}
	if len(flat) != 0 {
		t.Errorf("HSCAN absent inner array = %v, want empty", flat)
	}
}

// TestHScanUnknownCursorIsInvalid verifies a non-zero cursor that was never handed
// out (and a non-numeric cursor) is rejected with the byte-for-byte invalid-cursor
// error, matching SCAN. Requirement 6.3 (reuses SCAN's cursor contract).
func TestHScanUnknownCursorIsInvalid(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "HSET h a 1")

	want := "-ERR invalid cursor"
	if got := sendRead(t, conn, r, "HSCAN h 123456789"); got != want {
		t.Errorf("HSCAN <unknown cursor> = %q, want %q", got, want)
	}
	if got := sendRead(t, conn, r, "HSCAN h notanumber"); got != want {
		t.Errorf("HSCAN <non-numeric cursor> = %q, want %q", got, want)
	}
}

// TestHScanEvictedCursorIsInvalid forces a real LRU eviction with a capacity-1
// registry: a first paged HSCAN mints a continuation cursor, a second independent
// HSCAN mints another that evicts the first, and replaying the evicted cursor is
// then rejected with the invalid-cursor error. Requirement 6.3 (SCAN cursor
// contract, requirement 13.5).
func TestHScanEvictedCursorIsInvalid(t *testing.T) {
	store := newFakeStringStore()
	reg := scan.New(scan.Config{InstID: scanInstID, Capacity: 1})
	router := NewRouterWithStorage(Config{MultiDB: true}, Storage{Store: store, Now: fixedNow(1000), Scan: reg})
	s := server.New(server.Options{Addr: "127.0.0.1:0", InstID: scanInstID}, router)

	signal := make(chan error, 1)
	go func() { _ = s.ListenServeAndSignal(signal) }()
	if err := <-signal; err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	r := bufio.NewReader(conn)

	// Seed enough fields that a COUNT-2 page does not exhaust the hash.
	cmd := "HMSET h"
	for i := 0; i < 5; i++ {
		cmd += fmt.Sprintf(" f%d v%d", i, i)
	}
	sendRead(t, conn, r, cmd)

	// First page yields a live continuation cursor C1.
	send(t, conn, "HSCAN h 0 COUNT 2")
	c1, _ := readScanReply(t, r)
	if c1 == "0" {
		t.Fatal("precondition: expected a non-terminating cursor from the first page")
	}

	// A second independent HSCAN 0 mints a new cursor, evicting C1 from the
	// capacity-1 registry.
	send(t, conn, "HSCAN h 0 COUNT 2")
	c2, _ := readScanReply(t, r)
	if c2 == "0" || c2 == c1 {
		t.Fatalf("precondition: expected a distinct non-terminating second cursor, got c1=%q c2=%q", c1, c2)
	}

	// Replaying the evicted cursor C1 is rejected.
	send(t, conn, "HSCAN h "+c1+" COUNT 2")
	if got, want := scanReadLine(t, r), "-ERR invalid cursor"; got != want {
		t.Errorf("HSCAN <evicted cursor> = %q, want %q", got, want)
	}
}
