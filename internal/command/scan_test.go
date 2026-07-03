package command

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/aura-studio/redimos/v2/internal/scan"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

// scanInstID is the instance identifier shared by the test server and its cursor
// registry. SCAN resolves continuation cursors with LoadOwned(cursor, conn.InstID),
// which succeeds only when the registry's InstID (stamped on every saved cursor)
// matches the connection's InstID. Wiring both to the same value here reproduces the
// production requirement (Storage.Scan doc) that the registry and server share an
// InstID, and lets the happy-path pagination tests succeed end-to-end.
const scanInstID = "inst-scan-test"

// startScanServer boots an in-process server whose router is wired to the given
// fake store, a fixed clock, and a cursor registry that shares scanInstID with the
// server. It returns a connected client.
func startScanServer(t *testing.T, store storage.Store, now func() int64) (net.Conn, *bufio.Reader) {
	t.Helper()

	reg := scan.New(scan.Config{InstID: scanInstID})
	r := NewRouterWithStorage(Config{}, Storage{Store: store, Now: now, Scan: reg})
	s := server.New(server.Options{Addr: "127.0.0.1:0", InstID: scanInstID}, r)

	signal := make(chan error, 1)
	go func() { _ = s.ListenServeAndSignal(signal) }()
	if err := <-signal; err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, bufio.NewReader(conn)
}

// scanReadLine reads one CRLF-terminated protocol line with the terminator stripped.
func scanReadLine(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read line: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}

// scanReadBulk reads a single RESP2 bulk string and returns its payload (empty for
// the null bulk "$-1").
func scanReadBulk(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	hdr := scanReadLine(t, r)
	if len(hdr) == 0 || hdr[0] != '$' {
		t.Fatalf("expected bulk header, got %q", hdr)
	}
	n, err := strconv.Atoi(hdr[1:])
	if err != nil {
		t.Fatalf("bad bulk header %q: %v", hdr, err)
	}
	if n < 0 {
		return ""
	}
	buf := make([]byte, n+2) // payload + CRLF
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("read bulk payload: %v", err)
	}
	return string(buf[:n])
}

// readScanReply parses a SCAN reply — the two-element array [cursor, [keys...]] —
// returning the cursor string and the key names. It fails the test if the outer
// shape is not exactly a 2-element array whose second element is a (non-null) array.
func readScanReply(t *testing.T, r *bufio.Reader) (string, []string) {
	t.Helper()
	if hdr := scanReadLine(t, r); hdr != "*2" {
		t.Fatalf("SCAN reply header = %q, want *2", hdr)
	}
	cursor := scanReadBulk(t, r)

	ah := scanReadLine(t, r)
	if len(ah) == 0 || ah[0] != '*' {
		t.Fatalf("expected keys array header, got %q", ah)
	}
	n, err := strconv.Atoi(ah[1:])
	if err != nil {
		t.Fatalf("bad keys array header %q: %v", ah, err)
	}
	if n < 0 {
		t.Fatalf("SCAN keys array is null (*-1), want an array (possibly empty)")
	}
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		keys = append(keys, scanReadBulk(t, r))
	}
	return cursor, keys
}

// TestScanSinglePageTerminates verifies SCAN 0 returns the whole (small) keyspace
// in a single page and reports the terminating cursor "0". Requirements 3.8, 13.3.
func TestScanSinglePageTerminates(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET a 1")
	sendRead(t, conn, r, "SET b 2")
	sendRead(t, conn, r, "SET c 3")

	send(t, conn, "SCAN 0")
	cursor, keys := readScanReply(t, r)
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\" (scan complete)", cursor)
	}
	sort.Strings(keys)
	if got, want := strings.Join(keys, ","), "a,b,c"; got != want {
		t.Errorf("keys = %v, want [a b c]", keys)
	}
}

// TestScanEmptyKeyspaceReturnsEmptyArray verifies an empty keyspace yields the
// terminating cursor and a (non-null) empty keys array. Requirement 13.3.
func TestScanEmptyKeyspaceReturnsEmptyArray(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	send(t, conn, "SCAN 0")
	cursor, keys := readScanReply(t, r)
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\"", cursor)
	}
	if len(keys) != 0 {
		t.Errorf("keys = %v, want empty", keys)
	}
}

// TestScanMatchFilters verifies MATCH applies a proxy-side glob filter to the key
// names. Requirement 13.4.
func TestScanMatchFilters(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	for _, k := range []string{"user:1", "user:2", "order:1", "user:10"} {
		sendRead(t, conn, r, "SET "+k+" v")
	}

	send(t, conn, "SCAN 0 MATCH user:*")
	cursor, keys := readScanReply(t, r)
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\"", cursor)
	}
	sort.Strings(keys)
	if got, want := strings.Join(keys, ","), "user:1,user:10,user:2"; got != want {
		t.Errorf("MATCH user:* keys = %v, want [user:1 user:10 user:2]", keys)
	}

	// A ?-class glob: user:? matches only single-char suffixes.
	send(t, conn, "SCAN 0 MATCH user:?")
	_, keys = readScanReply(t, r)
	sort.Strings(keys)
	if got, want := strings.Join(keys, ","), "user:1,user:2"; got != want {
		t.Errorf("MATCH user:? keys = %v, want [user:1 user:2]", keys)
	}

	// A pattern that matches nothing yields a (non-null) empty array.
	send(t, conn, "SCAN 0 MATCH nomatch*")
	_, keys = readScanReply(t, r)
	if len(keys) != 0 {
		t.Errorf("MATCH nomatch* keys = %v, want empty", keys)
	}
}

// TestScanCountPagingCoversKeyspace verifies that iterating SCAN with a small COUNT
// reassembles the ENTIRE keyspace across pages without omission, and terminates at
// cursor "0". Requirements 13.3, 13.7 (SCAN may repeat but must not omit live keys).
func TestScanCountPagingCoversKeyspace(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))

	const n = 20
	want := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%02d", i)
		sendRead(t, conn, r, "SET "+k+" v")
		want[k] = true
	}

	got := make(map[string]bool, n)
	cursor := "0"
	pages := 0
	for {
		send(t, conn, "SCAN "+cursor+" COUNT 3")
		next, keys := readScanReply(t, r)
		for _, k := range keys {
			got[k] = true
		}
		pages++
		if pages > n+5 {
			t.Fatalf("SCAN did not terminate after %d pages (cursor=%q)", pages, next)
		}
		if next == "0" {
			break
		}
		cursor = next
	}

	if pages < 2 {
		t.Errorf("expected multiple pages with COUNT 3 over %d keys, got %d page(s)", n, pages)
	}
	if len(got) != len(want) {
		t.Fatalf("scanned %d distinct keys, want %d", len(got), len(want))
	}
	for k := range want {
		if !got[k] {
			t.Errorf("key %q was omitted by the paged SCAN", k)
		}
	}
}

// TestScanUnknownCursorIsInvalid verifies a cursor that was never handed out
// (evicted, from a restarted instance, or otherwise unknown) is rejected with the
// byte-for-byte invalid-cursor error. Requirement 13.5.
func TestScanUnknownCursorIsInvalid(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET a 1")

	want := "-ERR invalid cursor, restart scan"
	if got := sendRead(t, conn, r, "SCAN 123456789"); got != want {
		t.Errorf("SCAN <unknown cursor> = %q, want %q", got, want)
	}
	// A non-numeric cursor is likewise treated as invalid, not a syntax error.
	if got := sendRead(t, conn, r, "SCAN notanumber"); got != want {
		t.Errorf("SCAN <non-numeric cursor> = %q, want %q", got, want)
	}
}

// TestScanEvictedCursorIsInvalid forces a real LRU eviction with a capacity-1
// registry: a first paged SCAN mints a continuation cursor, a second independent
// SCAN mints another cursor that evicts the first, and replaying the evicted cursor
// is then rejected with the byte-for-byte invalid-cursor error. Requirement 13.5.
func TestScanEvictedCursorIsInvalid(t *testing.T) {
	store := newFakeStringStore()
	// Capacity 1: saving a second cursor evicts the first (LRU).
	reg := scan.New(scan.Config{InstID: scanInstID, Capacity: 1})
	router := NewRouterWithStorage(Config{}, Storage{Store: store, Now: fixedNow(1000), Scan: reg})
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

	for i := 0; i < 5; i++ {
		sendRead(t, conn, r, fmt.Sprintf("SET key%d v", i))
	}

	// First page yields a live continuation cursor C1.
	send(t, conn, "SCAN 0 COUNT 2")
	c1, _ := readScanReply(t, r)
	if c1 == "0" {
		t.Fatal("precondition: expected a non-terminating cursor from the first page")
	}

	// A second independent SCAN 0 mints a new cursor, evicting C1 from the
	// capacity-1 registry.
	send(t, conn, "SCAN 0 COUNT 2")
	c2, _ := readScanReply(t, r)
	if c2 == "0" || c2 == c1 {
		t.Fatalf("precondition: expected a distinct non-terminating second cursor, got c1=%q c2=%q", c1, c2)
	}

	// Replaying the evicted cursor C1 is rejected.
	send(t, conn, "SCAN "+c1+" COUNT 2")
	if got, want := scanReadLine(t, r), "-ERR invalid cursor, restart scan"; got != want {
		t.Errorf("SCAN <evicted cursor> = %q, want %q", got, want)
	}
}
