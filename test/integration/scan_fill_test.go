package integration

// Integration coverage for the SCAN fill-page loop and MATCH auto-wrap (bugfix
// spec v1-scan-substring-match, tasks 8.1-8.3). Unlike the in-memory unit
// tests, these run against the real redimo -> DynamoDB path, where the backend
// Limit counts ITEMS (every hash field is an item) — the exact condition that
// produced the "0 keys" / partial-match symptom in Tiny RDM. A mixed dataset
// with a multi-thousand-item hash makes the backend pages item-dominated by
// non-key items, so a pre-fix single-page SCAN would have surfaced empty or
// partial MATCH pages.
//
// Proxy-only assertions (no oracle): the auto-wrap of plain-text patterns is a
// deliberate redimos extension over stock Redis semantics, so these tests do
// not byte-diff against Redis.

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// seedSets writes the given keys as string keys in batches of pipelined SETs,
// failing the test on any non-OK reply.
func seedSets(t *testing.T, c *respConn, keys ...string) {
	t.Helper()
	const batch = 200
	for off := 0; off < len(keys); off += batch {
		end := off + batch
		if end > len(keys) {
			end = len(keys)
		}
		var wire []byte
		for _, k := range keys[off:end] {
			wire = append(wire, "*3\r\n"...)
			for _, a := range []string{"SET", k, "v"} {
				wire = append(wire, fmt.Sprintf("$%d\r\n%s\r\n", len(a), a)...)
			}
		}
		n := end - off
		replies := c.rawReplies(wire, n)
		if got := strings.Count(string(replies), "+OK\r\n"); got != n {
			t.Fatalf("seed SETs: %d/%d OK, raw replies head: %.200q", got, n, replies)
		}
	}
}

// TestIntegrationScanFillSubstringMixedDataset (task 8.1): over a mixed
// dataset — hundreds of string keys plus one hash holding thousands of field
// ITEMS — a single `SCAN 0 MATCH *substring* COUNT 10` must return EVERY
// matching key in its first reply (the fill loop keeps pulling backend pages
// until the table is exhausted), with the terminating cursor "0".
func TestIntegrationScanFillSubstringMixedDataset(t *testing.T) {
	c := dial(t, proxyAddr(t))
	p := fmt.Sprintf("fill%d", time.Now().UnixNano())

	matches := []string{
		p + ":a:sn52023464",
		p + ":m:sn52023464",
		p + ":z:sn52023464",
	}
	var fillers []string
	for i := 1; i <= 300; i++ {
		fillers = append(fillers, fmt.Sprintf("%s:f:%04d", p, i))
	}
	seedSets(t, c, append(append([]string{}, matches...), fillers...)...)

	// One hash with 2000 fields = 2000 extra items inflating the backend pages.
	const fields = 2000
	for off := 0; off < fields; off += 200 {
		args := [][]byte{bs("HMSET"), bs(p + ":big")}
		for i := off; i < off+200; i++ {
			args = append(args, bs(fmt.Sprintf("f%05d", i)), bs("v"))
		}
		if got := string(c.do(args...)); got != "+OK\r\n" {
			t.Fatalf("HMSET batch at %d = %q, want +OK", off, got)
		}
	}

	pat := "*" + p + "*sn52023464*"
	cursor, elems := parseScanReply(t, c.do(bs("SCAN"), bs("0"), bs("MATCH"), bs(pat), bs("COUNT"), bs("10")))
	sort.Strings(elems)
	if got, want := strings.Join(elems, ","), strings.Join(matches, ","); got != want {
		t.Errorf("single SCAN MATCH %q = %v, want all matches %v", pat, elems, matches)
	}
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\" (fill loop should reach the table end)", cursor)
	}
}

// TestIntegrationScanMatchPlainTextAutoWraps (task 8.2): a MATCH pattern with
// no glob metacharacters is treated as a substring query (*pattern*) — a PARTIAL
// fragment of a key name hits that key.
func TestIntegrationScanMatchPlainTextAutoWraps(t *testing.T) {
	c := dial(t, proxyAddr(t))
	p := fmt.Sprintf("wrap%d", time.Now().UnixNano())

	seedSets(t, c, p+":order:52023464:a", p+":order:1", p+":x:52023464:b")

	// A bare fragment — not even a full key segment — must substring-match.
	cursor, elems := parseScanReply(t, c.do(bs("SCAN"), bs("0"), bs("MATCH"), bs(p+":order:520234")))
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\"", cursor)
	}
	want := p + ":order:52023464:a"
	if len(elems) != 1 || elems[0] != want {
		t.Errorf("plain-text MATCH = %v, want [%s]", elems, want)
	}
}

// TestIntegrationScanFillIteratesExhaustively (task 8.3): with more matches
// than COUNT and a table larger than one backend chunk, iterating the cursor
// collects every match exactly once and terminates at cursor "0". The first
// reply must already be non-empty (no empty pages leak to the client).
func TestIntegrationScanFillIteratesExhaustively(t *testing.T) {
	c := dial(t, proxyAddr(t))
	p := fmt.Sprintf("iter%d", time.Now().UnixNano())

	const nMatches, nFillers = 25, 1500
	want := make(map[string]bool, nMatches)
	var keys []string
	for i := 0; i < nMatches; i++ {
		k := fmt.Sprintf("%s:q765:%02d", p, i)
		want[k] = true
		keys = append(keys, k)
	}
	for i := 1; i <= nFillers; i++ {
		keys = append(keys, fmt.Sprintf("%s:f:%05d", p, i))
	}
	seedSets(t, c, keys...)

	pat := "*" + p + ":q765:*"
	got := make(map[string]int, nMatches)
	cursor := "0"
	first := true
	for pages := 0; ; pages++ {
		if pages > 100 {
			t.Fatalf("SCAN did not terminate (cursor stuck at %q)", cursor)
		}
		next, elems := parseScanReply(t, c.do(bs("SCAN"), bs(cursor), bs("MATCH"), bs(pat), bs("COUNT"), bs("7")))
		if first {
			if len(elems) == 0 {
				t.Errorf("first reply is empty — the fill loop must not surface empty pages while matches remain")
			}
			first = false
		}
		for _, k := range elems {
			got[k]++
		}
		if next == "0" {
			break
		}
		cursor = next
	}
	if len(got) != nMatches {
		t.Fatalf("iterated scan collected %d distinct matches, want %d", len(got), nMatches)
	}
	for k := range want {
		if got[k] != 1 {
			t.Errorf("key %q returned %d times, want exactly 1", k, got[k])
		}
	}
}
