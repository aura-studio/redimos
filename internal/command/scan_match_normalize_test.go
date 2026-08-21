package command

// MATCH auto-wrap coverage for the collection scans (bugfix spec
// v1-scan-substring-match, tasks 7.1-7.4): HSCAN/SSCAN/ZSCAN share SCAN's MATCH
// parsing, so a plain-text pattern is normalized to *pattern* (substring
// match) there too, while patterns carrying glob metacharacters keep their
// strict Redis stringmatchlen semantics byte-for-byte.

import (
	"sort"
	"strings"
	"testing"
)

// TestHScanMatchPlainTextAutoWraps: a plain-text HSCAN MATCH hits fields by
// substring (task 7.1); a metacharacter pattern keeps exact glob semantics
// (task 7.4, hash side).
func TestHScanMatchPlainTextAutoWraps(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "HMSET h order:52023464:a 1 order:1 2 x:52023464:b 3")

	send(t, conn, "HSCAN h 0 MATCH 52023464")
	cursor, flat := readScanReply(t, r)
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\"", cursor)
	}
	got := hscanPairs(t, flat)
	want := map[string]string{"order:52023464:a": "1", "x:52023464:b": "3"}
	if len(got) != len(want) {
		t.Fatalf("plain-text MATCH pairs = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("HSCAN MATCH[%s] = %q, want %q", k, got[k], v)
		}
	}

	// Metacharacter regression: order:* stays a strict prefix glob.
	send(t, conn, "HSCAN h 0 MATCH order:*")
	_, flat = readScanReply(t, r)
	got = hscanPairs(t, flat)
	want = map[string]string{"order:52023464:a": "1", "order:1": "2"}
	if len(got) != len(want) {
		t.Fatalf("MATCH order:* pairs = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("HSCAN MATCH order:*[%s] = %q, want %q", k, got[k], v)
		}
	}
}

// TestSScanMatchPlainTextAutoWraps: a plain-text SSCAN MATCH hits members by
// substring (task 7.2); metacharacter patterns unchanged (task 7.4, set side).
func TestSScanMatchPlainTextAutoWraps(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SADD s order:52023464:a order:1 x:52023464:b")

	send(t, conn, "SSCAN s 0 MATCH 52023464")
	cursor, members := readScanReply(t, r)
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\"", cursor)
	}
	sort.Strings(members)
	if got, want := strings.Join(members, ","), "order:52023464:a,x:52023464:b"; got != want {
		t.Errorf("plain-text MATCH members = %v, want [%v]", members, want)
	}

	// Metacharacter regression: order:* stays a strict prefix glob.
	send(t, conn, "SSCAN s 0 MATCH order:*")
	_, members = readScanReply(t, r)
	sort.Strings(members)
	if got, want := strings.Join(members, ","), "order:1,order:52023464:a"; got != want {
		t.Errorf("MATCH order:* members = %v, want [%v]", members, want)
	}
}

// TestZScanMatchPlainTextAutoWraps: a plain-text ZSCAN MATCH hits members by
// substring (task 7.3); metacharacter patterns unchanged (task 7.4, zset side).
func TestZScanMatchPlainTextAutoWraps(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 order:52023464:a 2 order:1 3 x:52023464:b")

	// ZSCAN's inner array is flat [member, score, member, score, ...]; fold the
	// even indices for an order-independent member comparison.
	membersOf := func(flat []string) []string {
		t.Helper()
		if len(flat)%2 != 0 {
			t.Fatalf("ZSCAN inner array has odd length %d: %v", len(flat), flat)
		}
		out := make([]string, 0, len(flat)/2)
		for i := 0; i+1 < len(flat); i += 2 {
			out = append(out, flat[i])
		}
		sort.Strings(out)
		return out
	}

	send(t, conn, "ZSCAN z 0 MATCH 52023464")
	cursor, flat := readScanReply(t, r)
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\"", cursor)
	}
	if got, want := strings.Join(membersOf(flat), ","), "order:52023464:a,x:52023464:b"; got != want {
		t.Errorf("plain-text MATCH members = %v, want [%v]", membersOf(flat), want)
	}

	// Metacharacter regression: order:* stays a strict prefix glob.
	send(t, conn, "ZSCAN z 0 MATCH order:*")
	_, flat = readScanReply(t, r)
	if got, want := strings.Join(membersOf(flat), ","), "order:1,order:52023464:a"; got != want {
		t.Errorf("MATCH order:* members = %v, want [%v]", membersOf(flat), want)
	}
}
