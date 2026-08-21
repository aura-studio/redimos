package command

// Reproduction + branch-coverage tests for the SCAN fill-page fix and the
// plain-text MATCH auto-wrap (bugfix spec: v1-scan-substring-match).
//
// Background: a single SCAN call used to consume exactly ONE backend page —
// COUNT was passed straight through as the DynamoDB Limit, which counts ITEMS
// (every hash field / set member is an item), so a sparse MATCH filter produced
// "0 keys + non-zero cursor" replies even though matching keys lived on later
// pages. GUIs that do not iterate the cursor to the end (Tiny RDM) then showed
// "0 keys" or a partial subset. The fix makes handleScan keep pulling backend
// pages until it has collected ~COUNT MATCHING keys, the table is exhausted, or
// a safety valve trips. Additionally a MATCH pattern without any glob
// metacharacters is normalized to *pattern* (substring match), which is what
// users typing into a GUI search box expect.
//
// The fake store's ScanKeys pages by KEY count (limit = pks per page), which is
// exactly the seam the fill loop sees: pages of pks plus an opaque continuation
// token.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// fillKeys returns the sorted key names the fill tests seed: 22 non-matching
// keys followed (in sort order) by the three matching ones.
func fillKeys() (all []string, matches []string) {
	for i := 1; i <= 22; i++ {
		all = append(all, fmt.Sprintf("a%02d", i))
	}
	matches = []string{
		"m:52023464:a",
		"m:52023464:b",
		"z:52023464:c",
	}
	all = append(all, matches...)
	sort.Strings(all)
	return all, matches
}

// TestScanFillSubstringAcrossPages is the red-light reproduction for the
// fill-page fix: a single SCAN call with a substring MATCH must keep pulling
// backend pages until it has gathered the matching keys — the first reply must
// already contain ALL three matches (the table is small enough that the loop
// also reaches the end, so the cursor terminates at "0"). The pre-fix
// implementation returned only the first page's filter result (zero keys plus
// a non-zero cursor), failing this test.
func TestScanFillSubstringAcrossPages(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	all, matches := fillKeys()
	for _, k := range all {
		sendRead(t, conn, r, "SET "+k+" v")
	}

	send(t, conn, "SCAN 0 MATCH *52023464* COUNT 3")
	cursor, keys := readScanReply(t, r)
	sort.Strings(keys)
	if got, want := strings.Join(keys, ","), strings.Join(matches, ","); got != want {
		t.Errorf("single SCAN MATCH *52023464* returned %v, want all matches %v", keys, matches)
	}
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\" (fill loop should reach the table end for 3 matches in 25 keys)", cursor)
	}
}

// TestScanMatchPlainTextAutoWraps is the red-light reproduction for the
// auto-wrap behavior: a MATCH pattern without any glob metacharacters is
// treated as a substring query (*pattern*). The pre-fix implementation applied
// Redis exact-match semantics and returned an empty array, failing this test.
func TestScanMatchPlainTextAutoWraps(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	all, matches := fillKeys()
	for _, k := range all {
		sendRead(t, conn, r, "SET "+k+" v")
	}

	send(t, conn, "SCAN 0 MATCH 52023464 COUNT 10")
	cursor, keys := readScanReply(t, r)
	sort.Strings(keys)
	if got, want := strings.Join(keys, ","), strings.Join(matches, ","); got != want {
		t.Errorf("plain-text MATCH 52023464 returned %v, want substring matches %v", keys, matches)
	}
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\"", cursor)
	}
}

// --- Fill-loop branch coverage (task 4.3-4.11) -----------------------------

// withScanTuning shrinks the fill-loop knobs for the duration of a test so
// multi-page and safety-valve paths can be exercised without seeding thousands
// of keys. The knobs are package-level vars precisely to allow this.
func withScanTuning(t *testing.T, chunk int32, maxItems int) {
	t.Helper()
	oldChunk, oldMax := scanFetchChunkItems, scanMaxItemsPerCall
	scanFetchChunkItems, scanMaxItemsPerCall = chunk, maxItems
	t.Cleanup(func() { scanFetchChunkItems, scanMaxItemsPerCall = oldChunk, oldMax })
}

// scriptScanStore is a script/fault-injecting Store for the fill-loop branch
// tests: ScanKeys serves the scripted pages in order (one page per call), with
// an optional per-call error. A call beyond the script reports the table end
// (nil token). Every other operation delegates to the embedded fake. Pages
// carry pks in the MultiDB form ("0:<key>") the router's decodePK expects.
type scriptScanStore struct {
	*fakeStringStore
	pages [][]string
	errs  []error // optional per-call error, indexed like pages
	calls int
}

func (s *scriptScanStore) ScanKeys(_ context.Context, _ map[string]types.AttributeValue, _ int32, _ int64) ([]string, map[string]types.AttributeValue, error) {
	i := s.calls
	s.calls++
	if i < len(s.errs) && s.errs[i] != nil {
		return nil, nil, s.errs[i]
	}
	if i >= len(s.pages) {
		return nil, nil, nil // past the script: table end
	}
	// The page carries a continuation token whenever a later call still has
	// something scripted — a further page OR a scripted error (an erroring next
	// call means the table is not exhausted yet).
	more := i+1 < len(s.pages)
	if !more && i+1 < len(s.errs) && s.errs[i+1] != nil {
		more = true
	}
	var next map[string]types.AttributeValue
	if more {
		next = map[string]types.AttributeValue{"idx": &types.AttributeValueMemberN{Value: strconv.Itoa(i + 1)}}
	}
	return s.pages[i], next, nil
}

// TestScanFillMatchesBelowTargetTerminates: fewer matches than COUNT remain in
// the whole table → the single call returns them all with the terminating
// cursor "0" (task 4.3).
func TestScanFillMatchesBelowTargetTerminates(t *testing.T) {
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	for _, k := range []string{"x:one", "x:two", "m:52023464:a", "m:52023464:b", "z:last"} {
		sendRead(t, conn, r, "SET "+k+" v")
	}

	send(t, conn, "SCAN 0 MATCH *52023464* COUNT 10")
	cursor, keys := readScanReply(t, r)
	sort.Strings(keys)
	if got, want := strings.Join(keys, ","), "m:52023464:a,m:52023464:b"; got != want {
		t.Errorf("keys = %v, want the 2 matches", keys)
	}
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\" (matches < COUNT, table exhausted)", cursor)
	}
}

// TestScanFillTargetReachedKeepsCursor: once ~COUNT matches are collected with
// the table NOT exhausted, the reply carries a live cursor; iterating that
// cursor (repeating the MATCH, per Redis per-call semantics) collects every
// remaining match with no omission and no duplicate (task 4.4).
func TestScanFillTargetReachedKeepsCursor(t *testing.T) {
	withScanTuning(t, 2, scanMaxItemsPerCall) // 2-key backend pages force multi-page fills
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	all, matches := fillKeys()
	for _, k := range all {
		sendRead(t, conn, r, "SET "+k+" v")
	}

	send(t, conn, "SCAN 0 MATCH *52023464* COUNT 2")
	cursor, keys := readScanReply(t, r)
	if cursor == "0" {
		t.Fatalf("cursor = \"0\", want a live cursor (2 matches collected, table not exhausted)")
	}
	if got, want := strings.Join(keys, ","), "m:52023464:a,m:52023464:b"; got != want {
		t.Errorf("first reply keys = %v, want [m:52023464:a m:52023464:b]", keys)
	}

	got := map[string]int{}
	for _, k := range keys {
		got[k]++
	}
	for cursor != "0" {
		send(t, conn, "SCAN "+cursor+" MATCH *52023464* COUNT 2")
		cursor, keys = readScanReply(t, r)
		for _, k := range keys {
			got[k]++
		}
	}
	if len(got) != len(matches) {
		t.Fatalf("iterated scan collected %d distinct matches, want %d (%v)", len(got), len(matches), matches)
	}
	for _, m := range matches {
		if got[m] != 1 {
			t.Errorf("match %q returned %d times, want exactly 1", m, got[m])
		}
	}
}

// TestScanFillDedupsAcrossPages: a pk surfacing on two adjacent backend pages
// (DynamoDB may re-evaluate an item around a page boundary) is returned only
// once within a single SCAN reply (task 4.5).
func TestScanFillDedupsAcrossPages(t *testing.T) {
	store := &scriptScanStore{
		fakeStringStore: newFakeStringStore(),
		pages:           [][]string{{"0:k1", "0:k2"}, {"0:k2", "0:k3"}},
	}
	conn, r := startScanServer(t, store, fixedNow(1000))

	send(t, conn, "SCAN 0 COUNT 10")
	cursor, keys := readScanReply(t, r)
	if got, want := strings.Join(keys, ","), "k1,k2,k3"; got != want {
		t.Errorf("keys = %v, want [k1 k2 k3] with the cross-page duplicate k2 returned once", keys)
	}
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\"", cursor)
	}
}

// TestScanFillSkipsEmptyPages: leading backend pages that the MATCH filter
// empties are consumed INSIDE the call — the client never sees an empty page
// with a live cursor while matches remain (task 4.6; the Tiny RDM "0 keys"
// symptom).
func TestScanFillSkipsEmptyPages(t *testing.T) {
	withScanTuning(t, 2, scanMaxItemsPerCall)
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	for _, k := range []string{"a01", "a02", "a03", "a04", "a05", "m:52023464:a"} {
		sendRead(t, conn, r, "SET "+k+" v")
	}

	send(t, conn, "SCAN 0 MATCH *52023464* COUNT 1")
	cursor, keys := readScanReply(t, r)
	if got, want := strings.Join(keys, ","), "m:52023464:a"; got != want {
		t.Errorf("first reply keys = %v, want [m:52023464:a] (empty leading pages must not surface)", keys)
	}
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\"", cursor)
	}
}

// TestScanFillSafetyValveSettlesPartial: with the safety valve tripped, the
// call settles on the last successful page's token — a partial key list plus a
// live cursor — and iterating that cursor still covers the whole keyspace
// without omission or duplication (task 4.7).
func TestScanFillSafetyValveSettlesPartial(t *testing.T) {
	withScanTuning(t, 2, 3) // valve trips after the 2nd 2-item page (4 items >= 3)
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	const n = 10
	for i := 1; i <= n; i++ {
		sendRead(t, conn, r, fmt.Sprintf("SET a%02d v", i))
	}

	send(t, conn, "SCAN 0 COUNT 100")
	cursor, keys := readScanReply(t, r)
	if len(keys) == 0 || len(keys) >= n {
		t.Errorf("first reply keys = %v, want a strict partial (valve settled early)", keys)
	}
	if cursor == "0" {
		t.Fatalf("cursor = \"0\", want a live cursor after a valve settlement")
	}

	got := map[string]int{}
	for _, k := range keys {
		got[k]++
	}
	pages := 0
	for cursor != "0" {
		send(t, conn, "SCAN "+cursor+" COUNT 100")
		cursor, keys = readScanReply(t, r)
		for _, k := range keys {
			got[k]++
		}
		if pages++; pages > n {
			t.Fatalf("continuation did not terminate (cursor=%q)", cursor)
		}
	}
	if len(got) != n {
		t.Fatalf("valve-settled iteration covered %d distinct keys, want %d", len(got), n)
	}
	for k, c := range got {
		if c != 1 {
			t.Errorf("key %q returned %d times, want exactly 1", k, c)
		}
	}
}

// TestScanFillSecondPageErrorReturnsPartial: a backend error (here a deadline)
// after at least one successful page settles the call on the last successful
// page's token: the client gets the partial results plus a live cursor, NOT an
// error, and can resume (task 4.8).
func TestScanFillSecondPageErrorReturnsPartial(t *testing.T) {
	store := &scriptScanStore{
		fakeStringStore: newFakeStringStore(),
		pages:           [][]string{{"0:k1", "0:k2"}},
		errs:            []error{nil, context.DeadlineExceeded},
	}
	conn, r := startScanServer(t, store, fixedNow(1000))

	send(t, conn, "SCAN 0 COUNT 10")
	cursor, keys := readScanReply(t, r)
	if got, want := strings.Join(keys, ","), "k1,k2"; got != want {
		t.Errorf("keys = %v, want the first page's [k1 k2] despite the second page erroring", keys)
	}
	if cursor == "0" {
		t.Fatalf("cursor = \"0\", want a live cursor settled on the last successful page")
	}

	// Resuming past the failed page reaches the (scripted) table end cleanly.
	send(t, conn, "SCAN "+cursor+" COUNT 10")
	cursor, keys = readScanReply(t, r)
	if cursor != "0" || len(keys) != 0 {
		t.Errorf("resumed SCAN = (cursor %q, keys %v), want (\"0\", [])", cursor, keys)
	}
}

// TestScanFillFirstPageErrorSurfaces: a backend error with ZERO successful
// pages keeps the pre-fix behavior — the call fails with the store-error reply
// (task 4.9).
func TestScanFillFirstPageErrorSurfaces(t *testing.T) {
	store := &scriptScanStore{
		fakeStringStore: newFakeStringStore(),
		errs:            []error{errors.New("boom")},
	}
	conn, r := startScanServer(t, store, fixedNow(1000))

	if got, want := sendRead(t, conn, r, "SCAN 0"), "-ERR backend error, retry later"; got != want {
		t.Errorf("SCAN with first-page store error = %q, want %q", got, want)
	}
}

// TestScanFillNoMatchFillsToCount: a MATCH-less SCAN walks the same fill loop,
// collecting COUNT keys per call even when the backend pages are smaller than
// COUNT (task 4.10).
func TestScanFillNoMatchFillsToCount(t *testing.T) {
	withScanTuning(t, 5, scanMaxItemsPerCall)
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	for i := 1; i <= 10; i++ {
		sendRead(t, conn, r, fmt.Sprintf("SET a%02d v", i))
	}

	send(t, conn, "SCAN 0 COUNT 5")
	cursor, keys := readScanReply(t, r)
	if len(keys) != 5 {
		t.Errorf("first reply keys = %v, want exactly 5 (COUNT reached)", keys)
	}
	if cursor == "0" {
		t.Fatalf("cursor = \"0\", want a live cursor (5 of 10 keys returned)")
	}

	send(t, conn, "SCAN "+cursor+" COUNT 5")
	cursor, keys = readScanReply(t, r)
	if len(keys) != 5 {
		t.Errorf("second reply keys = %v, want the remaining 5", keys)
	}
	if cursor != "0" {
		t.Errorf("cursor = %q, want \"0\" (keyspace exhausted)", cursor)
	}
}

// TestScanFillDefaultCountTargets10: a COUNT-less SCAN aims for Redis' default
// of 10 keys per call (task 4.11).
func TestScanFillDefaultCountTargets10(t *testing.T) {
	withScanTuning(t, 5, scanMaxItemsPerCall)
	conn, r := startScanServer(t, newFakeStringStore(), fixedNow(1000))
	for i := 1; i <= 25; i++ {
		sendRead(t, conn, r, fmt.Sprintf("SET a%02d v", i))
	}

	send(t, conn, "SCAN 0")
	cursor, keys := readScanReply(t, r)
	if len(keys) != 10 {
		t.Errorf("COUNT-less SCAN returned %d keys, want the default target of 10", len(keys))
	}
	if cursor == "0" {
		t.Errorf("cursor = \"0\", want a live cursor (10 of 25 keys returned)")
	}
}
