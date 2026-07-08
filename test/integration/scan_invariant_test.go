package integration

import (
	"sort"
	"testing"
)

// Dimension G: SCAN-family guarantees. SCAN/HSCAN/SSCAN/ZSCAN cursors are opaque and differ
// between the proxy and Redis, so they cannot be compared byte-for-byte. Instead these
// assert the actual guarantees: iterating from cursor 0 back to cursor 0 returns EVERY
// element present for the whole scan, exactly once, and the accumulated set equals both the
// seeded set and the oracle's accumulated set.

// scanAll drives a *SCAN command to completion and returns every element across all pages.
// base is the command prefix without the cursor (e.g. {"SSCAN", key} or {"SCAN"}); trailing
// is appended after the cursor (e.g. MATCH/COUNT).
func scanAll(t *testing.T, c *respConn, base [][]byte, trailing ...[]byte) []string {
	t.Helper()
	cursor := "0"
	var all []string
	for iter := 0; ; iter++ {
		if iter > 100000 {
			t.Fatalf("scan did not terminate (cursor stuck at %q)", cursor)
		}
		args := append(append([][]byte{}, base...), bs(cursor))
		args = append(args, trailing...)
		next, elems := parseScanReply(t, c.do(args...))
		all = append(all, elems...)
		if next == "0" {
			return all
		}
		cursor = next
	}
}

// parseScanReply decodes a [cursor, [elements...]] SCAN reply into the cursor and its
// element payloads.
func parseScanReply(t *testing.T, reply []byte) (cursor string, elems []string) {
	t.Helper()
	if len(reply) == 0 || reply[0] != '*' {
		t.Fatalf("scan reply not an array: %q", reply)
	}
	_, rest := nextLine(reply)  // skip "*2"
	hdr, rest := nextLine(rest) // cursor bulk header "$n"
	if len(hdr) == 0 || hdr[0] != '$' {
		t.Fatalf("scan cursor not a bulk: %q", reply)
	}
	l := mustAtoi(t, string(hdr[1:]))
	if len(rest) < l+2 {
		t.Fatalf("truncated scan cursor: %q", reply)
	}
	cursor = string(rest[:l])
	rest = rest[l+2:]
	els, ok := respArrayElements(rest)
	if !ok {
		t.Fatalf("scan element array malformed: %q", reply)
	}
	return cursor, els
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n := 0
	neg := false
	for i, ch := range s {
		if i == 0 && ch == '-' {
			neg = true
			continue
		}
		if ch < '0' || ch > '9' {
			t.Fatalf("bad integer %q", s)
		}
		n = n*10 + int(ch-'0')
	}
	if neg {
		n = -n
	}
	return n
}

// stride2Keys keeps every other element (fields of HSCAN, members of ZSCAN).
func stride2Keys(flat []string) []string {
	out := make([]string, 0, len(flat)/2)
	for i := 0; i < len(flat); i += 2 {
		out = append(out, flat[i])
	}
	return out
}

func assertSameSet(t *testing.T, what string, got, want []string) {
	t.Helper()
	// no duplicates in got
	seen := map[string]int{}
	for _, g := range got {
		seen[g]++
	}
	for g, n := range seen {
		if n > 1 {
			t.Errorf("%s: element %q returned %d times (SCAN must not duplicate a stable element)", what, g, n)
		}
	}
	gs := append([]string(nil), got...)
	ws := append([]string(nil), want...)
	sort.Strings(gs)
	sort.Strings(ws)
	// dedup gs for set comparison
	gs = dedup(gs)
	ws = dedup(ws)
	if len(gs) != len(ws) {
		t.Errorf("%s: covered %d distinct elements, want %d\n  got =%v\n  want=%v", what, len(gs), len(ws), gs, ws)
		return
	}
	for i := range gs {
		if gs[i] != ws[i] {
			t.Errorf("%s: set mismatch at %d: %q vs %q", what, i, gs[i], ws[i])
			return
		}
	}
}

func dedup(sorted []string) []string {
	out := sorted[:0:0]
	for i, s := range sorted {
		if i == 0 || s != sorted[i-1] {
			out = append(out, s)
		}
	}
	return out
}

func TestScanFamilyInvariants(t *testing.T) {
	d := newDiffer(t)

	// --- SSCAN: full coverage of a set's members ---
	sk := d.k("sscan")
	var members []string
	for i := 0; i < 200; i++ {
		m := "m" + itoa(i)
		members = append(members, m)
		d.p.do(bs("SADD"), sk, bs(m))
		d.o.do(bs("SADD"), sk, bs(m))
	}
	pm := scanAll(t, d.p, [][]byte{bs("SSCAN"), sk}, bs("COUNT"), bs("17"))
	om := scanAll(t, d.o, [][]byte{bs("SSCAN"), sk}, bs("COUNT"), bs("17"))
	assertSameSet(t, "SSCAN vs seeded", pm, members)
	assertSameSet(t, "SSCAN proxy vs oracle", pm, om)

	// --- HSCAN: full coverage of a hash's fields ---
	hk := d.k("hscan")
	var fields []string
	for i := 0; i < 150; i++ {
		f := "f" + itoa(i)
		fields = append(fields, f)
		d.p.do(bs("HSET"), hk, bs(f), bs("v"+itoa(i)))
		d.o.do(bs("HSET"), hk, bs(f), bs("v"+itoa(i)))
	}
	pf := stride2Keys(scanAll(t, d.p, [][]byte{bs("HSCAN"), hk}, bs("COUNT"), bs("13")))
	assertSameSet(t, "HSCAN fields vs seeded", pf, fields)

	// --- ZSCAN: full coverage of a zset's members ---
	zk := d.k("zscan")
	var zmembers []string
	for i := 0; i < 150; i++ {
		m := "z" + itoa(i)
		zmembers = append(zmembers, m)
		d.p.do(bs("ZADD"), zk, bs(itoa(i)), bs(m))
		d.o.do(bs("ZADD"), zk, bs(itoa(i)), bs(m))
	}
	pz := stride2Keys(scanAll(t, d.p, [][]byte{bs("ZSCAN"), zk}, bs("COUNT"), bs("11")))
	assertSameSet(t, "ZSCAN members vs seeded", pz, zmembers)

	// --- SCAN: full coverage of the keyspace under a unique MATCH prefix ---
	pat := d.k("scankey:*")
	var keys []string
	for i := 0; i < 120; i++ {
		k := string(d.k("scankey:" + itoa(i)))
		keys = append(keys, k)
		d.p.do(bs("SET"), bs(k), bs("v"))
	}
	pk := scanAll(t, d.p, [][]byte{bs("SCAN")}, bs("MATCH"), pat, bs("COUNT"), bs("19"))
	assertSameSet(t, "SCAN MATCH vs seeded keys", pk, keys)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
