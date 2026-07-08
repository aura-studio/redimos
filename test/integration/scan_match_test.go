package integration

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

// Dimension M: SCAN-family MATCH glob-pattern semantics. Dimension G proves the SCAN cursor
// guarantees (full coverage, no cursor loop); this proves the PATTERN filter itself matches
// Redis' glob engine (stringmatchlen): '*', '?', '[abc]', '[a-c]', '[^a]' negation, and
// literal patterns must select the same members on the proxy as on Redis 3.2. Cursors are
// opaque and differ, so we iterate each side to completion (scanAll) and compare the matched
// element multisets.

// scanMatchEq iterates a SCAN subcommand with a MATCH pattern on both endpoints and compares
// the returned elements as sorted multisets. base is the command + key (e.g. {"SSCAN", key}).
func (d *differ) scanMatchEq(what string, base [][]byte, pat string) {
	d.n++
	gp := scanAll(d.t, d.p, base, bs("MATCH"), bs(pat), bs("COUNT"), bs("1000"))
	go_ := scanAll(d.t, d.o, base, bs("MATCH"), bs(pat), bs("COUNT"), bs("1000"))
	sort.Strings(gp)
	sort.Strings(go_)
	if !reflect.DeepEqual(gp, go_) {
		d.t.Errorf("%s: MATCH %q mismatch\n  proxy =%v\n  oracle=%v", what, pat, gp, go_)
	}
}

// the shared member/field names exercising each glob metacharacter.
var globNames = []string{"apple", "apricot", "banana", "berry", "cherry", "a1", "a2", "b1"}

var globPatterns = []string{
	"*",       // everything
	"a*",      // prefix
	"a?",      // '?' single char -> a1, a2
	"*rr*",    // substring
	"[ab]*",   // char class
	"[a-c]*",  // range
	"[^a]*",   // negation
	"ban*",    // literal prefix
	"apple",   // exact literal
	"z*",      // matches nothing
	"?????",   // exactly 5 chars -> apple, berry
}

func TestDiffScanMatch_SSCAN(t *testing.T) {
	d := newDiffer(t)
	k := d.k("set")
	args := append([][]byte{bs("SADD"), k}, bssAll(globNames)...)
	d.eq("SADD members", args...)
	for _, p := range globPatterns {
		d.scanMatchEq("SSCAN", [][]byte{bs("SSCAN"), k}, p)
	}
}

func TestDiffScanMatch_HSCAN(t *testing.T) {
	d := newDiffer(t)
	k := d.k("hash")
	// HSET is single-pair in 3.2; set each field individually to stay wire-faithful.
	for _, f := range globNames {
		d.eq("HSET "+f, bs("HSET"), k, bs(f), bs("v_"+f))
	}
	// MATCH filters on the FIELD; scanAll flattens field/value pairs, so both sides carry the
	// same values and the sorted multiset still agrees iff the matched field set agrees.
	for _, p := range globPatterns {
		d.scanMatchEq("HSCAN", [][]byte{bs("HSCAN"), k}, p)
	}
}

func TestDiffScanMatch_ZSCAN(t *testing.T) {
	d := newDiffer(t)
	k := d.k("zset")
	for i, m := range globNames {
		d.eq("ZADD "+m, bs("ZADD"), k, bs(fmt.Sprintf("%d", i)), bs(m))
	}
	for _, p := range globPatterns {
		d.scanMatchEq("ZSCAN", [][]byte{bs("ZSCAN"), k}, p)
	}
}

// TestDiffScanMatch_Keyspace exercises SCAN MATCH over the keyspace using the per-run nonce
// prefix so only this run's keys can match (both endpoints are otherwise unrelated).
func TestDiffScanMatch_Keyspace(t *testing.T) {
	d := newDiffer(t)
	for _, n := range []string{"kA", "kB", "kC", "kAB", "kX1"} {
		d.eq("SET "+n, bs("SET"), d.k(n), bs("v"))
	}
	base := fmt.Sprintf("dt:%s:", d.prefix)
	for _, suffix := range []string{"k*", "k?", "k[AB]", "k[^A]*", "kA*"} {
		d.scanMatchEq("SCAN keyspace", [][]byte{bs("SCAN")}, base+suffix)
	}
}

func bssAll(ss []string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = bs(s)
	}
	return out
}
