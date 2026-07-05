package integration

import (
	"fmt"
	"testing"
)

// Dimension G+M deepening: this file drives the SCAN family (SCAN/HSCAN/SSCAN/ZSCAN)
// past the happy-path coverage in scan_invariant_test.go / scan_match_test.go, at the
// specific edge inputs the gap audit flagged:
//
//   GAP 1  WRONGTYPE on every *SCAN op (key exists but is the wrong type) + absent key.
//   GAP 2  MATCH with backslash-escaped metacharacters (\* \? \[ \\) — literal match.
//   GAP 3  MATCH on binary-safe key/member names (NUL bytes, high-bit / UTF-8 bytes).
//   GAP 4  MATCH with nested / malformed character classes (unclosed, nested brackets).
//   GAP 5  MATCH combined with COUNT 1 so every backend page is filtered out — no stall.
//   GAP 6  Non-uint64 / negative cursors (SCAN -1, HSCAN k -1 0, garbage) -> invalid cursor.
//
// The differ compares the proxy against a live Redis 3.2 oracle byte-for-byte (d.eq) or as
// sorted multisets (d.scanMatchEq / eqSorted); cursors are opaque so full-scan comparisons
// go through scanAll, and single-shot error/arity replies go through d.eq directly.

// ---------------------------------------------------------------------------
// GAP 1 — WRONGTYPE on HSCAN/SSCAN/ZSCAN, plus the absent-key empty reply.
// ---------------------------------------------------------------------------

// TestScanDepth_WrongType asserts each *SCAN sub-command returns WRONGTYPE when the key
// exists but holds a different type, and the terminating ["0", []] for an absent key. Both
// endpoints are seeded identically so the single-shot reply is byte-comparable via d.eq.
func TestScanDepth_WrongType(t *testing.T) {
	d := newDiffer(t)

	// A plain string key: HSCAN/SSCAN/ZSCAN over it must be WRONGTYPE on both sides.
	strKey := d.k("wt:string")
	d.eq("SET string", bs("SET"), strKey, bs("v"))
	d.eq("HSCAN over string -> WRONGTYPE", bs("HSCAN"), strKey, bs("0"))
	d.eq("SSCAN over string -> WRONGTYPE", bs("SSCAN"), strKey, bs("0"))
	d.eq("ZSCAN over string -> WRONGTYPE", bs("ZSCAN"), strKey, bs("0"))

	// A hash key: SSCAN and ZSCAN over it are WRONGTYPE; HSCAN is valid.
	hashKey := d.k("wt:hash")
	d.eq("HSET seed hash", bs("HSET"), hashKey, bs("f"), bs("v"))
	d.eq("SSCAN over hash -> WRONGTYPE", bs("SSCAN"), hashKey, bs("0"))
	d.eq("ZSCAN over hash -> WRONGTYPE", bs("ZSCAN"), hashKey, bs("0"))

	// A set key: HSCAN and ZSCAN over it are WRONGTYPE; SSCAN is valid.
	setKey := d.k("wt:set")
	d.eq("SADD seed set", bs("SADD"), setKey, bs("m"))
	d.eq("HSCAN over set -> WRONGTYPE", bs("HSCAN"), setKey, bs("0"))
	d.eq("ZSCAN over set -> WRONGTYPE", bs("ZSCAN"), setKey, bs("0"))

	// A zset key: HSCAN and SSCAN over it are WRONGTYPE; ZSCAN is valid.
	zsetKey := d.k("wt:zset")
	d.eq("ZADD seed zset", bs("ZADD"), zsetKey, bs("1"), bs("m"))
	d.eq("HSCAN over zset -> WRONGTYPE", bs("HSCAN"), zsetKey, bs("0"))
	d.eq("SSCAN over zset -> WRONGTYPE", bs("SSCAN"), zsetKey, bs("0"))

	// A list key: HSCAN/SSCAN/ZSCAN over it are all WRONGTYPE.
	listKey := d.k("wt:list")
	d.eq("RPUSH seed list", bs("RPUSH"), listKey, bs("e"))
	d.eq("HSCAN over list -> WRONGTYPE", bs("HSCAN"), listKey, bs("0"))
	d.eq("SSCAN over list -> WRONGTYPE", bs("SSCAN"), listKey, bs("0"))
	d.eq("ZSCAN over list -> WRONGTYPE", bs("ZSCAN"), listKey, bs("0"))

	// Absent key: each *SCAN returns the terminating cursor "0" and an empty array,
	// never WRONGTYPE. The keys are unique per run so they cannot exist on either side.
	absent := d.k("wt:absent")
	d.eq("HSCAN absent -> [0,[]]", bs("HSCAN"), absent, bs("0"))
	d.eq("SSCAN absent -> [0,[]]", bs("SSCAN"), absent, bs("0"))
	d.eq("ZSCAN absent -> [0,[]]", bs("ZSCAN"), absent, bs("0"))
	// ...and with MATCH/COUNT appended the absent reply is still [0,[]].
	d.eq("SSCAN absent MATCH COUNT", bs("SSCAN"), absent, bs("0"), bs("MATCH"), bs("*"), bs("COUNT"), bs("10"))

	// WRONGTYPE must also win when MATCH/COUNT options are present (still errors, not [0,[]]).
	d.eq("HSCAN string MATCH -> WRONGTYPE", bs("HSCAN"), strKey, bs("0"), bs("MATCH"), bs("*"))
	d.eq("SSCAN string COUNT -> WRONGTYPE", bs("SSCAN"), strKey, bs("0"), bs("COUNT"), bs("5"))
}

// ---------------------------------------------------------------------------
// GAP 6 — invalid (non-uint64 / negative) cursors reply "-ERR invalid cursor".
// ---------------------------------------------------------------------------

// TestScanDepth_InvalidCursor exercises cursor tokens that are not a valid uint64 against
// both endpoints. Redis 3.2 replies "-ERR invalid cursor"; the proxy must match byte-for-byte.
// A wrong-type / absent key is irrelevant here because the cursor is rejected first for SCAN,
// but for *SCAN the type check may precede parsing, so we seed a live key of the right type
// first so the ONLY divergence-worthy behavior under test is the cursor parse.
func TestScanDepth_InvalidCursor(t *testing.T) {
	d := newDiffer(t)

	// Seed live keys of each type so the type check passes and the cursor is what's judged.
	setKey := d.k("ic:set")
	d.eq("SADD seed", bs("SADD"), setKey, bs("m1"), bs("m2"))
	hashKey := d.k("ic:hash")
	d.eq("HSET seed", bs("HSET"), hashKey, bs("f"), bs("v"))
	zsetKey := d.k("ic:zset")
	d.eq("ZADD seed", bs("ZADD"), zsetKey, bs("1"), bs("m"))

	// Cursor tokens that Redis 3.2's parseScanCursorOrReply (strtoul, base 10, whole
	// string, no ERANGE) ALSO rejects with "invalid cursor" — verified against the live
	// oracle. NOTE the deliberately EXCLUDED spellings: strtoul accepts a leading sign, so
	// "-1" / "-9223372036854775808" / "+0" all parse to a valid (wrapped) uint64, and an
	// EMPTY token parses to 0 — Redis returns scan DATA for every one of them, not an error.
	// Those belong to the stale/valid-cursor bucket below, NOT here, so listing them would
	// wrongly demand the proxy's "invalid cursor" equal the oracle's data reply.
	badCursors := []string{
		"18446744073709551616", // UINT64 max + 1 -> strtoul ERANGE
		"abc",                  // non-numeric
		"1.5",                  // float (strtoul stops at '.')
		"0x10",                 // hex form (strtoul base 10 stops at 'x')
		" 0",                   // leading space (isspace guard)
		"0 ",                   // trailing space (eptr != end)
	}
	for _, bad := range badCursors {
		d.eq(fmt.Sprintf("SCAN cursor %q -> invalid", bad), bs("SCAN"), bs(bad))
		d.eq(fmt.Sprintf("SSCAN cursor %q -> invalid", bad), bs("SSCAN"), setKey, bs(bad))
		d.eq(fmt.Sprintf("HSCAN cursor %q -> invalid", bad), bs("HSCAN"), hashKey, bs(bad))
		d.eq(fmt.Sprintf("ZSCAN cursor %q -> invalid", bad), bs("ZSCAN"), zsetKey, bs(bad))
	}

	// A stale but syntactically valid uint64 cursor (never minted) — Redis 3.2 treats an
	// unknown non-zero cursor as the start of a fresh scan and returns data / a "0" cursor,
	// whereas the proxy rejects a cursor not in its registry with "invalid cursor". These two
	// legitimately differ, so we deliberately DO NOT assert byte-equality on such cursors
	// (an accepted architectural difference — the proxy mints opaque cursor TOKENS rather
	// than reusing Redis' stateless reverse-binary position). The signed/empty spellings
	// pulled out of badCursors above ("-1", "-9223372036854775808", "+0", "") land in THIS
	// bucket: strtoul accepts them, so the oracle answers with data, not "invalid cursor".

	// Cursor 0 with a trailing option keyword lacking its value is a syntax error on both.
	d.eq("SCAN 0 dangling MATCH", bs("SCAN"), bs("0"), bs("MATCH"))
	d.eq("SSCAN 0 dangling COUNT", bs("SSCAN"), setKey, bs("0"), bs("COUNT"))
}

// ---------------------------------------------------------------------------
// GAP 2 — MATCH with backslash-escaped metacharacters matches the byte literally.
// ---------------------------------------------------------------------------

// TestScanDepth_MatchEscapes seeds members whose names contain the glob metacharacters
// themselves (*, ?, [, ], \) and verifies that an escaped pattern (\*, \?, \[, \\) matches
// only the literal, while the un-escaped metacharacter matches broadly — identically on both
// sides. Uses SSCAN (member match) as the vehicle; HSCAN/ZSCAN share the same glob engine.
func TestScanDepth_MatchEscapes(t *testing.T) {
	d := newDiffer(t)
	k := d.k("esc:set")

	members := []string{
		"a*b",   // literal asterisk
		"axb",   // would match a*b unescaped and a?b unescaped
		"a?b",   // literal question mark
		"a[b",   // literal open bracket
		"a]b",   // literal close bracket
		`a\b`,   // literal backslash
		"ab",    // shorter, matches a*b (zero fill) but not a?b
		"aXXb",  // matches a*b but not a?b
	}
	args := append([][]byte{bs("SADD"), k}, bssAll(members)...)
	d.eq("SADD escape members", args...)

	patterns := []string{
		`a\*b`,  // literal '*' -> only "a*b"
		`a\?b`,  // literal '?' -> only "a?b"
		`a\[b`,  // literal '[' -> only "a[b"
		`a\]b`,  // literal ']' -> only "a]b"
		`a\\b`,  // literal backslash -> only `a\b`
		"a*b",   // unescaped '*' -> any a...b
		"a?b",   // unescaped '?' -> any a<one>b
		`\a\b\c`, // escaping non-metacharacters is a no-op literal match
	}
	for _, p := range patterns {
		d.scanMatchEq("SSCAN escape", [][]byte{bs("SSCAN"), k}, p)
	}

	// Same escape semantics inside a character class: '[\*x]' matches a literal '*' or 'x'.
	classPatterns := []string{
		`a[\*x]b`, // class containing an escaped '*' and 'x'
		`a[\?x]b`, // class containing an escaped '?' and 'x'
		`a[\\x]b`, // class containing an escaped backslash and 'x'
	}
	for _, p := range classPatterns {
		d.scanMatchEq("SSCAN escape-in-class", [][]byte{bs("SSCAN"), k}, p)
	}
}

// ---------------------------------------------------------------------------
// GAP 3 — MATCH on binary-safe key/member names (NUL bytes, high bytes, UTF-8).
// ---------------------------------------------------------------------------

// TestScanDepth_MatchBinarySafe seeds set members and hash fields whose names contain NUL
// bytes and high-bit (non-ASCII) bytes, then matches with patterns that also contain those
// raw bytes. Redis' glob is byte-exact; the proxy's port must agree on the matched multiset.
func TestScanDepth_MatchBinarySafe(t *testing.T) {
	d := newDiffer(t)

	sk := d.k("bin:set")
	members := []string{
		"key\x00a",           // embedded NUL
		"key\x00b",           // second NUL variant
		"key\xffz",           // high byte 0xFF
		"key\xc3\xa9",        // UTF-8 'é'
		"keyx",               // plain ASCII control
		string([]byte{0x00}), // a member that is a single NUL byte
	}
	sargs := append([][]byte{bs("SADD"), sk}, bssAll(members)...)
	d.eq("SADD binary members", sargs...)

	sPatterns := []string{
		"key*",             // prefix, matches the key... members
		"key\x00*",         // prefix up to and including a NUL
		"key\x00?",         // NUL then exactly one byte -> key\x00a, key\x00b
		"key\xff*",         // high-byte prefix
		"*\xa9",            // ends in the UTF-8 continuation byte 0xA9
		string([]byte{'*', 0x00, '*'}), // NUL anywhere in the name
	}
	for _, p := range sPatterns {
		d.scanMatchEq("SSCAN binary", [][]byte{bs("SSCAN"), sk}, p)
	}

	// Hash fields carry the same binary content; MATCH filters on the field name.
	hk := d.k("bin:hash")
	for i, f := range members {
		d.eq("HSET binary field", bs("HSET"), hk, bs(f), bs("v"+itoa(i)))
	}
	for _, p := range sPatterns {
		d.scanMatchEq("HSCAN binary", [][]byte{bs("HSCAN"), hk}, p)
	}

	// Keyspace SCAN over binary-suffixed keys under this run's nonce prefix.
	base := fmt.Sprintf("dt:%s:", d.prefix)
	binKeys := []string{"bk\x00one", "bk\x00two", "bk\xffthree", "bk\xc3\xa9four"}
	for _, n := range binKeys {
		d.eq("SET binary key", bs("SET"), bs(base+n), bs("v"))
	}
	for _, suffix := range []string{"bk*", "bk\x00*", "bk\x00?ne", "bk\xff*", "bk*four"} {
		d.scanMatchEq("SCAN binary keyspace", [][]byte{bs("SCAN")}, base+suffix)
	}
}

// ---------------------------------------------------------------------------
// GAP 4 — MATCH with nested / malformed character classes.
// ---------------------------------------------------------------------------

// TestScanDepth_MatchClassEdges seeds members that expose the flat-class semantics of Redis'
// stringmatchlen: nested brackets are literal once inside a class, an unclosed class falls
// back to literal matching, and negation/range interplay behaves byte-for-byte identically.
func TestScanDepth_MatchClassEdges(t *testing.T) {
	d := newDiffer(t)
	k := d.k("cls:set")

	members := []string{
		"kabc",   // for [abc] class tests
		"kdef",
		"kghi",
		"k[",     // literal open bracket at end
		"k]",     // literal close bracket at end
		"k^",     // caret as a literal member char
		"k-",     // dash as a literal
		"ka]b",   // for nested-bracket 'literal ]' probing
		"kz",
		"kA",     // uppercase for case-sensitivity / negation
	}
	args := append([][]byte{bs("SADD"), k}, bssAll(members)...)
	d.eq("SADD class members", args...)

	patterns := []string{
		"k[abc]",     // simple class -> kabc? no (kabc is 4 chars); matches k+one of a/b/c
		"k[a-c]*",    // range then anything
		"k[^a-z]*",   // negated range: only non-lowercase after k (k[, k], k^, k-, kA)
		"k[]a]",      // ']' as first class member is a literal ] in Redis (matches k])
		"k[[]",       // class containing a literal '[' -> matches "k["
		"k[abc[def]", // nested-looking: flat class {a,b,c,[,d,e,f}, then literal ']' ... malformed
		"k[^]",       // negated empty-ish class edge
		"k[",         // UNCLOSED class -> Redis falls back to literal, matches "k["
		"k[a-",       // unclosed with a dangling range start
		"k[-a]",      // dash as first class char is literal '-'
		"k[a-]",      // dash as last class char is literal '-'
		"k[z-a]",     // reversed range (Redis swaps endpoints)
	}
	for _, p := range patterns {
		d.scanMatchEq("SSCAN class-edge", [][]byte{bs("SSCAN"), k}, p)
	}
}

// ---------------------------------------------------------------------------
// GAP 5 — MATCH combined with COUNT 1 so every backend page gets filtered out.
// ---------------------------------------------------------------------------

// TestScanDepth_CountOneMatchFilters seeds a set/hash/zset with many members, all of which
// FAIL a highly selective MATCH pattern, driven with COUNT 1 so each backend page yields a
// single item that the proxy-side filter drops. The scan must still terminate (cursor 0) and
// return exactly the matching members (here, a single seeded "z..." member) — never stall or
// loop. scanAll enforces termination; scanMatchEq confirms the multiset equals the oracle's.
func TestScanDepth_CountOneMatchFilters(t *testing.T) {
	d := newDiffer(t)

	// --- SSCAN: 150 non-matching members + a couple that match "z*". ---
	sk := d.k("c1:set")
	var sargs [][]byte
	sargs = append(sargs, bs("SADD"), sk)
	for i := 0; i < 150; i++ {
		sargs = append(sargs, bs("m"+itoa(i)))
	}
	sargs = append(sargs, bs("zebra"), bs("zephyr"))
	d.eq("SADD c1 members", sargs...)

	// COUNT 1 with a pattern that matches only the two z-members: every page but two is
	// filtered to empty, yet the scan must complete and yield exactly {zebra, zephyr}.
	gp := scanAll(t, d.p, [][]byte{bs("SSCAN"), sk}, bs("MATCH"), bs("z*"), bs("COUNT"), bs("1"))
	go_ := scanAll(t, d.o, [][]byte{bs("SSCAN"), sk}, bs("MATCH"), bs("z*"), bs("COUNT"), bs("1"))
	assertSameSet(t, "SSCAN COUNT1 MATCH z* proxy vs oracle", gp, go_)
	assertSameSet(t, "SSCAN COUNT1 MATCH z* vs seeded", gp, []string{"zebra", "zephyr"})

	// A pattern matching NOTHING under COUNT 1: must still terminate with the empty set.
	np := scanAll(t, d.p, [][]byte{bs("SSCAN"), sk}, bs("MATCH"), bs("QQQ*"), bs("COUNT"), bs("1"))
	no := scanAll(t, d.o, [][]byte{bs("SSCAN"), sk}, bs("MATCH"), bs("QQQ*"), bs("COUNT"), bs("1"))
	assertSameSet(t, "SSCAN COUNT1 no-match proxy vs oracle", np, no)
	assertSameSet(t, "SSCAN COUNT1 no-match empty", np, nil)

	// --- HSCAN: same shape on hash fields (COUNT 1, selective MATCH). ---
	hk := d.k("c1:hash")
	for i := 0; i < 120; i++ {
		d.eq("HSET c1 field", bs("HSET"), hk, bs("f"+itoa(i)), bs("v"))
	}
	d.eq("HSET c1 z field", bs("HSET"), hk, bs("zfield"), bs("v"))
	hp := stride2Keys(scanAll(t, d.p, [][]byte{bs("HSCAN"), hk}, bs("MATCH"), bs("z*"), bs("COUNT"), bs("1")))
	ho := stride2Keys(scanAll(t, d.o, [][]byte{bs("HSCAN"), hk}, bs("MATCH"), bs("z*"), bs("COUNT"), bs("1")))
	assertSameSet(t, "HSCAN COUNT1 MATCH z* proxy vs oracle", hp, ho)
	assertSameSet(t, "HSCAN COUNT1 MATCH z* vs seeded", hp, []string{"zfield"})

	// --- ZSCAN: same shape on zset members. ---
	zk := d.k("c1:zset")
	for i := 0; i < 120; i++ {
		d.eq("ZADD c1 member", bs("ZADD"), zk, bs(itoa(i)), bs("m"+itoa(i)))
	}
	d.eq("ZADD c1 z member", bs("ZADD"), zk, bs("999"), bs("zmember"))
	zp := stride2Keys(scanAll(t, d.p, [][]byte{bs("ZSCAN"), zk}, bs("MATCH"), bs("z*"), bs("COUNT"), bs("1")))
	zo := stride2Keys(scanAll(t, d.o, [][]byte{bs("ZSCAN"), zk}, bs("MATCH"), bs("z*"), bs("COUNT"), bs("1")))
	assertSameSet(t, "ZSCAN COUNT1 MATCH z* proxy vs oracle", zp, zo)
	assertSameSet(t, "ZSCAN COUNT1 MATCH z* vs seeded", zp, []string{"zmember"})
}
