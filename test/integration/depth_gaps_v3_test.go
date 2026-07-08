package integration

import (
	"bytes"
	"testing"
)

// Lower-priority depth gaps from the 2026-07 audit: F (unordered edge cases), G (scan
// edge cases), J/N (bitops + coverage), plus a proxy-only S dimension that verifies every
// DELIBERATELY-REJECTED Redis 3.2 command returns a first-class error (never a crash, hang,
// or "unknown command") and leaves the connection usable. The rejected commands cannot be
// differentially compared — Redis would execute (and some would block) — so S asserts the
// proxy's own contract.

// --- Dimension F: unordered / empty-collection edge cases --------------------

func TestDiffUnordered_EmptyAndBinary(t *testing.T) {
	d := newDiffer(t)

	// SMEMBERS / HKEYS / HVALS / HGETALL on an ABSENT key -> empty array.
	miss := d.k("empty-miss")
	d.eqSorted("SMEMBERS absent -> *0", bs("SMEMBERS"), miss)
	d.eqSorted("HKEYS absent -> *0", bs("HKEYS"), miss)
	d.eqSorted("HVALS absent -> *0", bs("HVALS"), miss)
	d.eqSorted("HGETALL absent -> *0", bs("HGETALL"), miss)
	d.eqSorted("ZRANGE absent -> *0", bs("ZRANGE"), miss, bs("0"), bs("-1"))
	d.eqSorted("LRANGE absent -> *0", bs("LRANGE"), miss, bs("0"), bs("-1"))

	// Set algebra with empty / missing operands.
	a := d.k("alg-a")
	b := d.k("alg-b")
	d.eq("SADD a", bs("SADD"), a, bs("x"), bs("y"), bs("z"))
	d.eqSorted("SINTER with missing -> *0", bs("SINTER"), a, b)
	d.eqSorted("SUNION with missing -> a", bs("SUNION"), a, b)
	d.eqSorted("SDIFF with missing -> a", bs("SDIFF"), a, b)
	d.eqSorted("SINTER single -> a", bs("SINTER"), a)

	// Binary-safe members with embedded CRLF / NUL round-trip through SMEMBERS/HKEYS.
	kb := d.k("bin-set")
	d.eq("SADD binary members", bs("SADD"), kb, bs("a\r\nb"), bs("c\x00d"), bs("\xff\xfe"))
	d.eqSorted("SMEMBERS binary", bs("SMEMBERS"), kb)
	hb := d.k("bin-hash")
	d.eq("HMSET binary fields", bs("HMSET"), hb, bs("f\r\n"), bs("v\x00"), bs("g\xff"), bs("w"))
	d.eqSorted("HKEYS binary", bs("HKEYS"), hb)
	d.eqSorted("HVALS binary", bs("HVALS"), hb)

	t.Logf("compared %d unordered/empty/binary replies vs Redis 3.2", d.n)
}

// --- Dimension G: SCAN-family edge cases -------------------------------------

func TestDiffScan_EdgeCases(t *testing.T) {
	d := newDiffer(t)

	// SCAN COUNT 0 / negative is a syntax error on both.
	d.eq("SCAN COUNT 0 -> syntax error", bs("SCAN"), bs("0"), bs("COUNT"), bs("0"))
	d.eq("SCAN negative COUNT -> syntax error", bs("SCAN"), bs("0"), bs("COUNT"), bs("-1"))
	// HSCAN COUNT 0 on an EXISTING key -> syntax error on both. (On a MISSING collection
	// key the two DIVERGE and are not compared: Redis 3.2's hscanCommand replies emptyscan
	// from the key lookup BEFORE scanGenericCommand parses COUNT, so `HSCAN missing 0 COUNT
	// 0` is an empty scan there, whereas redimos validates COUNT first and errors. An
	// accepted minor ordering difference on a pathological input.)
	kh0 := d.k("g-count0")
	d.eq("HSET seed", bs("HSET"), kh0, bs("f"), bs("v"))
	d.eq("HSCAN COUNT 0 on existing -> syntax error", bs("HSCAN"), kh0, bs("0"), bs("COUNT"), bs("0"))

	// SSCAN/HSCAN/ZSCAN over a single-element collection: one page, cursor 0.
	ks := d.k("g-single-set")
	d.eq("SADD one", bs("SADD"), ks, bs("only"))
	d.scanMatchEq("SSCAN single element", [][]byte{bs("SSCAN"), ks}, "*")
	kh := d.k("g-single-hash")
	d.eq("HSET one", bs("HSET"), kh, bs("f"), bs("v"))
	d.scanMatchEq("HSCAN single element", [][]byte{bs("HSCAN"), kh}, "*")
	kz := d.k("g-single-zset")
	d.eq("ZADD one", bs("ZADD"), kz, bs("1"), bs("m"))
	d.scanMatchEq("ZSCAN single element", [][]byte{bs("ZSCAN"), kz}, "*")

	// SSCAN over an ABSENT key -> cursor 0, empty.
	d.scanMatchEq("SSCAN absent key", [][]byte{bs("SSCAN"), d.k("g-absent")}, "*")

	t.Logf("compared %d scan-edge replies vs Redis 3.2", d.n)
}

// --- Dimension N/J: bitmap edge cases ----------------------------------------

func TestDiffBitops_Depth(t *testing.T) {
	d := newDiffer(t)

	// BITPOS over all-zero and all-one bytes with/without range.
	kz := d.k("bp-zero")
	d.eq("SET all-zero", bs("SET"), kz, bs("\x00\x00\x00"))
	d.eq("BITPOS 1 in all-zero -> -1", bs("BITPOS"), kz, bs("1"))
	d.eq("BITPOS 0 in all-zero -> 0", bs("BITPOS"), kz, bs("0"))
	ko := d.k("bp-one")
	d.eq("SET all-one", bs("SET"), ko, bs("\xff\xff"))
	d.eq("BITPOS 0 in all-one -> 16 (past end)", bs("BITPOS"), ko, bs("0"))
	d.eq("BITPOS 0 all-one with range -> -1", bs("BITPOS"), ko, bs("0"), bs("0"), bs("-1"))
	d.eq("BITPOS 1 in all-one -> 0", bs("BITPOS"), ko, bs("1"))
	d.eq("BITPOS 1 range 1..1", bs("BITPOS"), ko, bs("1"), bs("1"), bs("1"))

	// BITCOUNT with ranges (positive, negative, inverted).
	kc := d.k("bc")
	d.eq("SET foobar", bs("SET"), kc, bs("foobar"))
	d.eq("BITCOUNT all", bs("BITCOUNT"), kc)
	d.eq("BITCOUNT 0 0", bs("BITCOUNT"), kc, bs("0"), bs("0"))
	d.eq("BITCOUNT 1 1", bs("BITCOUNT"), kc, bs("1"), bs("1"))
	d.eq("BITCOUNT -2 -1", bs("BITCOUNT"), kc, bs("-2"), bs("-1"))
	d.eq("BITCOUNT 5 2 inverted", bs("BITCOUNT"), kc, bs("5"), bs("2"))

	// BITOP across missing + unequal-length sources.
	s1 := d.k("bop-1")
	s2 := d.k("bop-2")
	dst := d.k("bop-dst")
	d.eq("SET s1 abc", bs("SET"), s1, bs("abc"))
	d.eq("SET s2 xy", bs("SET"), s2, bs("xy"))
	d.eq("BITOP AND unequal len", bs("BITOP"), bs("AND"), dst, s1, s2)
	d.eq("GET AND result", bs("GET"), dst)
	d.eq("BITOP OR with missing", bs("BITOP"), bs("OR"), dst, s1, d.k("bop-miss"))
	d.eq("GET OR result", bs("GET"), dst)
	d.eq("BITOP XOR", bs("BITOP"), bs("XOR"), dst, s1, s2)
	d.eq("BITOP NOT single", bs("BITOP"), bs("NOT"), dst, s1)
	d.eq("GET NOT result", bs("GET"), dst)

	// BITFIELD: a multi-op command where one op FAILs (OVERFLOW FAIL) still applies others.
	kf := d.k("bf-multi")
	d.eq("BITFIELD mixed ops with a failing SET",
		bs("BITFIELD"), kf,
		bs("SET"), bs("u8"), bs("0"), bs("255"),
		bs("OVERFLOW"), bs("FAIL"), bs("INCRBY"), bs("u8"), bs("0"), bs("10"),
		bs("GET"), bs("u8"), bs("0"))

	t.Logf("compared %d bitmap-depth replies vs Redis 3.2", d.n)
}

// --- Dimension S: rejected-command contract (proxy-only) ----------------------

// proxyRejects asserts the PROXY replies an error to args (a command redimos deliberately
// does not implement) and that the connection remains usable afterwards. It is not a
// differential: Redis 3.2 would execute (or block on) these, so only the proxy is checked.
func (d *differ) proxyRejects(desc string, args ...[]byte) {
	d.n++
	rp := d.p.do(args...)
	if len(rp) == 0 || rp[0] != '-' {
		d.t.Errorf("%s: expected an error reply, got %q", desc, rp)
	}
	if pong := d.p.do(bs("PING")); !bytes.Equal(pong, bs("+PONG\r\n")) {
		d.t.Errorf("%s: connection unusable after rejection, PING=%q", desc, pong)
	}
}

func TestProxyRejectedCommands(t *testing.T) {
	d := newDiffer(t)

	// Ops-only / unbounded keyspace scans.
	d.proxyRejects("KEYS", bs("KEYS"), bs("*"))
	d.proxyRejects("RANDOMKEY", bs("RANDOMKEY"))
	d.proxyRejects("FLUSHDB", bs("FLUSHDB"))
	d.proxyRejects("FLUSHALL", bs("FLUSHALL"))
	// Whole-collection copy / cross-DB moves.
	d.proxyRejects("RENAME", bs("RENAME"), d.k("a"), d.k("b"))
	d.proxyRejects("RENAMENX", bs("RENAMENX"), d.k("a"), d.k("b"))
	d.proxyRejects("MOVE", bs("MOVE"), d.k("a"), bs("1"))
	d.proxyRejects("SORT", bs("SORT"), d.k("a"))
	// Serialization / introspection.
	d.proxyRejects("DUMP", bs("DUMP"), d.k("a"))
	d.proxyRejects("OBJECT", bs("OBJECT"), bs("ENCODING"), d.k("a"))
	// Transactions.
	d.proxyRejects("MULTI", bs("MULTI"))
	d.proxyRejects("EXEC", bs("EXEC"))
	d.proxyRejects("DISCARD", bs("DISCARD"))
	d.proxyRejects("WATCH", bs("WATCH"), d.k("a"))
	// Pub/Sub.
	d.proxyRejects("SUBSCRIBE", bs("SUBSCRIBE"), bs("ch"))
	d.proxyRejects("PUBLISH", bs("PUBLISH"), bs("ch"), bs("msg"))
	d.proxyRejects("PSUBSCRIBE", bs("PSUBSCRIBE"), bs("ch*"))
	// Blocking list ops (must NOT block — reject immediately).
	d.proxyRejects("BLPOP", bs("BLPOP"), d.k("a"), bs("1"))
	d.proxyRejects("BRPOP", bs("BRPOP"), d.k("a"), bs("1"))
	d.proxyRejects("BRPOPLPUSH", bs("BRPOPLPUSH"), d.k("a"), d.k("b"), bs("1"))
	// Scripting.
	d.proxyRejects("EVAL", bs("EVAL"), bs("return 1"), bs("0"))
	d.proxyRejects("SCRIPT", bs("SCRIPT"), bs("LOAD"), bs("return 1"))

	t.Logf("verified %d rejected-command replies (proxy stays alive)", d.n)
}
