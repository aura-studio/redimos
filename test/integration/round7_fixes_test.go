package integration

import (
	"strings"
	"testing"
)

// Regression tests for the 2026-07-07 round-7 adversarial pass — a regression audit of
// the round-6 fixes plus store-family / GEO-STORE / list-noop depth. Each byte-diffs
// against the live redis:3.2 oracle. (Platform-error-quality cases — oversized ZADD-flag
// members and oversized keys — legitimately still differ from the oracle and are not
// asserted here.)

// TestFixListNoopBeforeSizeGuard: LINSERT resolves the pivot (and LSET the index) BEFORE
// the value-size guard, so an absent pivot / out-of-range index with an oversized value
// replies :-1 / index-out-of-range, not the size error.
func TestFixListNoopBeforeSizeGuard(t *testing.T) {
	d := newDiffer(t)
	big := bs(strings.Repeat("x", 400*1024))
	k := d.k("l")
	d.eq("RPUSH", bs("RPUSH"), k, bs("a"), bs("b"), bs("c"))
	d.eq("LINSERT absent-pivot oversized -> :-1", bs("LINSERT"), k, bs("BEFORE"), bs("nopivot"), big)
	d.eq("LSET out-of-range oversized -> index error", bs("LSET"), k, bs("9"), big)
	t.Logf("compared %d list-noop replies vs Redis 3.2", d.n)
}

// TestFixBitfieldOrderAndBound: BITFIELD on a wrong-type key replies WRONGTYPE even with
// an over-cap write offset (type check precedes the size guard); and Redis bounds the
// OFFSET alone (< 2^32), so a GET in the last <width> bits below 2^32 reads past the
// value and returns 0.
func TestFixBitfieldOrderAndBound(t *testing.T) {
	d := newDiffer(t)
	wt := d.k("wt")
	d.eq("RPUSH (make list)", bs("RPUSH"), wt, bs("a"))
	d.eq("BITFIELD wrong-type over-cap -> WRONGTYPE", bs("BITFIELD"), wt, bs("SET"), bs("u8"), bs("3200000"), bs("1"))
	d.eq("BITFIELD GET u8 4294967295 -> :0", bs("BITFIELD"), d.k("bf"), bs("GET"), bs("u8"), bs("4294967295"))
	d.eq("BITFIELD GET u63 #68174084 -> :0", bs("BITFIELD"), d.k("bf2"), bs("GET"), bs("u63"), bs("#68174084"))
	t.Logf("compared %d bitfield replies vs Redis 3.2", d.n)
}

// TestFixZStoreTypeCheckOrder: ZUNIONSTORE/ZINTERSTORE type-check the source keys before
// parsing WEIGHTS/AGGREGATE, so a wrong-type source with a bad weight replies WRONGTYPE.
func TestFixZStoreTypeCheckOrder(t *testing.T) {
	d := newDiffer(t)
	s := d.k("s")
	d.eq("SET string source", bs("SET"), s, bs("v"))
	d.eq("ZUNIONSTORE wrong-type src bad weight -> WRONGTYPE", bs("ZUNIONSTORE"), d.k("d1"), bs("1"), s, bs("WEIGHTS"), bs("abc"))
	d.eq("ZINTERSTORE wrong-type src bad weight -> WRONGTYPE", bs("ZINTERSTORE"), d.k("d2"), bs("1"), s, bs("WEIGHTS"), bs("abc"))
	t.Logf("compared %d zstore-order replies vs Redis 3.2", d.n)
}

// TestFixGeoMemberOversizedNoop: GEODIST/GEOPOS/GEOHASH reply nil and GEORADIUSBYMEMBER
// "could not decode requested zset member" for a member too large to be stored.
func TestFixGeoMemberOversizedNoop(t *testing.T) {
	d := newDiffer(t)
	big := bs(strings.Repeat("a", 2000))
	g := d.k("g")
	d.eq("GEOADD seed", bs("GEOADD"), g, bs("13.361389"), bs("38.115556"), bs("a"))
	d.eq("GEODIST oversized -> nil", bs("GEODIST"), g, bs("a"), big)
	d.eqSorted("GEOPOS oversized -> [nil]", bs("GEOPOS"), g, big)
	d.eqSorted("GEOHASH oversized -> [nil]", bs("GEOHASH"), g, big)
	d.eq("GEORADIUSBYMEMBER oversized -> decode error", bs("GEORADIUSBYMEMBER"), g, big, bs("100"), bs("m"))
	t.Logf("compared %d geo-oversized-member replies vs Redis 3.2", d.n)
}

// TestFixGeoStore: GEORADIUS/GEORADIUSBYMEMBER STORE writes the geohash-scored matches to
// a dest zset and replies the count; STOREDIST writes distances; an empty result deletes
// dest and replies 0; STORE is incompatible with WITH* and forbidden on the _RO variants.
func TestFixGeoStore(t *testing.T) {
	d := newDiffer(t)
	src := d.k("src")
	d.eq("GEOADD P", bs("GEOADD"), src, bs("13.361389"), bs("38.115556"), bs("P"))
	d.eq("GEOADD C", bs("GEOADD"), src, bs("15.087269"), bs("37.502669"), bs("C"))

	dst := d.k("dst")
	d.eq("GEORADIUS STORE -> :2", bs("GEORADIUS"), src, bs("15"), bs("37"), bs("200"), bs("km"), bs("STORE"), dst)
	d.eqSorted("STORE dest has geohash scores", bs("ZRANGE"), dst, bs("0"), bs("-1"), bs("WITHSCORES"))
	d.eq("GEORADIUS STORE COUNT 1 -> :1", bs("GEORADIUS"), src, bs("15"), bs("37"), bs("500"), bs("km"), bs("COUNT"), bs("1"), bs("STORE"), d.k("dst2"))
	d.eq("GEORADIUSBYMEMBER STORE", bs("GEORADIUSBYMEMBER"), src, bs("P"), bs("500"), bs("km"), bs("STORE"), d.k("dst3"))

	// Empty result deletes the (pre-existing) dest and replies 0.
	empt := d.k("empt")
	d.eq("ZADD pre-existing dest", bs("ZADD"), empt, bs("1"), bs("pre"))
	d.eq("GEORADIUS empty STORE -> :0", bs("GEORADIUS"), src, bs("80"), bs("80"), bs("1"), bs("km"), bs("STORE"), empt)
	d.eq("EXISTS emptied dest -> :0", bs("EXISTS"), empt)

	// Incompatibilities and read-only restriction.
	d.eq("STORE + WITHCOORD -> incompat", bs("GEORADIUS"), src, bs("13"), bs("38"), bs("500"), bs("km"), bs("WITHCOORD"), bs("STORE"), d.k("x"))
	d.eq("STORE + WITHDIST -> incompat", bs("GEORADIUS"), src, bs("13"), bs("38"), bs("500"), bs("km"), bs("WITHDIST"), bs("STORE"), d.k("x"))
	d.eq("GEORADIUS_RO STORE -> syntax error", bs("GEORADIUS_RO"), src, bs("13"), bs("38"), bs("500"), bs("km"), bs("STORE"), d.k("x"))
	t.Logf("compared %d geo-store replies vs Redis 3.2", d.n)
}

// TestFixClientArityAndCRLF: CLIENT LIST/PAUSE/REPLY gate on argc (wrong arity -> the
// CLIENT syntax error, bad PAUSE timeout / REPLY value -> their specific errors); and an
// unknown-command reply maps CR/LF in the echoed name to spaces (RESP framing safety).
func TestFixClientArityAndCRLF(t *testing.T) {
	d := newDiffer(t)
	d.eq("CLIENT LIST extra -> syntax", bs("CLIENT"), bs("LIST"), bs("extra"))
	d.eq("CLIENT PAUSE bare -> syntax", bs("CLIENT"), bs("PAUSE"))
	d.eq("CLIENT PAUSE abc -> timeout not int", bs("CLIENT"), bs("PAUSE"), bs("abc"))
	d.eq("CLIENT PAUSE 100 -> OK", bs("CLIENT"), bs("PAUSE"), bs("100"))
	d.eq("CLIENT REPLY bare -> syntax", bs("CLIENT"), bs("REPLY"))
	d.eq("CLIENT REPLY xyz -> syntax error", bs("CLIENT"), bs("REPLY"), bs("xyz"))
	d.eq("CLIENT REPLY on -> OK", bs("CLIENT"), bs("REPLY"), bs("on"))
	d.eq("unknown command CRLF sanitized", bs("BAD\r\nCMD"))
	t.Logf("compared %d client/crlf replies vs Redis 3.2", d.n)
}
