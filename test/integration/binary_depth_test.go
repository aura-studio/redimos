package integration

import (
	"fmt"
	"testing"
)

// Dimension XCUT: binary/byte-safety and error-text-style parity, DEEPENED.
//
// The existing charset_test.go proves the proxy ROUND-TRIPS arbitrary bytes to
// itself. What it never does is push those bytes through the DIFFERENTIAL harness
// (proxy-vs-Redis-3.2) and, critically, through the unordered-collection replies
// (SMEMBERS/HGETALL/HKEYS/HVALS/SUNION/SSCAN...) whose harness comparison routes
// through respArrayElements -> string() -> sort.Strings(). If either side mangled
// a high byte (0x80-0xff), an embedded NUL (0x00) or an embedded CRLF, the sorted
// multiset would diverge. These tests deliberately place non-UTF8 bytes in EVERY
// position (key name, hash field, hash value, set member, zset member, list
// element, string value, and SCAN MATCH pattern) and compare byte-for-byte with a
// live Redis 3.2. They also pin the redimos-specific error-text special cases
// (HMSET arity, invalid SCAN/*SCAN cursor) against Redis so a drifted string is
// caught.
//
// Only via-redimo commands the proxy actually registers are used.

// binPayloads is the adversarial byte-set exercised as key/field/member/value:
// individual high bytes, NUL, the DEL byte, embedded CRLF (would break a naive
// RESP codec), a RESP-injection lookalike, and a full 0x00..0xff run. Members are
// chosen so no two are equal AND their byte-sorted order differs from any
// accidental UTF-8-interpreted order, so eqSorted genuinely validates byte-safety.
func binPayloads() [][]byte {
	all := make([]byte, 256)
	for i := range all {
		all[i] = byte(i)
	}
	return [][]byte{
		{0x00},                         // NUL alone
		{0x01},                         // low control
		{0x00, 0x01},                   // NUL then 0x01
		{0x7f},                         // DEL
		{0x80},                         // first high byte
		{0x81},                         // high byte
		{0xfe},                         // high byte
		{0xff},                         // last byte
		{0x80, 0x00, 0x81},             // high-NUL-high
		[]byte("a\x00b"),               // embedded NUL between ASCII
		[]byte("a\r\nb"),               // embedded CRLF
		[]byte("\r\n"),                 // bare CRLF
		[]byte("$5\r\nhello\r\n"),      // RESP-injection lookalike
		[]byte("\xff\xfe\xfd\x00\x01"), // descending high bytes then low
		all,                            // every byte value 0x00..0xff
	}
}

// TestDiffBinary_SetMembers puts high/NUL/CRLF bytes into set members and compares
// SADD counts, SMEMBERS (unordered -> eqSorted), SISMEMBER (byte-exact), SCARD, and
// SREM against Redis 3.2. GAP 1 / GAP 6: SMEMBERS binary-safety through the sorted
// multiset comparison; a mangled high byte on either side diverges.
func TestDiffBinary_SetMembers(t *testing.T) {
	d := newDiffer(t)
	k := d.k("binset")

	args := [][]byte{bs("SADD"), k}
	for _, p := range binPayloads() {
		args = append(args, p)
	}
	d.eq("SADD binary members (count)", args...)
	d.eq("SCARD binary set", bs("SCARD"), k)
	d.eqSorted("SMEMBERS binary set", bs("SMEMBERS"), k)

	// SISMEMBER of each exact binary member must be :1 on both sides (byte-exact).
	for i, p := range binPayloads() {
		d.eq(fmt.Sprintf("SISMEMBER binary member#%d", i), bs("SISMEMBER"), k, p)
	}
	// A near-miss (0x7f vs 0x80) must be :0 on both sides.
	d.eq("SISMEMBER 0x7e absent", bs("SISMEMBER"), k, bs("\x7e"))

	// Remove a high-byte member and re-observe: SREM count and residual SMEMBERS.
	d.eq("SREM 0xff", bs("SREM"), k, bs("\xff"))
	d.eq("SREM 0xff again (0)", bs("SREM"), k, bs("\xff"))
	d.eq("SCARD after SREM", bs("SCARD"), k)
	d.eqSorted("SMEMBERS after SREM", bs("SMEMBERS"), k)

	t.Logf("compared %d binary set-member replies vs Redis 3.2", d.n)
}

// TestDiffBinary_SetAlgebra puts binary members into two sets and compares the
// unordered set-algebra replies SUNION/SINTER/SDIFF, plus a SUNIONSTORE followed by
// SMEMBERS of the destination. GAP 3 (respArrayElements binary round-trip) via the
// multi-element unordered replies.
func TestDiffBinary_SetAlgebra(t *testing.T) {
	d := newDiffer(t)
	a := d.k("binsetA")
	b := d.k("binsetB")
	dst := d.k("binsetDST")

	d.eq("SADD A", bs("SADD"), a, bs("\x80"), bs("\x00"), bs("a\r\nb"), bs("shared\xff"))
	d.eq("SADD B", bs("SADD"), b, bs("\x81"), bs("\x00"), bs("shared\xff"), bs("b\x00c"))

	d.eqSorted("SUNION binary", bs("SUNION"), a, b)
	d.eqSorted("SINTER binary", bs("SINTER"), a, b)
	d.eqSorted("SDIFF binary A-B", bs("SDIFF"), a, b)
	d.eqSorted("SDIFF binary B-A", bs("SDIFF"), b, a)

	d.eq("SUNIONSTORE binary (count)", bs("SUNIONSTORE"), dst, a, b)
	d.eqSorted("SMEMBERS of SUNIONSTORE dst", bs("SMEMBERS"), dst)
	d.eq("SINTERSTORE binary (count)", bs("SINTERSTORE"), dst, a, b)
	d.eqSorted("SMEMBERS of SINTERSTORE dst", bs("SMEMBERS"), dst)

	t.Logf("compared %d binary set-algebra replies vs Redis 3.2", d.n)
}

// TestDiffBinary_HashFieldsValues puts binary bytes into BOTH hash fields and hash
// values and compares HGETALL/HKEYS/HVALS (unordered -> eqSorted) plus HGET (exact)
// and HMGET (ordered array, exact). GAP 3: respArrayElements flattens field/value
// pairs; a mangled high byte or NUL in either position diverges under eqSorted.
func TestDiffBinary_HashFieldsValues(t *testing.T) {
	d := newDiffer(t)
	k := d.k("binhash")

	// Build one HMSET with binary fields AND binary values.
	args := [][]byte{bs("HMSET"), k}
	ps := binPayloads()
	for i, p := range ps {
		// field = payload; value = a different payload so field!=value.
		field := p
		value := ps[(i+1)%len(ps)]
		args = append(args, field, value)
	}
	d.eq("HMSET binary fields+values", args...)
	d.eq("HLEN binary hash", bs("HLEN"), k)
	d.eqSorted("HKEYS binary", bs("HKEYS"), k)
	d.eqSorted("HVALS binary", bs("HVALS"), k)
	d.eqSorted("HGETALL binary (flattened pairs)", bs("HGETALL"), k)

	// HGET of each exact binary field returns the exact binary value (byte-exact).
	for i, p := range ps {
		d.eq(fmt.Sprintf("HGET binary field#%d", i), bs("HGET"), k, p)
		d.eq(fmt.Sprintf("HEXISTS binary field#%d", i), bs("HEXISTS"), k, p)
	}
	d.eq("HGET missing high byte", bs("HGET"), k, bs("\x7d"))

	// HMGET preserves request order, so the ordered array must be byte-identical.
	d.eq("HMGET binary fields ordered",
		bs("HMGET"), k, ps[4], ps[0], bs("\x7d"), ps[7])

	// HDEL a binary field and re-observe.
	d.eq("HDEL binary field 0x80", bs("HDEL"), k, bs("\x80"))
	d.eq("HDEL binary field 0x80 again (0)", bs("HDEL"), k, bs("\x80"))
	d.eq("HLEN after HDEL", bs("HLEN"), k)

	t.Logf("compared %d binary hash replies vs Redis 3.2", d.n)
}

// TestDiffBinary_ZSetMembers puts binary bytes into zset members (all at the same
// score, so ordering is by member byte value) and compares ZRANGE WITHSCORES
// (ordered, byte-exact — this is the strongest test of lexical byte ordering),
// ZSCORE (exact), ZRANK (exact), and ZREM. GAP 1: byte-value lexical ordering
// 0x00 < 0x01 < ... < 0x80 < ... < 0xff must be identical on both sides.
func TestDiffBinary_ZSetMembers(t *testing.T) {
	d := newDiffer(t)
	k := d.k("binzset")

	// Distinct single-byte members at equal score -> Redis orders by member bytes.
	members := [][]byte{{0x00}, {0x01}, {0x7f}, {0x80}, {0x81}, {0xfe}, {0xff}}
	args := [][]byte{bs("ZADD"), k}
	for _, m := range members {
		args = append(args, bs("5"), m)
	}
	d.eq("ZADD equal-score binary members", args...)
	d.eq("ZCARD binary zset", bs("ZCARD"), k)
	// Ordered range reply: byte-exact tie-break ordering by member value.
	d.eq("ZRANGE 0 -1 WITHSCORES (byte order)", bs("ZRANGE"), k, bs("0"), bs("-1"), bs("WITHSCORES"))
	d.eq("ZREVRANGE 0 -1 (reverse byte order)", bs("ZREVRANGE"), k, bs("0"), bs("-1"))

	for i, m := range members {
		d.eq(fmt.Sprintf("ZSCORE binary member#%d", i), bs("ZSCORE"), k, m)
		d.eq(fmt.Sprintf("ZRANK binary member#%d", i), bs("ZRANK"), k, m)
	}
	d.eq("ZSCORE missing 0x7e", bs("ZSCORE"), k, bs("\x7e"))

	// Members with embedded NUL / CRLF, distinct scores -> score ordering.
	k2 := d.k("binzset2")
	d.eq("ZADD nul member", bs("ZADD"), k2, bs("1"), bs("a\x00b"))
	d.eq("ZADD crlf member", bs("ZADD"), k2, bs("2"), bs("a\r\nb"))
	d.eq("ZADD high member", bs("ZADD"), k2, bs("3"), bs("\xff\xfe"))
	d.eq("ZRANGE k2 0 -1 WITHSCORES", bs("ZRANGE"), k2, bs("0"), bs("-1"), bs("WITHSCORES"))
	d.eq("ZSCORE a\\x00b", bs("ZSCORE"), k2, bs("a\x00b"))
	d.eq("ZREM crlf member", bs("ZREM"), k2, bs("a\r\nb"))
	d.eq("ZRANGE k2 after ZREM", bs("ZRANGE"), k2, bs("0"), bs("-1"))

	t.Logf("compared %d binary zset replies vs Redis 3.2", d.n)
}

// TestDiffBinary_ListElements puts binary bytes into list elements and compares
// LRANGE (ordered, byte-exact), LINDEX (exact), and LREM. List order is
// insertion-defined so LRANGE must be byte-identical.
func TestDiffBinary_ListElements(t *testing.T) {
	d := newDiffer(t)
	k := d.k("binlist")

	ps := binPayloads()
	args := [][]byte{bs("RPUSH"), k}
	for _, p := range ps {
		args = append(args, p)
	}
	d.eq("RPUSH binary elements (len)", args...)
	d.eq("LLEN binary list", bs("LLEN"), k)
	d.eq("LRANGE 0 -1 binary (ordered)", bs("LRANGE"), k, bs("0"), bs("-1"))

	for i := range ps {
		d.eq(fmt.Sprintf("LINDEX %d binary", i), bs("LINDEX"), k, bs(fmt.Sprintf("%d", i)))
	}
	d.eq("LINDEX -1 binary", bs("LINDEX"), k, bs("-1"))

	// LREM of a high-byte element (unique) removes exactly one.
	d.eq("LREM 0 0xff", bs("LREM"), k, bs("0"), bs("\xff"))
	d.eq("LLEN after LREM", bs("LLEN"), k)
	d.eq("LRANGE after LREM", bs("LRANGE"), k, bs("0"), bs("-1"))

	// LSET a binary element into place, then read it back.
	d.eq("LSET 0 binary", bs("LSET"), k, bs("0"), bs("\x80\x00\x80"))
	d.eq("LINDEX 0 after LSET", bs("LINDEX"), k, bs("0"))

	t.Logf("compared %d binary list replies vs Redis 3.2", d.n)
}

// TestDiffBinary_StringValueAndKey exercises binary bytes as the STRING VALUE and as
// the KEY NAME through GET/GETSET/APPEND/STRLEN/GETRANGE/SETRANGE. GAP: value and key
// binary-safety through the string family, compared byte-for-byte with Redis 3.2.
func TestDiffBinary_StringValueAndKey(t *testing.T) {
	d := newDiffer(t)

	for i, p := range binPayloads() {
		k := d.k(fmt.Sprintf("binstrval%d", i))
		d.eq(fmt.Sprintf("SET binary value#%d", i), bs("SET"), k, p)
		d.eq(fmt.Sprintf("GET binary value#%d", i), bs("GET"), k)
		d.eq(fmt.Sprintf("STRLEN binary value#%d", i), bs("STRLEN"), k)
	}

	// Binary bytes embedded in the KEY NAME itself.
	for i, p := range binPayloads() {
		if len(p) == 0 {
			continue
		}
		bk := append(append([]byte(nil), d.k(fmt.Sprintf("bk%d", i))...), p...)
		d.eq(fmt.Sprintf("SET on binary key#%d", i), bs("SET"), bk, bs("v"))
		d.eq(fmt.Sprintf("GET on binary key#%d", i), bs("GET"), bk)
		d.eq(fmt.Sprintf("EXISTS binary key#%d", i), bs("EXISTS"), bk)
		d.eq(fmt.Sprintf("TYPE binary key#%d", i), bs("TYPE"), bk)
	}

	// GETSET returns the old binary value, then GET the new binary value.
	gk := d.k("bingetset")
	d.eq("SET old binary", bs("SET"), gk, bs("old\x80\x00"))
	d.eq("GETSET binary", bs("GETSET"), gk, bs("new\xff\r\n"))
	d.eq("GET after GETSET", bs("GET"), gk)

	// APPEND onto a binary value and re-read.
	ak := d.k("binappend")
	d.eq("SET base binary", bs("SET"), ak, bs("\x00\x01"))
	d.eq("APPEND binary", bs("APPEND"), ak, bs("\xfe\xff"))
	d.eq("GET after APPEND", bs("GET"), ak)
	d.eq("STRLEN after APPEND", bs("STRLEN"), ak)

	// GETRANGE / SETRANGE over binary content (byte offsets, not rune offsets).
	rk := d.k("binrange")
	d.eq("SET range base", bs("SET"), rk, bs("\x00\x80\xff\x01\x7f"))
	d.eq("GETRANGE 1 3 binary", bs("GETRANGE"), rk, bs("1"), bs("3"))
	d.eq("GETRANGE 0 -1 binary", bs("GETRANGE"), rk, bs("0"), bs("-1"))
	d.eq("GETRANGE -2 -1 binary", bs("GETRANGE"), rk, bs("-2"), bs("-1"))
	d.eq("SETRANGE 1 binary", bs("SETRANGE"), rk, bs("1"), bs("\xaa\xbb"))
	d.eq("GET after SETRANGE", bs("GET"), rk)

	t.Logf("compared %d binary string/key replies vs Redis 3.2", d.n)
}

// TestDiffBinary_ScanMatchPatterns is GAP 2: SCAN-family MATCH patterns are never
// tested with binary payloads. Redis' stringmatchlen is byte-exact — pattern
// '*\x80*' matches 'a\x80b' but not 'a\x7fb'; class '[\x80-\xff]' matches raw high
// bytes. The proxy's glob.go is a direct port, but that agreement is never
// differentially verified. These SADD binary members then SSCAN ... MATCH <binary
// glob> and compare the MATCHED MEMBER MULTISET against Redis 3.2.
//
// The comparison MUST route through scanMatchEq (which drives scanAll to completion
// on both endpoints and compares the sorted matched-member multisets). A SCAN reply
// is the nested [cursor, [members...]] shape, so eqSorted cannot be used: it decodes
// only the outer array via respArrayElements, which rejects the inner array element
// as a non-bulk reply, and the opaque per-endpoint cursor makes the raw replies
// differ so the byte-equal short-circuit never fires. scanMatchEq also owns the
// COUNT trailing arg, so only the base command and the pattern are passed here.
func TestDiffBinary_ScanMatchPatterns(t *testing.T) {
	d := newDiffer(t)

	// SSCAN MATCH with binary glob patterns on a set of binary members.
	sk := d.k("scanmatchset")
	d.eq("SADD scan members", bs("SADD"), sk,
		bs("a\x80b"), bs("a\x7fb"), bs("a\xffz"), bs("b\x00c"), bs("plainASCII"), bs("a\x81b"))

	sbase := [][]byte{bs("SSCAN"), sk}
	// '*\x80*' matches only a\x80b.
	d.scanMatchEq("SSCAN MATCH *\\x80*", sbase, "*\x80*")
	// '*\x7f*' matches only a\x7fb (must NOT match a\x80b).
	d.scanMatchEq("SSCAN MATCH *\\x7f*", sbase, "*\x7f*")
	// class [\x80-\xff]: 'a[\x80-\xff]b' matches a\x80b and a\x81b but not a\x7fb.
	d.scanMatchEq("SSCAN MATCH a[\\x80-\\xff]b", sbase, "a[\x80-\xff]b")
	// '?\x00?' — '?' is one byte, matches b\x00c.
	d.scanMatchEq("SSCAN MATCH ?\\x00?", sbase, "?\x00?")
	// '*' matches everything.
	d.scanMatchEq("SSCAN MATCH *", sbase, "*")
	// A pattern matching nothing -> empty matched set on both sides.
	d.scanMatchEq("SSCAN MATCH nomatch\\xee", sbase, "nomatch\xee")

	// HSCAN MATCH with binary field-name patterns. scanAll flattens [field, value,
	// ...] identically on both sides, so the sorted multiset agrees iff the matched
	// field set agrees.
	hk := d.k("scanmatchhash")
	// Populate with HMSET, not multi-field HSET: Redis 3.2's HSET takes exactly ONE
	// field/value pair (multi-field HSET is a Redis 4.0 addition that redimos supports
	// as a deliberate superset), so a multi-field HSET would arity-error on the oracle,
	// leave the hash uncreated there, and make every later HSCAN compare [] vs the
	// proxy's fields. HMSET has taken multiple pairs since 2.0 and behaves identically
	// on both endpoints.
	d.eq("HMSET scan fields", bs("HMSET"), hk,
		bs("f\x80"), bs("v1"), bs("f\x7f"), bs("v2"), bs("g\xff"), bs("v3"))
	hbase := [][]byte{bs("HSCAN"), hk}
	d.scanMatchEq("HSCAN MATCH f\\x80", hbase, "f\x80")
	d.scanMatchEq("HSCAN MATCH f?", hbase, "f?")
	d.scanMatchEq("HSCAN MATCH *", hbase, "*")

	// ZSCAN MATCH with binary member patterns. ZSCAN returns [member, score, ...].
	zk := d.k("scanmatchzset")
	d.eq("ZADD scan members", bs("ZADD"), zk,
		bs("1"), bs("m\x80"), bs("2"), bs("m\x7f"), bs("3"), bs("n\xff"))
	zbase := [][]byte{bs("ZSCAN"), zk}
	d.scanMatchEq("ZSCAN MATCH m[\\x80-\\xff]", zbase, "m[\x80-\xff]")
	d.scanMatchEq("ZSCAN MATCH *", zbase, "*")

	t.Logf("compared %d binary SCAN-family MATCH replies vs Redis 3.2", d.n)
}

// TestDiffBinary_ErrTextCursor is GAP 4: an invalid / mangled SCAN-family cursor must
// reply the exact Redis 3.2 text "-ERR invalid cursor". A non-numeric cursor is the
// canonical case (it never parses to a uint64 on either side). Covers SCAN, SSCAN,
// HSCAN, ZSCAN.
func TestDiffBinary_ErrTextCursor(t *testing.T) {
	d := newDiffer(t)

	// Seed a set/hash/zset so *SCAN reaches the cursor-parse path (an absent key
	// short-circuits some implementations before the cursor is validated in Redis;
	// non-numeric cursors are rejected regardless, but seed anyway for parity).
	sk := d.k("curset")
	hk := d.k("curhash")
	zk := d.k("curzset")
	d.eq("seed set", bs("SADD"), sk, bs("m"))
	d.eq("seed hash", bs("HSET"), hk, bs("f"), bs("v"))
	d.eq("seed zset", bs("ZADD"), zk, bs("1"), bs("m"))

	// Non-numeric cursor -> "-ERR invalid cursor".
	d.eq("SCAN invalid cursor (garbage)", bs("SCAN"), bs("notacursor"))
	d.eq("SSCAN invalid cursor (garbage)", bs("SSCAN"), sk, bs("notacursor"))
	d.eq("HSCAN invalid cursor (garbage)", bs("HSCAN"), hk, bs("notacursor"))
	d.eq("ZSCAN invalid cursor (garbage)", bs("ZSCAN"), zk, bs("notacursor"))

	// A cursor that overflows uint64 also fails to parse -> invalid cursor.
	huge := bs("99999999999999999999999999")
	d.eq("SCAN cursor overflow", bs("SCAN"), huge)
	d.eq("SSCAN cursor overflow", bs("SSCAN"), sk, huge)
	d.eq("HSCAN cursor overflow", bs("HSCAN"), hk, huge)
	d.eq("ZSCAN cursor overflow", bs("ZSCAN"), zk, huge)

	// A cursor with binary bytes never parses either.
	d.eq("SCAN cursor binary", bs("SCAN"), bs("\x80\x00"))

	t.Logf("compared %d invalid-cursor error replies vs Redis 3.2", d.n)
}

// TestDiffBinary_ErrTextHMSetArity is GAP 5: HMSET with an odd number of field/value
// arguments returns the non-standard, un-lowercased literal "-ERR wrong number of
// arguments for HMSET" (uppercase HMSET, no 'command' suffix). Verify it matches
// Redis 3.2 byte-for-byte.
func TestDiffBinary_ErrTextHMSetArity(t *testing.T) {
	d := newDiffer(t)
	k := d.k("hmsetarity")

	// Odd trailing arg (field with no value) -> HMSET-specific arity error.
	d.eq("HMSET odd (1 pair + dangling field)", bs("HMSET"), k, bs("f1"), bs("v1"), bs("f2"))
	d.eq("HMSET odd (single dangling field)", bs("HMSET"), k, bs("f1"))
	d.eq("HMSET odd (three dangling)", bs("HMSET"), k, bs("f1"), bs("v1"), bs("f2"), bs("v2"), bs("f3"))

	// Below minimum arity (-4 means >= 4 total args): HMSET with just a key.
	d.eq("HMSET below arity (key only)", bs("HMSET"), k)

	t.Logf("compared %d HMSET arity error replies vs Redis 3.2", d.n)
}

// TestDiffBinary_ErrTextWrongType exercises WRONGTYPE parity when the KEY NAME and/or
// the offending member/field contain binary bytes, across each collection family.
// The error text must not depend on the payload bytes, and must match Redis 3.2.
func TestDiffBinary_ErrTextWrongType(t *testing.T) {
	d := newDiffer(t)

	// A string key whose NAME contains binary bytes.
	bk := append(append([]byte(nil), d.k("wtbinkey")...), []byte("\x80\x00\xff")...)
	d.eq("seed binary-named string", bs("SET"), bk, bs("v"))

	d.eq("LPUSH on binary-named string", bs("LPUSH"), bk, bs("\x80"))
	d.eq("SADD on binary-named string", bs("SADD"), bk, bs("\xff"))
	d.eq("HSET on binary-named string", bs("HSET"), bk, bs("f\x00"), bs("v"))
	d.eq("ZADD on binary-named string", bs("ZADD"), bk, bs("1"), bs("m\xff"))
	d.eq("SSCAN on binary-named string", bs("SSCAN"), bk, bs("0"))
	d.eq("HGETALL on binary-named string", bs("HGETALL"), bk)

	// A list key -> string ops give WRONGTYPE regardless of binary element.
	lk := d.k("wtlist")
	d.eq("seed list w/ binary element", bs("RPUSH"), lk, bs("\x00\x80"))
	d.eq("GET on list", bs("GET"), lk)
	d.eq("APPEND on list", bs("APPEND"), lk, bs("\xff"))
	d.eq("INCR on list", bs("INCR"), lk)
	d.eq("GETRANGE on list", bs("GETRANGE"), lk, bs("0"), bs("-1"))

	t.Logf("compared %d binary-payload WRONGTYPE replies vs Redis 3.2", d.n)
}

// TestDiffBinary_EmptyAndDeleteOnEmpty exercises binary members on the delete-on-empty
// path: removing the last binary member of a set/zset/hash must delete the key so a
// subsequent EXISTS/TYPE matches Redis 3.2. Also covers the empty-string member/value.
func TestDiffBinary_EmptyAndDeleteOnEmpty(t *testing.T) {
	d := newDiffer(t)

	// Empty-string set member round-trips and is distinct from an absent member.
	sk := d.k("emptymember")
	d.eq("SADD empty member", bs("SADD"), sk, bs(""))
	d.eq("SISMEMBER empty", bs("SISMEMBER"), sk, bs(""))
	d.eq("SCARD w/ empty member", bs("SCARD"), sk)
	d.eqSorted("SMEMBERS w/ empty member", bs("SMEMBERS"), sk)
	// Removing the sole (empty) member empties -> key must be gone.
	d.eq("SREM empty (last)", bs("SREM"), sk, bs(""))
	d.eq("EXISTS after emptying set", bs("EXISTS"), sk)
	d.eq("TYPE after emptying set", bs("TYPE"), sk)
	d.eqSorted("SMEMBERS after emptying set", bs("SMEMBERS"), sk)

	// zset: remove the last binary member -> key gone.
	zk := d.k("zdelempty")
	d.eq("ZADD single high member", bs("ZADD"), zk, bs("1"), bs("\xff\x00"))
	d.eq("ZREM last binary member", bs("ZREM"), zk, bs("\xff\x00"))
	d.eq("EXISTS after emptying zset", bs("EXISTS"), zk)
	d.eq("ZCARD after emptying zset", bs("ZCARD"), zk)

	// hash: delete the last binary field -> key gone.
	hk := d.k("hdelempty")
	d.eq("HSET single binary field", bs("HSET"), hk, bs("f\x80"), bs("v\x00"))
	d.eq("HDEL last binary field", bs("HDEL"), hk, bs("f\x80"))
	d.eq("EXISTS after emptying hash", bs("EXISTS"), hk)
	d.eq("HLEN after emptying hash", bs("HLEN"), hk)

	// list: pop the last binary element -> key gone.
	lk := d.k("ldelempty")
	d.eq("RPUSH single binary element", bs("RPUSH"), lk, bs("\x00\xff"))
	d.eq("LPOP last binary element", bs("LPOP"), lk)
	d.eq("EXISTS after emptying list", bs("EXISTS"), lk)
	d.eq("LLEN after emptying list", bs("LLEN"), lk)

	// Empty-string VALUE for a string key.
	vk := d.k("emptyval")
	d.eq("SET empty value", bs("SET"), vk, bs(""))
	d.eq("GET empty value", bs("GET"), vk)
	d.eq("STRLEN empty value", bs("STRLEN"), vk)
	d.eq("EXISTS empty-value key", bs("EXISTS"), vk)

	t.Logf("compared %d binary delete-on-empty replies vs Redis 3.2", d.n)
}
