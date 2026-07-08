package integration

import "testing"

// Dimension C (depth): reply-SHAPE parity for the states shape_test.go left untested — a
// PRESENT collection queried for a MISSING member/field, and the SCAN-family's exact
// two-element structure on an absent key. Redis 3.2 is precise about the RESP2 shape it
// picks here (null bulk $-1 vs empty array *0 vs a bulk-per-request array), and a client
// that switch()es on the reply type breaks if the proxy substitutes a different shape.
// Every case below drives the exact input through BOTH endpoints and byte-compares.

// --- GAP 1: SPOP / SRANDMEMBER count-form on an ABSENT key ---
//
// With a count argument these switch from the scalar (null-bulk on absent) form to the
// array form: Redis 3.2 returns *0 (a non-null empty array) for SPOP/SRANDMEMBER of a
// missing key, including SRANDMEMBER with a NEGATIVE count (repeat-allowed). The withCount
// path is a different code branch from the no-count path (which shape_test.go covers via
// LPOP/RPOP style scalars only for other commands), so the array-empty shape is verified
// here directly.
func TestDiffShapeSetPopCountAbsent(t *testing.T) {
	d := newDiffer(t)
	miss := d.k("setpop-absent")

	// SPOP key count on a missing key -> *0 (empty array), NOT $-1.
	d.eq("SPOP absent count=5 -> *0", bs("SPOP"), miss, bs("5"))
	d.eq("SPOP absent count=1 -> *0", bs("SPOP"), miss, bs("1"))
	d.eq("SPOP absent count=0 -> *0", bs("SPOP"), miss, bs("0"))

	// SRANDMEMBER key count on a missing key -> *0 for positive, negative, and zero counts.
	d.eq("SRANDMEMBER absent count=3 -> *0", bs("SRANDMEMBER"), miss, bs("3"))
	d.eq("SRANDMEMBER absent count=-3 -> *0", bs("SRANDMEMBER"), miss, bs("-3"))
	d.eq("SRANDMEMBER absent count=0 -> *0", bs("SRANDMEMBER"), miss, bs("0"))

	// The bare no-count form still returns the scalar null-bulk on absent — contrast the
	// shape against the count form above.
	d.eq("SPOP absent no-count -> $-1", bs("SPOP"), miss)
	d.eq("SRANDMEMBER absent no-count -> $-1", bs("SRANDMEMBER"), miss)

	t.Logf("compared %d SPOP/SRANDMEMBER-count absent-key shapes vs Redis 3.2", d.n)
}

// --- GAP 2: ZSCORE / ZRANK / ZREVRANK on a PRESENT zset but a MISSING member ---
//
// shape_test.go only asserts $-1 for these when the KEY is absent. Redis 3.2 also returns
// $-1 (null bulk) when the key EXISTS but the queried member is not in the sorted set —
// a distinct code path (key found, member lookup miss) that must produce the identical
// null-bulk shape, not an empty array or an error.
func TestDiffShapeZScoreRankMissingMember(t *testing.T) {
	d := newDiffer(t)
	zk := d.k("z-present")

	d.eq("seed zset", bs("ZADD"), zk, bs("1.5"), bs("m1"), bs("2.5"), bs("m2"))

	// Present member sanity (should be a real bulk, exercises the found path).
	d.eq("ZSCORE present member", bs("ZSCORE"), zk, bs("m1"))
	d.eq("ZRANK present member", bs("ZRANK"), zk, bs("m1"))
	d.eq("ZREVRANK present member", bs("ZREVRANK"), zk, bs("m1"))

	// Missing member on a present key -> $-1 for all three.
	d.eq("ZSCORE present-key missing-member -> $-1", bs("ZSCORE"), zk, bs("nope"))
	d.eq("ZRANK present-key missing-member -> $-1", bs("ZRANK"), zk, bs("nope"))
	d.eq("ZREVRANK present-key missing-member -> $-1", bs("ZREVRANK"), zk, bs("nope"))

	// Empty-string member and binary member that are absent still take the null-bulk path.
	d.eq("ZSCORE missing empty-member -> $-1", bs("ZSCORE"), zk, bs(""))
	d.eq("ZRANK missing binary-member -> $-1", bs("ZRANK"), zk, bs("a\x00b"))
	d.eq("ZREVRANK missing binary-member -> $-1", bs("ZREVRANK"), zk, bs("\xff\xfe"))

	t.Logf("compared %d ZSCORE/ZRANK present-key/missing-member shapes vs Redis 3.2", d.n)
}

// --- GAP 3: HMGET on a PRESENT hash with MISSING fields (and mixed hit/miss) ---
//
// HMGET returns one array element per requested field: a real bulk when the field exists,
// $-1 when it is missing OR the key is absent. shape_test.go only covers the absent-key
// case; here we verify the per-field null-bulk positioning on a PRESENT hash, including the
// interleaving of hits and misses so the array positions line up byte-for-byte.
func TestDiffShapeHMGetMixedFields(t *testing.T) {
	d := newDiffer(t)
	hk := d.k("h-present")

	// Seed with two single-pair HSETs: Redis 3.2's HSET arity is exactly 4 (a single
	// field-value pair), so a multi-pair `HSET k f1 v1 f2 v2` would arity-error on the
	// oracle (redimos accepts it as a Redis-4.0 superset) and leave the oracle hash
	// unseeded. Single-pair HSET is byte-identical on both endpoints.
	d.eq("seed hash f1", bs("HSET"), hk, bs("f1"), bs("v1"))
	d.eq("seed hash f2", bs("HSET"), hk, bs("f2"), bs("v2"))

	// All missing on a present hash -> [ $-1, $-1 ].
	d.eq("HMGET present-key all-missing", bs("HMGET"), hk, bs("x1"), bs("x2"))

	// Mixed hit/miss/hit -> [ v1, $-1, v2 ]; position of the null bulk matters.
	d.eq("HMGET present-key hit-miss-hit", bs("HMGET"), hk, bs("f1"), bs("gone"), bs("f2"))

	// Miss then hit then miss -> [ $-1, v1, $-1 ].
	d.eq("HMGET present-key miss-hit-miss", bs("HMGET"), hk, bs("gone"), bs("f1"), bs("also-gone"))

	// Duplicate requested field (both hits) -> [ v1, v1 ].
	d.eq("HMGET present-key duplicate-hit", bs("HMGET"), hk, bs("f1"), bs("f1"))

	// Empty-string field name (missing) -> $-1 in that slot.
	d.eq("HMGET present-key empty-field-miss", bs("HMGET"), hk, bs(""), bs("f1"))

	// Binary field name (missing) -> $-1 in that slot.
	d.eq("HMGET present-key binary-field-miss", bs("HMGET"), hk, bs("\x00\x01"), bs("f2"))

	t.Logf("compared %d HMGET present-key mixed-field shapes vs Redis 3.2", d.n)
}

// --- GAP 4: SCAN-family reply STRUCTURE on an ABSENT key ---
//
// HSCAN/SSCAN/ZSCAN of a missing key return the two-element array [cursor, elements] where
// cursor is the bulk string "0" and elements is a NON-null empty array *0 (never a null
// array *-1). This nested shape is what a paging client relies on to terminate cleanly; a
// substituted null array or a missing outer element breaks the loop. Byte-comparing the
// whole reply verifies both the outer 2-element frame and the inner empty *0.
func TestDiffShapeScanFamilyAbsent(t *testing.T) {
	d := newDiffer(t)
	miss := d.k("scan-absent")

	// Cursor "0" starts (and, on a missing key, ends) a fresh scan.
	d.eq("HSCAN absent 0 -> [\"0\", *0]", bs("HSCAN"), miss, bs("0"))
	d.eq("SSCAN absent 0 -> [\"0\", *0]", bs("SSCAN"), miss, bs("0"))
	d.eq("ZSCAN absent 0 -> [\"0\", *0]", bs("ZSCAN"), miss, bs("0"))

	// Same absent shape with an explicit COUNT (does not change the empty result frame).
	d.eq("HSCAN absent 0 COUNT 10", bs("HSCAN"), miss, bs("0"), bs("COUNT"), bs("10"))
	d.eq("SSCAN absent 0 COUNT 10", bs("SSCAN"), miss, bs("0"), bs("COUNT"), bs("10"))
	d.eq("ZSCAN absent 0 COUNT 10", bs("ZSCAN"), miss, bs("0"), bs("COUNT"), bs("10"))

	// With a MATCH that no member could satisfy — still the [ "0", *0 ] frame on absent key.
	d.eq("HSCAN absent 0 MATCH nope*", bs("HSCAN"), miss, bs("0"), bs("MATCH"), bs("nope*"))
	d.eq("SSCAN absent 0 MATCH nope*", bs("SSCAN"), miss, bs("0"), bs("MATCH"), bs("nope*"))
	d.eq("ZSCAN absent 0 MATCH nope*", bs("ZSCAN"), miss, bs("0"), bs("MATCH"), bs("nope*"))

	t.Logf("compared %d SCAN-family absent-key reply structures vs Redis 3.2", d.n)
}

// --- GAP 5: ZRANGE / ZREVRANGE WITHSCORES producing an EMPTY result ---
//
// When the matched set is empty, Redis 3.2 returns *0 (empty array) REGARDLESS of the
// WITHSCORES flag — it does NOT emit a null array and does not vary the shape by flag. Two
// empty-result triggers are exercised: an absent key, and a present zset with an
// out-of-range index window. The WITHSCORES form is a separate reply-building branch
// (member+score pairs) whose empty case must still collapse to a bare *0.
func TestDiffShapeZRangeEmptyWithScores(t *testing.T) {
	d := newDiffer(t)
	miss := d.k("zrange-absent")
	zk := d.k("zrange-present")

	// Absent key, WITHSCORES -> *0 for both range directions.
	d.eq("ZRANGE absent 0 -1 WITHSCORES -> *0", bs("ZRANGE"), miss, bs("0"), bs("-1"), bs("WITHSCORES"))
	d.eq("ZREVRANGE absent 0 -1 WITHSCORES -> *0", bs("ZREVRANGE"), miss, bs("0"), bs("-1"), bs("WITHSCORES"))

	// Present zset with a single member; query an index window that matches nothing.
	d.eq("seed single-member zset", bs("ZADD"), zk, bs("1"), bs("m1"))
	d.eq("ZRANGE present out-of-range WITHSCORES -> *0", bs("ZRANGE"), zk, bs("10"), bs("20"), bs("WITHSCORES"))
	d.eq("ZREVRANGE present out-of-range WITHSCORES -> *0", bs("ZREVRANGE"), zk, bs("10"), bs("20"), bs("WITHSCORES"))

	// start > stop after negative-index normalization also yields the empty frame.
	d.eq("ZRANGE present start>stop WITHSCORES -> *0", bs("ZRANGE"), zk, bs("-1"), bs("-2"), bs("WITHSCORES"))
	d.eq("ZREVRANGE present start>stop WITHSCORES -> *0", bs("ZREVRANGE"), zk, bs("-1"), bs("-2"), bs("WITHSCORES"))

	// The same empty windows WITHOUT scores must produce the identical *0 shape — verify
	// the flag does not change the empty-case shape on either side.
	d.eq("ZRANGE present out-of-range no-scores -> *0", bs("ZRANGE"), zk, bs("10"), bs("20"))
	d.eq("ZREVRANGE present out-of-range no-scores -> *0", bs("ZREVRANGE"), zk, bs("10"), bs("20"))

	t.Logf("compared %d ZRANGE/ZREVRANGE empty-WITHSCORES shapes vs Redis 3.2", d.n)
}
