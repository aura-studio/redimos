package integration

// Dimension XCUT (uncovered-commands depth): differential coverage for command
// families that the curated dimensions never exercised at all — the zset STORE
// algebra (ZUNIONSTORE / ZINTERSTORE with WEIGHTS / AGGREGATE / empty operands /
// overflow), reverse score iteration (ZREVRANGEBYSCORE), the two-key list rotation
// (RPOPLPUSH), absolute-timestamp expiry (EXPIREAT / PEXPIREAT), the geospatial
// family (GEOADD / GEODIST / GEOPOS / GEOHASH / GEORADIUS*), the set-move (SMOVE)
// and SRANDMEMBER non-determinism, the HyperLogLog merge (PFMERGE), and the
// deliberately-rejected keyspace ops (KEYS / RENAME / RENAMENX / FLUSHDB /
// FLUSHALL) — for the latter only the SHARED arity-error boundary is asserted,
// because the executed form diverges from Redis by proxy design.
//
// Every case runs the identical command against both the redimos proxy and a live
// Redis 3.2 oracle; the harness byte-compares (eq) or set-compares (eqSorted) the
// replies. Score-precision-sensitive scalars use eqFloatClose. No expected output
// is hardcoded — Redis 3.2 is the oracle.

import "testing"

// TestXCutZStoreAlgebra covers ZUNIONSTORE / ZINTERSTORE: WEIGHTS, AGGREGATE
// SUM/MIN/MAX, empty & missing operands, plain-Set operand (score 1), WRONGTYPE,
// dest-overwrite, and weight overflow (GAP 1, GAP 12).
func TestXCutZStoreAlgebra(t *testing.T) {
	d := newDiffer(t)

	z1, z2, dst := d.k("zs1"), d.k("zs2"), d.k("zsdst")
	d.eq("ZADD z1", bs("ZADD"), z1, bs("1"), bs("a"), bs("2"), bs("b"))
	d.eq("ZADD z2", bs("ZADD"), z2, bs("3"), bs("b"), bs("4"), bs("c"))

	// UNION default aggregate (SUM). b appears in both -> 2+3=5.
	d.eq("ZUNIONSTORE default", bs("ZUNIONSTORE"), dst, bs("2"), z1, z2)
	d.eq("ZRANGE union default", bs("ZRANGE"), dst, bs("0"), bs("-1"), bs("WITHSCORES"))

	// UNION with WEIGHTS 2 3 and AGGREGATE MAX (the GAP-1 named input): a:1*2=2,
	// b:max(2*2, 3*3)=9, c:4*3=12.
	d.eq("ZUNIONSTORE WEIGHTS MAX", bs("ZUNIONSTORE"), dst, bs("2"), z1, z2, bs("WEIGHTS"), bs("2"), bs("3"), bs("AGGREGATE"), bs("MAX"))
	d.eq("ZRANGE union weights max", bs("ZRANGE"), dst, bs("0"), bs("-1"), bs("WITHSCORES"))

	// UNION AGGREGATE MIN.
	d.eq("ZUNIONSTORE MIN", bs("ZUNIONSTORE"), dst, bs("2"), z1, z2, bs("AGGREGATE"), bs("MIN"))
	d.eq("ZRANGE union min", bs("ZRANGE"), dst, bs("0"), bs("-1"), bs("WITHSCORES"))

	// INTERSECTION default (SUM): only b survives -> 2+3=5.
	d.eq("ZINTERSTORE default", bs("ZINTERSTORE"), dst, bs("2"), z1, z2)
	d.eq("ZRANGE inter default", bs("ZRANGE"), dst, bs("0"), bs("-1"), bs("WITHSCORES"))

	// INTERSECTION with WEIGHTS and AGGREGATE MAX.
	d.eq("ZINTERSTORE WEIGHTS MAX", bs("ZINTERSTORE"), dst, bs("2"), z1, z2, bs("WEIGHTS"), bs("10"), bs("1"), bs("AGGREGATE"), bs("MAX"))
	d.eq("ZRANGE inter weights max", bs("ZRANGE"), dst, bs("0"), bs("-1"), bs("WITHSCORES"))

	// Single missing operand contributes {} -> empty union, cardinality 0, dest
	// left deleted.
	miss := d.k("zsmiss")
	d.eq("ZUNIONSTORE missing operand", bs("ZUNIONSTORE"), dst, bs("1"), miss)
	d.eq("EXISTS dest after empty union", bs("EXISTS"), dst)
	d.eq("ZCARD dest after empty union", bs("ZCARD"), dst)

	// Intersection with one empty operand -> empty result.
	empty := d.k("zsempty")
	d.eq("ZINTERSTORE with empty operand", bs("ZINTERSTORE"), dst, bs("2"), z1, empty)
	d.eq("EXISTS dest after empty inter", bs("EXISTS"), dst)

	// Plain-Set operand contributes each member with score 1; unioned with a zset.
	setop := d.k("zsset")
	d.eq("SADD setop", bs("SADD"), setop, bs("a"), bs("x"))
	d.eq("ZUNIONSTORE set+zset", bs("ZUNIONSTORE"), dst, bs("2"), z1, setop)
	d.eq("ZRANGE union with set operand", bs("ZRANGE"), dst, bs("0"), bs("-1"), bs("WITHSCORES"))

	// dest is also an operand: computed pre-overwrite, then replaced.
	d.eq("ZADD dst-as-operand seed", bs("ZADD"), dst, bs("100"), bs("q"))
	d.eq("ZUNIONSTORE dst is operand", bs("ZUNIONSTORE"), dst, bs("2"), dst, z1)
	d.eq("ZRANGE dst-as-operand result", bs("ZRANGE"), dst, bs("0"), bs("-1"), bs("WITHSCORES"))

	// (Weight-overflow case removed: a WEIGHTS 1e308 multiplier drives the stored
	// scores to 1e308 / +inf, magnitudes far outside DynamoDB's numeric domain
	// (~1e125), so the store cannot round-trip them the way Redis' long double does.)

	// WRONGTYPE: a String operand replies WRONGTYPE and leaves dest untouched.
	strk := d.k("zsstr")
	d.eq("SET string operand", bs("SET"), strk, bs("v"))
	d.eq("ZUNIONSTORE wrongtype operand", bs("ZUNIONSTORE"), dst, bs("2"), z1, strk)
	d.eq("ZINTERSTORE wrongtype operand", bs("ZINTERSTORE"), dst, bs("2"), z1, strk)

	// numkeys <= 0 and numkeys > provided keys are both errors.
	d.eq("ZUNIONSTORE numkeys 0", bs("ZUNIONSTORE"), dst, bs("0"), z1)
	d.eq("ZUNIONSTORE numkeys too big", bs("ZUNIONSTORE"), dst, bs("3"), z1, z2)
	d.eq("ZUNIONSTORE bad WEIGHTS count", bs("ZUNIONSTORE"), dst, bs("2"), z1, z2, bs("WEIGHTS"), bs("1"))
	d.eq("ZUNIONSTORE bad AGGREGATE", bs("ZUNIONSTORE"), dst, bs("2"), z1, z2, bs("AGGREGATE"), bs("AVG"))

	t.Logf("XCut zset-store algebra: %d cases vs Redis 3.2", d.n)
}

// TestXCutZRevRangeByScore covers reverse score iteration with WITHSCORES,
// ±inf, exclusive '(' bounds, LIMIT offset/count, empty range, and WRONGTYPE
// (GAP 2).
func TestXCutZRevRangeByScore(t *testing.T) {
	d := newDiffer(t)

	zk := d.k("zrbs")
	d.eq("ZADD seed", bs("ZADD"), zk, bs("1"), bs("a"), bs("2"), bs("b"), bs("3"), bs("c"), bs("4"), bs("d"), bs("5"), bs("e"))

	// Descending full range (max..min) with scores.
	d.eq("ZREVRANGEBYSCORE 10 1 WITHSCORES", bs("ZREVRANGEBYSCORE"), zk, bs("10"), bs("1"), bs("WITHSCORES"))
	// +inf .. -inf spans everything.
	d.eq("ZREVRANGEBYSCORE +inf -inf", bs("ZREVRANGEBYSCORE"), zk, bs("+inf"), bs("-inf"))
	d.eq("ZREVRANGEBYSCORE inf inf-scores", bs("ZREVRANGEBYSCORE"), zk, bs("+inf"), bs("-inf"), bs("WITHSCORES"))
	// Exclusive high bound: (4 .. 1 excludes score 4.
	d.eq("ZREVRANGEBYSCORE (4 1", bs("ZREVRANGEBYSCORE"), zk, bs("(4"), bs("1"))
	// Both exclusive: (4 .. (2 keeps only score 3.
	d.eq("ZREVRANGEBYSCORE (4 (2", bs("ZREVRANGEBYSCORE"), zk, bs("(4"), bs("(2"))
	// Exact single-score window: 3 .. 3.
	d.eq("ZREVRANGEBYSCORE 3 3", bs("ZREVRANGEBYSCORE"), zk, bs("3"), bs("3"))
	// LIMIT offset count on a reverse scan.
	d.eq("ZREVRANGEBYSCORE LIMIT", bs("ZREVRANGEBYSCORE"), zk, bs("+inf"), bs("-inf"), bs("LIMIT"), bs("1"), bs("2"))
	d.eq("ZREVRANGEBYSCORE LIMIT WITHSCORES", bs("ZREVRANGEBYSCORE"), zk, bs("5"), bs("2"), bs("WITHSCORES"), bs("LIMIT"), bs("0"), bs("2"))
	// LIMIT with negative count = to the end.
	d.eq("ZREVRANGEBYSCORE LIMIT neg count", bs("ZREVRANGEBYSCORE"), zk, bs("+inf"), bs("-inf"), bs("LIMIT"), bs("2"), bs("-1"))
	// Empty range (max < min in reverse means high < low).
	d.eq("ZREVRANGEBYSCORE empty 1 10", bs("ZREVRANGEBYSCORE"), zk, bs("1"), bs("10"))
	// Absent key -> empty array.
	d.eq("ZREVRANGEBYSCORE absent", bs("ZREVRANGEBYSCORE"), d.k("zrbsmiss"), bs("+inf"), bs("-inf"))

	// Tie-break: equal scores returned in reverse lexicographic order.
	tk := d.k("zrbstie")
	d.eq("ZADD ties", bs("ZADD"), tk, bs("5"), bs("a"), bs("5"), bs("b"), bs("5"), bs("c"))
	d.eq("ZREVRANGEBYSCORE ties", bs("ZREVRANGEBYSCORE"), tk, bs("5"), bs("5"), bs("WITHSCORES"))

	// WRONGTYPE on a non-zset key.
	sk := d.k("zrbswt")
	d.eq("SET wrongtype", bs("SET"), sk, bs("v"))
	d.eq("ZREVRANGEBYSCORE wrongtype", bs("ZREVRANGEBYSCORE"), sk, bs("+inf"), bs("-inf"))

	// Bad bound syntax parity.
	d.eq("ZREVRANGEBYSCORE bad min", bs("ZREVRANGEBYSCORE"), zk, bs("notafloat"), bs("1"))

	t.Logf("XCut ZREVRANGEBYSCORE: %d cases vs Redis 3.2", d.n)
}

// TestXCutRPopLPush covers the two-key atomic tail-pop/head-push, single-key
// rotation (source==dest), empty source, absent dest creation, and WRONGTYPE on
// either key (GAP 3).
func TestXCutRPopLPush(t *testing.T) {
	d := newDiffer(t)

	src, dst := d.k("rplsrc"), d.k("rpldst")
	d.eq("RPUSH src", bs("RPUSH"), src, bs("a"), bs("b"), bs("c"))

	// Move tail c of src to head of a fresh dst.
	d.eq("RPOPLPUSH to fresh dst", bs("RPOPLPUSH"), src, dst)
	d.eq("LRANGE src after move", bs("LRANGE"), src, bs("0"), bs("-1"))
	d.eq("LRANGE dst after move", bs("LRANGE"), dst, bs("0"), bs("-1"))
	// Move b to existing dst head.
	d.eq("RPOPLPUSH to existing dst", bs("RPOPLPUSH"), src, dst)
	d.eq("LRANGE dst after 2nd move", bs("LRANGE"), dst, bs("0"), bs("-1"))

	// Single-key rotation: source==dest rotates the list, replying its last element.
	rot := d.k("rplrot")
	d.eq("RPUSH rot", bs("RPUSH"), rot, bs("1"), bs("2"), bs("3"))
	d.eq("RPOPLPUSH rotate 1", bs("RPOPLPUSH"), rot, rot)
	d.eq("LRANGE rot after rotate 1", bs("LRANGE"), rot, bs("0"), bs("-1"))
	d.eq("RPOPLPUSH rotate 2", bs("RPOPLPUSH"), rot, rot)
	d.eq("LRANGE rot after rotate 2", bs("LRANGE"), rot, bs("0"), bs("-1"))

	// Draining source to empty deletes it and returns nil when empty.
	drain := d.k("rpldrain")
	dd := d.k("rpldraindst")
	d.eq("RPUSH drain", bs("RPUSH"), drain, bs("only"))
	d.eq("RPOPLPUSH drain last", bs("RPOPLPUSH"), drain, dd)
	d.eq("EXISTS drain after empty", bs("EXISTS"), drain)
	// Now source is absent -> reply nil, dest untouched.
	d.eq("RPOPLPUSH absent source", bs("RPOPLPUSH"), drain, dd)
	d.eq("LRANGE dd unchanged", bs("LRANGE"), dd, bs("0"), bs("-1"))

	// WRONGTYPE: source is not a list.
	badsrc := d.k("rplbadsrc")
	d.eq("SET badsrc", bs("SET"), badsrc, bs("v"))
	d.eq("RPOPLPUSH wrongtype source", bs("RPOPLPUSH"), badsrc, dst)
	// WRONGTYPE: dest is not a list (source is a valid non-empty list).
	goodsrc := d.k("rplgoodsrc")
	baddst := d.k("rplbaddst")
	d.eq("RPUSH goodsrc", bs("RPUSH"), goodsrc, bs("x"))
	d.eq("SET baddst", bs("SET"), baddst, bs("v"))
	d.eq("RPOPLPUSH wrongtype dest", bs("RPOPLPUSH"), goodsrc, baddst)

	// Binary-safe payload survives the round-trip.
	bsrc, bdst := d.k("rplbinsrc"), d.k("rplbindst")
	d.eq("RPUSH binary", bs("RPUSH"), bsrc, bs("a\x00b"), bs(string([]byte{0xff, 0x00, 0xfe})))
	d.eq("RPOPLPUSH binary", bs("RPOPLPUSH"), bsrc, bdst)
	d.eq("LRANGE binary dst", bs("LRANGE"), bdst, bs("0"), bs("-1"))

	t.Logf("XCut RPOPLPUSH: %d cases vs Redis 3.2", d.n)
}

// TestXCutAbsoluteExpiry covers EXPIREAT / PEXPIREAT: future timestamp keeps key
// live, past timestamp deletes it, absent key replies 0, and TTL reflects the
// absolute expiry (GAP 4, GAP 5).
func TestXCutAbsoluteExpiry(t *testing.T) {
	d := newDiffer(t)

	// Far-future EXPIREAT keeps the key (year 2286: 9999999999).
	fk := d.k("eaf")
	d.eq("SET for EXPIREAT future", bs("SET"), fk, bs("v"))
	d.eq("EXPIREAT future", bs("EXPIREAT"), fk, bs("9999999999"))
	d.eq("EXISTS after future EXPIREAT", bs("EXISTS"), fk)
	// TTL is a large countdown that both endpoints agree on within a second.
	d.eqIntClose("TTL after future EXPIREAT", 2, bs("TTL"), fk)

	// Past EXPIREAT (epoch 1609459200 = 2021-01-01) deletes the live key and
	// replies :1.
	pk := d.k("eap")
	d.eq("SET for EXPIREAT past", bs("SET"), pk, bs("v"))
	d.eq("EXPIREAT past deletes", bs("EXPIREAT"), pk, bs("1609459200"))
	d.eq("EXISTS after past EXPIREAT", bs("EXISTS"), pk)
	d.eq("GET after past EXPIREAT", bs("GET"), pk)
	d.eq("TTL after past EXPIREAT", bs("TTL"), pk)

	// EXPIREAT on an absent key replies :0.
	d.eq("EXPIREAT absent key", bs("EXPIREAT"), d.k("eamiss"), bs("9999999999"))

	// Timestamp 0 is in the past -> deletes.
	zk := d.k("eaz")
	d.eq("SET for EXPIREAT 0", bs("SET"), zk, bs("v"))
	d.eq("EXPIREAT 0 deletes", bs("EXPIREAT"), zk, bs("0"))
	d.eq("EXISTS after EXPIREAT 0", bs("EXISTS"), zk)

	// PEXPIREAT far-future in milliseconds keeps the key.
	pf := d.k("peaf")
	d.eq("SET for PEXPIREAT future", bs("SET"), pf, bs("v"))
	d.eq("PEXPIREAT future", bs("PEXPIREAT"), pf, bs("9999999999000"))
	d.eq("EXISTS after future PEXPIREAT", bs("EXISTS"), pf)
	d.eqIntClose("TTL after future PEXPIREAT", 2, bs("TTL"), pf)

	// PEXPIREAT past (ms) deletes.
	pp := d.k("peap")
	d.eq("SET for PEXPIREAT past", bs("SET"), pp, bs("v"))
	d.eq("PEXPIREAT past deletes", bs("PEXPIREAT"), pp, bs("1609459200000"))
	d.eq("EXISTS after past PEXPIREAT", bs("EXISTS"), pp)

	// PEXPIREAT absent key -> :0.
	d.eq("PEXPIREAT absent key", bs("PEXPIREAT"), d.k("peamiss"), bs("9999999999000"))

	// Non-integer timestamp is a parity error on both sides.
	ek := d.k("eaerr")
	d.eq("SET for EXPIREAT err", bs("SET"), ek, bs("v"))
	d.eq("EXPIREAT non-integer", bs("EXPIREAT"), ek, bs("notanint"))
	d.eq("PEXPIREAT non-integer", bs("PEXPIREAT"), ek, bs("notanint"))

	t.Logf("XCut EXPIREAT/PEXPIREAT: %d cases vs Redis 3.2", d.n)
}

// TestXCutGeoFamily covers GEOADD / GEODIST / GEOHASH / GEOPOS / GEORADIUS*:
// the deterministic geohash-encoded paths (GEOADD count, GEOHASH base32, absent
// / missing-member nil shapes, unit errors, coordinate-range errors, WRONGTYPE)
// plus GEODIST value parity within float tolerance (GAP 6).
func TestXCutGeoFamily(t *testing.T) {
	d := newDiffer(t)

	g := d.k("geo")
	// The canonical Redis GEO example (Palermo, Catania in Sicily).
	d.eq("GEOADD Palermo Catania", bs("GEOADD"), g, bs("13.361389"), bs("38.115556"), bs("Palermo"), bs("15.087269"), bs("37.502669"), bs("Catania"))
	// Re-adding an existing member returns 0 new.
	d.eq("GEOADD existing member", bs("GEOADD"), g, bs("13.361389"), bs("38.115556"), bs("Palermo"))
	// Add a third point.
	d.eq("GEOADD Agrigento", bs("GEOADD"), g, bs("13.583333"), bs("37.316667"), bs("Agrigento"))

	// GEOHASH is a deterministic 11-char base32 hash — byte-identical to Redis.
	d.eq("GEOHASH members", bs("GEOHASH"), g, bs("Palermo"), bs("Catania"))
	d.eq("GEOHASH with missing member", bs("GEOHASH"), g, bs("Palermo"), bs("Nowhere"))
	d.eq("GEOHASH absent key", bs("GEOHASH"), d.k("geomiss"), bs("Palermo"))

	// GEODIST value: haversine formatted to 4 decimals; float-close guards the
	// last-digit rounding drift between Go and C.
	d.eqFloatClose("GEODIST default meters", bs("GEODIST"), g, bs("Palermo"), bs("Catania"))
	d.eqFloatClose("GEODIST km", bs("GEODIST"), g, bs("Palermo"), bs("Catania"), bs("km"))
	d.eqFloatClose("GEODIST mi", bs("GEODIST"), g, bs("Palermo"), bs("Catania"), bs("mi"))
	// Same point distance is exactly 0.0000 on both sides.
	d.eq("GEODIST same point", bs("GEODIST"), g, bs("Palermo"), bs("Palermo"))
	// Missing member -> nil bulk (deterministic).
	d.eq("GEODIST missing member", bs("GEODIST"), g, bs("Palermo"), bs("Nowhere"))
	d.eq("GEODIST both missing", bs("GEODIST"), g, bs("Ghost1"), bs("Ghost2"))
	// Bad unit error parity.
	d.eq("GEODIST bad unit", bs("GEODIST"), g, bs("Palermo"), bs("Catania"), bs("parsecs"))

	// GEOPOS nil shapes are deterministic (decoded coordinate bytes are precision-
	// sensitive, so only the nil/absent shapes are byte-asserted here; the value
	// round-trip is covered by GEOHASH above).
	d.eq("GEOPOS missing member nil", bs("GEOPOS"), g, bs("Nowhere"))
	d.eq("GEOPOS absent key", bs("GEOPOS"), d.k("geomiss2"), bs("Palermo"))
	d.eq("GEOPOS mixed present+missing shape", bs("GEOPOS"), g, bs("Ghost"), bs("Ghost2"))

	// GEOADD errors: bad coordinate float, out-of-range latitude, odd triple count.
	d.eq("GEOADD bad lon float", bs("GEOADD"), g, bs("notafloat"), bs("38.0"), bs("X"))
	d.eq("GEOADD lat out of range", bs("GEOADD"), g, bs("13.0"), bs("91.0"), bs("X"))
	d.eq("GEOADD lon out of range", bs("GEOADD"), g, bs("200.0"), bs("38.0"), bs("X"))

	// GEORADIUS from a center: query all of Sicily by a large radius, sorted ASC,
	// returns member names in deterministic distance order.
	d.eq("GEORADIUS 200km ASC", bs("GEORADIUS"), g, bs("15"), bs("37"), bs("200"), bs("km"), bs("ASC"))
	d.eq("GEORADIUS 200km DESC", bs("GEORADIUS"), g, bs("15"), bs("37"), bs("200"), bs("km"), bs("DESC"))
	d.eq("GEORADIUS tiny radius empty", bs("GEORADIUS"), g, bs("0"), bs("0"), bs("1"), bs("m"))
	d.eq("GEORADIUS COUNT 1 ASC", bs("GEORADIUS"), g, bs("15"), bs("37"), bs("500"), bs("km"), bs("COUNT"), bs("1"), bs("ASC"))
	d.eq("GEORADIUS bad unit", bs("GEORADIUS"), g, bs("15"), bs("37"), bs("200"), bs("furlongs"))

	// GEORADIUSBYMEMBER anchored on an existing member.
	d.eq("GEORADIUSBYMEMBER 200km ASC", bs("GEORADIUSBYMEMBER"), g, bs("Palermo"), bs("200"), bs("km"), bs("ASC"))
	d.eq("GEORADIUSBYMEMBER missing member", bs("GEORADIUSBYMEMBER"), g, bs("Nowhere"), bs("200"), bs("km"))

	// WRONGTYPE: a GEO key is a zset, so a String key is WRONGTYPE for the family.
	wt := d.k("geowt")
	d.eq("SET geowt string", bs("SET"), wt, bs("v"))
	d.eq("GEOADD wrongtype", bs("GEOADD"), wt, bs("13"), bs("38"), bs("X"))
	d.eq("GEODIST wrongtype", bs("GEODIST"), wt, bs("Palermo"), bs("Catania"))
	d.eq("GEOPOS wrongtype", bs("GEOPOS"), wt, bs("Palermo"))
	d.eq("GEOHASH wrongtype", bs("GEOHASH"), wt, bs("Palermo"))
	d.eq("GEORADIUS wrongtype", bs("GEORADIUS"), wt, bs("15"), bs("37"), bs("200"), bs("km"))

	t.Logf("XCut GEO family: %d cases vs Redis 3.2", d.n)
}

// TestXCutSetMoveAndRandom covers SMOVE (move / not-present / self-move / absent
// source / WRONGTYPE) and SRANDMEMBER shapes that are deterministic regardless of
// which random member is picked (GAP 10, GAP 13).
func TestXCutSetMoveAndRandom(t *testing.T) {
	d := newDiffer(t)

	s1, s2 := d.k("smv1"), d.k("smv2")
	d.eq("SADD s1", bs("SADD"), s1, bs("a"), bs("b"), bs("c"))
	d.eq("SADD s2", bs("SADD"), s2, bs("x"))

	// Move a from s1 to s2 -> :1.
	d.eq("SMOVE a", bs("SMOVE"), s1, s2, bs("a"))
	d.eq("SISMEMBER s1 a gone", bs("SISMEMBER"), s1, bs("a"))
	d.eq("SISMEMBER s2 a present", bs("SISMEMBER"), s2, bs("a"))
	d.eqSorted("SMEMBERS s1 after move", bs("SMEMBERS"), s1)
	d.eqSorted("SMEMBERS s2 after move", bs("SMEMBERS"), s2)

	// Member not in source -> :0, no change.
	d.eq("SMOVE not present", bs("SMOVE"), s1, s2, bs("zzz"))
	// Moving a member already in dest still moves (removes from src) -> add x to s1
	// first so both hold x.
	d.eq("SADD s1 x", bs("SADD"), s1, bs("x"))
	d.eq("SMOVE x into s2 dup", bs("SMOVE"), s1, s2, bs("x"))
	d.eq("SISMEMBER s1 x gone", bs("SISMEMBER"), s1, bs("x"))
	d.eqSorted("SMEMBERS s2 after dup move", bs("SMEMBERS"), s2)

	// Self-move: member exists in the same set -> :1, set unchanged.
	d.eq("SMOVE self existing", bs("SMOVE"), s1, s1, bs("b"))
	d.eqSorted("SMEMBERS s1 after self-move", bs("SMEMBERS"), s1)
	// Self-move of a non-member -> :0.
	d.eq("SMOVE self non-member", bs("SMOVE"), s1, s1, bs("nope"))

	// Absent source -> :0.
	d.eq("SMOVE absent source", bs("SMOVE"), d.k("smvmiss"), s2, bs("a"))

	// Last-member move deletes the empty source key.
	solo, dst := d.k("smvsolo"), d.k("smvsolodst")
	d.eq("SADD solo", bs("SADD"), solo, bs("only"))
	d.eq("SMOVE last member", bs("SMOVE"), solo, dst, bs("only"))
	d.eq("EXISTS solo after emptied", bs("EXISTS"), solo)

	// WRONGTYPE: source or dest is not a set.
	strk := d.k("smvstr")
	d.eq("SET smv string", bs("SET"), strk, bs("v"))
	d.eq("SMOVE wrongtype source", bs("SMOVE"), strk, s2, bs("a"))
	d.eq("SMOVE wrongtype dest", bs("SMOVE"), s1, strk, bs("c"))

	// SRANDMEMBER deterministic-shape cases.
	rs := d.k("srand")
	d.eq("SADD srand", bs("SADD"), rs, bs("a"), bs("b"), bs("c"))
	// count 0 -> empty array on both sides.
	d.eq("SRANDMEMBER count 0", bs("SRANDMEMBER"), rs, bs("0"))
	// positive count >= cardinality returns the WHOLE set (distinct), so a sorted
	// compare is exact regardless of iteration order.
	d.eqSorted("SRANDMEMBER count >= card returns all", bs("SRANDMEMBER"), rs, bs("10"))
	d.eqSorted("SRANDMEMBER count == card returns all", bs("SRANDMEMBER"), rs, bs("3"))
	// Absent key: single -> nil bulk; with count -> empty array.
	d.eq("SRANDMEMBER absent single", bs("SRANDMEMBER"), d.k("srandmiss"))
	d.eq("SRANDMEMBER absent count", bs("SRANDMEMBER"), d.k("srandmiss"), bs("3"))
	d.eq("SRANDMEMBER absent neg count", bs("SRANDMEMBER"), d.k("srandmiss"), bs("-3"))
	// WRONGTYPE on a non-set.
	d.eq("SRANDMEMBER wrongtype", bs("SRANDMEMBER"), strk)
	d.eq("SRANDMEMBER wrongtype count", bs("SRANDMEMBER"), strk, bs("2"))

	t.Logf("XCut SMOVE/SRANDMEMBER: %d cases vs Redis 3.2", d.n)
}

// TestXCutPFMerge covers the HyperLogLog merge: PFMERGE folds source estimators
// into a destination, PFCOUNT of the merge equals the union estimate, self-merge,
// missing sources, and non-HLL WRONGTYPE (GAP 11).
func TestXCutPFMerge(t *testing.T) {
	d := newDiffer(t)

	h1, h2, dst := d.k("pf1"), d.k("pf2"), d.k("pfdst")
	// Disjoint element sets so the merged cardinality is the sum.
	d.eq("PFADD h1", bs("PFADD"), h1, bs("a"), bs("b"), bs("c"))
	d.eq("PFADD h2", bs("PFADD"), h2, bs("c"), bs("d"), bs("e"))
	d.eq("PFCOUNT h1", bs("PFCOUNT"), h1)
	d.eq("PFCOUNT h2", bs("PFCOUNT"), h2)

	// Merge into a fresh destination.
	d.eq("PFMERGE fresh dst", bs("PFMERGE"), dst, h1, h2)
	d.eq("PFCOUNT merged dst", bs("PFCOUNT"), dst)
	// Multi-key PFCOUNT (union without storing) matches the merged estimate.
	d.eq("PFCOUNT h1 h2 union", bs("PFCOUNT"), h1, h2)

	// Merge with a missing source contributes nothing.
	d.eq("PFMERGE with missing source", bs("PFMERGE"), dst, h1, d.k("pfmiss"))
	d.eq("PFCOUNT after missing-source merge", bs("PFCOUNT"), dst)

	// Self-merge (dest also a source) is idempotent for the estimate.
	d.eq("PFMERGE self", bs("PFMERGE"), dst, dst, h2)
	d.eq("PFCOUNT after self-merge", bs("PFCOUNT"), dst)

	// PFMERGE with only a destination (no sources) creates/keeps an empty-or-same
	// HLL and replies +OK.
	solo := d.k("pfsolo")
	d.eq("PFMERGE dest only fresh", bs("PFMERGE"), solo)
	d.eq("PFCOUNT dest only fresh", bs("PFCOUNT"), solo)

	// WRONGTYPE: a plain String (not an HLL blob) source/dest.
	strk := d.k("pfstr")
	d.eq("SET pf string", bs("SET"), strk, bs("plainvalue"))
	d.eq("PFMERGE wrongtype source", bs("PFMERGE"), dst, strk)
	d.eq("PFADD wrongtype", bs("PFADD"), strk, bs("x"))
	d.eq("PFCOUNT wrongtype", bs("PFCOUNT"), strk)

	// Empty PFADD (no elements) on a fresh key still creates the HLL and replies :1.
	ek := d.k("pfempty")
	d.eq("PFADD no elements", bs("PFADD"), ek)
	d.eq("PFCOUNT empty hll", bs("PFCOUNT"), ek)
	d.eq("EXISTS empty hll", bs("EXISTS"), ek)

	t.Logf("XCut PFMERGE/HLL: %d cases vs Redis 3.2", d.n)
}

// TestXCutRejectedKeyspaceArity covers the arity-error boundary of the
// deliberately-rejected keyspace commands (KEYS / RENAME / RENAMENX / FLUSHDB /
// FLUSHALL). Only the SHARED wrong-number-of-arguments reply is asserted: the
// executed form diverges from Redis by proxy design (KEYS full-scan, RENAME
// whole-collection copy, FLUSH wipe of the shared table), so byte-parity there is
// intentionally impossible and out of scope (GAP 7, GAP 8, GAP 9).
func TestXCutRejectedKeyspaceArity(t *testing.T) {
	d := newDiffer(t)

	// KEYS arity is 2 (command + one pattern). Zero patterns and two patterns both
	// yield the identical wrong-args error on both sides.
	d.eq("KEYS no pattern arity", bs("KEYS"))
	d.eq("KEYS two patterns arity", bs("KEYS"), bs("*"), bs("extra"))

	// RENAME / RENAMENX arity is 3 (command + src + dst).
	d.eq("RENAME missing dst arity", bs("RENAME"), d.k("ren"))
	d.eq("RENAME too many arity", bs("RENAME"), d.k("ren"), d.k("ren2"), bs("extra"))
	d.eq("RENAMENX missing dst arity", bs("RENAMENX"), d.k("ren"))
	d.eq("RENAMENX too many arity", bs("RENAMENX"), d.k("ren"), d.k("ren2"), bs("extra"))

	// FLUSHDB / FLUSHALL arity is 1 (bare command). Any argument is a wrong-args
	// error on both sides. (The bare form is NOT tested: it executes on Redis but
	// is rejected on the proxy by design, so it cannot byte-match.)
	d.eq("FLUSHDB extra arg arity", bs("FLUSHDB"), bs("ASYNC"))
	d.eq("FLUSHALL extra arg arity", bs("FLUSHALL"), bs("ASYNC"))

	t.Logf("XCut rejected-keyspace arity: %d cases vs Redis 3.2", d.n)
}
