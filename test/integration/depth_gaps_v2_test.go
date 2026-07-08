package integration

import "testing"

// This file closes the HIGH-priority depth gaps the 2026-07 dimension audit surfaced in
// the existing A-P dimensions: TTL cross-command preservation (E), mutation return values
// (L), type/error precedence (A), numeric parsing (B), reply shape (C), index boundaries
// (D), per-command multi-DB isolation (H), and type-overwrite introspection (P). Every
// case runs against the live Redis 3.2 oracle, so a divergence surfaces as a failing case.

// --- Dimension E: TTL preservation / clearing across command families --------

func TestDiffTTL_CrossCommandPreservation(t *testing.T) {
	d := newDiffer(t)

	// APPEND preserves the TTL (it is an in-place string mutation).
	ka := d.k("ttl-append")
	d.eq("SET EX 1000", bs("SET"), ka, bs("hello"), bs("EX"), bs("1000"))
	d.eq("APPEND keeps key", bs("APPEND"), ka, bs(" world"))
	d.eqIntClose("TTL preserved after APPEND", 2, bs("TTL"), ka)

	// SETRANGE preserves the TTL.
	ksr := d.k("ttl-setrange")
	d.eq("SET EX 1000", bs("SET"), ksr, bs("hello"), bs("EX"), bs("1000"))
	d.eq("SETRANGE", bs("SETRANGE"), ksr, bs("1"), bs("XY"))
	d.eqIntClose("TTL preserved after SETRANGE", 2, bs("TTL"), ksr)

	// INCRBYFLOAT preserves the TTL (unlike a plain SET).
	kf := d.k("ttl-incrbyfloat")
	d.eq("SET 3.0 EX 1000", bs("SET"), kf, bs("3.0"), bs("EX"), bs("1000"))
	d.eqFloatClose("INCRBYFLOAT", bs("INCRBYFLOAT"), kf, bs("1.5"))
	d.eqIntClose("TTL preserved after INCRBYFLOAT", 2, bs("TTL"), kf)

	// GETSET is a SET: it CLEARS the TTL (-1 after).
	kgs := d.k("ttl-getset")
	d.eq("SET EX 1000", bs("SET"), kgs, bs("v1"), bs("EX"), bs("1000"))
	d.eq("GETSET returns old", bs("GETSET"), kgs, bs("v2"))
	d.eq("TTL cleared after GETSET -> -1", bs("TTL"), kgs)

	// A plain SET (no EX) over a key with a TTL clears the TTL.
	kov := d.k("ttl-setover")
	d.eq("SET EX 1000", bs("SET"), kov, bs("v1"), bs("EX"), bs("1000"))
	d.eq("SET overwrite", bs("SET"), kov, bs("v2"))
	d.eq("TTL cleared after plain SET -> -1", bs("TTL"), kov)

	// SET ... KEEPTTL is post-3.2; a SET with a fresh EX resets the countdown.
	kre := d.k("ttl-reset")
	d.eq("SET EX 1000", bs("SET"), kre, bs("v"), bs("EX"), bs("1000"))
	d.eq("SET EX 50 (reset)", bs("SET"), kre, bs("v"), bs("EX"), bs("50"))
	d.eqIntClose("TTL reset to ~50", 2, bs("TTL"), kre)

	// DEL clears the expiry meta: re-created key has no TTL.
	kdel := d.k("ttl-del")
	d.eq("SET EX 1000", bs("SET"), kdel, bs("v"), bs("EX"), bs("1000"))
	d.eq("DEL", bs("DEL"), kdel)
	d.eq("re-SET no EX", bs("SET"), kdel, bs("v2"))
	d.eq("TTL -1 after DEL+recreate", bs("TTL"), kdel)

	t.Logf("compared %d TTL cross-command replies vs Redis 3.2", d.n)
}

func TestDiffTTL_OnCollectionTypes(t *testing.T) {
	d := newDiffer(t)

	// EXPIRE/TTL are type-agnostic: they apply to sets/hashes/lists/zsets identically.
	ks := d.k("ttl-set")
	d.eq("SADD", bs("SADD"), ks, bs("a"), bs("b"))
	d.eq("EXPIRE set -> 1", bs("EXPIRE"), ks, bs("1000"))
	d.eqIntClose("TTL of set ~1000", 2, bs("TTL"), ks)
	d.eq("PERSIST set -> 1", bs("PERSIST"), ks)
	d.eq("TTL of set -> -1 after PERSIST", bs("TTL"), ks)

	kh := d.k("ttl-hash")
	d.eq("HSET", bs("HSET"), kh, bs("f"), bs("v"))
	d.eq("PEXPIRE hash", bs("PEXPIRE"), kh, bs("1000000"))
	d.eqIntClose("PTTL of hash ~1000000", 1500, bs("PTTL"), kh)

	kz := d.k("ttl-zset")
	d.eq("ZADD", bs("ZADD"), kz, bs("1"), bs("m"))
	d.eq("EXPIREAT zset far future", bs("EXPIREAT"), kz, bs("99999999999"))
	d.eq("TTL positive (persisted)", bs("PERSIST"), kz)

	// A collection command on a key whose TTL clears it: adding to an expired set behaves
	// as a fresh set. (EXPIRE 1s then the key logically expires; testing PERSIST/TTL here.)
	kl := d.k("ttl-list")
	d.eq("RPUSH", bs("RPUSH"), kl, bs("x"))
	d.eq("EXPIRE list", bs("EXPIRE"), kl, bs("1000"))
	d.eq("RPUSH again keeps TTL", bs("RPUSH"), kl, bs("y"))
	d.eqIntClose("TTL still set after RPUSH", 2, bs("TTL"), kl)

	t.Logf("compared %d TTL-on-collection replies vs Redis 3.2", d.n)
}

func TestDiffTTL_SetOptionsOnState(t *testing.T) {
	d := newDiffer(t)

	// SET NX on an existing key -> null, no change. SET XX on a missing key -> null.
	knx := d.k("setnx-exist")
	d.eq("SET initial", bs("SET"), knx, bs("v1"))
	d.eq("SET NX on existing -> nil", bs("SET"), knx, bs("v2"), bs("NX"))
	d.eq("GET unchanged", bs("GET"), knx)
	kxx := d.k("setxx-missing")
	d.eq("SET XX on missing -> nil", bs("SET"), kxx, bs("v"), bs("XX"))
	d.eq("GET still missing", bs("GET"), kxx)
	d.eq("SET XX on existing after create", bs("SET"), kxx, bs("v0"))
	d.eq("SET XX now applies", bs("SET"), kxx, bs("v1"), bs("XX"))
	d.eq("GET v1", bs("GET"), kxx)
	// SET NX EX atomic set+expire on a fresh key.
	knex := d.k("setnxex")
	d.eq("SET NX EX fresh", bs("SET"), knex, bs("v"), bs("NX"), bs("EX"), bs("500"))
	d.eqIntClose("TTL ~500 after SET NX EX", 2, bs("TTL"), knex)

	t.Logf("compared %d SET-option-on-state replies vs Redis 3.2", d.n)
}

// --- Dimension L: mutation-command return values -----------------------------

func TestDiffMutationReturns_Depth(t *testing.T) {
	d := newDiffer(t)

	// SMOVE WRONGTYPE: a live non-set source or destination.
	src := d.k("smove-strsrc")
	dst := d.k("smove-set")
	d.eq("SET string src", bs("SET"), src, bs("v"))
	d.eq("SADD real dst", bs("SADD"), dst, bs("m"))
	d.eq("SMOVE wrong-type source -> WRONGTYPE", bs("SMOVE"), src, dst, bs("m"))
	src2 := d.k("smove-set2")
	dst2 := d.k("smove-strdst")
	d.eq("SADD real src", bs("SADD"), src2, bs("m"))
	d.eq("SET string dst", bs("SET"), dst2, bs("v"))
	d.eq("SMOVE present member, wrong-type dst -> WRONGTYPE", bs("SMOVE"), src2, dst2, bs("m"))

	// ZADD CH: counts changed (added + score-updated) members.
	kz := d.k("zadd-ch")
	d.eq("ZADD initial", bs("ZADD"), kz, bs("1"), bs("a"), bs("2"), bs("b"))
	d.eq("ZADD CH update+add -> 2", bs("ZADD"), kz, bs("CH"), bs("1"), bs("a"), bs("9"), bs("b"), bs("3"), bs("c"))
	d.eq("ZADD CH no change -> 0", bs("ZADD"), kz, bs("CH"), bs("9"), bs("b"))
	// ZADD INCR returns the new score (bulk), not a count.
	d.eq("ZADD INCR", bs("ZADD"), kz, bs("INCR"), bs("5"), bs("a"))
	d.eq("ZADD NX INCR on existing -> nil", bs("ZADD"), kz, bs("NX"), bs("INCR"), bs("5"), bs("a"))
	d.eq("ZADD XX INCR on missing -> nil", bs("ZADD"), kz, bs("XX"), bs("INCR"), bs("5"), bs("zzz"))

	// HINCRBYFLOAT on a non-numeric field -> error.
	kh := d.k("hincrbyfloat-bad")
	d.eq("HSET non-numeric", bs("HSET"), kh, bs("f"), bs("notanumber"))
	d.eq("HINCRBYFLOAT bad -> error", bs("HINCRBYFLOAT"), kh, bs("f"), bs("1.5"))

	// APPEND on a collection -> WRONGTYPE (not a length).
	kl := d.k("append-list")
	d.eq("RPUSH", bs("RPUSH"), kl, bs("x"))
	d.eq("APPEND on list -> WRONGTYPE", bs("APPEND"), kl, bs("y"))

	// DEL with duplicate + missing keys returns the count of keys actually removed.
	kd1 := d.k("del-a")
	kd2 := d.k("del-b")
	d.eq("SET a", bs("SET"), kd1, bs("v"))
	d.eq("SET b", bs("SET"), kd2, bs("v"))
	d.eq("DEL a a b missing -> 2", bs("DEL"), kd1, kd1, kd2, d.k("del-missing"))

	// HDEL / SREM / ZREM / LREM return counts.
	khh := d.k("hdel-multi")
	// HMSET, not multi-field HSET: Redis 3.2 HSET takes exactly one field/value pair.
	d.eq("HMSET multi", bs("HMSET"), khh, bs("f1"), bs("v"), bs("f2"), bs("v"), bs("f3"), bs("v"))
	d.eq("HDEL some+missing -> 2", bs("HDEL"), khh, bs("f1"), bs("f2"), bs("nope"))

	t.Logf("compared %d mutation-return replies vs Redis 3.2", d.n)
}

// --- Dimension A: type-check precedence for string-only read-write commands ---

func TestDiffWrongType_StringOps(t *testing.T) {
	d := newDiffer(t)

	// Build one of each collection type, then run string-only commands -> WRONGTYPE.
	set := d.k("wt-set")
	hash := d.k("wt-hash")
	list := d.k("wt-list")
	zset := d.k("wt-zset")
	d.eq("SADD", bs("SADD"), set, bs("m"))
	d.eq("HSET", bs("HSET"), hash, bs("f"), bs("v"))
	d.eq("RPUSH", bs("RPUSH"), list, bs("x"))
	d.eq("ZADD", bs("ZADD"), zset, bs("1"), bs("m"))

	for _, k := range [][]byte{set, hash, list, zset} {
		d.eq("APPEND -> WRONGTYPE", bs("APPEND"), k, bs("x"))
		d.eq("GETSET -> WRONGTYPE", bs("GETSET"), k, bs("v"))
		d.eq("GETRANGE -> WRONGTYPE", bs("GETRANGE"), k, bs("0"), bs("-1"))
		d.eq("SETRANGE -> WRONGTYPE", bs("SETRANGE"), k, bs("0"), bs("x"))
		d.eq("STRLEN -> WRONGTYPE", bs("STRLEN"), k)
		d.eq("INCR -> WRONGTYPE", bs("INCR"), k)
		d.eq("SETBIT -> WRONGTYPE", bs("SETBIT"), k, bs("0"), bs("1"))
	}

	// ZADD NX vs XX on existing/non-existing member (return-count precision).
	kz := d.k("zadd-nxxx")
	d.eq("ZADD initial", bs("ZADD"), kz, bs("1"), bs("a"))
	d.eq("ZADD NX existing member -> 0", bs("ZADD"), kz, bs("NX"), bs("5"), bs("a"))
	d.eq("ZADD NX new member -> 1", bs("ZADD"), kz, bs("NX"), bs("5"), bs("b"))
	d.eq("ZADD XX new member -> 0 (no add)", bs("ZADD"), kz, bs("XX"), bs("5"), bs("c"))
	d.eq("ZSCORE a unchanged by NX", bs("ZSCORE"), kz, bs("a"))
	d.eq("ZSCORE c absent", bs("ZSCORE"), kz, bs("c"))

	t.Logf("compared %d string-op WRONGTYPE + ZADD-flag replies vs Redis 3.2", d.n)
}

// --- Dimension B: integer/float parsing on non-numeric stored/arg values ------

func TestDiffNumeric_NonIntegerParsing(t *testing.T) {
	d := newDiffer(t)

	// INCR/DECR family on a non-integer stored value -> "not an integer or out of range".
	for i, val := range []string{"abc", "1.5", "10 ", " 10", "0x10", "+10", "10a", ""} {
		k := d.k("incr-bad-" + itoa(i))
		d.eq("SET non-int", bs("SET"), k, bs(val))
		d.eq("INCR bad -> error", bs("INCR"), k)
		d.eq("DECR bad -> error", bs("DECR"), k)
		d.eq("INCRBY bad -> error", bs("INCRBY"), k, bs("5"))
	}
	// INCRBY/DECRBY with a non-integer DELTA -> error even on a valid stored int.
	kv := d.k("incrby-baddelta")
	d.eq("SET 10", bs("SET"), kv, bs("10"))
	d.eq("INCRBY 1.5 -> error", bs("INCRBY"), kv, bs("1.5"))
	d.eq("INCRBY abc -> error", bs("INCRBY"), kv, bs("abc"))
	d.eq("value untouched", bs("GET"), kv)

	// ZADD score with surrounding whitespace / odd forms.
	for i, sc := range []string{" 1", "1 ", "1.0", "+1", "1e2", ".5", "5."} {
		kz := d.k("zadd-score-" + itoa(i))
		d.eq("ZADD odd score", bs("ZADD"), kz, bs(sc), bs("m"))
		d.eq("ZSCORE readback", bs("ZSCORE"), kz, bs("m"))
	}

	t.Logf("compared %d numeric-parsing replies vs Redis 3.2", d.n)
}

// --- Dimension C: MGET reply shape over mixed key states ----------------------

func TestDiffReplyShape_MGETMixed(t *testing.T) {
	d := newDiffer(t)

	ks := d.k("mget-str")
	kempty := d.k("mget-empty")
	kmiss := d.k("mget-miss")
	kcoll := d.k("mget-coll")
	d.eq("SET str", bs("SET"), ks, bs("hello"))
	d.eq("SET empty", bs("SET"), kempty, bs(""))
	d.eq("SADD collection", bs("SADD"), kcoll, bs("m"))
	// MGET over string, empty-string, missing, and a WRONG-TYPE key: Redis returns the
	// wrong-type key's slot as nil (MGET never errors on type), others as value/$0/$-1.
	d.eq("MGET mixed", bs("MGET"), ks, kempty, kmiss, kcoll, ks)

	t.Logf("compared %d MGET reply-shape replies vs Redis 3.2", d.n)
}

// --- Dimension D: index/range boundaries -------------------------------------

func TestDiffBoundary_IndexRangeDepth(t *testing.T) {
	d := newDiffer(t)

	// LINDEX with int64 extremes and out-of-range negatives.
	kl := d.k("lindex")
	d.eq("RPUSH", bs("RPUSH"), kl, bs("a"), bs("b"), bs("c"))
	for _, idx := range []string{"0", "2", "-1", "-3", "3", "-4", "9223372036854775807", "-9223372036854775808"} {
		d.eq("LINDEX "+idx, bs("LINDEX"), kl, bs(idx))
	}

	// GETRANGE with negative, inverted, and int64-extreme bounds.
	ks := d.k("getrange")
	d.eq("SET hello", bs("SET"), ks, bs("Hello World"))
	for _, r := range [][2]string{{"0", "-1"}, {"-5", "-1"}, {"0", "0"}, {"5", "2"}, {"-100", "-1"}, {"0", "100"}, {"-1", "-100"}, {"9223372036854775807", "9223372036854775807"}} {
		d.eq("GETRANGE "+r[0]+" "+r[1], bs("GETRANGE"), ks, bs(r[0]), bs(r[1]))
	}

	// SETRANGE past the end zero-pads.
	ksr := d.k("setrange-pad")
	d.eq("SET short", bs("SET"), ksr, bs("Hi"))
	d.eq("SETRANGE offset 5", bs("SETRANGE"), ksr, bs("5"), bs("X"))
	d.eq("GET zero-padded", bs("GET"), ksr)
	d.eq("STRLEN zero-padded", bs("STRLEN"), ksr)

	t.Logf("compared %d index/range boundary replies vs Redis 3.2", d.n)
}

// --- Dimension H: per-command multi-DB isolation ------------------------------

func TestDiffMultiDB_PerCommandIsolation(t *testing.T) {
	d := newDiffer(t)
	k := d.k("mdb-key")

	// Same-named key: string in DB 0, set in DB 3 — independent value AND type AND TTL.
	d.eq("DB0 SET string EX", bs("SET"), k, bs("in-db0"), bs("EX"), bs("1000"))
	d.eq("SELECT 3", bs("SELECT"), bs("3"))
	d.eq("DB3 key absent", bs("EXISTS"), k)
	d.eq("DB3 SADD (different type)", bs("SADD"), k, bs("m1"), bs("m2"))
	d.eq("DB3 TYPE is set", bs("TYPE"), k)
	d.eq("DB3 no TTL", bs("TTL"), k)
	d.eqSorted("DB3 SMEMBERS", bs("SMEMBERS"), k)
	d.eq("SELECT 0", bs("SELECT"), bs("0"))
	d.eq("DB0 TYPE still string", bs("TYPE"), k)
	d.eq("DB0 GET still in-db0", bs("GET"), k)
	d.eqIntClose("DB0 TTL still ~1000", 3, bs("TTL"), k)
	// Delete in DB0 does not affect DB3.
	d.eq("DB0 DEL", bs("DEL"), k)
	d.eq("SELECT 3 again", bs("SELECT"), bs("3"))
	d.eq("DB3 set survives DB0 DEL", bs("EXISTS"), k)
	d.eqSorted("DB3 SMEMBERS intact", bs("SMEMBERS"), k)
	d.selectBack()

	t.Logf("compared %d multi-DB per-command replies vs Redis 3.2", d.n)
}

// --- Dimension P: type-overwrite introspection --------------------------------

func TestDiffTypeOverwrite_Introspection(t *testing.T) {
	d := newDiffer(t)

	// TYPE reflects the current type through overwrites; empty collection -> none.
	k := d.k("to-type")
	d.eq("TYPE missing -> none", bs("TYPE"), k)
	d.eq("SADD", bs("SADD"), k, bs("m"))
	d.eq("TYPE set", bs("TYPE"), k)
	d.eq("SREM last -> key gone", bs("SREM"), k, bs("m"))
	d.eq("TYPE none after empty", bs("TYPE"), k)
	d.eq("SET string over gone key", bs("SET"), k, bs("v"))
	d.eq("TYPE string", bs("TYPE"), k)
	d.eq("RPUSH over string -> WRONGTYPE", bs("RPUSH"), k, bs("x"))
	d.eq("TYPE still string", bs("TYPE"), k)

	// MSET overwrites mixed collection types to strings in one batch.
	ksa := d.k("to-mset-set")
	kha := d.k("to-mset-hash")
	kza := d.k("to-mset-zset")
	d.eq("SADD", bs("SADD"), ksa, bs("a"), bs("b"))
	d.eq("HSET", bs("HSET"), kha, bs("f"), bs("v"))
	d.eq("ZADD", bs("ZADD"), kza, bs("1"), bs("m"))
	d.eq("MSET over 3 collections -> OK", bs("MSET"), ksa, bs("s1"), kha, bs("s2"), kza, bs("s3"))
	d.eq("TYPE set->string", bs("TYPE"), ksa)
	d.eq("TYPE hash->string", bs("TYPE"), kha)
	d.eq("TYPE zset->string", bs("TYPE"), kza)
	d.eq("GET s1", bs("GET"), ksa)
	d.eq("SCARD 0 (no stale set)", bs("SCARD"), ksa)

	t.Logf("compared %d type-overwrite introspection replies vs Redis 3.2", d.n)
}
