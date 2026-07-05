package integration

import (
	"testing"
)

// Dimension E (depth): TTL/expiry edge semantics that the base ttl_test.go leaves
// uncovered. Every case runs the SAME command on the redimos proxy and a live
// Redis 3.2 oracle and byte-compares (d.eq) — or, for a countdown TTL that can
// straddle a second boundary between the two endpoints, compares within a small
// integer tolerance (d.eqIntClose). Nothing here is hardcoded; the oracle defines
// the expected reply at runtime.
//
// The gaps exercised (from the E depth gap list):
//   1. SET ... EX/PX <= 0 rejection (and SETEX/PSETEX <= 0).
//   2. EXPIRE with a negative/zero TTL deletes a live key and replies :1.
//   3. PEXPIRE/PEXPIREAT resolving to a past/at-now epoch deletes and replies :1.
//   4. APPEND / SETRANGE preserve TTL; GETSET clears it.
//   5. INCR / DECR / INCRBY / DECRBY preserve TTL.
//   6. MSET / MSETNX clear TTL on every key they write.
//   7. SET XX clears the existing TTL (like any plain SET); SET NX creates a key with no TTL.
//   8. Collection mutation (RPUSH) preserves the key's TTL; a type mismatch on a
//      TTL'd key still replies WRONGTYPE.
//   9. EXPIREAT with a past absolute epoch deletes and replies :1.

// TestDiffTTLInvalidExpireRejection covers GAP 1: SET EX/PX and SETEX/PSETEX with a
// non-positive expiry are rejected byte-for-byte with the invalid-expire-time error,
// and the key is NOT created as a side effect.
func TestDiffTTLInvalidExpireRejection(t *testing.T) {
	d := newDiffer(t)

	// SET k v EX 0 / EX -100 / PX 0 / PX -1 all reject; the key must stay absent.
	ex0 := d.k("set-ex0")
	d.eq("SET EX 0 -> invalid expire", bs("SET"), ex0, bs("v"), bs("EX"), bs("0"))
	d.eq("GET after rejected SET EX 0 -> nil", bs("GET"), ex0)
	d.eq("EXISTS after rejected SET EX 0 -> 0", bs("EXISTS"), ex0)

	exNeg := d.k("set-exneg")
	d.eq("SET EX -100 -> invalid expire", bs("SET"), exNeg, bs("v"), bs("EX"), bs("-100"))
	d.eq("EXISTS after rejected SET EX -100 -> 0", bs("EXISTS"), exNeg)

	px0 := d.k("set-px0")
	d.eq("SET PX 0 -> invalid expire", bs("SET"), px0, bs("v"), bs("PX"), bs("0"))
	d.eq("EXISTS after rejected SET PX 0 -> 0", bs("EXISTS"), px0)

	pxNeg := d.k("set-pxneg")
	d.eq("SET PX -1 -> invalid expire", bs("SET"), pxNeg, bs("v"), bs("PX"), bs("-1"))
	d.eq("EXISTS after rejected SET PX -1 -> 0", bs("EXISTS"), pxNeg)

	// A non-integer EX value is the not-an-integer error (distinct text), also byte-checked.
	exNaN := d.k("set-exnan")
	d.eq("SET EX notanint -> not-integer", bs("SET"), exNaN, bs("v"), bs("EX"), bs("abc"))

	// SETEX / PSETEX with <= 0 reject with the (per-command) invalid-expire-time error.
	se0 := d.k("setex0")
	d.eq("SETEX 0 -> invalid expire", bs("SETEX"), se0, bs("0"), bs("v"))
	d.eq("EXISTS after rejected SETEX 0 -> 0", bs("EXISTS"), se0)

	seNeg := d.k("setexneg")
	d.eq("SETEX -5 -> invalid expire", bs("SETEX"), seNeg, bs("-5"), bs("v"))

	pse0 := d.k("psetex0")
	d.eq("PSETEX 0 -> invalid expire", bs("PSETEX"), pse0, bs("0"), bs("v"))
	d.eq("EXISTS after rejected PSETEX 0 -> 0", bs("EXISTS"), pse0)

	pseNeg := d.k("psetexneg")
	d.eq("PSETEX -1000 -> invalid expire", bs("PSETEX"), pseNeg, bs("-1000"), bs("v"))
}

// TestDiffExpirePastDeletes covers GAP 2 + GAP 9 + part of GAP 3: an EXPIRE with a
// resolved expiry in the past (negative/zero TTL, or an EXPIREAT/PEXPIREAT with a past
// absolute epoch) deletes the LIVE key immediately and replies :1 — distinct from :0
// on an absent key. After deletion the key is gone (GET nil, EXISTS 0, TTL -2, TYPE
// none) on both sides.
func TestDiffExpirePastDeletes(t *testing.T) {
	d := newDiffer(t)

	// EXPIRE k -1 on a live key -> :1 and the key is deleted.
	neg := d.k("expire-neg")
	d.eq("SET", bs("SET"), neg, bs("v"))
	d.eq("EXPIRE -1 -> 1 (delete live key)", bs("EXPIRE"), neg, bs("-1"))
	d.eq("GET after EXPIRE -1 -> nil", bs("GET"), neg)
	d.eq("EXISTS after EXPIRE -1 -> 0", bs("EXISTS"), neg)
	d.eq("TTL after EXPIRE -1 -> -2", bs("TTL"), neg)
	d.eq("TYPE after EXPIRE -1 -> none", bs("TYPE"), neg)

	// EXPIRE k 0 (exactly-now boundary) also deletes.
	zero := d.k("expire-zero")
	d.eq("SET", bs("SET"), zero, bs("v"))
	d.eq("EXPIRE 0 -> 1 (delete live key)", bs("EXPIRE"), zero, bs("0"))
	d.eq("EXISTS after EXPIRE 0 -> 0", bs("EXISTS"), zero)

	// EXPIRE on an ABSENT key replies :0 (contrast with the :1 delete above).
	absent := d.k("expire-absent")
	d.eq("EXPIRE -1 on absent -> 0", bs("EXPIRE"), absent, bs("-1"))

	// EXPIREAT k 1 (epoch second 1, year 1970 — definitely past) deletes and replies :1.
	eat := d.k("expireat-past")
	d.eq("SET", bs("SET"), eat, bs("v"))
	d.eq("EXPIREAT 1 -> 1 (delete live key)", bs("EXPIREAT"), eat, bs("1"))
	d.eq("EXISTS after EXPIREAT 1 -> 0", bs("EXISTS"), eat)
	d.eq("TTL after EXPIREAT 1 -> -2", bs("TTL"), eat)

	// EXPIREAT k 0 (epoch 0) — past — deletes as well.
	eat0 := d.k("expireat-epoch0")
	d.eq("SET", bs("SET"), eat0, bs("v"))
	d.eq("EXPIREAT 0 -> 1 (delete live key)", bs("EXPIREAT"), eat0, bs("0"))
	d.eq("EXISTS after EXPIREAT 0 -> 0", bs("EXISTS"), eat0)

	// PEXPIREAT with a past millisecond timestamp (1000 ms = epoch second 1) deletes.
	pat := d.k("pexpireat-past")
	d.eq("SET", bs("SET"), pat, bs("v"))
	d.eq("PEXPIREAT 1000 -> 1 (delete live key)", bs("PEXPIREAT"), pat, bs("1000"))
	d.eq("EXISTS after PEXPIREAT 1000 -> 0", bs("EXISTS"), pat)

	// PEXPIRE with a negative millisecond TTL resolves to a past epoch -> delete, :1.
	pneg := d.k("pexpire-neg")
	d.eq("SET", bs("SET"), pneg, bs("v"))
	d.eq("PEXPIRE -1 -> 1 (delete live key)", bs("PEXPIRE"), pneg, bs("-1"))
	d.eq("EXISTS after PEXPIRE -1 -> 0", bs("EXISTS"), pneg)

	// Past-expire also works on a collection key (TTL is per-key meta, type-agnostic).
	coll := d.k("expire-list-past")
	d.eq("RPUSH", bs("RPUSH"), coll, bs("a"), bs("b"))
	d.eq("EXPIRE -1 on list -> 1 (delete)", bs("EXPIRE"), coll, bs("-1"))
	d.eq("EXISTS after list EXPIRE -1 -> 0", bs("EXISTS"), coll)
	d.eq("TYPE after list EXPIRE -1 -> none", bs("TYPE"), coll)
}

// TestDiffExpirePExpireReturnCode covers GAP 3's return-code parity for a sub-second /
// at-now PEXPIRE. The immediate integer reply matches on both sides; the post-state
// (whether the key survives its sub-second window) is the documented second-precision
// divergence and is deliberately NOT asserted here.
func TestDiffExpirePExpireReturnCode(t *testing.T) {
	d := newDiffer(t)

	// PEXPIRE k 1 on a live key: both reply :1 (redimos truncates to now and deletes;
	// Redis sets a 1ms TTL). Only the reply code is compared — it agrees.
	sub := d.k("pexpire-1ms")
	d.eq("SET", bs("SET"), sub, bs("v"))
	d.eq("PEXPIRE 1 -> 1", bs("PEXPIRE"), sub, bs("1"))

	// PEXPIRE on an absent key replies :0 on both.
	miss := d.k("pexpire-absent")
	d.eq("PEXPIRE 1 on absent -> 0", bs("PEXPIRE"), miss, bs("1"))

	// PEXPIREAT on an absent key -> :0.
	pmiss := d.k("pexpireat-absent")
	d.eq("PEXPIREAT 1000 on absent -> 0", bs("PEXPIREAT"), pmiss, bs("1000"))
}

// TestDiffTTLPreservingStringOps covers GAP 4 + GAP 5 + GAP 7: value-only String
// mutations keep the key's TTL, while a fresh SET (and GETSET) clear it.
//
//   - APPEND, SETRANGE, INCR/DECR/INCRBY/DECRBY all preserve meta.exp -> TTL stays ~999s.
//   - GETSET clears TTL like a plain SET -> TTL -1.
//   - SET XX on a live key clears the existing TTL like any plain SET -> TTL -1.
//   - SET NX on an absent key creates it with no TTL.
//
// TTL is a countdown that can differ by a second between the two endpoints across the
// extra round-trip, so the "preserved" TTLs are compared with a small tolerance.
func TestDiffTTLPreservingStringOps(t *testing.T) {
	d := newDiffer(t)

	// APPEND preserves TTL.
	ap := d.k("ttl-append")
	d.eq("SET EX 1000", bs("SET"), ap, bs("v"), bs("EX"), bs("1000"))
	d.eq("APPEND", bs("APPEND"), ap, bs("xyz"))
	d.eqIntClose("TTL after APPEND ~1000", 2, bs("TTL"), ap)

	// SETRANGE preserves TTL.
	sr := d.k("ttl-setrange")
	d.eq("SET EX 1000", bs("SET"), sr, bs("hello"), bs("EX"), bs("1000"))
	d.eq("SETRANGE", bs("SETRANGE"), sr, bs("1"), bs("ELL"))
	d.eqIntClose("TTL after SETRANGE ~1000", 2, bs("TTL"), sr)

	// INCR preserves TTL.
	in := d.k("ttl-incr")
	d.eq("SET 5 EX 1000", bs("SET"), in, bs("5"), bs("EX"), bs("1000"))
	d.eq("INCR", bs("INCR"), in)
	d.eqIntClose("TTL after INCR ~1000", 2, bs("TTL"), in)

	// DECR preserves TTL.
	de := d.k("ttl-decr")
	d.eq("SET 5 EX 1000", bs("SET"), de, bs("5"), bs("EX"), bs("1000"))
	d.eq("DECR", bs("DECR"), de)
	d.eqIntClose("TTL after DECR ~1000", 2, bs("TTL"), de)

	// INCRBY preserves TTL.
	ib := d.k("ttl-incrby")
	d.eq("SET 5 EX 1000", bs("SET"), ib, bs("5"), bs("EX"), bs("1000"))
	d.eq("INCRBY 10", bs("INCRBY"), ib, bs("10"))
	d.eqIntClose("TTL after INCRBY ~1000", 2, bs("TTL"), ib)

	// DECRBY preserves TTL.
	db := d.k("ttl-decrby")
	d.eq("SET 5 EX 1000", bs("SET"), db, bs("5"), bs("EX"), bs("1000"))
	d.eq("DECRBY 3", bs("DECRBY"), db, bs("3"))
	d.eqIntClose("TTL after DECRBY ~1000", 2, bs("TTL"), db)

	// GETSET clears TTL (like a fresh SET).
	gs := d.k("ttl-getset")
	d.eq("SET EX 1000", bs("SET"), gs, bs("old"), bs("EX"), bs("1000"))
	d.eq("GETSET -> old", bs("GETSET"), gs, bs("new"))
	d.eq("TTL after GETSET -> -1", bs("TTL"), gs)

	// SET XX on a live key replaces the value but — like any plain SET without
	// EX/PX/KEEPTTL (KEEPTTL does not exist in Redis 3.2) — CLEARS the existing TTL
	// on both endpoints, so TTL reads back as -1.
	xx := d.k("ttl-setxx")
	d.eq("SET EX 1000", bs("SET"), xx, bs("v"), bs("EX"), bs("1000"))
	d.eq("SET v2 XX -> OK", bs("SET"), xx, bs("v2"), bs("XX"))
	d.eq("TTL after SET XX -> -1", bs("TTL"), xx)
	d.eq("GET after SET XX -> v2", bs("GET"), xx)

	// SET NX on an absent key creates it with NO TTL.
	nx := d.k("ttl-setnx")
	d.eq("SET NX -> OK", bs("SET"), nx, bs("v"), bs("NX"))
	d.eq("TTL after SET NX -> -1", bs("TTL"), nx)
}

// TestDiffTTLMSetClears covers GAP 6: MSET (and a fresh MSETNX) clears any prior TTL
// on every key it writes, so TTL reads back as -1.
func TestDiffTTLMSetClears(t *testing.T) {
	d := newDiffer(t)

	// MSET over two TTL'd keys clears both TTLs.
	k1 := d.k("mset-ttl1")
	k2 := d.k("mset-ttl2")
	d.eq("SET k1 EX 1000", bs("SET"), k1, bs("v1"), bs("EX"), bs("1000"))
	d.eq("SET k2 EX 1000", bs("SET"), k2, bs("v2"), bs("EX"), bs("1000"))
	d.eq("MSET k1 new1 k2 new2 -> OK", bs("MSET"), k1, bs("new1"), k2, bs("new2"))
	d.eq("TTL k1 after MSET -> -1", bs("TTL"), k1)
	d.eq("TTL k2 after MSET -> -1", bs("TTL"), k2)

	// MSETNX only fires when all keys are absent, so it creates fresh keys with no TTL.
	n1 := d.k("msetnx-ttl1")
	n2 := d.k("msetnx-ttl2")
	d.eq("MSETNX n1 v1 n2 v2 -> 1", bs("MSETNX"), n1, bs("v1"), n2, bs("v2"))
	d.eq("TTL n1 after MSETNX -> -1", bs("TTL"), n1)
	d.eq("TTL n2 after MSETNX -> -1", bs("TTL"), n2)
}

// TestDiffTTLCollectionMutations covers GAP 8: pushing more members onto a TTL'd list
// preserves its TTL (TTL is per-key metadata, independent of the collection's growth),
// and a String op / list op against a key of the wrong type still replies WRONGTYPE
// even while that key carries a TTL.
func TestDiffTTLCollectionMutations(t *testing.T) {
	d := newDiffer(t)

	// RPUSH then LPUSH under an active TTL: TTL is unchanged.
	lst := d.k("ttl-list")
	d.eq("RPUSH", bs("RPUSH"), lst, bs("a"))
	d.eq("EXPIRE 1000 -> 1", bs("EXPIRE"), lst, bs("1000"))
	d.eq("LPUSH b -> 2", bs("LPUSH"), lst, bs("b"))
	d.eq("RPUSH c -> 3", bs("RPUSH"), lst, bs("c"))
	d.eqIntClose("TTL after list mutation ~1000", 2, bs("TTL"), lst)
	d.eq("LLEN after mutation -> 3", bs("LLEN"), lst)

	// SADD under an active TTL preserves TTL.
	st := d.k("ttl-set")
	d.eq("SADD", bs("SADD"), st, bs("x"))
	d.eq("EXPIRE 1000 -> 1", bs("EXPIRE"), st, bs("1000"))
	d.eq("SADD y -> 1", bs("SADD"), st, bs("y"))
	d.eqIntClose("TTL after SADD ~1000", 2, bs("TTL"), st)

	// HSET under an active TTL preserves TTL.
	ht := d.k("ttl-hash")
	d.eq("HSET", bs("HSET"), ht, bs("f"), bs("1"))
	d.eq("EXPIRE 1000 -> 1", bs("EXPIRE"), ht, bs("1000"))
	d.eq("HSET g 2 -> 1", bs("HSET"), ht, bs("g"), bs("2"))
	d.eqIntClose("TTL after HSET ~1000", 2, bs("TTL"), ht)

	// ZADD under an active TTL preserves TTL.
	zt := d.k("ttl-zset")
	d.eq("ZADD", bs("ZADD"), zt, bs("1"), bs("m1"))
	d.eq("EXPIRE 1000 -> 1", bs("EXPIRE"), zt, bs("1000"))
	d.eq("ZADD 2 m2 -> 1", bs("ZADD"), zt, bs("2"), bs("m2"))
	d.eqIntClose("TTL after ZADD ~1000", 2, bs("TTL"), zt)

	// A TTL'd String hit with a list op replies WRONGTYPE (the TTL does not change the type error).
	wt := d.k("ttl-wrongtype")
	d.eq("SET EX 1000", bs("SET"), wt, bs("v"), bs("EX"), bs("1000"))
	d.eq("LPUSH on TTL'd string -> WRONGTYPE", bs("LPUSH"), wt, bs("a"))
	d.eq("SADD on TTL'd string -> WRONGTYPE", bs("SADD"), wt, bs("a"))
	// The failed type-mismatch op must not have disturbed the TTL.
	d.eqIntClose("TTL after WRONGTYPE ~1000", 2, bs("TTL"), wt)
}
