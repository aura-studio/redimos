package integration

import (
	"fmt"
	"strconv"
	"testing"
)

// Dimensions O (encoding invariance) and P (type overwrite / key-creation),
// deepened. Every case here is a byte-for-byte (or sorted-multiset) differential
// against a live Redis 3.2 oracle; the harness catches any divergence, so we only
// enumerate the exact edge inputs — never hardcode expected replies.
//
// The "expired collection" gaps (GAP 1, 4, 5, 7) are exercised through the ONLY
// observable client path: expire a live collection into the past with EXPIREAT k 1
// (epoch 1 = 1970-01-01, unconditionally in the past on both sides), which makes
// the key logically absent, then drive the string-creating command over it. On
// redimos this hits the internal stale-member-reclamation path (DeleteMembers in
// the NX / overwriteAnyType branches); on Redis the key is simply gone. The
// observable result — TYPE, GET/STRLEN, and a follow-up collection op returning
// WRONGTYPE — must agree, and any leaked stale member would surface as a divergent
// collection read (HGETALL / SMEMBERS / LRANGE / ZRANGE) after the overwrite.

// pastEpoch is an EXPIREAT argument guaranteed to be in the past on both endpoints
// (1 second after the Unix epoch). Redis deletes the key; redimos marks its meta
// expired so the key reads as logically absent.
const pastEpoch = "1"

// seedHash / seedSet / seedList / seedZSet append a collection-building command to
// both sides via d.eq so proxy and oracle reach identical state before the test.
func seedHash(d *differ, k []byte, n int) {
	args := [][]byte{bs("HMSET"), k}
	for i := 0; i < n; i++ {
		args = append(args, bs(fmt.Sprintf("f%d", i)), bs(fmt.Sprintf("v%d", i)))
	}
	d.eq("seed hash", args...)
}

func seedSet(d *differ, k []byte, n int) {
	args := [][]byte{bs("SADD"), k}
	for i := 0; i < n; i++ {
		args = append(args, bs(fmt.Sprintf("m%d", i)))
	}
	d.eq("seed set", args...)
}

func seedList(d *differ, k []byte, elems ...string) {
	args := [][]byte{bs("RPUSH"), k}
	for _, e := range elems {
		args = append(args, bs(e))
	}
	d.eq("seed list", args...)
}

func seedZSet(d *differ, k []byte, n int) {
	args := [][]byte{bs("ZADD"), k}
	for i := 0; i < n; i++ {
		args = append(args, bs(strconv.Itoa(i)), bs(fmt.Sprintf("z%d", i)))
	}
	d.eq("seed zset", args...)
}

// -----------------------------------------------------------------------------
// Dimension P: string-creating writes over an EXPIRED collection (GAP 1,4,5,7).
// After the collection is logically gone, the write must (a) succeed as if the key
// were missing, (b) leave a plain string, and (c) leave NO trace of the old
// collection — verified by a same-type collection read that must be empty/absent
// and identical on both sides.
// -----------------------------------------------------------------------------

// TestDiffOverwrite_SetNXoverExpiredCollection covers GAP 1: SET NX / SETNX on an
// expired collection succeeds (key logically absent) and the stale members must be
// fully reclaimed, not merely hidden.
func TestDiffOverwrite_SetNXoverExpiredCollection(t *testing.T) {
	d := newDiffer(t)

	// SET NX over an expired hash.
	kh := d.k("nx-exp-hash")
	seedHash(d, kh, 10)
	d.eq("EXPIREAT hash past", bs("EXPIREAT"), kh, bs(pastEpoch))
	d.eq("EXISTS after expire", bs("EXISTS"), kh)
	d.eq("TYPE after expire (none)", bs("TYPE"), kh)
	d.eq("SET NX over expired hash -> +OK", bs("SET"), kh, bs("fresh"), bs("NX"))
	d.eq("TYPE now string", bs("TYPE"), kh)
	d.eq("GET fresh", bs("GET"), kh)
	d.eq("HGET on new string -> WRONGTYPE", bs("HGET"), kh, bs("f0"))
	// No stale members must remain: a hash read on the now-string key is WRONGTYPE
	// on both sides, and a fresh hash op after DEL must see nothing extra.
	d.eq("HLEN on new string -> WRONGTYPE", bs("HLEN"), kh)

	// SETNX (the standalone command) over an expired set.
	ks := d.k("setnx-exp-set")
	seedSet(d, ks, 12)
	d.eq("EXPIREAT set past", bs("EXPIREAT"), ks, bs(pastEpoch))
	d.eq("SETNX over expired set -> :1", bs("SETNX"), ks, bs("fresh"))
	d.eq("TYPE now string", bs("TYPE"), ks)
	d.eq("GET fresh", bs("GET"), ks)
	d.eq("SMEMBERS on new string -> WRONGTYPE", bs("SMEMBERS"), ks)
	d.eq("SCARD on new string -> WRONGTYPE", bs("SCARD"), ks)

	// SETNX must still REJECT a live string (returns :0), independent of prior type.
	kl := d.k("setnx-live")
	d.eq("SET live", bs("SET"), kl, bs("v1"))
	d.eq("SETNX over live string -> :0", bs("SETNX"), kl, bs("v2"))
	d.eq("GET unchanged", bs("GET"), kl)
}

// TestDiffOverwrite_GetSetOverExpiredCollection covers GAP 4: GETSET over an
// expired collection returns nil (no previous value) and installs a fresh string,
// leaving no stale members.
func TestDiffOverwrite_GetSetOverExpiredCollection(t *testing.T) {
	d := newDiffer(t)

	kh := d.k("getset-exp-hash")
	seedHash(d, kh, 5)
	d.eq("EXPIREAT hash past", bs("EXPIREAT"), kh, bs(pastEpoch))
	d.eq("GETSET over expired hash -> nil", bs("GETSET"), kh, bs("newval"))
	d.eq("TYPE now string", bs("TYPE"), kh)
	d.eq("GET newval", bs("GET"), kh)
	d.eq("HGETALL on new string -> WRONGTYPE", bs("HGETALL"), kh)

	// GETSET over an expired zset.
	kz := d.k("getset-exp-zset")
	seedZSet(d, kz, 5)
	d.eq("EXPIREAT zset past", bs("EXPIREAT"), kz, bs(pastEpoch))
	d.eq("GETSET over expired zset -> nil", bs("GETSET"), kz, bs("nv"))
	d.eq("TYPE now string", bs("TYPE"), kz)
	d.eq("ZRANGE on new string -> WRONGTYPE", bs("ZRANGE"), kz, bs("0"), bs("-1"))
}

// TestDiffOverwrite_AppendSetbitSetrangeOverExpiredCollection covers GAP 5: APPEND
// / SETBIT / SETRANGE on an expired collection create a fresh string as if the key
// were missing.
func TestDiffOverwrite_AppendSetbitSetrangeOverExpiredCollection(t *testing.T) {
	d := newDiffer(t)

	// APPEND over an expired list.
	kl := d.k("append-exp-list")
	seedList(d, kl, "a", "b", "c")
	d.eq("EXPIREAT list past", bs("EXPIREAT"), kl, bs(pastEpoch))
	d.eq("APPEND over expired list -> len", bs("APPEND"), kl, bs("suffix"))
	d.eq("TYPE now string", bs("TYPE"), kl)
	d.eq("GET suffix", bs("GET"), kl)
	d.eq("LRANGE on new string -> WRONGTYPE", bs("LRANGE"), kl, bs("0"), bs("-1"))
	d.eq("STRLEN", bs("STRLEN"), kl)

	// SETBIT over an expired set.
	ks := d.k("setbit-exp-set")
	seedSet(d, ks, 8)
	d.eq("EXPIREAT set past", bs("EXPIREAT"), ks, bs(pastEpoch))
	d.eq("SETBIT over expired set -> old bit 0", bs("SETBIT"), ks, bs("5"), bs("1"))
	d.eq("TYPE now string", bs("TYPE"), ks)
	d.eq("STRLEN setbit", bs("STRLEN"), ks)
	d.eq("SMEMBERS on new string -> WRONGTYPE", bs("SMEMBERS"), ks)

	// SETRANGE over an expired hash (zero-fills the gap).
	kh := d.k("setrange-exp-hash")
	seedHash(d, kh, 6)
	d.eq("EXPIREAT hash past", bs("EXPIREAT"), kh, bs(pastEpoch))
	d.eq("SETRANGE over expired hash -> len", bs("SETRANGE"), kh, bs("3"), bs("XY"))
	d.eq("TYPE now string", bs("TYPE"), kh)
	d.eq("GET setrange (zero-filled)", bs("GET"), kh)
	d.eq("STRLEN setrange", bs("STRLEN"), kh)
	d.eq("HKEYS on new string -> WRONGTYPE", bs("HKEYS"), kh)
}

// TestDiffOverwrite_IncrOverExpiredCollection covers GAP 7: INCR / INCRBY /
// INCRBYFLOAT / DECR on an expired collection treat the key as missing and create a
// fresh integer/float string, while INCR on a LIVE collection is WRONGTYPE.
func TestDiffOverwrite_IncrOverExpiredCollection(t *testing.T) {
	d := newDiffer(t)

	// INCR over an expired hash -> creates string "1".
	kh := d.k("incr-exp-hash")
	seedHash(d, kh, 3)
	d.eq("EXPIREAT hash past", bs("EXPIREAT"), kh, bs(pastEpoch))
	d.eq("INCR over expired hash -> :1", bs("INCR"), kh)
	d.eq("TYPE now string", bs("TYPE"), kh)
	d.eq("GET =1", bs("GET"), kh)
	d.eq("HGET on new string -> WRONGTYPE", bs("HGET"), kh, bs("f0"))

	// INCRBY over an expired list.
	kl := d.k("incrby-exp-list")
	seedList(d, kl, "x", "y")
	d.eq("EXPIREAT list past", bs("EXPIREAT"), kl, bs(pastEpoch))
	d.eq("INCRBY 41 over expired list -> :41", bs("INCRBY"), kl, bs("41"))
	d.eq("GET =41", bs("GET"), kl)
	d.eq("LLEN on new string -> WRONGTYPE", bs("LLEN"), kl)

	// INCRBYFLOAT over an expired set (exact score formatting, byte-for-byte).
	ks := d.k("incrbyfloat-exp-set")
	seedSet(d, ks, 4)
	d.eq("EXPIREAT set past", bs("EXPIREAT"), ks, bs(pastEpoch))
	d.eq("INCRBYFLOAT 3.5 over expired set", bs("INCRBYFLOAT"), ks, bs("3.5"))
	d.eq("GET =3.5", bs("GET"), ks)
	d.eq("SCARD on new string -> WRONGTYPE", bs("SCARD"), ks)

	// DECR over an expired zset -> creates string "-1".
	kz := d.k("decr-exp-zset")
	seedZSet(d, kz, 3)
	d.eq("EXPIREAT zset past", bs("EXPIREAT"), kz, bs(pastEpoch))
	d.eq("DECR over expired zset -> :-1", bs("DECR"), kz)
	d.eq("GET =-1", bs("GET"), kz)
	d.eq("ZCARD on new string -> WRONGTYPE", bs("ZCARD"), kz)

	// INCR on a LIVE collection is WRONGTYPE (the contrast case).
	live := d.k("incr-live-hash")
	d.eq("HSET live hash", bs("HSET"), live, bs("f"), bs("v"))
	d.eq("INCR on live hash -> WRONGTYPE", bs("INCR"), live)
	d.eq("INCRBYFLOAT on live hash -> WRONGTYPE", bs("INCRBYFLOAT"), live, bs("1.0"))
	d.eq("APPEND on live hash -> WRONGTYPE", bs("APPEND"), live, bs("x"))
	d.eq("SETRANGE on live hash -> WRONGTYPE", bs("SETRANGE"), live, bs("0"), bs("x"))
	d.eq("SETBIT on live hash -> WRONGTYPE", bs("SETBIT"), live, bs("0"), bs("1"))
}

// TestDiffOverwrite_SetXXoverCollection covers GAP 8: SET XX succeeds over a LIVE
// key of any collection type (XX passes because the key exists) and overwrites it
// as a string; SET XX on a missing key is rejected (nil), and SET XX on an EXPIRED
// collection is also rejected (logically absent).
func TestDiffOverwrite_SetXXoverCollection(t *testing.T) {
	d := newDiffer(t)

	cases := []struct {
		name    string
		create  [][]byte
		wrongOp [][]byte
	}{
		{"hash", [][]byte{bs("HSET"), nil, bs("f"), bs("v")}, [][]byte{bs("HGET"), nil, bs("f")}},
		{"set", [][]byte{bs("SADD"), nil, bs("m")}, [][]byte{bs("SMEMBERS"), nil}},
		{"list", [][]byte{bs("RPUSH"), nil, bs("e")}, [][]byte{bs("LRANGE"), nil, bs("0"), bs("-1")}},
		{"zset", [][]byte{bs("ZADD"), nil, bs("1"), bs("m")}, [][]byte{bs("ZRANGE"), nil, bs("0"), bs("-1")}},
	}
	for _, c := range cases {
		k := d.k("xx-" + c.name)
		create := append([][]byte{}, c.create...)
		create[1] = k
		d.eq("create "+c.name, create...)
		d.eq("SET XX over live "+c.name+" -> +OK", bs("SET"), k, bs("nowstring"), bs("XX"))
		d.eq("TYPE now string", bs("TYPE"), k)
		d.eq("GET nowstring", bs("GET"), k)
		wrong := append([][]byte{}, c.wrongOp...)
		wrong[1] = k
		d.eq(c.name+" op now WRONGTYPE", wrong...)
	}

	// SET XX on a missing key -> nil (rejected).
	miss := d.k("xx-missing")
	d.eq("SET XX missing -> nil", bs("SET"), miss, bs("v"), bs("XX"))
	d.eq("EXISTS still 0", bs("EXISTS"), miss)

	// SET XX on an EXPIRED collection -> nil (logically absent, XX fails).
	exp := d.k("xx-expired-list")
	seedList(d, exp, "a", "b")
	d.eq("EXPIREAT list past", bs("EXPIREAT"), exp, bs(pastEpoch))
	d.eq("SET XX over expired list -> nil", bs("SET"), exp, bs("v"), bs("XX"))
	d.eq("TYPE after (none)", bs("TYPE"), exp)
}

// TestDiffOverwrite_SetexPsetexOverCollection covers GAP 9: SETEX / PSETEX
// unconditionally overwrite a collection of any type as a string with a TTL.
func TestDiffOverwrite_SetexPsetexOverCollection(t *testing.T) {
	d := newDiffer(t)

	// SETEX over a zset.
	kz := d.k("setex-zset")
	seedZSet(d, kz, 5)
	d.eq("SETEX over zset -> +OK", bs("SETEX"), kz, bs("60"), bs("newval"))
	d.eq("TYPE now string", bs("TYPE"), kz)
	d.eq("GET newval", bs("GET"), kz)
	d.eq("ZRANGE on new string -> WRONGTYPE", bs("ZRANGE"), kz, bs("0"), bs("-1"))
	// TTL is present and in (0,60]; both sides tick, so compare with a small tolerance.
	d.eqIntClose("TTL ~60", 2, bs("TTL"), kz)

	// PSETEX over a hash.
	kh := d.k("psetex-hash")
	seedHash(d, kh, 7)
	d.eq("PSETEX over hash -> +OK", bs("PSETEX"), kh, bs("60000"), bs("hv"))
	d.eq("TYPE now string", bs("TYPE"), kh)
	d.eq("GET hv", bs("GET"), kh)
	d.eq("HGETALL on new string -> WRONGTYPE", bs("HGETALL"), kh)
	d.eqIntClose("TTL ~60", 2, bs("TTL"), kh)

	// SETEX over a set and a list.
	ks := d.k("setex-set")
	seedSet(d, ks, 6)
	d.eq("SETEX over set -> +OK", bs("SETEX"), ks, bs("100"), bs("sv"))
	d.eq("TYPE now string", bs("TYPE"), ks)
	d.eq("SMEMBERS on new string -> WRONGTYPE", bs("SMEMBERS"), ks)

	kl := d.k("setex-list")
	seedList(d, kl, "a", "b", "c")
	d.eq("SETEX over list -> +OK", bs("SETEX"), kl, bs("100"), bs("lv"))
	d.eq("TYPE now string", bs("TYPE"), kl)
	d.eq("LRANGE on new string -> WRONGTYPE", bs("LRANGE"), kl, bs("0"), bs("-1"))

	// SETEX / PSETEX non-positive expiry is an invalid-expire-time error, and a
	// non-integer expiry is a not-an-integer error — arity/validation boundaries.
	inv := d.k("setex-invalid")
	d.eq("SETEX 0 -> invalid expire", bs("SETEX"), inv, bs("0"), bs("v"))
	d.eq("SETEX -5 -> invalid expire", bs("SETEX"), inv, bs("-5"), bs("v"))
	d.eq("SETEX notint -> not integer", bs("SETEX"), inv, bs("abc"), bs("v"))
	d.eq("PSETEX 0 -> invalid expire", bs("PSETEX"), inv, bs("0"), bs("v"))
	d.eq("still absent after invalid setex", bs("EXISTS"), inv)
}

// -----------------------------------------------------------------------------
// GAP 3: MSET / MSETNX over mixed collection types.
// -----------------------------------------------------------------------------

// TestDiffOverwrite_MsetOverMixedTypes covers GAP 3 (MSET half): MSET overwrites
// keys of any prior type (list, hash, zset, set, string) all as strings in one
// call; every prior collection is gone afterward.
func TestDiffOverwrite_MsetOverMixedTypes(t *testing.T) {
	d := newDiffer(t)

	k1 := d.k("mset-list")
	k2 := d.k("mset-hash")
	k3 := d.k("mset-zset")
	k4 := d.k("mset-set")
	seedList(d, k1, "a", "b", "c")
	seedHash(d, k2, 4)
	seedZSet(d, k3, 4)
	seedSet(d, k4, 4)

	d.eq("MSET over mixed types -> +OK",
		bs("MSET"), k1, bs("v1"), k2, bs("v2"), k3, bs("v3"), k4, bs("v4"))

	d.eq("TYPE k1 string", bs("TYPE"), k1)
	d.eq("TYPE k2 string", bs("TYPE"), k2)
	d.eq("TYPE k3 string", bs("TYPE"), k3)
	d.eq("TYPE k4 string", bs("TYPE"), k4)
	d.eq("MGET all four", bs("MGET"), k1, k2, k3, k4)

	// The old collections must leave no trace: each collection op is now WRONGTYPE.
	d.eq("LRANGE k1 -> WRONGTYPE", bs("LRANGE"), k1, bs("0"), bs("-1"))
	d.eq("HGETALL k2 -> WRONGTYPE", bs("HGETALL"), k2)
	d.eq("ZRANGE k3 -> WRONGTYPE", bs("ZRANGE"), k3, bs("0"), bs("-1"))
	d.eq("SMEMBERS k4 -> WRONGTYPE", bs("SMEMBERS"), k4)
}

// TestDiffOverwrite_MsetnxOverMixedTypes covers GAP 3 (MSETNX half): MSETNX aborts
// the whole batch (:0, no writes) if ANY target key already exists — including a
// live collection — and only sets when ALL are absent (:1). It also treats an
// expired collection as absent.
func TestDiffOverwrite_MsetnxOverMixedTypes(t *testing.T) {
	d := newDiffer(t)

	// One live collection among fresh keys -> MSETNX must be all-or-nothing :0.
	fresh1 := d.k("msetnx-fresh1")
	existingHash := d.k("msetnx-hash")
	fresh2 := d.k("msetnx-fresh2")
	seedHash(d, existingHash, 3)

	d.eq("MSETNX with one existing collection -> :0",
		bs("MSETNX"), fresh1, bs("a"), existingHash, bs("b"), fresh2, bs("c"))
	// None of the fresh keys may have been created (all-or-nothing).
	d.eq("fresh1 still absent", bs("EXISTS"), fresh1)
	d.eq("fresh2 still absent", bs("EXISTS"), fresh2)
	// The pre-existing hash is untouched (still a hash).
	d.eq("existing hash TYPE unchanged", bs("TYPE"), existingHash)
	d.eqSorted("existing hash HGETALL unchanged", bs("HGETALL"), existingHash)

	// All-absent -> MSETNX sets all as strings (:1).
	a := d.k("msetnx-a")
	b := d.k("msetnx-b")
	cc := d.k("msetnx-c")
	d.eq("MSETNX all absent -> :1", bs("MSETNX"), a, bs("1"), b, bs("2"), cc, bs("3"))
	d.eq("TYPE a string", bs("TYPE"), a)
	d.eq("MGET a b c", bs("MGET"), a, b, cc)

	// A second MSETNX touching any of them now fails (:0) and leaves them unchanged.
	dkey := d.k("msetnx-d")
	d.eq("MSETNX re-touch existing -> :0", bs("MSETNX"), a, bs("X"), dkey, bs("Y"))
	d.eq("a unchanged", bs("GET"), a)
	d.eq("d not created", bs("EXISTS"), dkey)

	// MSETNX treats an EXPIRED collection as absent, so a batch over it succeeds.
	expZ := d.k("msetnx-expired-zset")
	seedZSet(d, expZ, 4)
	d.eq("EXPIREAT zset past", bs("EXPIREAT"), expZ, bs(pastEpoch))
	otherFresh := d.k("msetnx-otherfresh")
	d.eq("MSETNX over expired zset + fresh -> :1",
		bs("MSETNX"), expZ, bs("z"), otherFresh, bs("o"))
	d.eq("TYPE expired-now string", bs("TYPE"), expZ)
	d.eq("GET expired-now value", bs("GET"), expZ)
	d.eq("ZRANGE on new string -> WRONGTYPE", bs("ZRANGE"), expZ, bs("0"), bs("-1"))
}

// -----------------------------------------------------------------------------
// GAP 6: overwrite must reset the member count — a fresh collection built on the
// same key after an overwrite must report exactly its own members, no stale count.
// -----------------------------------------------------------------------------

// TestDiffOverwrite_CountResetAfterOverwrite covers GAP 6: after SET overwrites a
// populated collection to a string, DEL'ing and rebuilding a NEW collection on the
// same key must report only the new members' count (no leaked meta.cnt) AND expose no
// stale items.
//
// This is also the regression guard for a real phantom-empty-member bug: the String value
// item written by "SET over <collection>" lives at the reserved 0x00 sort key, which a
// collection's empty member also encodes to. If a DEL'd String's data reclaim were left to
// the async deleter, that deleter would skip the key once it was rebuilt as a live
// collection, and the orphaned value item would surface as a phantom empty "" member/field
// in SMEMBERS/HKEYS under load. handleDel now reclaims a String's single value item
// SYNCHRONOUSLY, so this sequence is deterministic and the exact-membership checks below
// hold even under heavy concurrent load.
func TestDiffOverwrite_CountResetAfterOverwrite(t *testing.T) {
	d := newDiffer(t)

	// Set (10 members) -> SET string -> DEL -> rebuild set with 3 members.
	k := d.k("cnt-set")
	seedSet(d, k, 10)
	d.eq("SCARD 10", bs("SCARD"), k)
	d.eq("SET over set -> string", bs("SET"), k, bs("plain"))
	d.eq("TYPE string", bs("TYPE"), k)
	d.eq("STRLEN plain", bs("STRLEN"), k)
	d.eq("DEL string", bs("DEL"), k)
	// Rebuild a smaller set on the same key: count must be exactly 3, members exactly {x,y,z}.
	d.eq("SADD 3 fresh members", bs("SADD"), k, bs("x"), bs("y"), bs("z"))
	d.eq("SCARD 3 (no stale count)", bs("SCARD"), k)
	d.eqSorted("SMEMBERS exactly x,y,z (no phantom empty member)", bs("SMEMBERS"), k)

	// Hash (10) -> SET string -> rebuild hash directly (SET already cleared it) with 2.
	kh := d.k("cnt-hash")
	seedHash(d, kh, 10)
	d.eq("HLEN 10", bs("HLEN"), kh)
	d.eq("SET over hash -> string", bs("SET"), kh, bs("plain"))
	d.eq("DEL string", bs("DEL"), kh)
	d.eq("HSET 2 fresh fields", bs("HSET"), kh, bs("a"), bs("1"))
	d.eq("HSET 1 more field", bs("HSET"), kh, bs("b"), bs("2"))
	d.eq("HLEN 2 (no stale count)", bs("HLEN"), kh)
	d.eqSorted("HKEYS exactly a,b (no phantom empty field)", bs("HKEYS"), kh)

	// ZSet (10) -> SET string -> rebuild zset with 2, verify ZCARD/ZRANGE.
	kz := d.k("cnt-zset")
	seedZSet(d, kz, 10)
	d.eq("ZCARD 10", bs("ZCARD"), kz)
	d.eq("SET over zset -> string", bs("SET"), kz, bs("plain"))
	d.eq("DEL string", bs("DEL"), kz)
	d.eq("ZADD 2 fresh members", bs("ZADD"), kz, bs("1"), bs("aa"), bs("2"), bs("bb"))
	d.eq("ZCARD 2 (no stale count)", bs("ZCARD"), kz)
	d.eq("ZRANGE 0 -1 WITHSCORES", bs("ZRANGE"), kz, bs("0"), bs("-1"), bs("WITHSCORES"))

	// List (10) -> SET string -> rebuild list with 2, verify LLEN/LRANGE.
	kl := d.k("cnt-list")
	seedList(d, kl, "0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
	d.eq("LLEN 10", bs("LLEN"), kl)
	d.eq("SET over list -> string", bs("SET"), kl, bs("plain"))
	d.eq("DEL string", bs("DEL"), kl)
	d.eq("RPUSH 2 fresh elems", bs("RPUSH"), kl, bs("p"), bs("q"))
	d.eq("LLEN 2 (no stale count)", bs("LLEN"), kl)
	d.eq("LRANGE 0 -1 exactly p,q", bs("LRANGE"), kl, bs("0"), bs("-1"))
}

// -----------------------------------------------------------------------------
// Dimension O: encoding-threshold invariance at the EXACT boundaries (GAP 2).
// Redis flips hash/zset ziplist->hashtable/skiplist at 128 entries; the observable
// commands must be identical at 127 / 128 / 129 and for a large value that trips
// the value-size threshold (hash-max-ziplist-value = 64 bytes in 3.2).
// -----------------------------------------------------------------------------

// TestDiffEncoding_HashBoundary covers GAP 2 (hash): 127 / 128 / 129 members, plus
// a member value longer than the 64-byte ziplist-value threshold.
func TestDiffEncoding_HashBoundary(t *testing.T) {
	d := newDiffer(t)

	for _, n := range []int{127, 128, 129} {
		k := d.k(fmt.Sprintf("hb%d", n))
		args := [][]byte{bs("HMSET"), k}
		for i := 0; i < n; i++ {
			args = append(args, bs(fmt.Sprintf("f%d", i)), bs(fmt.Sprintf("v%d", i)))
		}
		d.eq(fmt.Sprintf("HMSET %d", n), args...)
		d.eq(fmt.Sprintf("HLEN %d", n), bs("HLEN"), k)
		d.eqSorted(fmt.Sprintf("HKEYS %d", n), bs("HKEYS"), k)
		d.eqSorted(fmt.Sprintf("HGETALL %d", n), bs("HGETALL"), k)
		d.eq(fmt.Sprintf("HGET f0 (%d)", n), bs("HGET"), k, bs("f0"))
		d.eq(fmt.Sprintf("HGET last (%d)", n), bs("HGET"), k, bs(fmt.Sprintf("f%d", n-1)))
		d.eq(fmt.Sprintf("HEXISTS mid (%d)", n), bs("HEXISTS"), k, bs(fmt.Sprintf("f%d", n/2)))
	}

	// Value-size threshold: a single field whose value exceeds 64 bytes forces
	// hashtable encoding in Redis 3.2; the observable HGET/HSTRLEN must not change.
	kv := d.k("hash-bigval")
	big := ""
	for i := 0; i < 100; i++ { // 100 bytes > 64
		big += "z"
	}
	d.eq("HSET small field", bs("HSET"), kv, bs("small"), bs("v"))
	d.eq("HSET big field (>64B value)", bs("HSET"), kv, bs("big"), bs(big))
	d.eq("HGET big", bs("HGET"), kv, bs("big"))
	d.eq("HSTRLEN big", bs("HSTRLEN"), kv, bs("big"))
	d.eq("HLEN 2", bs("HLEN"), kv)
	d.eqSorted("HGETALL big+small", bs("HGETALL"), kv)
}

// TestDiffEncoding_ZSetBoundary covers GAP 2 (zset): 127 / 128 / 129 members with
// ordered-range parity across the ziplist->skiplist threshold.
func TestDiffEncoding_ZSetBoundary(t *testing.T) {
	d := newDiffer(t)

	for _, n := range []int{127, 128, 129} {
		k := d.k(fmt.Sprintf("zb%d", n))
		args := [][]byte{bs("ZADD"), k}
		for i := 0; i < n; i++ {
			args = append(args, bs(strconv.Itoa(i)), bs(fmt.Sprintf("m%d", i)))
		}
		d.eq(fmt.Sprintf("ZADD %d", n), args...)
		d.eq(fmt.Sprintf("ZCARD %d", n), bs("ZCARD"), k)
		d.eq(fmt.Sprintf("ZRANGE 0 -1 WITHSCORES (%d)", n),
			bs("ZRANGE"), k, bs("0"), bs("-1"), bs("WITHSCORES"))
		d.eq(fmt.Sprintf("ZREVRANGE 0 4 (%d)", n), bs("ZREVRANGE"), k, bs("0"), bs("4"))
		d.eq(fmt.Sprintf("ZRANK last (%d)", n), bs("ZRANK"), k, bs(fmt.Sprintf("m%d", n-1)))
		d.eq(fmt.Sprintf("ZSCORE mid (%d)", n), bs("ZSCORE"), k, bs(fmt.Sprintf("m%d", n/2)))
		d.eq(fmt.Sprintf("ZCOUNT 0 %d (%d)", n-1, n),
			bs("ZCOUNT"), k, bs("0"), bs(strconv.Itoa(n-1)))
	}
}

// TestDiffEncoding_SetBoundary covers GAP 2 (set / intset): 511 / 512 / 513 integer
// members straddle the intset->hashtable threshold (set-max-intset-entries = 512),
// and adding a non-integer member forces the conversion. Observable behaviour must
// not change. Also checks the small 127/128/129 boundary for a string-set.
func TestDiffEncoding_SetBoundary(t *testing.T) {
	d := newDiffer(t)

	for _, n := range []int{511, 512, 513} {
		k := d.k(fmt.Sprintf("sb-int%d", n))
		args := [][]byte{bs("SADD"), k}
		for i := 0; i < n; i++ {
			args = append(args, bs(strconv.Itoa(i)))
		}
		d.eq(fmt.Sprintf("SADD %d ints", n), args...)
		d.eq(fmt.Sprintf("SCARD %d", n), bs("SCARD"), k)
		d.eqSorted(fmt.Sprintf("SMEMBERS %d", n), bs("SMEMBERS"), k)
		d.eq(fmt.Sprintf("SISMEMBER 0 (%d)", n), bs("SISMEMBER"), k, bs("0"))
		d.eq(fmt.Sprintf("SISMEMBER last (%d)", n), bs("SISMEMBER"), k, bs(strconv.Itoa(n-1)))
		// Force intset->hashtable by adding a non-integer; behaviour must be stable.
		d.eq(fmt.Sprintf("SADD non-int (%d)", n), bs("SADD"), k, bs("notanint"))
		d.eq(fmt.Sprintf("SCARD after (%d)", n), bs("SCARD"), k)
		d.eq(fmt.Sprintf("SISMEMBER notanint (%d)", n), bs("SISMEMBER"), k, bs("notanint"))
	}

	// String-set at the 127/128/129 boundary (always hashtable in Redis, but the
	// proxy has no encodings — confirm parity anyway).
	for _, n := range []int{127, 128, 129} {
		k := d.k(fmt.Sprintf("sb-str%d", n))
		args := [][]byte{bs("SADD"), k}
		for i := 0; i < n; i++ {
			args = append(args, bs(fmt.Sprintf("m%d", i)))
		}
		d.eq(fmt.Sprintf("SADD %d strings", n), args...)
		d.eq(fmt.Sprintf("SCARD %d", n), bs("SCARD"), k)
		d.eqSorted(fmt.Sprintf("SMEMBERS %d", n), bs("SMEMBERS"), k)
	}
}
