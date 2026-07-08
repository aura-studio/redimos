package integration

import "testing"

// Dimension L: mutation return-value & idempotency semantics. The exact integer a mutation
// returns is a distinct contract from its reply SHAPE (dimension C): SADD returns the number
// ADDED (not the total), ZADD the number added (CH: changed), HSET 1-new/0-updated, and a
// repeated no-op returns 0. These are easy to get subtly wrong on a proxy that maintains its
// own cardinality counter, so they are compared byte-for-byte with Redis 3.2.

func TestDiffReturnValues_Set(t *testing.T) {
	d := newDiffer(t)

	k := d.k("set")
	d.eq("SADD 3 new -> 3", bs("SADD"), k, bs("a"), bs("b"), bs("c"))
	d.eq("SADD all present -> 0", bs("SADD"), k, bs("a"), bs("b"))
	d.eq("SADD mixed -> 1", bs("SADD"), k, bs("a"), bs("d"))
	d.eq("SREM present -> 2", bs("SREM"), k, bs("a"), bs("b"))
	d.eq("SREM absent -> 0", bs("SREM"), k, bs("zzz"))
	d.eq("SREM mixed -> 1", bs("SREM"), k, bs("c"), bs("nope"))
	d.eq("SISMEMBER present -> 1", bs("SISMEMBER"), k, bs("d"))
	d.eq("SISMEMBER absent -> 0", bs("SISMEMBER"), k, bs("a"))

	// SMOVE: 1 when moved, 0 when the member is not in the source.
	src, dst := d.k("smsrc"), d.k("smdst")
	d.eq("SADD src", bs("SADD"), src, bs("m1"), bs("m2"))
	d.eq("SMOVE present -> 1", bs("SMOVE"), src, dst, bs("m1"))
	d.eq("SMOVE absent -> 0", bs("SMOVE"), src, dst, bs("nope"))
}

func TestDiffReturnValues_Hash(t *testing.T) {
	d := newDiffer(t)

	k := d.k("hash")
	d.eq("HSET new -> 1", bs("HSET"), k, bs("f"), bs("v"))
	d.eq("HSET update -> 0", bs("HSET"), k, bs("f"), bs("v2"))
	d.eq("HSETNX exists -> 0", bs("HSETNX"), k, bs("f"), bs("x"))
	d.eq("HSETNX new -> 1", bs("HSETNX"), k, bs("g"), bs("y"))
	d.eq("HDEL present -> 1", bs("HDEL"), k, bs("f"))
	d.eq("HDEL absent -> 0", bs("HDEL"), k, bs("zzz"))
	d.eq("HEXISTS present -> 1", bs("HEXISTS"), k, bs("g"))
	d.eq("HEXISTS absent -> 0", bs("HEXISTS"), k, bs("f"))
}

func TestDiffReturnValues_ZSet(t *testing.T) {
	d := newDiffer(t)

	k := d.k("zset")
	d.eq("ZADD 2 new -> 2", bs("ZADD"), k, bs("1"), bs("a"), bs("2"), bs("b"))
	d.eq("ZADD existing same-score -> 0", bs("ZADD"), k, bs("1"), bs("a"))
	d.eq("ZADD existing new-score -> 0 (updated, not added)", bs("ZADD"), k, bs("5"), bs("a"))
	d.eq("ZADD CH changed -> 1", bs("ZADD"), k, bs("CH"), bs("9"), bs("a"))
	d.eq("ZADD CH unchanged -> 0", bs("ZADD"), k, bs("CH"), bs("9"), bs("a"))
	d.eq("ZADD NX on existing -> 0", bs("ZADD"), k, bs("NX"), bs("100"), bs("a"))
	d.eq("ZSCORE unchanged by NX", bs("ZSCORE"), k, bs("a"))
	d.eq("ZREM present -> 1", bs("ZREM"), k, bs("b"))
	d.eq("ZREM absent -> 0", bs("ZREM"), k, bs("zzz"))
}

func TestDiffReturnValues_List(t *testing.T) {
	d := newDiffer(t)

	k := d.k("list")
	d.eq("LPUSH -> len 1", bs("LPUSH"), k, bs("a"))
	d.eq("RPUSH 2 -> len 3", bs("RPUSH"), k, bs("b"), bs("c"))
	d.eq("LPUSHX existing -> len 4", bs("LPUSHX"), k, bs("z"))
	d.eq("RPUSHX existing -> len 5", bs("RPUSHX"), k, bs("y"))
	d.eq("LINSERT before present -> len 6", bs("LINSERT"), k, bs("BEFORE"), bs("b"), bs("BB"))
	d.eq("LINSERT before absent pivot -> -1", bs("LINSERT"), k, bs("BEFORE"), bs("nope"), bs("X"))
	d.eq("LREM 0 present -> count", bs("LREM"), k, bs("0"), bs("a"))

	// PUSHX / LINSERT on a missing key -> 0 / 0.
	miss := d.k("miss")
	d.eq("LPUSHX missing -> 0", bs("LPUSHX"), miss, bs("a"))
	d.eq("RPUSHX missing -> 0", bs("RPUSHX"), miss, bs("a"))
	d.eq("LINSERT missing -> 0", bs("LINSERT"), miss, bs("BEFORE"), bs("p"), bs("v"))
	d.eq("missing still absent", bs("EXISTS"), miss)
}

func TestDiffReturnValues_KeysAndString(t *testing.T) {
	d := newDiffer(t)

	a, b, c := d.k("a"), d.k("b"), d.k("c")
	d.eq("SET a", bs("SET"), a, bs("1"))
	d.eq("SET b", bs("SET"), b, bs("2"))
	d.eq("EXISTS multi w/ dup -> counts each", bs("EXISTS"), a, b, a, c)
	d.eq("DEL multi -> 2", bs("DEL"), a, b, c)
	d.eq("DEL again -> 0", bs("DEL"), a, b)

	// SETNX / string counters.
	s := d.k("s")
	d.eq("SETNX new -> 1", bs("SETNX"), s, bs("v"))
	d.eq("SETNX existing -> 0", bs("SETNX"), s, bs("w"))
	d.eq("value unchanged by failed SETNX", bs("GET"), s)

	// EXPIRE / PERSIST return codes.
	e := d.k("e")
	d.eq("SET e", bs("SET"), e, bs("v"))
	d.eq("EXPIRE existing -> 1", bs("EXPIRE"), e, bs("1000"))
	d.eq("PERSIST had-ttl -> 1", bs("PERSIST"), e)
	d.eq("PERSIST no-ttl -> 0", bs("PERSIST"), e)
	d.eq("EXPIRE missing -> 0", bs("EXPIRE"), d.k("nope"), bs("1000"))

	// APPEND / INCR return the new length / value.
	ap := d.k("ap")
	d.eq("APPEND new -> len 3", bs("APPEND"), ap, bs("abc"))
	d.eq("APPEND more -> len 5", bs("APPEND"), ap, bs("de"))
	ic := d.k("ic")
	d.eq("INCR new -> 1", bs("INCR"), ic)
	d.eq("INCRBY 9 -> 10", bs("INCRBY"), ic, bs("9"))
	d.eq("DECR -> 9", bs("DECR"), ic)
}
