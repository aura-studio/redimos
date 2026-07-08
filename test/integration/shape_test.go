package integration

import "testing"

// Dimension C: reply-shape parity. Redis is exacting about which RESP2 shape a command
// returns for absent/empty/present keys — a null bulk ($-1) vs an empty array (*0) vs an
// empty bulk ($0) vs an integer, and the exact TYPE / TTL sentinels. A client that
// switch()es on the reply type breaks if the proxy picks a different shape. These compare
// the shape byte-for-byte across the absent / empty / present states.

func TestDiffReplyShapeAbsent(t *testing.T) {
	d := newDiffer(t)
	miss := d.k("missing")

	// Null bulk ($-1) for absent scalar reads.
	d.eq("GET absent", bs("GET"), miss)
	d.eq("LPOP absent", bs("LPOP"), miss)
	d.eq("RPOP absent", bs("RPOP"), miss)
	d.eq("HGET absent", bs("HGET"), miss, bs("f"))
	d.eq("LINDEX absent", bs("LINDEX"), miss, bs("0"))
	d.eq("ZSCORE absent", bs("ZSCORE"), miss, bs("m"))
	d.eq("ZRANK absent", bs("ZRANK"), miss, bs("m"))
	d.eq("GETSET absent", bs("GETSET"), d.k("gs"), bs("v")) // returns $-1 then sets

	// Empty array (*0) for absent collection reads.
	d.eq("LRANGE absent", bs("LRANGE"), miss, bs("0"), bs("-1"))
	d.eq("SMEMBERS absent", bs("SMEMBERS"), miss)
	d.eq("HGETALL absent", bs("HGETALL"), miss)
	d.eq("HKEYS absent", bs("HKEYS"), miss)
	d.eq("HVALS absent", bs("HVALS"), miss)
	d.eq("ZRANGE absent", bs("ZRANGE"), miss, bs("0"), bs("-1"))
	d.eq("HMGET absent", bs("HMGET"), miss, bs("a"), bs("b"))

	// Integer sentinels.
	d.eq("EXISTS absent", bs("EXISTS"), miss)
	d.eq("STRLEN absent", bs("STRLEN"), miss)
	d.eq("LLEN absent", bs("LLEN"), miss)
	d.eq("SCARD absent", bs("SCARD"), miss)
	d.eq("ZCARD absent", bs("ZCARD"), miss)
	d.eq("HLEN absent", bs("HLEN"), miss)
	d.eq("DEL absent", bs("DEL"), miss)
	d.eq("SISMEMBER absent", bs("SISMEMBER"), miss, bs("m"))
	d.eq("HEXISTS absent", bs("HEXISTS"), miss, bs("f"))

	// Type / TTL sentinels for an absent key.
	d.eq("TYPE absent -> none", bs("TYPE"), miss)
	d.eq("TTL absent -> -2", bs("TTL"), miss)
	d.eq("PTTL absent -> -2", bs("PTTL"), miss)
	d.eq("PERSIST absent -> 0", bs("PERSIST"), miss)
	d.eq("EXPIRE absent -> 0", bs("EXPIRE"), miss, bs("100"))

	t.Logf("compared %d absent-key reply shapes vs Redis 3.2", d.n)
}

func TestDiffReplyShapePresent(t *testing.T) {
	d := newDiffer(t)

	sk := d.k("s")
	d.eq("SET -> +OK", bs("SET"), sk, bs("hello"))
	d.eq("TYPE string", bs("TYPE"), sk)
	d.eq("EXISTS present", bs("EXISTS"), sk)
	d.eq("TTL no-expire -> -1", bs("TTL"), sk)
	d.eq("PTTL no-expire -> -1", bs("PTTL"), sk)
	d.eq("STRLEN present", bs("STRLEN"), sk)
	d.eq("SET overwrite -> +OK", bs("SET"), sk, bs("hi"))
	d.eq("SETNX existing -> 0", bs("SETNX"), sk, bs("x"))

	// Type strings for each family.
	lk := d.k("l")
	d.eq("seed list", bs("RPUSH"), lk, bs("a"))
	d.eq("TYPE list", bs("TYPE"), lk)

	stk := d.k("st")
	d.eq("seed set", bs("SADD"), stk, bs("m"))
	d.eq("TYPE set", bs("TYPE"), stk)

	zk := d.k("z")
	d.eq("seed zset", bs("ZADD"), zk, bs("1"), bs("m"))
	d.eq("TYPE zset", bs("TYPE"), zk)

	hk := d.k("h")
	d.eq("seed hash", bs("HSET"), hk, bs("f"), bs("v"))
	d.eq("TYPE hash", bs("TYPE"), hk)

	// Empty-bulk vs null-bulk: an existing empty string value.
	ek := d.k("empty")
	d.eq("SET empty -> +OK", bs("SET"), ek, bs(""))
	d.eq("GET empty -> $0", bs("GET"), ek)
	d.eq("STRLEN empty -> 0", bs("STRLEN"), ek)

	t.Logf("compared %d present-key reply shapes vs Redis 3.2", d.n)
}
