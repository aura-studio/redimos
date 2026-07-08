package integration

import "testing"

// Dimension A: error-path parity. Redis clients frequently branch on error text, so a
// drifted error string is a real compatibility break. These compare the proxy's error
// replies byte-for-byte with Redis 3.2 across three families: wrong arity, WRONGTYPE, and
// invalid-argument (non-integer / non-float / out-of-range). Only via-redimo commands are
// used (proxy-reject commands would legitimately differ from Redis).

func TestDiffErrorArity(t *testing.T) {
	d := newDiffer(t)

	d.eq("GET no key", bs("GET"))
	d.eq("GET too many", bs("GET"), bs("a"), bs("b"))
	d.eq("SET missing value", bs("SET"), d.k("s"))
	d.eq("SETNX missing value", bs("SETNX"), d.k("s"))
	d.eq("STRLEN too many", bs("STRLEN"), d.k("s"), bs("x"))
	d.eq("APPEND arity", bs("APPEND"), d.k("s"))
	d.eq("HSET odd args", bs("HSET"), d.k("h"), bs("f"))
	d.eq("HGET arity", bs("HGET"), d.k("h"))
	d.eq("HDEL arity", bs("HDEL"), d.k("h"))
	d.eq("LPUSH arity", bs("LPUSH"), d.k("l"))
	d.eq("LSET arity", bs("LSET"), d.k("l"), bs("0"))
	d.eq("LRANGE arity", bs("LRANGE"), d.k("l"), bs("0"))
	d.eq("SADD arity", bs("SADD"), d.k("st"))
	d.eq("SISMEMBER arity", bs("SISMEMBER"), d.k("st"))
	d.eq("ZADD missing member", bs("ZADD"), d.k("z"), bs("1"))
	d.eq("ZSCORE arity", bs("ZSCORE"), d.k("z"))
	d.eq("EXPIRE arity", bs("EXPIRE"), d.k("s"))
	d.eq("TTL too many", bs("TTL"), d.k("s"), bs("x"))
	d.eq("INCR too many", bs("INCR"), d.k("s"), bs("2"))
	d.eq("SETBIT arity", bs("SETBIT"), d.k("s"), bs("7"))
	d.eq("GETRANGE arity", bs("GETRANGE"), d.k("s"), bs("0"))
	d.eq("PFADD arity", bs("PFADD"))
	d.eq("PFCOUNT arity", bs("PFCOUNT"))
	d.eq("TYPE arity", bs("TYPE"))
	d.eq("EXISTS arity", bs("EXISTS"))

	t.Logf("compared %d arity error replies vs Redis 3.2", d.n)
}

func TestDiffErrorWrongType(t *testing.T) {
	d := newDiffer(t)

	sk := d.k("str")
	d.eq("seed string", bs("SET"), sk, bs("v"))

	// Collection ops on a string key must reply WRONGTYPE identically.
	d.eq("LPUSH on string", bs("LPUSH"), sk, bs("x"))
	d.eq("RPUSH on string", bs("RPUSH"), sk, bs("x"))
	d.eq("LRANGE on string", bs("LRANGE"), sk, bs("0"), bs("-1"))
	d.eq("LLEN on string", bs("LLEN"), sk)
	d.eq("SADD on string", bs("SADD"), sk, bs("m"))
	d.eq("SMEMBERS on string", bs("SMEMBERS"), sk)
	d.eq("SCARD on string", bs("SCARD"), sk)
	d.eq("HSET on string", bs("HSET"), sk, bs("f"), bs("v"))
	d.eq("HGETALL on string", bs("HGETALL"), sk)
	d.eq("ZADD on string", bs("ZADD"), sk, bs("1"), bs("m"))
	d.eq("ZRANGE on string", bs("ZRANGE"), sk, bs("0"), bs("-1"))

	// String ops on a list key must reply WRONGTYPE identically.
	lk := d.k("lst")
	d.eq("seed list", bs("RPUSH"), lk, bs("a"))
	d.eq("GET on list", bs("GET"), lk)
	d.eq("APPEND on list", bs("APPEND"), lk, bs("x"))
	d.eq("STRLEN on list", bs("STRLEN"), lk)
	d.eq("INCR on list", bs("INCR"), lk)
	d.eq("SETBIT on list", bs("SETBIT"), lk, bs("0"), bs("1"))

	t.Logf("compared %d wrongtype error replies vs Redis 3.2", d.n)
}

func TestDiffErrorBadArgs(t *testing.T) {
	d := newDiffer(t)

	nk := d.k("nonnum")
	d.eq("seed non-numeric", bs("SET"), nk, bs("notanumber"))

	d.eq("INCR non-integer", bs("INCR"), nk)
	d.eq("DECR non-integer", bs("DECR"), nk)
	d.eq("INCRBY non-integer value", bs("INCRBY"), nk, bs("5"))
	d.eq("INCRBY bad delta", bs("INCRBY"), d.k("ctr"), bs("notanumber"))
	d.eq("INCRBYFLOAT bad delta", bs("INCRBYFLOAT"), d.k("f"), bs("notafloat"))
	d.eq("EXPIRE bad ttl", bs("EXPIRE"), nk, bs("notanumber"))
	d.eq("GETRANGE bad start", bs("GETRANGE"), nk, bs("a"), bs("b"))
	d.eq("SETRANGE negative offset", bs("SETRANGE"), nk, bs("-1"), bs("x"))
	d.eq("SETBIT bad bit value", bs("SETBIT"), d.k("b"), bs("0"), bs("2"))
	d.eq("SETBIT bad offset", bs("SETBIT"), d.k("b"), bs("notanoffset"), bs("1"))
	d.eq("GETBIT bad offset", bs("GETBIT"), d.k("b"), bs("notanoffset"))
	d.eq("ZADD bad score", bs("ZADD"), d.k("z"), bs("notanumber"), bs("m"))
	d.eq("LSET no such key", bs("LSET"), d.k("missing"), bs("0"), bs("x"))

	t.Logf("compared %d bad-argument error replies vs Redis 3.2", d.n)
}
