package integration

import "testing"

// Dimension J: command-coverage sweep. Beyond the curated differential (83) and the
// error/shape/boundary/unordered dimensions, this exercises a broad set of via-redimo
// commands NOT otherwise covered — the second-tier string/hash/list/set/zset/keyspace
// commands — with valid arguments, comparing each reply to Redis 3.2 (byte-for-byte, or
// sorted / float-close where order or long-double precision applies). It maximizes breadth
// so no supported command silently drifts.

func TestCoverageSweepStringsHashes(t *testing.T) {
	d := newDiffer(t)

	// Strings
	d.eq("SETEX", bs("SETEX"), d.k("se"), bs("1000"), bs("v"))
	d.eqIntClose("TTL after SETEX", 1, bs("TTL"), d.k("se"))
	d.eq("PSETEX", bs("PSETEX"), d.k("pse"), bs("1000000"), bs("v"))
	d.eq("GETSET fresh", bs("GETSET"), d.k("gs"), bs("v1"))
	d.eq("GETSET replace", bs("GETSET"), d.k("gs"), bs("v2"))
	d.eq("MSET", bs("MSET"), d.k("m1"), bs("a"), d.k("m2"), bs("b"))
	d.eq("MGET", bs("MGET"), d.k("m1"), d.k("m2"), d.k("miss"))
	d.eq("MSETNX new", bs("MSETNX"), d.k("n1"), bs("x"), d.k("n2"), bs("y"))
	d.eq("MSETNX conflict", bs("MSETNX"), d.k("n1"), bs("z"), d.k("n3"), bs("w"))
	d.eq("GET n3 not set", bs("GET"), d.k("n3"))
	d.eq("DECRBY", bs("DECRBY"), d.k("ctr"), bs("7"))
	d.eq("INCRBY", bs("INCRBY"), d.k("ctr"), bs("3"))

	// Hashes
	hk := d.k("h")
	d.eq("HMSET", bs("HMSET"), hk, bs("f1"), bs("v1"), bs("f2"), bs("v2"))
	d.eq("HSETNX new", bs("HSETNX"), hk, bs("f3"), bs("v3"))
	d.eq("HSETNX existing", bs("HSETNX"), hk, bs("f1"), bs("x"))
	d.eq("HINCRBY", bs("HINCRBY"), hk, bs("cnt"), bs("5"))
	d.eq("HINCRBY neg", bs("HINCRBY"), hk, bs("cnt"), bs("-2"))
	d.eqFloatClose("HINCRBYFLOAT", bs("HINCRBYFLOAT"), hk, bs("fl"), bs("1.5"))
	d.eq("HSTRLEN", bs("HSTRLEN"), hk, bs("f1"))
	d.eq("HEXISTS yes", bs("HEXISTS"), hk, bs("f1"))
	d.eq("HEXISTS no", bs("HEXISTS"), hk, bs("nope"))
	d.eq("HLEN", bs("HLEN"), hk)
	d.eqSorted("HMGET partial", bs("HMGET"), hk, bs("f1"), bs("miss"), bs("f2"))

	t.Logf("swept %d string/hash commands vs Redis 3.2", d.n)
}

func TestCoverageSweepListsSetsZSets(t *testing.T) {
	d := newDiffer(t)

	// Lists
	lk := d.k("l")
	d.eq("RPUSH seed", bs("RPUSH"), lk, bs("a"), bs("b"), bs("c"))
	d.eq("LINSERT BEFORE", bs("LINSERT"), lk, bs("BEFORE"), bs("b"), bs("X"))
	d.eq("LINSERT AFTER", bs("LINSERT"), lk, bs("AFTER"), bs("c"), bs("Y"))
	d.eq("LINSERT missing pivot", bs("LINSERT"), lk, bs("BEFORE"), bs("zzz"), bs("Q"))
	d.eq("LRANGE after insert", bs("LRANGE"), lk, bs("0"), bs("-1"))
	d.eq("LPUSHX existing", bs("LPUSHX"), lk, bs("H"))
	d.eq("LPUSHX missing", bs("LPUSHX"), d.k("nolist"), bs("H"))
	d.eq("RPUSHX missing", bs("RPUSHX"), d.k("nolist"), bs("H"))
	d.eq("LTRIM", bs("LTRIM"), lk, bs("1"), bs("3"))
	d.eq("LRANGE after trim", bs("LRANGE"), lk, bs("0"), bs("-1"))
	d.eq("LLEN", bs("LLEN"), lk)

	// Sets
	s1, s2, dst := d.k("s1"), d.k("s2"), d.k("dst")
	d.eq("SADD s1", bs("SADD"), s1, bs("a"), bs("b"), bs("c"), bs("d"))
	d.eq("SADD s2", bs("SADD"), s2, bs("c"), bs("d"), bs("e"))
	d.eq("SMOVE", bs("SMOVE"), s1, s2, bs("a"))
	d.eq("SISMEMBER moved", bs("SISMEMBER"), s2, bs("a"))
	d.eq("SINTERSTORE count", bs("SINTERSTORE"), dst, s1, s2)
	d.eqSorted("SMEMBERS interstore dst", bs("SMEMBERS"), dst)
	d.eq("SUNIONSTORE count", bs("SUNIONSTORE"), dst, s1, s2)
	d.eqSorted("SMEMBERS unionstore dst", bs("SMEMBERS"), dst)
	d.eq("SDIFFSTORE count", bs("SDIFFSTORE"), dst, s2, s1)
	d.eqSorted("SMEMBERS diffstore dst", bs("SMEMBERS"), dst)
	d.eq("SCARD s2", bs("SCARD"), s2)

	// Sorted sets (integer scores keep ZINCRBY deterministic)
	zk := d.k("z")
	d.eq("ZADD", bs("ZADD"), zk, bs("1"), bs("a"), bs("2"), bs("b"), bs("3"), bs("c"), bs("4"), bs("d"))
	d.eq("ZINCRBY int", bs("ZINCRBY"), zk, bs("10"), bs("a"))
	d.eq("ZRANK", bs("ZRANK"), zk, bs("b"))
	d.eq("ZREVRANK", bs("ZREVRANK"), zk, bs("b"))
	d.eq("ZREVRANGE", bs("ZREVRANGE"), zk, bs("0"), bs("-1"))
	d.eq("ZREVRANGE WITHSCORES", bs("ZREVRANGE"), zk, bs("0"), bs("-1"), bs("WITHSCORES"))
	d.eq("ZCOUNT", bs("ZCOUNT"), zk, bs("0"), bs("100"))
	d.eq("ZREMRANGEBYRANK", bs("ZREMRANGEBYRANK"), zk, bs("0"), bs("0"))
	d.eq("ZREMRANGEBYSCORE", bs("ZREMRANGEBYSCORE"), zk, bs("2"), bs("2"))
	d.eq("ZCARD", bs("ZCARD"), zk)

	// Keyspace
	d.eq("EXISTS multi", bs("EXISTS"), zk, zk, d.k("miss"))
	d.eq("DEL multi", bs("DEL"), s1, s2, d.k("miss"))

	t.Logf("swept %d list/set/zset/key commands vs Redis 3.2", d.n)
}
