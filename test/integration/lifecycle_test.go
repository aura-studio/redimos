package integration

import "testing"

// Dimension K: key lifecycle / delete-on-empty. In Redis a collection key is removed the
// instant its last element is gone, so EXISTS -> 0, TYPE -> none, TTL -> -2, and a fresh
// add recreates it clean (with no lingering TTL). redimos implements this with its own
// meta delete-if-empty logic, so every removal path must reproduce it byte-for-byte. These
// are single-connection sequential flows (no concurrency), which is exactly the regime where
// redimos is expected to match; the concurrent DEL+recreate divergence is documented
// separately and is NOT exercised here.

// assertGone checks the standard "key does not exist" trio against both endpoints.
func assertGone(d *differ, what string, key []byte) {
	d.eq(what+" EXISTS=0", bs("EXISTS"), key)
	d.eq(what+" TYPE=none", bs("TYPE"), key)
	d.eq(what+" TTL=-2", bs("TTL"), key)
}

func TestDiffDeleteOnEmpty_Set(t *testing.T) {
	d := newDiffer(t)

	k := d.k("set")
	d.eq("SADD", bs("SADD"), k, bs("a"), bs("b"), bs("c"))
	d.eq("SREM two", bs("SREM"), k, bs("a"), bs("b"))
	d.eq("EXISTS still", bs("EXISTS"), k)
	d.eq("SREM last", bs("SREM"), k, bs("c"))
	assertGone(d, "after SREM last", k)
	// Re-adding recreates a clean key.
	d.eq("SADD again", bs("SADD"), k, bs("x"))
	d.eq("TYPE after re-add", bs("TYPE"), k)
	d.eq("SCARD after re-add", bs("SCARD"), k)

	// SPOP of the last member also deletes the key.
	sp := d.k("spop")
	d.eq("SADD spop", bs("SADD"), sp, bs("only"))
	d.eq("SPOP last", bs("SPOP"), sp)
	assertGone(d, "after SPOP last", sp)
}

func TestDiffDeleteOnEmpty_Hash(t *testing.T) {
	d := newDiffer(t)

	k := d.k("hash")
	d.eq("HSET f1", bs("HSET"), k, bs("f1"), bs("v1"))
	d.eq("HSET f2", bs("HSET"), k, bs("f2"), bs("v2"))
	d.eq("HDEL f1", bs("HDEL"), k, bs("f1"))
	d.eq("EXISTS still", bs("EXISTS"), k)
	d.eq("HDEL last", bs("HDEL"), k, bs("f2"))
	assertGone(d, "after HDEL last", k)
}

func TestDiffDeleteOnEmpty_ZSet(t *testing.T) {
	d := newDiffer(t)

	k := d.k("zset")
	d.eq("ZADD", bs("ZADD"), k, bs("1"), bs("a"), bs("2"), bs("b"), bs("3"), bs("c"))
	d.eq("ZREM one", bs("ZREM"), k, bs("a"))
	d.eq("ZREMRANGEBYRANK rest", bs("ZREMRANGEBYRANK"), k, bs("0"), bs("-1"))
	assertGone(d, "after ZREMRANGEBYRANK all", k)

	// ZREMRANGEBYSCORE emptying deletes the key.
	zs := d.k("zscore")
	d.eq("ZADD zs", bs("ZADD"), zs, bs("1"), bs("a"), bs("2"), bs("b"))
	d.eq("ZREMRANGEBYSCORE -inf +inf", bs("ZREMRANGEBYSCORE"), zs, bs("-inf"), bs("+inf"))
	assertGone(d, "after ZREMRANGEBYSCORE all", zs)

	// ZREMRANGEBYLEX emptying deletes the key.
	zl := d.k("zlex")
	d.eq("ZADD zl", bs("ZADD"), zl, bs("0"), bs("a"), bs("0"), bs("b"))
	d.eq("ZREMRANGEBYLEX - +", bs("ZREMRANGEBYLEX"), zl, bs("-"), bs("+"))
	assertGone(d, "after ZREMRANGEBYLEX all", zl)
}

func TestDiffDeleteOnEmpty_List(t *testing.T) {
	d := newDiffer(t)

	// LPOP/RPOP to empty.
	k := d.k("list")
	d.eq("RPUSH", bs("RPUSH"), k, bs("a"), bs("b"))
	d.eq("LPOP", bs("LPOP"), k)
	d.eq("RPOP last", bs("RPOP"), k)
	assertGone(d, "after popping all", k)

	// LREM removing every occurrence empties and deletes.
	lr := d.k("lrem")
	d.eq("RPUSH lr", bs("RPUSH"), lr, bs("x"), bs("x"), bs("x"))
	d.eq("LREM 0 x", bs("LREM"), lr, bs("0"), bs("x"))
	assertGone(d, "after LREM all", lr)

	// LTRIM to an empty range deletes the key.
	lt := d.k("ltrim")
	d.eq("RPUSH lt", bs("RPUSH"), lt, bs("a"), bs("b"), bs("c"))
	d.eq("LTRIM 5 10 (empty)", bs("LTRIM"), lt, bs("5"), bs("10"))
	assertGone(d, "after LTRIM empty", lt)
}

// TestDiffDeleteOnEmpty_TTLCleared verifies a re-created key does NOT inherit the prior
// incarnation's TTL: set a TTL, empty the key, re-add, and the TTL must be gone (-1) on both.
func TestDiffDeleteOnEmpty_TTLCleared(t *testing.T) {
	d := newDiffer(t)

	k := d.k("ttl")
	d.eq("SADD", bs("SADD"), k, bs("a"))
	d.eq("EXPIRE", bs("EXPIRE"), k, bs("10000"))
	d.eqIntClose("TTL set ~10000", 2, bs("TTL"), k) // tolerant: may straddle a second boundary
	d.eq("SREM last", bs("SREM"), k, bs("a"))
	assertGone(d, "after empty", k)
	d.eq("SADD recreate", bs("SADD"), k, bs("b"))
	d.eq("TTL after recreate = -1", bs("TTL"), k)
}
