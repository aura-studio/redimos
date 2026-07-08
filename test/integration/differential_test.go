package integration

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"
)

// TestDifferentialVsRedis32 runs a curated matrix of redimo-backed commands against
// BOTH the redimos proxy and a live Redis 3.2 oracle and asserts every reply is
// byte-for-byte identical. Only order-DETERMINISTIC commands are included (lists keep
// insertion order; sorted sets are score-ordered; string/key/hash-scalar/set-count
// replies are fixed), so an unordered SMEMBERS/HKEYS mismatch can never cause a false
// failure. Requires REDIMOS_PROXY_ADDR and REDIMOS_REDIS_ORACLE.
func TestDifferentialVsRedis32(t *testing.T) {
	paddr := proxyAddr(t)
	oaddr := oracleAddr(t)
	p := dial(t, paddr)
	o := dial(t, oaddr)
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)

	n := 0
	// diff sends args to both and fails on any byte difference.
	diff := func(desc string, args ...[]byte) {
		n++
		gotP := p.do(args...)
		gotO := o.do(args...)
		if !bytes.Equal(gotP, gotO) {
			t.Errorf("%s\n  cmd=%s\n  proxy =%q\n  oracle=%q", desc, joinArgs(args), gotP, gotO)
		}
	}
	k := func(fam string) []byte { return []byte(fmt.Sprintf("df:%s:%s", nonce, fam)) }

	// --- Strings / Keys ---
	s := k("str")
	diff("SET", bs("SET"), s, bs("hello"))
	diff("GET", bs("GET"), s)
	diff("APPEND", bs("APPEND"), s, bs(" world"))
	diff("STRLEN", bs("STRLEN"), s)
	diff("GETRANGE", bs("GETRANGE"), s, bs("0"), bs("4"))
	diff("SETRANGE", bs("SETRANGE"), s, bs("6"), bs("REDIS"))
	diff("GET after setrange", bs("GET"), s)
	diff("EXISTS", bs("EXISTS"), s)
	diff("TYPE", bs("TYPE"), s)
	diff("GETSET", bs("GETSET"), s, bs("42"))
	diff("INCR", bs("INCR"), k("ctr"))
	diff("INCRBY", bs("INCRBY"), k("ctr"), bs("10"))
	diff("DECRBY", bs("DECRBY"), k("ctr"), bs("3"))
	diff("SETNX new", bs("SETNX"), k("nx"), bs("v"))
	diff("SETNX exists", bs("SETNX"), k("nx"), bs("w"))
	diff("SET NX exists", bs("SET"), k("nx"), bs("x"), bs("NX"))
	diff("DEL", bs("DEL"), s)
	diff("EXISTS after del", bs("EXISTS"), s)
	diff("GET missing", bs("GET"), k("missing"))
	diff("INCR wrongtype (list key)", bs("INCR"), k("wrongtype")) // reply parity on absent
	diff("STRLEN missing", bs("STRLEN"), k("missing"))

	// --- Hashes (scalar/deterministic only) ---
	h := k("hash")
	diff("HSET", bs("HSET"), h, bs("f1"), bs("v1"))
	diff("HSET f2", bs("HSET"), h, bs("f2"), bs("v2"))
	diff("HGET", bs("HGET"), h, bs("f1"))
	diff("HMGET", bs("HMGET"), h, bs("f1"), bs("nope"), bs("f2"))
	diff("HLEN", bs("HLEN"), h)
	diff("HEXISTS yes", bs("HEXISTS"), h, bs("f1"))
	diff("HEXISTS no", bs("HEXISTS"), h, bs("zz"))
	diff("HSTRLEN", bs("HSTRLEN"), h, bs("f1"))
	diff("HINCRBY", bs("HINCRBY"), h, bs("cnt"), bs("5"))
	diff("HINCRBY neg", bs("HINCRBY"), h, bs("cnt"), bs("-2"))
	diff("HSETNX exists", bs("HSETNX"), h, bs("f1"), bs("nope"))
	diff("HDEL", bs("HDEL"), h, bs("f2"))
	diff("HGET after del", bs("HGET"), h, bs("f2"))

	// --- Lists (insertion order = deterministic) ---
	l := k("list")
	diff("RPUSH", bs("RPUSH"), l, bs("a"), bs("b"), bs("c"))
	diff("LPUSH", bs("LPUSH"), l, bs("z"))
	diff("LRANGE all", bs("LRANGE"), l, bs("0"), bs("-1"))
	diff("LLEN", bs("LLEN"), l)
	diff("LINDEX", bs("LINDEX"), l, bs("2"))
	diff("LINDEX neg", bs("LINDEX"), l, bs("-1"))
	diff("LSET", bs("LSET"), l, bs("1"), bs("A"))
	diff("LRANGE after lset", bs("LRANGE"), l, bs("0"), bs("-1"))
	diff("LPOP", bs("LPOP"), l)
	diff("RPOP", bs("RPOP"), l)
	diff("LINSERT", bs("LINSERT"), l, bs("BEFORE"), bs("A"), bs("mid"))
	diff("LRANGE after insert", bs("LRANGE"), l, bs("0"), bs("-1"))
	diff("LREM", bs("LREM"), l, bs("0"), bs("A"))
	diff("LTRIM", bs("LTRIM"), l, bs("0"), bs("0"))
	diff("LRANGE after trim", bs("LRANGE"), l, bs("0"), bs("-1"))

	// LREM head-most selection must order occurrences by NUMERIC index, not the
	// decimal-string index ("10" < "2"). With a duplicate at positions 2 and 11, a
	// count=1 LREM must drop position 2, leaving the tail duplicate — matching Redis.
	lr := k("lremnum")
	diff("LREM num RPUSH", bs("RPUSH"), lr, bs("f1"), bs("DUP"), bs("f3"), bs("f4"), bs("f5"), bs("f6"), bs("f7"), bs("f8"), bs("f9"), bs("f10"), bs("DUP"))
	diff("LREM num count=1", bs("LREM"), lr, bs("1"), bs("DUP"))
	diff("LRANGE after LREM num", bs("LRANGE"), lr, bs("0"), bs("-1"))

	// --- Sets (count/membership = deterministic; avoid unordered SMEMBERS) ---
	st := k("set")
	diff("SADD", bs("SADD"), st, bs("m1"), bs("m2"), bs("m3"))
	diff("SADD dup", bs("SADD"), st, bs("m1"))
	diff("SCARD", bs("SCARD"), st)
	diff("SISMEMBER yes", bs("SISMEMBER"), st, bs("m2"))
	diff("SISMEMBER no", bs("SISMEMBER"), st, bs("zz"))
	diff("SREM", bs("SREM"), st, bs("m3"))
	diff("SCARD after srem", bs("SCARD"), st)

	// --- Sorted sets (score order = deterministic) ---
	z := k("zset")
	diff("ZADD", bs("ZADD"), z, bs("1"), bs("a"), bs("3"), bs("c"), bs("2"), bs("b"))
	diff("ZCARD", bs("ZCARD"), z)
	diff("ZSCORE", bs("ZSCORE"), z, bs("b"))
	diff("ZRANK", bs("ZRANK"), z, bs("c"))
	diff("ZRANGE", bs("ZRANGE"), z, bs("0"), bs("-1"))
	diff("ZRANGE WITHSCORES", bs("ZRANGE"), z, bs("0"), bs("-1"), bs("WITHSCORES"))
	diff("ZREVRANGE", bs("ZREVRANGE"), z, bs("0"), bs("-1"))
	diff("ZRANGEBYSCORE", bs("ZRANGEBYSCORE"), z, bs("1"), bs("2"))
	diff("ZINCRBY", bs("ZINCRBY"), z, bs("5"), bs("a"))
	diff("ZRANGE after incr", bs("ZRANGE"), z, bs("0"), bs("-1"))
	diff("ZCOUNT", bs("ZCOUNT"), z, bs("2"), bs("10"))
	diff("ZREM", bs("ZREM"), z, bs("b"))
	diff("ZCARD after zrem", bs("ZCARD"), z)

	// Lex range/count over an equal-score zset. The unbounded "- +" forms must not
	// leak or count the internal #meta bookkeeping item (they must match Redis exactly).
	zl := k("zlex")
	diff("ZADD lex", bs("ZADD"), zl, bs("0"), bs("a"), bs("0"), bs("b"), bs("0"), bs("c"))
	diff("ZRANGEBYLEX - +", bs("ZRANGEBYLEX"), zl, bs("-"), bs("+"))
	diff("ZREVRANGEBYLEX + -", bs("ZREVRANGEBYLEX"), zl, bs("+"), bs("-"))
	diff("ZRANGEBYLEX bounded", bs("ZRANGEBYLEX"), zl, bs("[a"), bs("(c"))
	diff("ZLEXCOUNT - +", bs("ZLEXCOUNT"), zl, bs("-"), bs("+"))
	diff("ZLEXCOUNT bounded", bs("ZLEXCOUNT"), zl, bs("[a"), bs("[b"))

	// --- BIT / HLL (redimo-backed value ops) ---
	diff("SETBIT", bs("SETBIT"), k("bit"), bs("7"), bs("1"))
	diff("GETBIT", bs("GETBIT"), k("bit"), bs("7"))
	diff("BITCOUNT", bs("BITCOUNT"), k("bit"))
	diff("PFADD", bs("PFADD"), k("hll"), bs("a"), bs("b"), bs("c"))
	diff("PFCOUNT", bs("PFCOUNT"), k("hll"))

	t.Logf("compared %d commands byte-for-byte vs Redis 3.2", n)
}

func joinArgs(args [][]byte) string {
	out := make([]byte, 0, 32)
	for i, a := range args {
		if i > 0 {
			out = append(out, ' ')
		}
		out = append(out, a...)
	}
	return string(out)
}
