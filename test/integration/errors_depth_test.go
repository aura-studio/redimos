package integration

// Dimension A (depth): error-path parity beyond the base errors_test.go coverage.
// Redis clients branch on error TEXT, so a drifted arity/WRONGTYPE/syntax string is a
// real compatibility break. These deepen three families the base file only sampled:
//   - exact-arity and variadic-arity boundaries for the many commands that were never
//     isolated (DECR/MSET/GETBIT/LINDEX/... — GAPs 1-3);
//   - the custom (non-generic) error literals commands emit off the generic template
//     (HMSET uppercase arity, ZADD NX/XX + INCR, BITCOUNT syntax-not-arity — GAPs 4,5,8);
//   - WRONGTYPE / order-of-check precedence across every collection op and the
//     type-check-before-parse commands (GAPs 6,7,9,10);
//   - negative/out-of-range index semantics and the exact inf/nan score spellings
//     (GAPs 11,12).
// All commands used are proxy-registered (checked against internal/command/*.go); the
// differ compares each reply byte-for-byte with a live Redis 3.2 oracle at runtime.

import "testing"

// TestDiffArityExact deepens GAP 1 & 3: every exact-arity (Arity>0) command that the
// base file never isolated, probed both under (too few) and over (too many) its exact
// arity. Redis rejects at the router with "wrong number of arguments for '<lower>'
// command"; redimos routes through CmdSpec.arityOK, so any off-by-one or wrong casing
// in the generic template surfaces here.
func TestDiffArityExact(t *testing.T) {
	d := newDiffer(t)

	// GAP 1: DECR key (arity 2 exact) — under and over.
	d.eq("DECR no key", bs("DECR"))
	d.eq("DECR too many", bs("DECR"), d.k("a"), d.k("b"))
	d.eq("INCR no key", bs("INCR"))
	d.eq("INCRBY too few", bs("INCRBY"), d.k("c"))
	d.eq("INCRBY too many", bs("INCRBY"), d.k("c"), bs("1"), bs("2"))
	d.eq("DECRBY too few", bs("DECRBY"), d.k("c"))
	d.eq("INCRBYFLOAT too few", bs("INCRBYFLOAT"), d.k("f"))
	d.eq("SETNX too many", bs("SETNX"), d.k("s"), bs("v"), bs("x"))
	d.eq("GETSET arity", bs("GETSET"), d.k("s"))

	// GAP 3: GETBIT key offset (arity 3 exact) — under and over.
	d.eq("GETBIT missing offset", bs("GETBIT"), d.k("b"))
	d.eq("GETBIT too many", bs("GETBIT"), d.k("b"), bs("0"), bs("1"))
	d.eq("SETBIT too few", bs("SETBIT"), d.k("b"), bs("0"))
	d.eq("SETBIT too many", bs("SETBIT"), d.k("b"), bs("0"), bs("1"), bs("x"))

	// String/range exact-arity.
	d.eq("SETRANGE too few", bs("SETRANGE"), d.k("s"), bs("0"))
	d.eq("SETRANGE too many", bs("SETRANGE"), d.k("s"), bs("0"), bs("v"), bs("x"))
	d.eq("GETRANGE too few", bs("GETRANGE"), d.k("s"), bs("0"))
	d.eq("GETRANGE too many", bs("GETRANGE"), d.k("s"), bs("0"), bs("-1"), bs("x"))

	// Hash exact-arity commands never isolated in the base file.
	d.eq("HGET too many", bs("HGET"), d.k("h"), bs("f"), bs("x"))
	d.eq("HEXISTS arity", bs("HEXISTS"), d.k("h"))
	d.eq("HSETNX arity", bs("HSETNX"), d.k("h"), bs("f"))
	d.eq("HSTRLEN arity", bs("HSTRLEN"), d.k("h"))
	d.eq("HINCRBY too few", bs("HINCRBY"), d.k("h"), bs("f"))
	d.eq("HINCRBY too many", bs("HINCRBY"), d.k("h"), bs("f"), bs("1"), bs("x"))
	d.eq("HINCRBYFLOAT too few", bs("HINCRBYFLOAT"), d.k("h"), bs("f"))
	d.eq("HLEN too many", bs("HLEN"), d.k("h"), bs("x"))

	// List exact-arity commands.
	d.eq("LINDEX too few", bs("LINDEX"), d.k("l"))
	d.eq("LINDEX too many", bs("LINDEX"), d.k("l"), bs("0"), bs("x"))
	d.eq("LLEN too many", bs("LLEN"), d.k("l"), bs("x"))
	d.eq("LPOP too many", bs("LPOP"), d.k("l"), bs("x"))
	d.eq("RPOP too many", bs("RPOP"), d.k("l"), bs("x"))
	d.eq("LRANGE too few", bs("LRANGE"), d.k("l"), bs("0"))
	d.eq("LSET too few", bs("LSET"), d.k("l"), bs("0"))
	d.eq("LSET too many", bs("LSET"), d.k("l"), bs("0"), bs("v"), bs("x"))
	d.eq("LTRIM arity", bs("LTRIM"), d.k("l"), bs("0"))
	d.eq("LREM arity", bs("LREM"), d.k("l"), bs("0"))
	d.eq("LINSERT too few", bs("LINSERT"), d.k("l"), bs("BEFORE"), bs("p"))
	d.eq("RPOPLPUSH arity", bs("RPOPLPUSH"), d.k("l"))

	// Set exact-arity commands.
	d.eq("SISMEMBER too many", bs("SISMEMBER"), d.k("st"), bs("m"), bs("x"))
	d.eq("SCARD too many", bs("SCARD"), d.k("st"), bs("x"))
	d.eq("SMEMBERS arity", bs("SMEMBERS"))
	d.eq("SMOVE too few", bs("SMOVE"), d.k("st"), d.k("st2"))
	d.eq("SMOVE too many", bs("SMOVE"), d.k("st"), d.k("st2"), bs("m"), bs("x"))

	// ZSet exact-arity commands.
	d.eq("ZSCORE too many", bs("ZSCORE"), d.k("z"), bs("m"), bs("x"))
	d.eq("ZCARD too many", bs("ZCARD"), d.k("z"), bs("x"))
	d.eq("ZINCRBY too few", bs("ZINCRBY"), d.k("z"), bs("1"))
	d.eq("ZINCRBY too many", bs("ZINCRBY"), d.k("z"), bs("1"), bs("m"), bs("x"))
	d.eq("ZRANK too few", bs("ZRANK"), d.k("z"))
	d.eq("ZREVRANK too few", bs("ZREVRANK"), d.k("z"))
	d.eq("ZCOUNT too few", bs("ZCOUNT"), d.k("z"), bs("0"))
	d.eq("ZCOUNT too many", bs("ZCOUNT"), d.k("z"), bs("0"), bs("1"), bs("x"))
	d.eq("ZREMRANGEBYRANK arity", bs("ZREMRANGEBYRANK"), d.k("z"), bs("0"))
	d.eq("ZREMRANGEBYSCORE arity", bs("ZREMRANGEBYSCORE"), d.k("z"), bs("0"))

	// Key/TTL exact-arity.
	d.eq("PERSIST arity", bs("PERSIST"))
	d.eq("PTTL too many", bs("PTTL"), d.k("s"), bs("x"))
	d.eq("PEXPIRE too few", bs("PEXPIRE"), d.k("s"))
	d.eq("RENAME too few", bs("RENAME"), d.k("s"))

	t.Logf("compared %d exact-arity error replies vs Redis 3.2", d.n)
}

// TestDiffArityVariadic deepens GAP 2: variadic commands (Arity<0) at their minimum-arg
// boundary and, for the pairwise ones, on an odd count. MSET requires odd argc (name +
// even key/value); MSET with a bare key (missing value) must reply the generic arity
// error at the router before the handler runs.
func TestDiffArityVariadic(t *testing.T) {
	d := newDiffer(t)

	// GAP 2: MSET key value [key value ...] (arity -3, args must be odd).
	d.eq("MSET no args", bs("MSET"))
	d.eq("MSET missing value", bs("MSET"), d.k("a"))
	d.eq("MSET odd pair", bs("MSET"), d.k("a"), bs("1"), d.k("b"))
	d.eq("MSETNX missing value", bs("MSETNX"), d.k("a"))
	d.eq("MSETNX odd pair", bs("MSETNX"), d.k("a"), bs("1"), d.k("b"))

	// Other variadic minimums.
	d.eq("MGET no keys", bs("MGET"))
	d.eq("DEL no keys", bs("DEL"))
	d.eq("LPUSH missing value", bs("LPUSH"), d.k("l"))
	d.eq("RPUSH missing value", bs("RPUSH"), d.k("l"))
	d.eq("LPUSHX missing value", bs("LPUSHX"), d.k("l"))
	d.eq("RPUSHX missing value", bs("RPUSHX"), d.k("l"))
	d.eq("SADD missing member", bs("SADD"), d.k("st"))
	d.eq("SREM missing member", bs("SREM"), d.k("st"))
	d.eq("SPOP no key", bs("SPOP"))
	d.eq("SRANDMEMBER no key", bs("SRANDMEMBER"))
	d.eq("HDEL missing field", bs("HDEL"), d.k("h"))
	d.eq("HMGET missing field", bs("HMGET"), d.k("h"))
	d.eq("SUNION no keys", bs("SUNION"))
	d.eq("SINTER no keys", bs("SINTER"))
	d.eq("SDIFF no keys", bs("SDIFF"))
	d.eq("SUNIONSTORE too few", bs("SUNIONSTORE"), d.k("dst"))
	d.eq("ZADD too few", bs("ZADD"), d.k("z"))
	d.eq("ZREM missing member", bs("ZREM"), d.k("z"))
	d.eq("ZRANGE too few", bs("ZRANGE"), d.k("z"), bs("0"))

	t.Logf("compared %d variadic-arity error replies vs Redis 3.2", d.n)
}

// TestDiffCustomErrorLiterals deepens GAPs 4, 5, 8: the commands that write a bespoke
// error string OFF the generic arity/syntax template. A drift here (case, quoting, word
// choice) is invisible to the generic-arity tests but breaks clients that match text.
func TestDiffCustomErrorLiterals(t *testing.T) {
	d := newDiffer(t)

	// GAP 4: HMSET odd field/value replies the uppercase, unquoted literal
	// "ERR wrong number of arguments for HMSET" — NOT the generic lowercase form.
	d.eq("HMSET odd field/value", bs("HMSET"), d.k("h"), bs("f1"), bs("v1"), bs("f2"))
	d.eq("HMSET single field no value", bs("HMSET"), d.k("h"), bs("only"))
	// The generic router-level arity error still fires below the handler's minimum.
	d.eq("HMSET no field at all", bs("HMSET"), d.k("h"))

	// GAP 5: BITCOUNT enforces a special 2-or-4 arg form. Exactly 3 args replies
	// "ERR syntax error" (NOT wrong-number-of-arguments), because -2 arity passes.
	sk := d.k("bcstr")
	d.eq("seed bitcount string", bs("SET"), sk, bs("foobar"))
	d.eq("BITCOUNT 3 args syntax", bs("BITCOUNT"), sk, bs("0"))
	d.eq("BITCOUNT full form ok", bs("BITCOUNT"), sk, bs("0"), bs("0"))
	d.eq("BITCOUNT bare ok", bs("BITCOUNT"), sk)
	// 5 args also passes -2 arity but is an illegal form -> syntax error.
	d.eq("BITCOUNT 5 args syntax", bs("BITCOUNT"), sk, bs("0"), bs("0"), bs("BIT"))

	// GAP 8: ZADD flag-combination errors, distinct from arity/syntax.
	zk := d.k("zflag")
	// (a) NX and XX together -> errZaddNxXx.
	d.eq("ZADD NX XX incompatible", bs("ZADD"), zk, bs("NX"), bs("XX"), bs("1"), bs("m"))
	d.eq("ZADD XX NX incompatible", bs("ZADD"), zk, bs("XX"), bs("NX"), bs("1"), bs("m"))
	// Lowercase / mixed-case flags parse the same (ToUpper), so still incompatible.
	d.eq("ZADD nx xx lowercase", bs("ZADD"), zk, bs("nx"), bs("xx"), bs("1"), bs("m"))
	// (b) INCR with more than one score/member pair -> errZaddIncr.
	d.eq("ZADD INCR two pairs", bs("ZADD"), zk, bs("INCR"), bs("1"), bs("m1"), bs("2"), bs("m2"))
	d.eq("ZADD CH INCR two pairs", bs("ZADD"), zk, bs("CH"), bs("INCR"), bs("1"), bs("a"), bs("2"), bs("b"))
	// (c) flags present but ZERO pairs -> syntax error (even-and-non-empty check).
	d.eq("ZADD NX no pairs syntax", bs("ZADD"), zk, bs("NX"))
	d.eq("ZADD flags then odd pairs", bs("ZADD"), zk, bs("CH"), bs("1"), bs("m"), bs("2"))
	// A single valid INCR pair with NX is legal (must NOT error) — confirms the
	// error only fires on the illegal shapes above.
	d.eq("ZADD NX INCR one pair ok", bs("ZADD"), d.k("zok"), bs("NX"), bs("INCR"), bs("1"), bs("m"))

	t.Logf("compared %d custom-error-literal replies vs Redis 3.2", d.n)
}

// TestDiffWrongTypeCollections deepens GAP 6: WRONGTYPE for every collection op that the
// base file left untested, in both directions (collection op on a string, string/other op
// on a collection). Each op must reply the exact
// "WRONGTYPE Operation against a key holding the wrong kind of value".
func TestDiffWrongTypeCollections(t *testing.T) {
	d := newDiffer(t)

	sk := d.k("str")
	d.eq("seed string", bs("SET"), sk, bs("v"))

	// List reads/mutations on a string key (GAP 6: LINDEX/LPOP/RPOP/LTRIM/LREM/LINSERT).
	d.eq("LINDEX on string", bs("LINDEX"), sk, bs("0"))
	d.eq("LPOP on string", bs("LPOP"), sk)
	d.eq("RPOP on string", bs("RPOP"), sk)
	d.eq("LSET on string", bs("LSET"), sk, bs("0"), bs("x"))
	d.eq("LTRIM on string", bs("LTRIM"), sk, bs("0"), bs("-1"))
	d.eq("LREM on string", bs("LREM"), sk, bs("0"), bs("x"))
	d.eq("LINSERT on string", bs("LINSERT"), sk, bs("BEFORE"), bs("p"), bs("x"))
	d.eq("LPUSHX on string", bs("LPUSHX"), sk, bs("x"))
	d.eq("RPUSHX on string", bs("RPUSHX"), sk, bs("x"))

	// Set reads/mutations on a string key (SREM/SPOP/SRANDMEMBER/SISMEMBER).
	d.eq("SREM on string", bs("SREM"), sk, bs("m"))
	d.eq("SPOP on string", bs("SPOP"), sk)
	d.eq("SRANDMEMBER on string", bs("SRANDMEMBER"), sk)
	d.eq("SISMEMBER on string", bs("SISMEMBER"), sk, bs("m"))

	// Hash arithmetic/scan on a string key (HINCRBY/HINCRBYFLOAT/HSCAN/HEXISTS/HKEYS).
	d.eq("HINCRBY on string", bs("HINCRBY"), sk, bs("f"), bs("1"))
	d.eq("HINCRBYFLOAT on string", bs("HINCRBYFLOAT"), sk, bs("f"), bs("1.5"))
	d.eq("HSCAN on string", bs("HSCAN"), sk, bs("0"))
	d.eq("HEXISTS on string", bs("HEXISTS"), sk, bs("f"))
	d.eq("HKEYS on string", bs("HKEYS"), sk)
	d.eq("HSTRLEN on string", bs("HSTRLEN"), sk, bs("f"))

	// ZSet ops on a string key (ZINCRBY/ZCOUNT/ZRANK/ZCARD/ZSCAN).
	d.eq("ZINCRBY on string", bs("ZINCRBY"), sk, bs("1"), bs("m"))
	d.eq("ZCOUNT on string", bs("ZCOUNT"), sk, bs("0"), bs("1"))
	d.eq("ZRANK on string", bs("ZRANK"), sk, bs("m"))
	d.eq("ZCARD on string", bs("ZCARD"), sk)
	d.eq("ZSCAN on string", bs("ZSCAN"), sk, bs("0"))

	// Now seed each collection type and cross-apply mismatched ops.
	lk := d.k("lst")
	d.eq("seed list", bs("RPUSH"), lk, bs("a"), bs("b"))
	d.eq("SMEMBERS on list", bs("SMEMBERS"), lk)
	d.eq("SCARD on list", bs("SCARD"), lk)
	d.eq("HGETALL on list", bs("HGETALL"), lk)
	d.eq("ZRANGE on list", bs("ZRANGE"), lk, bs("0"), bs("-1"))
	d.eq("GETRANGE on list", bs("GETRANGE"), lk, bs("0"), bs("-1"))
	d.eq("SETRANGE on list", bs("SETRANGE"), lk, bs("0"), bs("x"))
	d.eq("GETBIT on list", bs("GETBIT"), lk, bs("0"))
	d.eq("BITCOUNT on list", bs("BITCOUNT"), lk)

	stk := d.k("set")
	d.eq("seed set", bs("SADD"), stk, bs("m1"), bs("m2"))
	d.eq("LPUSH on set", bs("LPUSH"), stk, bs("x"))
	d.eq("LRANGE on set", bs("LRANGE"), stk, bs("0"), bs("-1"))
	d.eq("HGETALL on set", bs("HGETALL"), stk)
	d.eq("ZSCORE on set", bs("ZSCORE"), stk, bs("m1"))
	d.eq("GET on set", bs("GET"), stk)

	hk := d.k("hash")
	d.eq("seed hash", bs("HSET"), hk, bs("f"), bs("v"))
	d.eq("SADD on hash", bs("SADD"), hk, bs("m"))
	d.eq("RPUSH on hash", bs("RPUSH"), hk, bs("x"))
	d.eq("ZADD on hash", bs("ZADD"), hk, bs("1"), bs("m"))
	d.eq("GET on hash", bs("GET"), hk)
	d.eq("STRLEN on hash", bs("STRLEN"), hk)

	zk := d.k("zset")
	d.eq("seed zset", bs("ZADD"), zk, bs("1"), bs("m"))
	d.eq("SADD on zset", bs("SADD"), zk, bs("x"))
	d.eq("LPUSH on zset", bs("LPUSH"), zk, bs("x"))
	d.eq("HSET on zset", bs("HSET"), zk, bs("f"), bs("v"))
	d.eq("GET on zset", bs("GET"), zk)
	d.eq("APPEND on zset", bs("APPEND"), zk, bs("x"))

	// Set-algebra with a wrong-type operand: SUNION over {string, set} must WRONGTYPE.
	d.eq("SUNION mixed str+set", bs("SUNION"), sk, stk)
	d.eq("SINTER mixed set+list", bs("SINTER"), stk, lk)
	d.eq("SDIFF mixed set+hash", bs("SDIFF"), stk, hk)
	// STORE variants: wrong-type source operand.
	d.eq("SUNIONSTORE wrong src", bs("SUNIONSTORE"), d.k("dst"), sk)
	d.eq("SMOVE wrong src", bs("SMOVE"), sk, stk, bs("m"))
	d.eq("SMOVE wrong dst", bs("SMOVE"), stk, sk, bs("m1"))
	d.eq("RPOPLPUSH wrong src", bs("RPOPLPUSH"), sk, d.k("dst2"))
	d.eq("RPOPLPUSH wrong dst", bs("RPOPLPUSH"), lk, sk)

	t.Logf("compared %d collection wrongtype replies vs Redis 3.2", d.n)
}

// TestDiffErrorPrecedence deepens GAPs 7, 9, 10: which error wins when two conditions
// hold. Redis checks key existence + type BEFORE parsing numeric args, and enforces
// arity at the router before any handler parse. These pin the exact precedence.
func TestDiffErrorPrecedence(t *testing.T) {
	d := newDiffer(t)

	// GAP 7: LSET wrong-type key AND malformed (non-integer) index. Type check wins,
	// so the reply is WRONGTYPE, NOT the not-an-integer parse error.
	sk := d.k("str")
	d.eq("seed string", bs("SET"), sk, bs("v"))
	d.eq("LSET wrongtype+bad index", bs("LSET"), sk, bs("notanint"), bs("x"))
	// LSET on a MISSING key with a bad index -> "no such key" (existence before parse).
	d.eq("LSET missing+bad index", bs("LSET"), d.k("missing"), bs("notanint"), bs("x"))

	// GAP 9: arity is enforced at the router BEFORE any handler parse. ZINCRBY with a
	// non-float increment but correct argc (4) parses -> not-a-valid-float. ZADD with a
	// bad score but arity-OK (4) -> not-a-valid-float. But ZADD with too few args -> arity.
	zk := d.k("z")
	d.eq("ZINCRBY bad float (arity ok)", bs("ZINCRBY"), zk, bs("notafloat"), bs("m"))
	d.eq("ZADD bad score (arity ok)", bs("ZADD"), zk, bs("notafloat"), bs("m"))
	d.eq("ZADD arity fails before parse", bs("ZADD"), zk, bs("notafloat"))
	d.eq("ZCOUNT bad min (arity ok)", bs("ZCOUNT"), zk, bs("notafloat"), bs("1"))
	d.eq("ZCOUNT bad max (arity ok)", bs("ZCOUNT"), zk, bs("0"), bs("notafloat"))
	// HINCRBY on a wrong-type key with a bad delta: type check precedes delta parse.
	d.eq("HINCRBY wrongtype+bad delta", bs("HINCRBY"), sk, bs("f"), bs("notanint"))
	// INCRBY on a wrong-type key (string holds non-numeric) with valid delta -> the
	// value-not-integer error, since the stored value is non-numeric.
	nk := d.k("nonnum")
	d.eq("seed non-numeric", bs("SET"), nk, bs("abc"))
	d.eq("INCRBY non-numeric stored", bs("INCRBY"), nk, bs("5"))

	// GAP 10: SETRANGE with an EMPTY value on a non-string key still checks type first
	// (the WRONGTYPE gate precedes the empty-value short-circuit) -> WRONGTYPE, not int 0.
	lk := d.k("lst")
	d.eq("seed list", bs("RPUSH"), lk, bs("a"))
	d.eq("SETRANGE empty val on list", bs("SETRANGE"), lk, bs("0"), bs(""))
	// SETRANGE empty value on a MISSING key returns length 0 (no type conflict).
	d.eq("SETRANGE empty val on missing", bs("SETRANGE"), d.k("missing2"), bs("0"), bs(""))
	// SETRANGE empty value on a real string returns its current length, not 0.
	d.eq("SETRANGE empty val on string", bs("SETRANGE"), sk, bs("0"), bs(""))
	// SETRANGE negative offset with an EMPTY value: does the offset-range error still
	// fire when there is no write? (offset parse/range precedes the empty short-circuit.)
	d.eq("SETRANGE neg offset empty val", bs("SETRANGE"), sk, bs("-1"), bs(""))

	t.Logf("compared %d error-precedence replies vs Redis 3.2", d.n)
}

// TestDiffNegativeIndexSemantics deepens GAP 11: negative and out-of-range index/range
// args must resolve with Redis' exact from-the-end semantics, including the start>end
// empty-result path. Keys are seeded and REUSED (a fresh d.k per assertion would defeat
// the seed), so state is shared within the function.
func TestDiffNegativeIndexSemantics(t *testing.T) {
	d := newDiffer(t)

	// GETRANGE on a known 11-byte string "hello world".
	sk := d.k("grstr")
	d.eq("seed getrange string", bs("SET"), sk, bs("hello world"))
	d.eq("GETRANGE last 5 (neg,neg)", bs("GETRANGE"), sk, bs("-5"), bs("-1"))
	d.eq("GETRANGE start>end empty", bs("GETRANGE"), sk, bs("0"), bs("-100"))
	d.eq("GETRANGE 0 -5 tail-clip", bs("GETRANGE"), sk, bs("0"), bs("-5"))
	d.eq("GETRANGE full 0 -1", bs("GETRANGE"), sk, bs("0"), bs("-1"))
	d.eq("GETRANGE over-end clamp", bs("GETRANGE"), sk, bs("6"), bs("1000"))
	d.eq("GETRANGE both past end", bs("GETRANGE"), sk, bs("100"), bs("200"))
	d.eq("GETRANGE neg past start", bs("GETRANGE"), sk, bs("-100"), bs("-50"))
	d.eq("GETRANGE start==end", bs("GETRANGE"), sk, bs("4"), bs("4"))
	d.eq("GETRANGE start>end plain", bs("GETRANGE"), sk, bs("5"), bs("2"))

	// GETRANGE on a MISSING key -> empty bulk regardless of indices.
	d.eq("GETRANGE missing key", bs("GETRANGE"), d.k("grmiss"), bs("0"), bs("-1"))

	// LRANGE negative/out-of-range on a known 5-element list.
	lk := d.k("lrlist")
	d.eq("seed lrange list", bs("RPUSH"), lk, bs("a"), bs("b"), bs("c"), bs("d"), bs("e"))
	d.eq("LRANGE full 0 -1", bs("LRANGE"), lk, bs("0"), bs("-1"))
	d.eq("LRANGE last 2 (neg)", bs("LRANGE"), lk, bs("-2"), bs("-1"))
	d.eq("LRANGE start>end empty", bs("LRANGE"), lk, bs("3"), bs("1"))
	d.eq("LRANGE over-end clamp", bs("LRANGE"), lk, bs("2"), bs("100"))
	d.eq("LRANGE neg past start clamp", bs("LRANGE"), lk, bs("-100"), bs("2"))
	d.eq("LRANGE both past end empty", bs("LRANGE"), lk, bs("10"), bs("20"))

	// LINDEX negative and out-of-range.
	d.eq("LINDEX -1 last", bs("LINDEX"), lk, bs("-1"))
	d.eq("LINDEX -5 first", bs("LINDEX"), lk, bs("-5"))
	d.eq("LINDEX out-of-range nil", bs("LINDEX"), lk, bs("100"))
	d.eq("LINDEX neg out-of-range nil", bs("LINDEX"), lk, bs("-100"))

	// LSET out-of-range index (valid list, bad index) -> "index out of range".
	d.eq("LSET index out of range", bs("LSET"), lk, bs("100"), bs("x"))
	d.eq("LSET neg out of range", bs("LSET"), lk, bs("-100"), bs("x"))
	d.eq("LSET -1 in range ok", bs("LSET"), lk, bs("-1"), bs("E"))

	// ZRANGE negative rank on a known 3-member zset.
	zk := d.k("zrng")
	d.eq("seed zrange zset", bs("ZADD"), zk, bs("1"), bs("a"), bs("2"), bs("b"), bs("3"), bs("c"))
	d.eq("ZRANGE full 0 -1", bs("ZRANGE"), zk, bs("0"), bs("-1"))
	d.eq("ZRANGE last (neg)", bs("ZRANGE"), zk, bs("-1"), bs("-1"))
	d.eq("ZRANGE start>end empty", bs("ZRANGE"), zk, bs("2"), bs("0"))
	d.eq("ZRANGE over-end clamp", bs("ZRANGE"), zk, bs("1"), bs("100"))

	// SETBIT / GETBIT extreme offsets are covered elsewhere; here focus on GETBIT past
	// the current length -> 0 (no error).
	bk := d.k("bit")
	d.eq("seed bit", bs("SETBIT"), bk, bs("7"), bs("1"))
	d.eq("GETBIT past end 0", bs("GETBIT"), bk, bs("100"))

	t.Logf("compared %d negative/out-of-range index replies vs Redis 3.2", d.n)
}

// TestDiffScoreSpellings deepens GAP 12: the exact set of inf/-inf/nan spellings ZADD /
// ZINCRBY / ZCOUNT accept or reject. A silent divergence in accepted spellings breaks
// range queries. ZSCORE readback confirms the stored score formats identically too.
func TestDiffScoreSpellings(t *testing.T) {
	d := newDiffer(t)

	// NOTE — STORING an infinite score is an ACCEPTED architectural divergence and is
	// deliberately NOT compared here. Redis 3.2 accepts ZADD inf / +inf / -inf (in every
	// case form) and reads them back as "inf"/"-inf", but DynamoDB's Number type has no
	// representation for infinity, so redimos cannot persist one. redimos instead rejects
	// an infinite ZADD/ZINCRBY score up front with a clean, deterministic error
	// ("ERR score must be a finite number") rather than attempt a doomed backend write —
	// see the storeScore guard and TestScoreNotFinite in internal/command. What IS
	// compared below is the set of spellings BOTH sides handle identically: those both
	// REJECT at parse (nan / leading-space / non-float text), those both ACCEPT for a
	// finite stored score (empty->0, hex float), and inf as a range BOUND (never stored).
	zk := d.k("zinf")

	// nan (any case) is rejected -> not-a-valid-float, on both endpoints.
	d.eq("ZADD nan rejected", bs("ZADD"), zk, bs("nan"), bs("x"))
	d.eq("ZADD NaN rejected", bs("ZADD"), zk, bs("NaN"), bs("x"))
	d.eq("ZADD NAN rejected", bs("ZADD"), zk, bs("NAN"), bs("x"))
	// A leading space is rejected by both (Redis' getDoubleFromObject rejects isspace()
	// on the first byte; Go's strconv rejects it too).
	d.eq("ZADD ' inf' leading space", bs("ZADD"), zk, bs(" inf"), bs("x"))
	// Redis' strtod-based parse ACCEPTS an empty score as 0.0 and a hex float — redimos
	// matches both (empty is special-cased in parseScore; Go's strconv parses "0x1p4").
	// The two commands run in sequence: "" adds member x with score 0 (:1), then "0x1p4"
	// updates the same x to 16 (:0), so both replies agree on both endpoints. NOTE: the
	// full-word "infinity"/"+infinity" spelling is deliberately NOT compared — Redis parses
	// it to inf and stores it, which is the accepted DynamoDB-domain divergence documented
	// above (redimos rejects it via storeScore).
	d.eq("ZADD empty score -> 0.0", bs("ZADD"), zk, bs(""), bs("x"))
	d.eq("ZADD hex 0x1p4 score -> 16", bs("ZADD"), zk, bs("0x1p4"), bs("x"))

	// ZINCRBY with an infinite increment is likewise a store-path operation redimos
	// cannot persist (same DynamoDB numeric-domain limit as ZADD above), so it is not
	// compared. Only the nan-increment REJECTION — which both endpoints share — is.
	zik := d.k("zincr")
	d.eq("ZINCRBY nan rejected", bs("ZINCRBY"), zik, bs("nan"), bs("m2"))

	// ZCOUNT bounds accept inf/-inf and the exclusive '(' prefix; -inf..+inf spans all.
	zck := d.k("zcount")
	d.eq("seed zcount", bs("ZADD"), zck, bs("1"), bs("a"), bs("2"), bs("b"), bs("3"), bs("c"))
	d.eq("ZCOUNT -inf +inf all", bs("ZCOUNT"), zck, bs("-inf"), bs("+inf"))
	d.eq("ZCOUNT exclusive (1 3", bs("ZCOUNT"), zck, bs("(1"), bs("3"))
	d.eq("ZCOUNT exclusive (1 (3", bs("ZCOUNT"), zck, bs("(1"), bs("(3"))
	d.eq("ZCOUNT bad min not-float", bs("ZCOUNT"), zck, bs("notafloat"), bs("3"))
	d.eq("ZCOUNT nan bound reject", bs("ZCOUNT"), zck, bs("nan"), bs("3"))
	d.eq("ZCOUNT ( only", bs("ZCOUNT"), zck, bs("("), bs("3"))

	// ZRANGEBYSCORE with inf bounds returns the full ordered range.
	d.eq("ZRANGEBYSCORE -inf +inf", bs("ZRANGEBYSCORE"), zck, bs("-inf"), bs("+inf"))

	t.Logf("compared %d score-spelling replies vs Redis 3.2", d.n)
}
