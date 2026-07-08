package integration

import "testing"

// Dimension K (key lifecycle / delete-on-empty) + L (mutation return-value semantics),
// DEEPENED. These cases target the specific edge inputs enumerated in the depth gap file:
// String-only ops rejecting non-String collections (WRONGTYPE precedence over any old-value
// return), ZADD CH per-member change counting, SMOVE with source==destination, the *X /
// LINSERT distinction between an absent key (0) and an existing-key-missing-pivot (-1) after
// a delete-on-empty, TTL clearing on delete-recreate for EVERY collection type (not just
// Set), and the observable atomicity of last-element deletion. All flows are single
// connection / sequential — the regime redimos is expected to match — so a subsequent read
// on the SAME connection is the strongest observation the differ can make byte-for-byte; the
// documented concurrent-DEL divergences are intentionally NOT exercised here.

// GAP 1: GETSET / APPEND (String-only) on a non-String key must reply WRONGTYPE immediately,
// before any old-value return path, and must NOT mutate the key.
func TestDiffDepthLifecycle_StringOpsWrongType(t *testing.T) {
	d := newDiffer(t)

	// GETSET on a hash: WRONGTYPE, no update.
	h := d.k("gswt_hash")
	d.eq("HSET seed", bs("HSET"), h, bs("f"), bs("v"))
	d.eq("GETSET on hash -> WRONGTYPE", bs("GETSET"), h, bs("newval"))
	d.eq("hash field intact after GETSET", bs("HGET"), h, bs("f"))
	d.eq("hash TYPE unchanged", bs("TYPE"), h)

	// GETSET on a set.
	s := d.k("gswt_set")
	d.eq("SADD seed", bs("SADD"), s, bs("m1"), bs("m2"))
	d.eq("GETSET on set -> WRONGTYPE", bs("GETSET"), s, bs("newval"))
	d.eq("set intact after GETSET", bs("SCARD"), s)

	// GETSET on a list.
	l := d.k("gswt_list")
	d.eq("RPUSH seed", bs("RPUSH"), l, bs("a"), bs("b"))
	d.eq("GETSET on list -> WRONGTYPE", bs("GETSET"), l, bs("newval"))
	d.eq("list intact after GETSET", bs("LLEN"), l)

	// GETSET on a zset.
	z := d.k("gswt_zset")
	d.eq("ZADD seed", bs("ZADD"), z, bs("1"), bs("a"))
	d.eq("GETSET on zset -> WRONGTYPE", bs("GETSET"), z, bs("newval"))
	d.eq("zset intact after GETSET", bs("ZCARD"), z)

	// APPEND (String-only) on each collection type: WRONGTYPE, no length return, no mutation.
	ah := d.k("apwt_hash")
	d.eq("HSET seed", bs("HSET"), ah, bs("f"), bs("v"))
	d.eq("APPEND on hash -> WRONGTYPE", bs("APPEND"), ah, bs("x"))
	d.eq("hash field intact after APPEND", bs("HGET"), ah, bs("f"))

	as := d.k("apwt_set")
	d.eq("SADD seed", bs("SADD"), as, bs("m"))
	d.eq("APPEND on set -> WRONGTYPE", bs("APPEND"), as, bs("x"))
	d.eq("set intact after APPEND", bs("SCARD"), as)

	al := d.k("apwt_list")
	d.eq("RPUSH seed", bs("RPUSH"), al, bs("a"))
	d.eq("APPEND on list -> WRONGTYPE", bs("APPEND"), al, bs("x"))
	d.eq("list intact after APPEND", bs("LLEN"), al)

	az := d.k("apwt_zset")
	d.eq("ZADD seed", bs("ZADD"), az, bs("1"), bs("m"))
	d.eq("APPEND on zset -> WRONGTYPE", bs("APPEND"), az, bs("x"))
	d.eq("zset intact after APPEND", bs("ZCARD"), az)
}

// GAP 2: ZADD CH counts every member-change event (add=1 AND score-update=1) within one
// command, not just additions.
func TestDiffDepthMutation_ZAddCHPartial(t *testing.T) {
	d := newDiffer(t)

	k := d.k("zaddch")
	d.eq("ZADD 1 a 2 b -> 2", bs("ZADD"), k, bs("1"), bs("a"), bs("2"), bs("b"))
	// CH with one score change (a: 1->1 no, use different score) and one add (c).
	d.eq("ZADD CH 1 a 3 c -> 1 (a unchanged, c added)", bs("ZADD"), k, bs("CH"), bs("1"), bs("a"), bs("3"), bs("c"))
	// Now a changed score AND a new add in the same command -> counts both.
	d.eq("ZADD CH 9 a 4 dd -> 2 (a changed, dd added)", bs("ZADD"), k, bs("CH"), bs("9"), bs("a"), bs("4"), bs("dd"))
	// Multiple score updates only (no adds) -> counts each change.
	d.eq("ZADD CH 10 a 11 b -> 2 (both changed)", bs("ZADD"), k, bs("CH"), bs("10"), bs("a"), bs("11"), bs("b"))
	// Same scores again -> 0 changes under CH.
	d.eq("ZADD CH 10 a 11 b -> 0 (no change)", bs("ZADD"), k, bs("CH"), bs("10"), bs("a"), bs("11"), bs("b"))
	// Without CH the same repeat returns number ADDED only -> 0.
	d.eq("ZADD (no CH) 12 a -> 0 (updated not added)", bs("ZADD"), k, bs("12"), bs("a"))
	// Duplicate member listed twice in one CH command: last write wins, counted once.
	d.eq("ZADD CH dup member -> counted once", bs("ZADD"), k, bs("CH"), bs("20"), bs("a"), bs("21"), bs("a"))
	d.eq("final score of a reflects last dup", bs("ZSCORE"), k, bs("a"))
}

// GAP 3: SMOVE with source == destination. The member exists in the (single) set both before
// and after; SMOVE returns 1 (member present in source) and cardinality is unchanged.
func TestDiffDepthMutation_SMoveSameSet(t *testing.T) {
	d := newDiffer(t)

	k := d.k("smsame")
	d.eq("SADD m1 m2", bs("SADD"), k, bs("m1"), bs("m2"))
	d.eq("SMOVE src==dst present -> 1", bs("SMOVE"), k, k, bs("m1"))
	d.eq("member still present", bs("SISMEMBER"), k, bs("m1"))
	d.eq("cardinality unchanged", bs("SCARD"), k)
	// SMOVE src==dst of an absent member -> 0, no change.
	d.eq("SMOVE src==dst absent -> 0", bs("SMOVE"), k, k, bs("nope"))
	d.eq("cardinality still 2", bs("SCARD"), k)
	d.eqSorted("members unchanged", bs("SMEMBERS"), k)
}

// GAP 4 + GAP 8: after a key is emptied (and thus deleted) by a mutation, LPUSHX/RPUSHX must
// treat it as absent and return 0, and a subsequent same-connection read sees it gone — i.e.
// the delete on reaching cardinality 0 is observable before the *X reply.
func TestDiffDepthLifecycle_PushXAfterEmptied(t *testing.T) {
	d := newDiffer(t)

	// Empty via LREM, then LPUSHX/RPUSHX must return 0 (key deleted, not present-but-empty).
	lr := d.k("pushx_lrem")
	d.eq("RPUSH v v v", bs("RPUSH"), lr, bs("v"), bs("v"), bs("v"))
	d.eq("LREM 0 v empties -> 3", bs("LREM"), lr, bs("0"), bs("v"))
	assertGone(d, "after LREM all", lr)
	d.eq("LPUSHX on deleted -> 0", bs("LPUSHX"), lr, bs("a"))
	d.eq("RPUSHX on deleted -> 0", bs("RPUSHX"), lr, bs("b"))
	d.eq("still absent after failed *X", bs("EXISTS"), lr)
	d.eq("TYPE still none", bs("TYPE"), lr)

	// Empty via LTRIM to an out-of-range window, then *X -> 0.
	lt := d.k("pushx_ltrim")
	d.eq("RPUSH a b c", bs("RPUSH"), lt, bs("a"), bs("b"), bs("c"))
	d.eq("LTRIM 5 10 empties", bs("LTRIM"), lt, bs("5"), bs("10"))
	assertGone(d, "after LTRIM empty", lt)
	d.eq("RPUSHX on deleted -> 0", bs("RPUSHX"), lt, bs("z"))
	d.eq("LPUSHX on deleted -> 0", bs("LPUSHX"), lt, bs("z"))
	d.eq("still absent", bs("EXISTS"), lt)

	// Last element via LPOP/RPOP: after the pop the key is gone, and a subsequent LRANGE
	// (same connection) sees it as absent (empty array), not an empty-but-present list.
	pl := d.k("pushx_pop")
	d.eq("RPUSH only", bs("RPUSH"), pl, bs("only"))
	d.eq("RPOP last", bs("RPOP"), pl)
	assertGone(d, "after RPOP last", pl)
	d.eq("LRANGE on gone -> empty array", bs("LRANGE"), pl, bs("0"), bs("-1"))
	d.eq("LLEN on gone -> 0", bs("LLEN"), pl)
	d.eq("LPUSHX on gone -> 0", bs("LPUSHX"), pl, bs("x"))
	d.eq("still absent after LPUSHX", bs("EXISTS"), pl)
	// LPOP path symmetric.
	pl2 := d.k("pushx_pop2")
	d.eq("LPUSH only", bs("LPUSH"), pl2, bs("only"))
	d.eq("LPOP last", bs("LPOP"), pl2)
	assertGone(d, "after LPOP last", pl2)
	d.eq("LRANGE on gone -> empty array", bs("LRANGE"), pl2, bs("0"), bs("-1"))
	d.eq("RPUSHX on gone -> 0", bs("RPUSHX"), pl2, bs("x"))
}

// GAP 5: LINSERT must distinguish an ABSENT key (return 0) from an EXISTING key whose pivot
// is missing (return -1). Exercise both, including the absent-because-just-emptied path.
func TestDiffDepthMutation_LInsertAbsentVsPivot(t *testing.T) {
	d := newDiffer(t)

	// Never-existed key -> 0.
	miss := d.k("linsert_miss")
	d.eq("LINSERT on never-existed -> 0", bs("LINSERT"), miss, bs("BEFORE"), bs("p"), bs("v"))
	d.eq("AFTER on never-existed -> 0", bs("LINSERT"), miss, bs("AFTER"), bs("p"), bs("v"))
	d.eq("still absent", bs("EXISTS"), miss)

	// Key emptied by LTRIM (delete-on-empty) -> absent -> 0, not -1.
	emptied := d.k("linsert_emptied")
	d.eq("RPUSH a b c", bs("RPUSH"), emptied, bs("a"), bs("b"), bs("c"))
	d.eq("LTRIM 5 10 empties", bs("LTRIM"), emptied, bs("5"), bs("10"))
	assertGone(d, "after LTRIM", emptied)
	d.eq("LINSERT on emptied key -> 0", bs("LINSERT"), emptied, bs("BEFORE"), bs("p"), bs("v"))
	d.eq("still absent after LINSERT", bs("EXISTS"), emptied)

	// Existing key, pivot absent -> -1 (key survives).
	present := d.k("linsert_present")
	d.eq("RPUSH x y z", bs("RPUSH"), present, bs("x"), bs("y"), bs("z"))
	d.eq("LINSERT BEFORE absent-pivot -> -1", bs("LINSERT"), present, bs("BEFORE"), bs("nope"), bs("V"))
	d.eq("LINSERT AFTER absent-pivot -> -1", bs("LINSERT"), present, bs("AFTER"), bs("nope"), bs("V"))
	d.eq("length unchanged after failed pivot", bs("LLEN"), present)
	// Present key, pivot present -> new length.
	d.eq("LINSERT BEFORE present-pivot -> 4", bs("LINSERT"), present, bs("BEFORE"), bs("y"), bs("YY"))
	d.eq("length grew", bs("LLEN"), present)
	d.eq("order after insert", bs("LRANGE"), present, bs("0"), bs("-1"))
}

// GAP 6: TTL is cleared on delete-recreate for EVERY collection type. Set is covered in
// lifecycle_test.go; here we cover Hash, ZSet, and List. Set an EXPIRE, empty the key (which
// deletes it), re-add, and TTL must be -1 (no lingering expiry) on both endpoints.
func TestDiffDepthLifecycle_TTLClearedAllTypes(t *testing.T) {
	d := newDiffer(t)

	// ZSet: ZADD -> EXPIRE -> ZREM last (empties) -> ZADD recreate -> TTL -1.
	z := d.k("ttl_zset")
	d.eq("ZADD a", bs("ZADD"), z, bs("1"), bs("a"))
	d.eq("EXPIRE 10000", bs("EXPIRE"), z, bs("10000"))
	d.eqIntClose("TTL ~10000", 2, bs("TTL"), z)
	d.eq("ZREM last empties", bs("ZREM"), z, bs("a"))
	assertGone(d, "zset after empty", z)
	d.eq("ZADD recreate", bs("ZADD"), z, bs("2"), bs("b"))
	d.eq("TTL after recreate = -1", bs("TTL"), z)

	// Hash: HSET -> EXPIRE -> HDEL last -> HSET recreate -> TTL -1.
	h := d.k("ttl_hash")
	d.eq("HSET f v", bs("HSET"), h, bs("f"), bs("v"))
	d.eq("EXPIRE 10000", bs("EXPIRE"), h, bs("10000"))
	d.eqIntClose("TTL ~10000", 2, bs("TTL"), h)
	d.eq("HDEL last empties", bs("HDEL"), h, bs("f"))
	assertGone(d, "hash after empty", h)
	d.eq("HSET recreate", bs("HSET"), h, bs("g"), bs("w"))
	d.eq("TTL after recreate = -1", bs("TTL"), h)

	// List: RPUSH -> EXPIRE -> RPOP last -> RPUSH recreate -> TTL -1.
	l := d.k("ttl_list")
	d.eq("RPUSH only", bs("RPUSH"), l, bs("only"))
	d.eq("EXPIRE 10000", bs("EXPIRE"), l, bs("10000"))
	d.eqIntClose("TTL ~10000", 2, bs("TTL"), l)
	d.eq("RPOP last empties", bs("RPOP"), l)
	assertGone(d, "list after empty", l)
	d.eq("RPUSH recreate", bs("RPUSH"), l, bs("x"))
	d.eq("TTL after recreate = -1", bs("TTL"), l)

	// Also cover the ZREMRANGEBYRANK emptying path clearing TTL, and LTRIM emptying path.
	z2 := d.k("ttl_zset_rank")
	d.eq("ZADD a b", bs("ZADD"), z2, bs("1"), bs("a"), bs("2"), bs("b"))
	d.eq("EXPIRE 10000", bs("EXPIRE"), z2, bs("10000"))
	d.eqIntClose("TTL ~10000", 2, bs("TTL"), z2)
	d.eq("ZREMRANGEBYRANK 0 -1 empties", bs("ZREMRANGEBYRANK"), z2, bs("0"), bs("-1"))
	assertGone(d, "zset after rank-remove", z2)
	d.eq("ZADD recreate", bs("ZADD"), z2, bs("3"), bs("c"))
	d.eq("TTL after recreate = -1", bs("TTL"), z2)

	l2 := d.k("ttl_list_trim")
	d.eq("RPUSH a b c", bs("RPUSH"), l2, bs("a"), bs("b"), bs("c"))
	d.eq("EXPIRE 10000", bs("EXPIRE"), l2, bs("10000"))
	d.eqIntClose("TTL ~10000", 2, bs("TTL"), l2)
	d.eq("LTRIM 5 10 empties", bs("LTRIM"), l2, bs("5"), bs("10"))
	assertGone(d, "list after trim", l2)
	d.eq("RPUSH recreate", bs("RPUSH"), l2, bs("q"))
	d.eq("TTL after recreate = -1", bs("TTL"), l2)
}

// GAP 7 + GAP 8 (sequential observable): last-member removal deletes the key immediately and
// the deletion is visible to the very next same-connection read (EXISTS/TYPE/CARD/read). This
// asserts the delete-on-empty is committed before the mutation reply is returned, for each
// collection type — the strongest byte-for-byte observation the differ can make without
// cross-connection concurrency.
func TestDiffDepthLifecycle_LastElementDeleteVisible(t *testing.T) {
	d := newDiffer(t)

	// Set: SREM last -> gone; SISMEMBER/SCARD/SMEMBERS all see absent.
	s := d.k("last_set")
	d.eq("SADD only", bs("SADD"), s, bs("only"))
	d.eq("SREM last -> 1", bs("SREM"), s, bs("only"))
	d.eq("EXISTS -> 0", bs("EXISTS"), s)
	d.eq("SCARD -> 0", bs("SCARD"), s)
	d.eq("SISMEMBER -> 0", bs("SISMEMBER"), s, bs("only"))
	d.eqSorted("SMEMBERS -> empty", bs("SMEMBERS"), s)
	d.eq("SREM again -> 0 (already gone)", bs("SREM"), s, bs("only"))

	// Hash: HDEL last -> gone; HGET/HLEN/HGETALL see absent.
	h := d.k("last_hash")
	d.eq("HSET only", bs("HSET"), h, bs("f"), bs("v"))
	d.eq("HDEL last -> 1", bs("HDEL"), h, bs("f"))
	d.eq("EXISTS -> 0", bs("EXISTS"), h)
	d.eq("HLEN -> 0", bs("HLEN"), h)
	d.eq("HGET -> nil", bs("HGET"), h, bs("f"))
	d.eqSorted("HGETALL -> empty", bs("HGETALL"), h)
	d.eq("HDEL again -> 0", bs("HDEL"), h, bs("f"))

	// ZSet: ZREM last -> gone; ZSCORE/ZCARD/ZRANGE see absent.
	z := d.k("last_zset")
	d.eq("ZADD only", bs("ZADD"), z, bs("5"), bs("only"))
	d.eq("ZREM last -> 1", bs("ZREM"), z, bs("only"))
	d.eq("EXISTS -> 0", bs("EXISTS"), z)
	d.eq("ZCARD -> 0", bs("ZCARD"), z)
	d.eq("ZSCORE -> nil", bs("ZSCORE"), z, bs("only"))
	d.eq("ZRANGE -> empty", bs("ZRANGE"), z, bs("0"), bs("-1"))
	d.eq("ZREM again -> 0", bs("ZREM"), z, bs("only"))

	// List: RPOP last -> gone; LINDEX/LLEN/LRANGE see absent.
	l := d.k("last_list")
	d.eq("RPUSH only", bs("RPUSH"), l, bs("only"))
	d.eq("LPOP last", bs("LPOP"), l)
	d.eq("EXISTS -> 0", bs("EXISTS"), l)
	d.eq("LLEN -> 0", bs("LLEN"), l)
	d.eq("LINDEX 0 -> nil", bs("LINDEX"), l, bs("0"))
	d.eq("LRANGE -> empty", bs("LRANGE"), l, bs("0"), bs("-1"))
	d.eq("LPOP again -> nil (already gone)", bs("LPOP"), l)
	d.eq("RPOP on gone -> nil", bs("RPOP"), l)
}

// GAP 8 (SPOP variant): SPOP of the last member deletes the key, and the reply plus the
// post-state (gone) match. Also SPOP with count of the entire set empties+deletes.
func TestDiffDepthLifecycle_SPopEmpties(t *testing.T) {
	d := newDiffer(t)

	// SPOP with count == cardinality empties and deletes.
	s := d.k("spop_countall")
	d.eq("SADD a b c", bs("SADD"), s, bs("a"), bs("b"), bs("c"))
	d.eqSorted("SPOP 3 -> all members", bs("SPOP"), s, bs("3"))
	assertGone(d, "after SPOP count=all", s)
	d.eq("SCARD -> 0", bs("SCARD"), s)
	d.eqSorted("SMEMBERS -> empty", bs("SMEMBERS"), s)

	// SPOP with count > cardinality returns all and deletes.
	s2 := d.k("spop_countover")
	d.eq("SADD x y", bs("SADD"), s2, bs("x"), bs("y"))
	d.eqSorted("SPOP 100 -> x y", bs("SPOP"), s2, bs("100"))
	assertGone(d, "after SPOP count>card", s2)

	// SPOP count=0 on a populated set is a no-op returning empty, key survives.
	s3 := d.k("spop_zero")
	d.eq("SADD m", bs("SADD"), s3, bs("m"))
	d.eqSorted("SPOP 0 -> empty", bs("SPOP"), s3, bs("0"))
	d.eq("EXISTS still 1", bs("EXISTS"), s3)
	d.eq("SCARD still 1", bs("SCARD"), s3)

	// SPOP (no count) on a singleton deletes.
	s4 := d.k("spop_single")
	d.eq("SADD only", bs("SADD"), s4, bs("only"))
	d.eq("SPOP -> only", bs("SPOP"), s4)
	assertGone(d, "after SPOP single", s4)
}
