package integration

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Dimension AC (atomicity/concurrency), extending the existing SETNX/INCR/register tests.
// These are NOT byte-differentials against Redis (redimos is deliberately non-linearizable
// for some ops); they assert the INTERNAL safety guarantee each op actually provides and
// RE-MEASURE it on the current redimo/v3 build (the memory's contention numbers are from a
// much older version). Two flavours:
//   - "exact" invariants for the atomic paths (meta.cnt ADD / SETCAS / index counters):
//     the final cardinality/value must equal the acknowledged operations — a real
//     regression guard.
//   - "safety" invariants for the known non-atomic RMW paths: the structure must stay
//     valid (no corruption, no crash, bounded), documenting the divergence without a
//     brittle exact assertion.

func acThunk(t *testing.T, addr string, clients int, fn func(c *respConn, id int)) {
	t.Helper()
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := dial(t, addr)
			<-start
			fn(c, id)
		}(i)
	}
	close(start)
	wg.Wait()
}

func acInt(t *testing.T, c *respConn, args ...[]byte) int64 {
	t.Helper()
	n, ok := intReply(c.do(args...))
	if !ok {
		t.Fatalf("expected integer reply for %q", joinArgs(args))
	}
	return n
}

// TestConcurrentCollectionAddsAtomic: concurrent SADD/HSET/RPUSH of DISTINCT members
// must not lose a single acknowledged add — the meta.cnt ADD counter and the independent
// per-member item writes never conflict, so the final cardinality equals the number of
// acknowledged adds. A lost add here would be a real regression in the atomic write path.
func TestConcurrentCollectionAddsAtomic(t *testing.T) {
	addr := proxyAddr(t)
	const clients = 64
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)

	// SADD distinct members.
	ks := []byte("ac:sadd:" + nonce)
	var sAck int64
	acThunk(t, addr, clients, func(c *respConn, id int) {
		if n, ok := intReply(c.do(bs("SADD"), ks, bs("m"+itoa(id)))); ok && n == 1 {
			atomic.AddInt64(&sAck, 1)
		}
	})
	if got := acInt(t, dial(t, addr), bs("SCARD"), ks); got != sAck {
		t.Errorf("SADD: SCARD=%d, want %d (acknowledged distinct adds) — atomic count path lost an add", got, sAck)
	}

	// HSET distinct fields.
	kh := []byte("ac:hset:" + nonce)
	var hAck int64
	acThunk(t, addr, clients, func(c *respConn, id int) {
		if n, ok := intReply(c.do(bs("HSET"), kh, bs("f"+itoa(id)), bs("v"))); ok && n == 1 {
			atomic.AddInt64(&hAck, 1)
		}
	})
	if got := acInt(t, dial(t, addr), bs("HLEN"), kh); got != hAck {
		t.Errorf("HSET: HLEN=%d, want %d — atomic count path lost a field", got, hAck)
	}

	// RPUSH: every acknowledged push must be present (LLEN == acks). Order is not asserted
	// (concurrent interleaving), only that no element is lost.
	kl := []byte("ac:rpush:" + nonce)
	var lAck int64
	acThunk(t, addr, clients, func(c *respConn, id int) {
		if _, ok := intReply(c.do(bs("RPUSH"), kl, bs("e"+itoa(id)))); ok {
			atomic.AddInt64(&lAck, 1)
		}
	})
	if got := acInt(t, dial(t, addr), bs("LLEN"), kl); got != lAck {
		t.Errorf("RPUSH: LLEN=%d, want %d — a concurrent push was lost", got, lAck)
	}

	t.Logf("atomic collection adds: SADD SCARD=%d, HSET HLEN=%d, RPUSH LLEN=%d (all == acknowledged, %d-way contention)", sAck, hAck, lAck, clients)
}

// TestConcurrentHIncrByReMeasure re-measures HINCRBY on the same field under contention.
// redimo's HINCRBY uses a DynamoDB atomic ADD (UpdateExpression "ADD #val :delta"), so —
// unlike a naive read-modify-write — every acknowledged HINCRBY should move the field by
// its delta with NO lost update, even under heavy contention. (An earlier redimo version
// measured heavy loss here; this pins the current guarantee.)
func TestConcurrentHIncrByReMeasure(t *testing.T) {
	addr := proxyAddr(t)
	const clients, perClient = 32, 30
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	k := []byte("ac:hincrby:" + nonce)

	var acked int64
	acThunk(t, addr, clients, func(c *respConn, id int) {
		for j := 0; j < perClient; j++ {
			if _, ok := intReply(c.do(bs("HINCRBY"), k, bs("counter"), bs("1"))); ok {
				atomic.AddInt64(&acked, 1)
			}
		}
	})

	got, ok := intReply(dial(t, addr).do(bs("HGET"), k, bs("counter"))) // HGET returns a bulk; parse below
	if !ok {
		// HGET returns a bulk string, not :N — parse the decimal payload.
		p, okp := bulkPayload(dial(t, addr).do(bs("HGET"), k, bs("counter")))
		if !okp {
			t.Fatalf("HGET counter: unexpected reply")
		}
		v, err := strconv.ParseInt(string(p), 10, 64)
		if err != nil {
			t.Fatalf("HGET counter not an int: %q", p)
		}
		got = v
	}
	if got != acked {
		t.Errorf("HINCRBY: counter=%d, want %d (= acknowledged HINCRBYs) — the atomic ADD lost or double-counted an update", got, acked)
	}
	t.Logf("HINCRBY atomicity (DynamoDB ADD): counter=%d == %d acknowledged, %d-way contention", got, acked, clients)
}

// TestConcurrentIdempotentAdd: many clients racing to SADD the SAME member.
//
// After the redimo/v3.1 count fix, SADD writes each member with an individual PutItem +
// ReturnValue ALL_OLD, which DynamoDB serializes on the single member item — so the added
// count is concurrency-EXACT and all three quantities agree with Redis:
//   - EXACTLY ONE client sees :1 (its PutItem found no prior item); the other 47 see :0;
//   - SCARD (meta.cnt, advanced by the exact added count) EQUALS the true cardinality, 1;
//   - SMEMBERS is {"same"} — the member item is idempotent regardless.
//
// This is the regression guard for the fix. BEFORE it, SADD derived "added" from a pre-write
// BatchGetItem snapshot: several clients each read "absent" and returned :1, inflating both
// the return and SCARD above the true cardinality (the stored set was always correct). If
// this test starts logging winners/SCARD > 1 again, the snapshot path has regressed.
func TestConcurrentIdempotentAdd(t *testing.T) {
	addr := proxyAddr(t)
	const clients = 48
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	k := []byte("ac:idem:" + nonce)

	var winners int64
	acThunk(t, addr, clients, func(c *respConn, id int) {
		if n, ok := intReply(c.do(bs("SADD"), k, bs("same"))); ok && n == 1 {
			atomic.AddInt64(&winners, 1)
		}
	})
	final := dial(t, addr)
	members, ok := respArrayElements(final.do(bs("SMEMBERS"), k))
	if !ok {
		t.Fatalf("SMEMBERS not an array")
	}
	if len(members) != 1 || (len(members) == 1 && members[0] != "same") {
		t.Errorf("idempotent SADD: SMEMBERS=%v, want exactly [\"same\"] (stored membership must be idempotent)", members)
	}
	scard, _ := intReply(final.do(bs("SCARD"), k))
	if scard != 1 {
		t.Errorf("idempotent SADD: SCARD=%d, want 1 — the exact-count path regressed (cnt drift is back)", scard)
	}
	if winners != 1 {
		t.Errorf("idempotent SADD: %d clients saw :1, want exactly 1 (serialized ALL_OLD add must pick one winner)", winners)
	}
	t.Logf("idempotent SADD under %d-way contention: SMEMBERS={same}, SCARD=%d, winners=%d — all exact (cnt no longer drifts)", clients, scard, winners)
}

// TestConcurrentIdempotentRemove is the SREM mirror: many clients racing to remove the SAME
// member. SREM now deletes with DeleteItem + ReturnValue ALL_OLD (serialized), so EXACTLY
// ONE client sees :1 and SCARD lands at exactly the survivor count — before the fix the
// pre-write snapshot let several clients each see the member "present" and return :1,
// deflating SCARD below the true cardinality.
func TestConcurrentIdempotentRemove(t *testing.T) {
	addr := proxyAddr(t)
	const clients = 48
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	k := []byte("ac:idemrem:" + nonce)

	// Seed the victim plus a survivor so removing "same" never empties (and deletes) the key.
	dial(t, addr).do(bs("SADD"), k, bs("same"), bs("keep"))

	var winners int64
	acThunk(t, addr, clients, func(c *respConn, id int) {
		if n, ok := intReply(c.do(bs("SREM"), k, bs("same"))); ok && n == 1 {
			atomic.AddInt64(&winners, 1)
		}
	})
	final := dial(t, addr)
	scard, _ := intReply(final.do(bs("SCARD"), k))
	if scard != 1 {
		t.Errorf("idempotent SREM: SCARD=%d, want 1 (only the survivor remains; exact removal count)", scard)
	}
	if winners != 1 {
		t.Errorf("idempotent SREM: %d clients saw :1, want exactly 1 (serialized ALL_OLD remove must pick one winner)", winners)
	}
	members, _ := respArrayElements(final.do(bs("SMEMBERS"), k))
	if len(members) != 1 || (len(members) == 1 && members[0] != "keep") {
		t.Errorf("idempotent SREM: SMEMBERS=%v, want [\"keep\"]", members)
	}
	t.Logf("idempotent SREM under %d-way contention: SCARD=%d, winners=%d — all exact", clients, scard, winners)
}

// TestConcurrentHashZsetSameElement checks that the hash and zset add/remove counts are
// concurrency-EXACT for the SAME field/member — HLEN and ZCARD are meta.cnt just like SCARD.
// Add-style ops (HSET/ZADD) overwrite; remove-style ops (HDEL/ZREM) delete. Each: exactly one
// racing op should report the element as new/removed, and the final cardinality must match.
func TestConcurrentHashZsetSameElement(t *testing.T) {
	addr := proxyAddr(t)
	const clients = 48
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)

	countWinners := func(fn func(c *respConn, id int) bool) int64 {
		var w int64
		acThunk(t, addr, clients, func(c *respConn, id int) {
			if fn(c, id) {
				atomic.AddInt64(&w, 1)
			}
		})
		return w
	}

	// HSET same field: exactly one :1 (new), HLEN == 1.
	kh := []byte("ac:hset1:" + nonce)
	hw := countWinners(func(c *respConn, id int) bool {
		n, ok := intReply(c.do(bs("HSET"), kh, bs("f"), bs("v"+itoa(id))))
		return ok && n == 1
	})
	hlen := acInt(t, dial(t, addr), bs("HLEN"), kh)
	if hw != 1 || hlen != 1 {
		t.Errorf("concurrent HSET same field: winners=%d HLEN=%d, want 1/1", hw, hlen)
	}

	// HDEL same field: seed field+survivor, exactly one :1 (removed), HLEN == 1 (survivor).
	khd := []byte("ac:hdel1:" + nonce)
	dial(t, addr).do(bs("HMSET"), khd, bs("same"), bs("v"), bs("keep"), bs("v"))
	hdw := countWinners(func(c *respConn, id int) bool {
		n, ok := intReply(c.do(bs("HDEL"), khd, bs("same")))
		return ok && n == 1
	})
	hdlen := acInt(t, dial(t, addr), bs("HLEN"), khd)
	if hdw != 1 || hdlen != 1 {
		t.Errorf("concurrent HDEL same field: winners=%d HLEN=%d, want 1/1 (survivor keep)", hdw, hdlen)
	}

	// ZADD same member: exactly one :1 (new), ZCARD == 1.
	kz := []byte("ac:zadd1:" + nonce)
	zw := countWinners(func(c *respConn, id int) bool {
		n, ok := intReply(c.do(bs("ZADD"), kz, bs(itoa(id)), bs("m")))
		return ok && n == 1
	})
	zcard := acInt(t, dial(t, addr), bs("ZCARD"), kz)
	if zw != 1 || zcard != 1 {
		t.Errorf("concurrent ZADD same member: winners=%d ZCARD=%d, want 1/1", zw, zcard)
	}

	// ZREM same member: seed member+survivor, exactly one :1 (removed), ZCARD == 1.
	kzr := []byte("ac:zrem1:" + nonce)
	dial(t, addr).do(bs("ZADD"), kzr, bs("1"), bs("same"), bs("2"), bs("keep"))
	zrw := countWinners(func(c *respConn, id int) bool {
		n, ok := intReply(c.do(bs("ZREM"), kzr, bs("same")))
		return ok && n == 1
	})
	zrcard := acInt(t, dial(t, addr), bs("ZCARD"), kzr)
	if zrw != 1 || zrcard != 1 {
		t.Errorf("concurrent ZREM same member: winners=%d ZCARD=%d, want 1/1 (survivor keep)", zrw, zrcard)
	}

	t.Logf("hash/zset same-element under %d-way contention: HSET w=%d HLEN=%d; HDEL w=%d HLEN=%d; ZADD w=%d ZCARD=%d; ZREM w=%d ZCARD=%d",
		clients, hw, hlen, hdw, hdlen, zw, zcard, zrw, zrcard)
}

// TestConcurrentListRMWSafety exercises the KNOWN non-atomic list RMW ops (LSET/LTRIM/LREM)
// against concurrent RPUSH. redimos does not make these cross-connection atomic (a design
// trade-off: they read the whole list, modify, and rewrite without a version guard), so the
// exact final list is non-deterministic and NOT compared to Redis.
//
// SAFETY invariants (asserted, must hold): the key stays a STRUCTURALLY VALID list — every
// command returns a well-formed reply (never a crash or malformed frame), LRANGE returns a
// valid array, and TYPE stays list/none.
//
// COUNT/CONTENTS consistency (LLEN == len(LRANGE)) does NOT hold under concurrent RMW and is
// DOCUMENTED, not asserted: an LTRIM/LREM rewrite can race an RPUSH so meta.cnt (LLEN) and
// the actual element set diverge — the documented consequence of RMW-without-version-retry.
// Redis' single thread keeps them equal; this is an accepted architectural divergence.
func TestConcurrentListRMWSafety(t *testing.T) {
	addr := proxyAddr(t)
	const clients = 24
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	k := []byte("ac:listrmw:" + nonce)

	// Seed a list so the RMW ops have something to act on.
	dial(t, addr).do(bs("RPUSH"), k, bs("s0"), bs("s1"), bs("s2"), bs("s3"), bs("s4"))

	var malformed int64
	acThunk(t, addr, clients, func(c *respConn, id int) {
		var reply []byte
		switch id % 4 {
		case 0:
			reply = c.do(bs("RPUSH"), k, bs("p"+itoa(id)))
		case 1:
			reply = c.do(bs("LSET"), k, bs("0"), bs("x"+itoa(id)))
		case 2:
			reply = c.do(bs("LTRIM"), k, bs("0"), bs("10"))
		case 3:
			reply = c.do(bs("LREM"), k, bs("0"), bs("s0"))
		}
		// Well-formed: +OK / :N / -ERR (a transient error is fine). Never a malformed frame.
		if len(reply) == 0 || (reply[0] != '+' && reply[0] != ':' && reply[0] != '-') {
			atomic.AddInt64(&malformed, 1)
		}
	})
	if malformed != 0 {
		t.Errorf("list RMW under concurrency: %d malformed replies", malformed)
	}
	// SAFETY: the key is still a structurally valid, readable list.
	final := dial(t, addr)
	llen := acInt(t, final, bs("LLEN"), k)
	elems, ok := respArrayElements(final.do(bs("LRANGE"), k, bs("0"), bs("-1")))
	if !ok {
		t.Fatalf("LRANGE after concurrent RMW: not a valid array")
	}
	if typ := final.do(bs("TYPE"), k); string(typ) != "+list\r\n" && string(typ) != "+none\r\n" {
		t.Errorf("list RMW: TYPE=%q (want list or none)", typ)
	}
	// COUNT/CONTENTS may diverge — document, do not fail.
	if int64(len(elems)) != llen {
		t.Logf("ACCEPTED DIVERGENCE: LLEN(meta.cnt)=%d vs LRANGE=%d elements diverge under concurrent RMW (non-atomic read-modify-rewrite races RPUSH; Redis keeps them equal)", llen, len(elems))
	}
	t.Logf("list RMW safety under %d-way contention: structurally valid list, no malformed replies (exact contents/count non-deterministic, per design)", clients)
}
