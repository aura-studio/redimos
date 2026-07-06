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
// SAFETY invariant (asserted, must hold): the actual stored MEMBERSHIP is exactly one — the
// member item is written idempotently to one sort key, so no interleaving can produce a
// duplicate or lose it. SMEMBERS is the ground truth and is asserted == {"same"}.
//
// DOCUMENTED (not asserted) divergences under this contention, both from redimo choosing an
// approximate count over a serialized read-before-write (doc.go: "added/removed counts can be
// approximate under a concurrent write to the same member"):
//   - the per-command ADDED-COUNT return (:1 vs :0) over-reports — several clients each read
//     "absent" and return :1;
//   - SCARD (meta.cnt, maintained by an unconditional ADD per acknowledged SADD) over-counts
//     the same way, so SCARD can exceed the true cardinality. Redis keeps both exact; the
//     STORED SET is identical either way. (A fix would make SADD's cnt increment conditional
//     on the member item not already existing — a more expensive conditional write.)
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
	// SAFETY: the actual membership (SMEMBERS) is exactly {"same"}, regardless of count drift.
	final := dial(t, addr)
	members, ok := respArrayElements(final.do(bs("SMEMBERS"), k))
	if !ok {
		t.Fatalf("SMEMBERS not an array")
	}
	if len(members) != 1 || (len(members) == 1 && members[0] != "same") {
		t.Errorf("idempotent SADD: SMEMBERS=%v, want exactly [\"same\"] (stored membership must be idempotent)", members)
	}
	scard, _ := intReply(final.do(bs("SCARD"), k))
	t.Logf("idempotent SADD: SMEMBERS={same} (1 real member); SCARD(meta.cnt)=%d, %d/%d clients saw :1 — count/return over-report under contention (accepted approximate-count divergence; stored set correct)", scard, winners, clients)
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
