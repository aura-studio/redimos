package integration

import (
	"testing"
	"time"
)

// Regression tests for the 2026-07-06 "deep alignment" pass — six Redis-3.2 divergences
// that were FIXABLE (not platform-bound) and are now fixed. Each pins the corrected
// behavior so it cannot silently regress. Where Redis and redimos genuinely agree the test
// byte-diffs against the live oracle (d.eq); where the residual is a documented platform /
// C-UB boundary (ZADD out-of-range, SET EX overflow) the test asserts redimos' own
// deterministic invariant instead.

// TestFixSubSecondTTL: a positive sub-second TTL is accepted (Redis-identical acks) and,
// per the fix, no longer instant-deletes the key. Only the deterministic ACK replies are
// byte-diffed here — the "key survives the immediate GET" guarantee is asserted precisely
// (against meta.exp) in the command-package unit test TestSubSecondTTLDoesNotInstantExpire,
// because at second-granularity storage an immediate GET-after-set is inherently timing-
// sensitive across a wall-clock second boundary and would make an integration diff flaky.
func TestFixSubSecondTTL(t *testing.T) {
	d := newDiffer(t)

	kp := d.k("subsec-px")
	d.eq("SET PX 300 -> +OK", bs("SET"), kp, bs("v"), bs("PX"), bs("300"))

	kpe := d.k("subsec-pexpire")
	d.eq("SET then PEXPIRE 200", bs("SET"), kpe, bs("v"))
	d.eq("PEXPIRE 200 -> :1", bs("PEXPIRE"), kpe, bs("200"))

	kps := d.k("subsec-psetex")
	d.eq("PSETEX 200 -> +OK", bs("PSETEX"), kps, bs("200"), bs("v"))

	t.Logf("compared %d sub-second-TTL ack replies vs Redis 3.2", d.n)
}

// TestFixSMoveSameKey: SMOVE src dst where src==dst is a pure no-op in Redis (reports
// membership, never removes/recreates the key). Before the fix redimos ran SREM+SADD, which
// for a single-member set transiently emptied and rebuilt the key, dropping its TTL.
func TestFixSMoveSameKey(t *testing.T) {
	d := newDiffer(t)
	k := d.k("smove-same")

	d.eq("SADD only", bs("SADD"), k, bs("only"))
	d.eq("EXPIRE 1000", bs("EXPIRE"), k, bs("1000"))
	d.eq("SMOVE k k only (present) -> :1", bs("SMOVE"), k, k, bs("only"))
	d.eq("SMOVE k k absent (missing) -> :0", bs("SMOVE"), k, k, bs("absent"))
	// The TTL must be intact (not dropped to -1). Both keep ~1000; assert the proxy did not
	// lose it (exact value can jitter by a second, so require > 0).
	if ttl, ok := intReply(d.p.do(bs("TTL"), k)); !ok || ttl <= 0 {
		t.Errorf("SMOVE k k must preserve TTL, proxy TTL=%d (want > 0)", ttl)
	}
	d.eq("member still present after same-key SMOVE", bs("SISMEMBER"), k, bs("only"))

	t.Logf("compared %d same-key-SMOVE replies vs Redis 3.2", d.n)
}

// TestFixExpiredKeyTypeTakeover: a write of a NEW type to a logically-expired-but-not-yet-
// reclaimed key must treat the expired key as absent and create the new type (Redis
// expire-if-needed), NOT reply WRONGTYPE. Uses a real 1s expiry + a short wait.
func TestFixExpiredKeyTypeTakeover(t *testing.T) {
	d := newDiffer(t)

	ks := d.k("exp-str") // wrong-type takeover: expired STRING, then SADD
	d.eq("SET str EX 1", bs("SET"), ks, bs("v"), bs("EX"), bs("1"))
	kz := d.k("exp-set") // wrong-type takeover: expired SET, then INCR
	d.eq("SADD then EXPIRE 1", bs("SADD"), kz, bs("x"))
	d.eq("EXPIRE 1", bs("EXPIRE"), kz, bs("1"))
	ksame := d.k("exp-same") // SAME-type takeover: expired SET, then SADD again
	d.eq("SADD a b then EXPIRE 1", bs("SADD"), ksame, bs("a"), bs("b"))
	d.eq("EXPIRE same 1", bs("EXPIRE"), ksame, bs("1"))

	time.Sleep(1200 * time.Millisecond) // let all logically expire (no read/DEL in between)

	// Wrong-type: SADD onto expired STRING -> :1 (fresh set), not WRONGTYPE.
	d.eq("SADD on expired string -> :1", bs("SADD"), ks, bs("m"))
	d.eqSorted("SMEMBERS of taken-over string key", bs("SMEMBERS"), ks)
	// Wrong-type: INCR onto expired SET -> :1 (fresh string), not WRONGTYPE.
	d.eq("INCR on expired set -> :1", bs("INCR"), kz)
	d.eq("GET the taken-over set key", bs("GET"), kz)
	// SAME-type: SADD onto an expired SET must start a FRESH set (stale a,b gone), not
	// resurrect them or be swallowed. Redis: SCARD 1, SMEMBERS {c}.
	d.eq("SADD c on expired same-type set -> :1", bs("SADD"), ksame, bs("c"))
	d.eq("SCARD of fresh same-type set -> 1", bs("SCARD"), ksame)
	d.eqSorted("SMEMBERS fresh (only c, stale a/b reaped)", bs("SMEMBERS"), ksame)

	t.Logf("compared %d expired-key-takeover replies vs Redis 3.2", d.n)
}

// TestFixZAddOutOfRangeNoTornWrite: a finite score beyond the DynamoDB Number domain
// (Redis accepts it, redimos can't store it) is rejected DETERMINISTICALLY up front — not
// after a partial backend write. The key invariant is NO TORN STATE: a multi-member ZADD
// whose second member is out of range writes NEITHER member (ZCARD 0, first member absent),
// rather than leaving the first member stored while ZCARD stays 0. (Not a byte-diff: Redis
// stores 1e300; this asserts redimos' own no-torn-write contract — doc §4.1.)
func TestFixZAddOutOfRangeNoTornWrite(t *testing.T) {
	d := newDiffer(t)
	k := d.k("zadd-oor")

	rp := d.p.do(bs("ZADD"), k, bs("1"), bs("a"), bs("1e300"), bs("b"))
	if len(rp) == 0 || rp[0] != '-' {
		t.Errorf("ZADD with out-of-range score: want an error reply, got %q", rp)
	}
	if card, _ := intReply(d.p.do(bs("ZCARD"), k)); card != 0 {
		t.Errorf("ZADD out-of-range torn write: ZCARD=%d, want 0 (no member stored)", card)
	}
	// The in-range member 'a' must NOT be left behind (that was the torn state).
	if sc := d.p.do(bs("ZSCORE"), k, bs("a")); string(sc) != "$-1\r\n" {
		t.Errorf("ZADD out-of-range torn write: ZSCORE a=%q, want nil (member must not be stored)", sc)
	}
	// A single out-of-range ZADD / ZINCRBY is a clean error too.
	if rp := d.p.do(bs("ZADD"), d.k("zadd-oor2"), bs("1e308"), bs("m")); len(rp) == 0 || rp[0] != '-' {
		t.Errorf("ZADD 1e308: want an error reply, got %q", rp)
	}

	t.Logf("verified ZADD out-of-range rejects deterministically with no torn write")
}

// TestFixSetExOverflowDeterministic: an expire time so large it overflows the millisecond
// domain must NOT create a bogus permanent (or negative-TTL) key. redimos resolves the
// overflow to an immediately-expired key (created then gone), deterministically — instead
// of Redis' C-UB wrap. (Proxy-only: Redis' result here is undefined-behaviour-dependent.)
func TestFixSetExOverflowDeterministic(t *testing.T) {
	d := newDiffer(t)

	for _, tc := range []struct {
		name       string
		setArgs    [][]byte
	}{
		{"SET EX overflow", [][]byte{bs("SET"), d.k("ovf-ex"), bs("v"), bs("EX"), bs("9300000000000000")}},
		{"SET PX overflow", [][]byte{bs("SET"), d.k("ovf-px"), bs("v"), bs("PX"), bs("9223372036854775807")}},
		{"SETEX overflow", [][]byte{bs("SETEX"), d.k("ovf-setex"), bs("9223372036854775807"), bs("v")}},
	} {
		if rp := d.p.do(tc.setArgs...); string(rp) != "+OK\r\n" {
			t.Errorf("%s: want +OK, got %q", tc.name, rp)
		}
		key := tc.setArgs[1]
		if got := d.p.do(bs("GET"), key); string(got) != "$-1\r\n" {
			t.Errorf("%s: GET must be nil (key created-then-expired), got %q", tc.name, got)
		}
		if ttl, _ := intReply(d.p.do(bs("TTL"), key)); ttl != -2 {
			t.Errorf("%s: TTL must be -2 (gone), got %d", tc.name, ttl)
		}
	}

	t.Logf("verified overflow expire times resolve deterministically (no bogus permanent key)")
}
