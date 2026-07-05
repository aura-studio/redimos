package integration

import (
	"testing"
	"time"
)

// Dimension E: TTL / expiry semantics. Covers the TTL/PTTL/PERSIST reply sentinels, the
// value reported right after EXPIRE (within a 1s tolerance since it counts down), and that
// a key actually disappears once its TTL elapses. The exact inclusive-vs-exclusive expiry
// boundary (redimos IsExpired uses exp<=now, a deliberate Pika-3.2 alignment) is documented
// separately and not asserted here because it is a sub-second edge that cannot be pinned
// deterministically.

func TestDiffTTLSemantics(t *testing.T) {
	d := newDiffer(t)

	sk := d.k("s")
	d.eq("SET", bs("SET"), sk, bs("v"))
	d.eq("TTL no-expire -> -1", bs("TTL"), sk)
	d.eq("PTTL no-expire -> -1", bs("PTTL"), sk)

	d.eq("EXPIRE 1000 -> 1", bs("EXPIRE"), sk, bs("1000"))
	d.eqIntClose("TTL after EXPIRE ~1000", 1, bs("TTL"), sk)
	d.eqIntClose("PTTL after EXPIRE ~1000000ms", 1500, bs("PTTL"), sk)

	d.eq("PERSIST -> 1", bs("PERSIST"), sk)
	d.eq("TTL after PERSIST -> -1", bs("TTL"), sk)
	d.eq("PERSIST again -> 0", bs("PERSIST"), sk)

	// EXPIRE / PEXPIRE / TTL sentinels on an absent key.
	miss := d.k("missing")
	d.eq("EXPIRE absent -> 0", bs("EXPIRE"), miss, bs("100"))
	d.eq("PEXPIRE absent -> 0", bs("PEXPIRE"), miss, bs("100"))
	d.eq("PERSIST absent -> 0", bs("PERSIST"), miss)
	d.eq("TTL absent -> -2", bs("TTL"), miss)
	d.eq("PTTL absent -> -2", bs("PTTL"), miss)

	// SET clears a prior TTL (Redis semantics).
	tk := d.k("reset")
	d.eq("SET with future expire", bs("SET"), tk, bs("v"))
	d.eq("EXPIRE 1000", bs("EXPIRE"), tk, bs("1000"))
	d.eq("SET overwrites and clears TTL", bs("SET"), tk, bs("v2"))
	d.eq("TTL after overwrite -> -1", bs("TTL"), tk)
}

// TestDiffExpiryActuallyExpires verifies a key disappears from both endpoints once its TTL
// elapses (passive/lazy expiry parity). It uses SECOND granularity: redimos TTL is
// second-precision (Pika v3.2.2-aligned), so a sub-second PEXPIRE rounds down and diverges
// from Redis' millisecond precision — a documented limitation, not exercised here. It
// sleeps ~1.5s, so it is a slower test.
func TestDiffExpiryActuallyExpires(t *testing.T) {
	d := newDiffer(t)

	k := d.k("short")
	d.eq("SET", bs("SET"), k, bs("v"))
	d.eq("EXPIRE 1s -> 1", bs("EXPIRE"), k, bs("1"))
	// Still present immediately (the 1s window has not elapsed).
	d.eq("EXISTS before expiry -> 1", bs("EXISTS"), k)

	time.Sleep(1500 * time.Millisecond)

	// Gone from both after the TTL elapses.
	d.eq("GET after expiry -> nil", bs("GET"), k)
	d.eq("EXISTS after expiry -> 0", bs("EXISTS"), k)
	d.eq("TTL after expiry -> -2", bs("TTL"), k)
	d.eq("TYPE after expiry -> none", bs("TYPE"), k)
}
