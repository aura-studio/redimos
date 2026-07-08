package integration

import (
	"fmt"
	"testing"
	"time"
)

// TestProxyCrashSafety_HugeCounts drives the client-supplied huge/overflowing counts that
// previously CRASHED or OOM'd the proxy (a panic in any connection goroutine has no recover, so
// it takes down the whole process). These are asserted against the proxy directly rather than
// differentially, because Redis itself misbehaves on some of them (e.g. SRANDMEMBER MinInt64).
// The contract here is simply: the proxy bounds/rejects them and stays alive.
func TestProxyCrashSafety_HugeCounts(t *testing.T) {
	addr := proxyAddr(t)
	c := dial(t, addr)
	key := bs(fmt.Sprintf("crash:%d", time.Now().UnixNano()))

	c.do(bs("SADD"), key, bs("a"), bs("b"), bs("c"))

	// SRANDMEMBER negative count: -MinInt64 stays negative (makeslice panic) and a large
	// negative allocates ~40GB. Now clamped.
	c.do(bs("SRANDMEMBER"), key, bs("-9223372036854775808"))
	c.do(bs("SRANDMEMBER"), key, bs("-5000000000"))
	c.do(bs("SRANDMEMBER"), key, bs("-1048576"))

	// BITFIELD offset: an overflowing #idx produced a negative slice index (panic); a huge plain
	// offset grew the buffer to terabytes (OOM). Now bounded like SETBIT.
	bf := bs(fmt.Sprintf("crashbf:%d", time.Now().UnixNano()))
	c.do(bs("BITFIELD"), bf, bs("SET"), bs("i64"), bs("#999999999999999999"), bs("1"))
	c.do(bs("BITFIELD"), bf, bs("SET"), bs("u8"), bs("999999999999999"), bs("1"))
	c.do(bs("BITFIELD"), bf, bs("INCRBY"), bs("i16"), bs("#9999999999999999"), bs("1"))

	// SETBIT far offset: valid up to 2^32 but would allocate ~512MB before the guard.
	sb := bs(fmt.Sprintf("crashsb:%d", time.Now().UnixNano()))
	c.do(bs("SETBIT"), sb, bs("4000000000"), bs("1")) // > 390KB*8 -> rejected before alloc

	// Proxy must still be responsive.
	reply := c.do(bs("PING"))
	if len(reply) == 0 || reply[0] != '+' {
		t.Fatalf("proxy not alive after huge-count commands: PING=%q", reply)
	}
}
