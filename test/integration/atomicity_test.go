package integration

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSetnxAtomic verifies SETNX is atomic on the redimo->DynamoDB path: many
// clients racing on the same fresh key must see EXACTLY ONE :1 (the winner), like
// Redis' single-threaded SETNX — never two winners (the TOCTOU bug fixed in
// redimos v1.9.0 via the conditional meta claim).
func TestSetnxAtomic(t *testing.T) {
	addr := proxyAddr(t)
	const rounds, conc = 20, 40

	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	for round := 0; round < rounds; round++ {
		k := []byte(fmt.Sprintf("atom:setnx:%s:%d", nonce, round))
		var ones int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < conc; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c := dial(t, addr)
				<-start // release all at once for maximum contention
				if bytes.Equal(c.do(bs("SETNX"), k, bs("v")), bs(":1\r\n")) {
					atomic.AddInt64(&ones, 1)
				}
			}()
		}
		close(start)
		wg.Wait()
		if ones != 1 {
			t.Errorf("round %d: %d clients got :1, want exactly 1", round, ones)
		}
	}
}

// TestIncrAtomic verifies INCR atomicity under contention: redimos implements INCR
// as an optimistic compare-and-set retry loop, so the guaranteed invariant is that
// the final counter equals EXACTLY the number of ACKNOWLEDGED increments (every INCR
// that returned an integer reply really moved the counter by one — no lost or
// double-counted update). Unlike Redis' single thread, a highly-contended INCR may
// exhaust the bounded CAS retries and return a *retryable* error instead of an
// integer; those are simply not acknowledged (and a real client would retry). This
// test asserts counter == acknowledged, and reports the contention drop-off.
func TestIncrAtomic(t *testing.T) {
	addr := proxyAddr(t)
	const clients, perClient = 16, 50

	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	k := []byte("atom:incr:" + nonce)

	var acked int64 // INCRs that returned an integer reply (moved the counter)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := dial(t, addr)
			<-start
			for j := 0; j < perClient; j++ {
				if reply := c.do(bs("INCR"), k); len(reply) > 0 && reply[0] == ':' {
					atomic.AddInt64(&acked, 1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	got, ok := bulkOrIntValue(dial(t, addr).do(bs("GET"), k))
	if !ok {
		t.Fatalf("GET final counter: unexpected reply")
	}
	// Atomicity: the counter is exactly the number of acknowledged increments —
	// the CAS loop never loses or double-counts an acknowledged INCR.
	if got != acked {
		t.Errorf("final counter = %d, want %d (= acknowledged INCRs); a CAS race lost or double-counted an update", got, acked)
	}
	total := int64(clients * perClient)
	t.Logf("INCR atomicity: counter=%d == %d acknowledged; %d/%d exhausted the bounded CAS retries under %d-way contention and returned a retryable error (Redis' single thread never does)",
		got, acked, total-acked, total, clients)
}

// bulkOrIntValue parses the integer value out of a bulk-string reply (GET returns
// the counter as a decimal bulk string).
func bulkOrIntValue(reply []byte) (int64, bool) {
	payload, ok := bulkPayload(reply)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(string(payload), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
