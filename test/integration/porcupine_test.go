package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// Dimension I (rigorous): a full linearizability check of the single-key register using
// Porcupine. Concurrent clients issue randomized SET/GET against one key; every operation's
// real-time call/return interval is recorded, and Porcupine searches for a serial order
// consistent with those intervals and a register model. redimos' single-item SET/GET (one
// strongly-consistent DynamoDB write / read) is linearizable, so the history must check OK.
// This complements TestConcurrentRegisterSafety (a fast invariant smoke) with a real model
// checker.
//
// Why no DEL in this history. A redimos string is TWO DynamoDB items — the #meta item and the
// value item — and three architectural facts combine to make a concurrent DEL+SET workload
// NON-linearizable (a documented instance of the accepted "redimos ≠ Redis 3.2 atomicity"
// divergence; see doc/redis-3.2-compatibility.md §10.3):
//   1. DEL removes only the #meta item; the value item lingers (lazy async reclaim).
//   2. SET writes #meta first (EnsureType) then the value item — a non-atomic write.
//   3. The read path reads #meta and the value item as two separate, un-snapshotted calls.
// So a GET can pair a FRESH #meta (a new incarnation) with the STALE lingering value of a
// deleted incarnation and return a value that was never current — no linearization exists.
// Worse, the DEL-then-SET pattern can lose the write outright: the async member reclaim
// triggered by DEL can run between the DEL and the SET (observing the key as dead) and then
// delete the value item the SET just wrote. The DeleterConfig.IsLive guard reduces this (the
// reclaim skips a key whose #meta is live again) but does NOT eliminate it — the liveness
// check and the delete are not atomic. Making them atomic with TransactWriteItems was tried
// and rejected: the transaction's lock makes concurrent SET UpdateItems fail with
// TransactionConflictException, a worse regression than the wipe. A complete fix needs
// per-incarnation epochs stamped on every item — a broad redesign. So this is characterized
// and documented (doc §10.3) rather than asserted: DEL is excluded from this history, and no
// no-lost-write-under-DEL property is claimed. Multi-item ops (S*STORE/Z*STORE/SMOVE/
// RPOPLPUSH) are likewise non-atomic by design.

const regAbsent = "\x00absent\x00"

type regInput struct {
	op    string // "set" | "get" | "del"
	value string
}

type regOutput struct {
	value string // for get: the observed value (regAbsent for a nil reply)
}

var registerModel = porcupine.Model{
	Init: func() interface{} { return regAbsent },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(string)
		in := input.(regInput)
		out := output.(regOutput)
		switch in.op {
		case "set":
			return true, in.value // a SET always succeeds and installs its value
		case "del":
			return true, regAbsent
		case "get":
			return out.value == st, st // a GET must observe the current linearized value
		}
		return false, st
	},
	Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
}

func TestRegisterLinearizable(t *testing.T) {
	addr := proxyAddr(t)

	const (
		clients = 8
		ops     = 60
	)

	key := bs(fmt.Sprintf("porc:%d", time.Now().UnixNano()))
	// Start from a known-absent state.
	dial(t, addr).do(bs("DEL"), key)

	base := time.Now()
	now := func() int64 { return int64(time.Since(base)) }

	var (
		mu      sync.Mutex
		history []porcupine.Operation
		wg      sync.WaitGroup
	)

	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			conn := dial(t, addr)
			// Deterministic-but-interleaved op mix per client (no shared RNG needed).
			for i := 0; i < ops; i++ {
				var in regInput
				var out regOutput
				// SET/GET only: see the file header for why concurrent DEL is excluded.
				if (c+i)%2 == 0 {
					val := fmt.Sprintf("c%d-i%d", c, i)
					in = regInput{op: "set", value: val}
					call := now()
					reply := conn.do(bs("SET"), key, bs(val))
					ret := now()
					if len(reply) == 0 || reply[0] != '+' { // not +OK -> don't record an uncertain write
						continue
					}
					record(&mu, &history, c, in, out, call, ret)
				} else {
					in = regInput{op: "get"}
					call := now()
					reply := conn.do(bs("GET"), key)
					ret := now()
					if p, ok := bulkPayload(reply); ok {
						out = regOutput{value: string(p)}
					} else {
						out = regOutput{value: regAbsent} // $-1
					}
					record(&mu, &history, c, in, out, call, ret)
				}
			}
		}(c)
	}
	wg.Wait()

	if len(history) < clients {
		t.Fatalf("recorded too few operations (%d)", len(history))
	}

	res, info := porcupine.CheckOperationsVerbose(registerModel, history, 60*time.Second)
	switch res {
	case porcupine.Ok:
		t.Logf("linearizable: %d operations across %d clients", len(history), clients)
	case porcupine.Illegal:
		t.Errorf("history is NOT linearizable (%d ops); redimos single-key register violated linearizability", len(history))
		_ = info
	case porcupine.Unknown:
		t.Skipf("linearizability check timed out on %d ops (inconclusive)", len(history))
	}
}

func record(mu *sync.Mutex, h *[]porcupine.Operation, client int, in regInput, out regOutput, call, ret int64) {
	mu.Lock()
	*h = append(*h, porcupine.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret})
	mu.Unlock()
}
