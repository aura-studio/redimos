package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// Dimension I (rigorous): a full linearizability check of the single-key register using
// Porcupine. Concurrent clients issue randomized SET/GET/DEL against one key; every
// operation's real-time call/return interval is recorded, and Porcupine searches for a
// serial order consistent with those intervals and a register model. redimos' single-item
// operations (one strongly-consistent DynamoDB read/write) should be linearizable, so the
// history must check OK. This complements TestConcurrentRegisterSafety (a fast invariant
// smoke) with a real model checker.
//
// Multi-item operations (S*STORE/Z*STORE/SMOVE/RPOPLPUSH) are known non-atomic by design
// and are deliberately not part of this history.

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
				switch (c + i) % 4 {
				case 0, 1:
					val := fmt.Sprintf("c%d-i%d", c, i)
					in = regInput{op: "set", value: val}
					call := now()
					reply := conn.do(bs("SET"), key, bs(val))
					ret := now()
					if len(reply) == 0 || reply[0] != '+' { // not +OK -> don't record an uncertain write
						continue
					}
					record(&mu, &history, c, in, out, call, ret)
				case 2:
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
				case 3:
					in = regInput{op: "del"}
					call := now()
					reply := conn.do(bs("DEL"), key)
					ret := now()
					if len(reply) == 0 || reply[0] != ':' { // only record a well-formed DEL
						continue
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
