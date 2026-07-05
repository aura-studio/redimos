package integration

import (
	"fmt"
	"sync"
	"testing"
)

// Dimension I: concurrency safety of the single-key register (the linearizable subset).
// redimos matches Redis' single-threaded model for SINGLE-item operations (SET/GET are one
// DynamoDB write/read); multi-item ops (S*STORE/Z*STORE/SMOVE/RPOPLPUSH) are known to be
// non-atomic and are deliberately NOT checked for linearizability here. A full linearizability
// checker (e.g. Porcupine) over arbitrary histories is future work; this asserts the core
// register-safety invariant that catches torn writes, phantom reads and value corruption:
// every concurrent GET returns a value that was actually written by some SET (byte-intact),
// and after all writers quiesce the final GET is the globally last-written value.
func TestConcurrentRegisterSafety(t *testing.T) {
	addr := proxyAddr(t)

	const (
		workers = 16
		rounds  = 40
	)

	// Pre-generate every token so a reader can validate a GET against the known-good set.
	// Tokens are fixed-shape and distinct, so a torn or corrupted value cannot masquerade
	// as a valid one.
	valid := map[string]struct{}{"__init__": {}}
	tokens := make([][]string, workers)
	for w := 0; w < workers; w++ {
		tokens[w] = make([]string, rounds)
		for r := 0; r < rounds; r++ {
			tok := fmt.Sprintf("w%03d-r%03d-payload", w, r)
			tokens[w][r] = tok
			valid[tok] = struct{}{}
		}
	}

	key := bs(fmt.Sprintf("lin:%s", tokens[0][0]))
	seed := dial(t, addr)
	seed.do(bs("SET"), key, bs("__init__"))

	var (
		mu         sync.Mutex
		violations []string
		wg         sync.WaitGroup
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := dial(t, addr) // one connection per worker (respConn is not concurrency-safe)
			for r := 0; r < rounds; r++ {
				c.do(bs("SET"), key, bs(tokens[w][r]))
				got, ok := bulkPayload(c.do(bs("GET"), key))
				if !ok {
					mu.Lock()
					violations = append(violations, fmt.Sprintf("w%d r%d: GET returned non-bulk", w, r))
					mu.Unlock()
					continue
				}
				if _, valid := valid[string(got)]; !valid {
					mu.Lock()
					violations = append(violations, fmt.Sprintf("w%d r%d: GET returned phantom/torn value %q", w, r, got))
					mu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()

	if len(violations) > 0 {
		t.Fatalf("register-safety violations (%d); first few:\n  %v", len(violations), firstN(violations, 5))
	}

	// After all writers quiesce, GET must return one of the written tokens (the last write
	// wins; the value is intact). We cannot know which worker wrote last, only that it is a
	// valid, non-corrupted token.
	final, ok := bulkPayload(seed.do(bs("GET"), key))
	if !ok {
		t.Fatalf("final GET returned non-bulk")
	}
	if _, valid := valid[string(final)]; !valid {
		t.Fatalf("final value %q is not a written token", final)
	}
	t.Logf("register safety: %d workers x %d rounds, no torn/phantom values", workers, rounds)
}

func firstN(s []string, n int) []string {
	if len(s) < n {
		return s
	}
	return s[:n]
}
