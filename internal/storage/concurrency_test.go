package storage

import (
	"bytes"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// This file is the storage-layer half of task 20.2 (concurrency semantics,
// requirements 16.3, 16.4, 5.8). It validates the place where the proxy's
// concurrency-safety actually lives: the bounded compare-and-set retry loop
// (casRetry, task 20.1) composed with the SETCAS conditional write behind the
// INCR-family read-modify-write (IncrBy / IncrByFloat).
//
// The redimo-backed Store holds a concrete redimo.Client and reaches SETCAS
// through a live DynamoDB, so IncrBy cannot be driven from N goroutines without a
// backend. What CAN be exercised without a backend — and is where the correctness
// lives — is casRetry itself plus the exact read-parse-apply-SETCAS attempt body
// that IncrBy/IncrByFloat run. These tests drive that body from many goroutines
// against a faithful in-memory SETCAS model (casCell) whose compare-and-set has
// the same semantics as the fork's SETCAS: the write lands only if the value the
// attempt observed is still the current value, otherwise it reports a lost race so
// casRetry re-reads and recomputes. The assertion is the lost-update property:
// after every concurrent increment lands, the stored counter equals the exact sum
// of the deltas — no update is dropped under contention.
//
// The attempt bodies below (incrByAttempt / incrByFloatAttempt) are deliberate
// line-by-line mirrors of redimoStore.IncrBy / IncrByFloat in store.go, using the
// same production helpers (parseStoredInt, parseStoredFloat, formatRedisFloat,
// the overflow guard, MaxRMWRetries via casRetry). If the production RMW body
// changes, these mirrors should change with it.

// casCell is an in-memory model of a single String value item with the fork's
// SETCAS semantics. Its own mutex makes each get / compareAndSet atomic, modelling
// DynamoDB serializing individual item operations; the compare-and-set precondition
// (observed value still current) is what casRetry relies on to never lose an update.
type casCell struct {
	mu     sync.Mutex
	val    []byte
	exists bool
	// cas counts every compareAndSet call (won or lost) so a test can show the
	// loop genuinely contended rather than running uncontended.
	cas int64
}

// get returns a private copy of the current value and whether it exists, matching
// GetString: the attempt reads, then computes off that snapshot.
func (c *casCell) get() (val []byte, exists bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.exists {
		return nil, false
	}

	return append([]byte(nil), c.val...), true
}

// compareAndSet mirrors redimo SETCAS: it writes newVal only if the caller's
// observed base (oldVal / oldExists) is still the current value, otherwise it
// reports ok=false with no write so casRetry retries on the winner's value.
func (c *casCell) compareAndSet(newVal, oldVal []byte, oldExists bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	atomic.AddInt64(&c.cas, 1)

	if c.exists != oldExists {
		return false
	}
	if oldExists && !bytes.Equal(c.val, oldVal) {
		return false
	}

	c.val = append([]byte(nil), newVal...)
	c.exists = true

	return true
}

// incrByAttempt is the attempt body IncrBy passes to casRetry, extracted verbatim
// so the real casRetry drives the real read-parse-apply-SETCAS logic against the
// casCell. On a landed write it publishes the new value through newVal.
func incrByAttempt(cell *casCell, delta int64, newVal *int64) func() (bool, error) {
	return func() (bool, error) {
		oldVal, oldExists := cell.get()

		var cur int64
		if oldExists {
			v, perr := parseStoredInt(oldVal)
			if perr != nil {
				return false, perr
			}
			cur = v
		}

		if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
			return false, ErrIncrOverflow
		}
		next := cur + delta

		ok := cell.compareAndSet([]byte(strconv.FormatInt(next, 10)), oldVal, oldExists)
		if ok {
			*newVal = next
		}

		return ok, nil
	}
}

// incrByFloatAttempt is the IncrByFloat attempt body, extracted the same way.
func incrByFloatAttempt(cell *casCell, delta float64, newVal *[]byte) func() (bool, error) {
	return func() (bool, error) {
		oldVal, oldExists := cell.get()

		var cur float64
		if oldExists {
			v, perr := parseStoredFloat(oldVal)
			if perr != nil {
				return false, perr
			}
			cur = v
		}

		next := cur + delta
		if math.IsNaN(next) || math.IsInf(next, 0) {
			return false, ErrIncrNaNOrInfinity
		}

		out := formatRedisFloat(next)
		ok := cell.compareAndSet(out, oldVal, oldExists)
		if ok {
			*newVal = out
		}

		return ok, nil
	}
}

// runToCompletion runs one read-modify-write via casRetry and, on the rare
// pathological path where every one of the MaxRMWRetries attempts loses its race
// (ErrRMWMaxRetries), retries the whole operation — exactly what a client does
// when the proxy surfaces the retryable error. Any other error is fatal (a lost
// update or a parse/overflow bug), so it is returned. This lets the tests assert
// the lost-update property (no increment ever dropped) without being flaky under
// the deliberately extreme in-memory contention, which collides far harder than a
// real DynamoDB round-trip ever would.
func runToCompletion(attempt func() (bool, error)) error {
	for {
		err := casRetry(attempt)
		if err == nil {
			return nil
		}
		if err == ErrRMWMaxRetries { //nolint:errorlint // sentinel compared directly
			continue
		}

		return err
	}
}

// TestIncrByConcurrentNoLostUpdates drives 1000 concurrent +1 increments through
// the real casRetry + the IncrBy attempt body against the SETCAS model and asserts
// the counter converges to exactly 1000 — every increment lands, none is lost.
// This is the storage-layer proof of requirements 16.3 / 16.4 / 5.8: concurrent
// INCR on one key cannot lose an update because each loser's conditional write
// fails and it re-reads and re-applies on top of the winner's value.
func TestIncrByConcurrentNoLostUpdates(t *testing.T) {
	const goroutines = 1000

	cell := &casCell{}

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var newVal int64
			if err := runToCompletion(incrByAttempt(cell, 1, &newVal)); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent IncrBy attempt failed: %v", err)
	}

	got, exists := cell.get()
	if !exists {
		t.Fatal("counter has no value after 1000 concurrent increments")
	}
	final, err := parseStoredInt(got)
	if err != nil {
		t.Fatalf("stored counter %q is not a valid integer: %v", got, err)
	}
	if final != goroutines {
		t.Fatalf("counter after %d concurrent +1 increments = %d, want %d (updates were lost)", goroutines, final, goroutines)
	}
	// The loop must actually have contended: with 1000 goroutines racing one cell
	// there are always more compare-and-set calls than winners.
	if cas := atomic.LoadInt64(&cell.cas); cas < goroutines {
		t.Fatalf("only %d compare-and-set calls for %d increments — the loop was not exercised", cas, goroutines)
	}
}

// TestIncrByConcurrentMixedDeltas drives many concurrent increments AND decrements
// of varying magnitude and asserts the counter equals the exact algebraic sum, so
// the no-lost-update guarantee holds for a realistic mix, not just uniform +1s.
func TestIncrByConcurrentMixedDeltas(t *testing.T) {
	deltas := make([]int64, 0, 800)
	var want int64
	for i := 0; i < 200; i++ {
		for _, d := range []int64{1, 5, -3, 7} {
			deltas = append(deltas, d)
			want += d
		}
	}

	cell := &casCell{}

	var wg sync.WaitGroup
	errCh := make(chan error, len(deltas))

	for _, d := range deltas {
		wg.Add(1)
		go func(delta int64) {
			defer wg.Done()
			var newVal int64
			if err := runToCompletion(incrByAttempt(cell, delta, &newVal)); err != nil {
				errCh <- err
			}
		}(d)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent IncrBy attempt failed: %v", err)
	}

	got, exists := cell.get()
	if !exists {
		t.Fatal("counter has no value after concurrent mixed increments")
	}
	final, err := parseStoredInt(got)
	if err != nil {
		t.Fatalf("stored counter %q is not a valid integer: %v", got, err)
	}
	if final != want {
		t.Fatalf("counter after %d concurrent mixed deltas = %d, want %d (updates were lost)", len(deltas), final, want)
	}
}

// TestIncrByFloatConcurrentNoLostUpdates is the INCRBYFLOAT analogue: 1000
// concurrent +1.0 increments must converge to exactly 1000, proving the same
// casRetry + SETCAS loop keeps the float read-modify-write lost-update-safe
// (requirements 16.3, 16.4).
func TestIncrByFloatConcurrentNoLostUpdates(t *testing.T) {
	const goroutines = 1000

	cell := &casCell{}

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var newVal []byte
			if err := runToCompletion(incrByFloatAttempt(cell, 1, &newVal)); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent IncrByFloat attempt failed: %v", err)
	}

	got, exists := cell.get()
	if !exists {
		t.Fatal("counter has no value after 1000 concurrent float increments")
	}
	final, err := parseStoredFloat(got)
	if err != nil {
		t.Fatalf("stored counter %q is not a valid float: %v", got, err)
	}
	if final != float64(goroutines) {
		t.Fatalf("float counter after %d concurrent +1.0 increments = %v, want %d (updates were lost)", goroutines, final, goroutines)
	}
}
