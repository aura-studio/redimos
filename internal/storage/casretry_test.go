package storage

import (
	"errors"
	"testing"
)

// This file unit-tests casRetry, the storage-layer bounded compare-and-set retry
// helper behind the read-modify-write value writes (task 20.1, requirements 15.2,
// 16.3, 16.4). It drives the loop with an in-process attempt closure that reports
// a lost compare-and-set race (ok=false) a fixed number of times before letting
// the write land, so the converge-after-retry, give-up-at-the-bound and
// error-propagation paths are exercised deterministically without a live backend.

// TestCASRetryConvergesAfterLostRaces asserts casRetry keeps retrying while the
// conditional write loses its race (ok=false) and returns nil once an attempt
// finally lands, calling the attempt exactly (losses + 1) times. This models a hot
// key where other writers keep winning until this read-modify-write finally
// commits (requirements 15.2, 16.4).
func TestCASRetryConvergesAfterLostRaces(t *testing.T) {
	for _, losses := range []int{0, 1, 3, MaxRMWRetries - 1} {
		losses := losses
		t.Run("", func(t *testing.T) {
			calls := 0
			err := casRetry(func() (bool, error) {
				calls++
				// Report a lost race for the first `losses` attempts, then land.
				return calls > losses, nil
			})
			if err != nil {
				t.Fatalf("casRetry with %d lost races returned err %v, want nil (it should converge)", losses, err)
			}
			if want := losses + 1; calls != want {
				t.Fatalf("casRetry with %d lost races made %d attempts, want %d", losses, calls, want)
			}
		})
	}
}

// TestCASRetrySucceedsFirstAttempt asserts the no-contention fast path: an attempt
// that lands immediately runs exactly once and returns nil.
func TestCASRetrySucceedsFirstAttempt(t *testing.T) {
	calls := 0
	err := casRetry(func() (bool, error) {
		calls++
		return true, nil
	})
	if err != nil {
		t.Fatalf("casRetry landing on the first attempt returned err %v, want nil", err)
	}
	if calls != 1 {
		t.Fatalf("casRetry landing on the first attempt made %d attempts, want 1", calls)
	}
}

// TestCASRetryGivesUpAtBound asserts that when every attempt loses its race
// casRetry stops at exactly MaxRMWRetries attempts and returns ErrRMWMaxRetries,
// rather than looping forever or silently dropping the write (requirement 16.4).
func TestCASRetryGivesUpAtBound(t *testing.T) {
	calls := 0
	err := casRetry(func() (bool, error) {
		calls++
		return false, nil // never lands
	})
	if !errors.Is(err, ErrRMWMaxRetries) {
		t.Fatalf("casRetry under permanent contention returned err %v, want ErrRMWMaxRetries", err)
	}
	if calls != MaxRMWRetries {
		t.Fatalf("casRetry under permanent contention made %d attempts, want the bound %d", calls, MaxRMWRetries)
	}
}

// TestCASRetryPropagatesErrorImmediately asserts an unrecoverable error from the
// attempt (a backend failure, or a value/overflow validation error such as
// ErrNotInteger) aborts the loop at once — it is returned verbatim and no further
// attempts are made. This is the path INCR takes when the stored value is not an
// integer: it must surface the error, not retry it away.
func TestCASRetryPropagatesErrorImmediately(t *testing.T) {
	sentinel := errors.New("backend unavailable")
	calls := 0
	err := casRetry(func() (bool, error) {
		calls++
		return false, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("casRetry returned err %v, want the propagated sentinel %v", err, sentinel)
	}
	if calls != 1 {
		t.Fatalf("casRetry made %d attempts after an error, want 1 (it must abort immediately)", calls)
	}
}

// TestCASRetryReturnsErrNotIntegerWithoutRetry pins the concrete INCR case: a
// value-parse failure (ErrNotInteger) is treated as unrecoverable and is not
// retried, mirroring how IncrBy surfaces it to the caller as the Redis
// "not an integer" reply.
func TestCASRetryReturnsErrNotIntegerWithoutRetry(t *testing.T) {
	calls := 0
	err := casRetry(func() (bool, error) {
		calls++
		return false, ErrNotInteger
	})
	if !errors.Is(err, ErrNotInteger) {
		t.Fatalf("casRetry returned err %v, want ErrNotInteger", err)
	}
	if calls != 1 {
		t.Fatalf("casRetry made %d attempts on a parse error, want 1", calls)
	}
}
