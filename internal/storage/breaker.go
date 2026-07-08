package storage

import (
	"sync/atomic"
	"time"
)

// CircuitBreaker sheds load during a DynamoDB throttle storm. After `threshold`
// throttles accumulate (any non-throttle outcome resets the count), it OPENS for
// `cooldown`, during which the command layer fails fast — replying the retryable
// throttle error immediately instead of piling more doomed requests onto an
// already-overwhelmed table, which both protects the backend and gives clients a
// fast, clear signal to back off. After the cooldown elapses one request is let
// through (an implicit half-open probe): a success closes the breaker, another
// throttle re-opens it.
//
// It is OPT-IN — nil means disabled and imposes no overhead. The zero value is not
// usable; construct with NewCircuitBreaker. All methods are safe for concurrent use
// (lock-free atomics).
type CircuitBreaker struct {
	threshold int64
	cooldown  time.Duration
	now       func() time.Time

	failures  atomic.Int64 // consecutive-ish throttles since the last reset
	openUntil atomic.Int64 // unix-nano; 0 = closed
	trips     atomic.Int64 // total times the breaker has opened (observability)
}

// NewCircuitBreaker builds a breaker that opens after threshold throttles and stays
// open for cooldown. A threshold < 1 is clamped to 1.
func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	if threshold < 1 {
		threshold = 1
	}
	return &CircuitBreaker{threshold: int64(threshold), cooldown: cooldown, now: time.Now}
}

// Record reports one operation outcome. A throttle advances the failure count and
// may open the breaker; any non-throttle outcome (success or a different error)
// resets it so a transient throttle does not latch the breaker open.
func (b *CircuitBreaker) Record(throttled bool) {
	if b == nil {
		return
	}
	if !throttled {
		b.failures.Store(0)
		return
	}
	if b.failures.Add(1) >= b.threshold {
		b.failures.Store(0)
		b.openUntil.Store(b.now().Add(b.cooldown).UnixNano())
		b.trips.Add(1)
	}
}

// Open reports whether the breaker is currently shedding load. It is nil-safe
// (a nil breaker is never open) so callers need not branch on enablement.
func (b *CircuitBreaker) Open() bool {
	if b == nil {
		return false
	}
	until := b.openUntil.Load()
	return until != 0 && b.now().UnixNano() < until
}

// Trips returns how many times the breaker has opened, for a Prometheus gauge.
func (b *CircuitBreaker) Trips() uint64 {
	if b == nil {
		return 0
	}
	return uint64(b.trips.Load())
}
