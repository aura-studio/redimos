package storage

import (
	"testing"
	"time"
)

func TestCircuitBreaker_OpensClosesAndResets(t *testing.T) {
	b := NewCircuitBreaker(3, 100*time.Millisecond)
	now := time.Unix(1000, 0)
	b.now = func() time.Time { return now }

	// Below threshold: stays closed.
	b.Record(true)
	b.Record(true)
	if b.Open() {
		t.Fatal("breaker opened below threshold")
	}

	// The threshold-th throttle opens it and counts a trip.
	b.Record(true)
	if !b.Open() {
		t.Fatal("breaker did not open at threshold")
	}
	if b.Trips() != 1 {
		t.Fatalf("Trips() = %d, want 1", b.Trips())
	}

	// Still open within the cooldown.
	now = now.Add(50 * time.Millisecond)
	if !b.Open() {
		t.Fatal("breaker closed within cooldown")
	}

	// Closes after the cooldown elapses.
	now = now.Add(60 * time.Millisecond)
	if b.Open() {
		t.Fatal("breaker stayed open past cooldown")
	}

	// A non-throttle outcome resets the accumulating failure count, so a scattered
	// throttle does not latch it open.
	b.Record(true)
	b.Record(true)
	b.Record(false) // reset
	b.Record(true)
	b.Record(true)
	if b.Open() {
		t.Fatal("breaker opened despite a reset between throttles")
	}
}

func TestCircuitBreaker_NilIsSafeAndClosed(t *testing.T) {
	var b *CircuitBreaker
	b.Record(true) // must not panic
	if b.Open() {
		t.Fatal("nil breaker reported open")
	}
	if b.Trips() != 0 {
		t.Fatal("nil breaker reported trips")
	}
}
