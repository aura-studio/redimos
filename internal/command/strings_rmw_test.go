package command

import (
	"context"
	"testing"

	"github.com/aura-studio/redimos/v2/internal/storage"
)

// This file covers the bounded compare-and-set retry loop behind the
// read-modify-write String commands APPEND / SETRANGE (task 20.1, requirements
// 15.2, 16.4). It drives the loop deterministically with a store double whose
// conditional write reports a lost race a fixed number of times before letting the
// write land, so both the converge-after-retry and give-up-at-the-bound paths are
// exercised without needing real concurrency or a live backend.

// flakyCASStore wraps a fakeStringStore and makes the first failN
// SetStringIfEquals calls report a lost compare-and-set race (ok=false, no write),
// then delegates to the real fake. It models a hot key where other writers keep
// winning the race until this connection's read-modify-write finally lands. Every
// other Store method is inherited from the embedded fake, and the failed attempts
// leave the value untouched, so a retry re-reads the same base and recomputes the
// same result — exactly what the command layer relies on.
type flakyCASStore struct {
	*fakeStringStore
	failN int // number of leading CAS attempts to reject
	calls int // total SetStringIfEquals attempts observed
}

func (s *flakyCASStore) SetStringIfEquals(ctx context.Context, pk string, newVal, oldVal []byte, oldExists bool) (bool, error) {
	s.calls++
	if s.calls <= s.failN {
		return false, nil
	}

	return s.fakeStringStore.SetStringIfEquals(ctx, pk, newVal, oldVal, oldExists)
}

var _ storage.Store = (*flakyCASStore)(nil)

func TestAppendRetriesThenConverges(t *testing.T) {
	// The first three compare-and-set attempts lose the race; the fourth lands.
	// APPEND must retry and converge to the correct appended value and length,
	// not lose the update (requirements 15.2, 16.4).
	store := &flakyCASStore{fakeStringStore: newFakeStringStore(), failN: 3}
	conn, r := startStringServer(t, store, fixedNow(1000))

	if got, want := sendRead(t, conn, r, "APPEND k hello"), ":5"; got != want {
		t.Fatalf("APPEND k hello = %q, want %q", got, want)
	}
	if store.calls != 4 {
		t.Fatalf("APPEND made %d compare-and-set attempts, want 4 (3 rejected + 1 success)", store.calls)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$hello"; got != want {
		t.Fatalf("GET k = %q, want %q after APPEND converged", got, want)
	}
}

func TestSetRangeRetriesThenConverges(t *testing.T) {
	// SETRANGE shares the same retry loop; verify it also converges after losing
	// a few races.
	store := &flakyCASStore{fakeStringStore: newFakeStringStore(), failN: 2}
	conn, r := startStringServer(t, store, fixedNow(1000))

	// Offset 0 into an empty key writes "abc" and reports length 3.
	if got, want := sendRead(t, conn, r, "SETRANGE k 0 abc"), ":3"; got != want {
		t.Fatalf("SETRANGE k 0 abc = %q, want %q", got, want)
	}
	if store.calls != 3 {
		t.Fatalf("SETRANGE made %d compare-and-set attempts, want 3 (2 rejected + 1 success)", store.calls)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$abc"; got != want {
		t.Fatalf("GET k = %q, want %q after SETRANGE converged", got, want)
	}
}

func TestAppendGivesUpAfterMaxRetries(t *testing.T) {
	// A compare-and-set that never lands drives the loop to its bound and surfaces
	// the generic retryable error rather than looping forever or silently losing
	// the write (requirement 16.4).
	store := &flakyCASStore{fakeStringStore: newFakeStringStore(), failN: storage.MaxRMWRetries + 5}
	conn, r := startStringServer(t, store, fixedNow(1000))

	got := sendRead(t, conn, r, "APPEND k hello")
	want := "-ERR " + storage.ErrRMWMaxRetries.Error()
	if got != want {
		t.Fatalf("APPEND under permanent contention = %q, want %q", got, want)
	}
	if store.calls != storage.MaxRMWRetries {
		t.Fatalf("APPEND made %d compare-and-set attempts, want %d (the retry bound)", store.calls, storage.MaxRMWRetries)
	}
	// The value must not have been written on any rejected attempt.
	if got, want := sendRead(t, conn, r, "GET k"), "$-1"; got != want {
		t.Fatalf("GET k = %q, want %q (no partial write)", got, want)
	}
}
