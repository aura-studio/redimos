package command

import (
	"context"
	"testing"

	"github.com/aura-studio/redimos/v2/internal/storage"
)

// throttlingSAddStore is a fakeStringStore whose SAdd always reports a DynamoDB
// throttle (as the storage seam would after the SDK's retry/backoff is exhausted),
// so the command layer's writeStoreError mapping can be exercised end-to-end
// through a real in-process server.
type throttlingSAddStore struct {
	*fakeStringStore
}

func (s *throttlingSAddStore) SAdd(context.Context, string, []string) (int, error) {
	return 0, storage.ErrThrottled
}

var _ storage.Store = (*throttlingSAddStore)(nil)

// TestWriteStoreError_MapsThrottleToRetryableErr verifies requirement 18.8: a
// storage.ErrThrottled surfaced by an operation is mapped to the retryable
// "-ERR backend throttled, retry later" reply (a plain -ERR that preserves
// retryable semantics for the client).
func TestWriteStoreError_MapsThrottleToRetryableErr(t *testing.T) {
	store := &throttlingSAddStore{fakeStringStore: newFakeStringStore()}
	conn, r := startStringServer(t, store, fixedNow(1000))

	got := sendLine(t, conn, r, "SADD k m1")
	want := "-ERR backend throttled, retry later"
	if got != want {
		t.Errorf("SADD under throttle = %q, want %q", got, want)
	}
}
