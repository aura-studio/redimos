package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	smithy "github.com/aws/smithy-go"
)

// fakeAPIError is a minimal smithy.APIError so the detection helper can be
// exercised against a throttling error surfaced as a generic API error (rather
// than the modelled ProvisionedThroughputExceededException struct).
type fakeAPIError struct{ code string }

func (e fakeAPIError) Error() string                 { return e.code }
func (e fakeAPIError) ErrorCode() string             { return e.code }
func (e fakeAPIError) ErrorMessage() string          { return e.code }
func (e fakeAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

func TestIsThrottled_ProvisionedThroughputExceeded(t *testing.T) {
	err := &types.ProvisionedThroughputExceededException{}
	if !IsThrottled(err) {
		t.Fatal("ProvisionedThroughputExceededException should be classified as throttled")
	}
	// Detection must see through wrapping (the SDK/redimo may wrap the exception).
	if !IsThrottled(fmt.Errorf("UpdateItem failed: %w", err)) {
		t.Fatal("wrapped ProvisionedThroughputExceededException should be classified as throttled")
	}
}

func TestIsThrottled_ThrottlingAPIErrorCodes(t *testing.T) {
	for _, code := range []string{
		"ProvisionedThroughputExceededException",
		"ThrottlingException",
		"RequestLimitExceeded",
		"TooManyRequestsException",
	} {
		if !IsThrottled(fakeAPIError{code: code}) {
			t.Errorf("APIError code %q should be classified as throttled", code)
		}
	}
}

func TestIsThrottled_NonThrottle(t *testing.T) {
	if IsThrottled(nil) {
		t.Error("nil error must not be classified as throttled")
	}
	if IsThrottled(errors.New("boom")) {
		t.Error("a generic error must not be classified as throttled")
	}
	if IsThrottled(fakeAPIError{code: "ValidationException"}) {
		t.Error("a non-throttling APIError must not be classified as throttled")
	}
	// A different modelled exception must not be mistaken for a throttle.
	if IsThrottled(&types.ConditionalCheckFailedException{}) {
		t.Error("ConditionalCheckFailedException must not be classified as throttled")
	}
}

// stubStore embeds the Store interface (left nil) so it satisfies Store while only
// the methods a test overrides are ever called; every other method would panic if
// invoked, which is fine because these tests call only the overridden ones. It
// lets the decorator be exercised without a full in-memory Store double.
type stubStore struct {
	Store
	err error
}

func (s stubStore) SetString(context.Context, string, []byte) error { return s.err }

func (s stubStore) GetString(context.Context, string) ([]byte, bool, error) {
	return nil, false, s.err
}

func TestThrottleStore_FiresHookAndMapsToErrThrottled(t *testing.T) {
	fired := 0
	throttle := &types.ProvisionedThroughputExceededException{}
	ts := newThrottleStore(stubStore{err: throttle}, func() { fired++ }, nil)

	// A write path: SetString returns the throttle, so the decorator must map it
	// to ErrThrottled and fire the alert hook exactly once.
	err := ts.SetString(context.Background(), "0:k", []byte("v"))
	if !errors.Is(err, ErrThrottled) {
		t.Fatalf("SetString error = %v, want it to wrap ErrThrottled", err)
	}
	if fired != 1 {
		t.Fatalf("OnThrottle fired %d times, want 1", fired)
	}

	// A read path fires the hook again (once per observed throttle).
	if _, _, gerr := ts.GetString(context.Background(), "0:k"); !errors.Is(gerr, ErrThrottled) {
		t.Fatalf("GetString error = %v, want it to wrap ErrThrottled", gerr)
	}
	if fired != 2 {
		t.Fatalf("OnThrottle fired %d times total, want 2", fired)
	}
}

func TestThrottleStore_PassesThroughNonThrottleAndSuccess(t *testing.T) {
	fired := 0
	hook := func() { fired++ }

	// A non-throttle error passes through unchanged and does not fire the hook.
	other := errors.New("some other backend error")
	ts := newThrottleStore(stubStore{err: other}, hook, nil)
	if err := ts.SetString(context.Background(), "0:k", nil); !errors.Is(err, other) {
		t.Fatalf("non-throttle error = %v, want it to pass through unchanged", err)
	}
	if errors.Is(ts.obs(other), ErrThrottled) {
		t.Fatal("a non-throttle error must not be mapped to ErrThrottled")
	}
	if fired != 0 {
		t.Fatalf("OnThrottle fired %d times on a non-throttle error, want 0", fired)
	}

	// Success passes through with no hook.
	ok := newThrottleStore(stubStore{err: nil}, hook, nil)
	if err := ok.SetString(context.Background(), "0:k", nil); err != nil {
		t.Fatalf("SetString on success = %v, want nil", err)
	}
	if fired != 0 {
		t.Fatalf("OnThrottle fired %d times on success, want 0", fired)
	}
}

func TestThrottleStore_NilHookStillMaps(t *testing.T) {
	// With no alerting hook, throttles must still be surfaced as ErrThrottled so
	// the command layer can reply (requirement 18.8).
	ts := newThrottleStore(stubStore{err: &types.ProvisionedThroughputExceededException{}}, nil, nil)
	if err := ts.SetString(context.Background(), "0:k", nil); !errors.Is(err, ErrThrottled) {
		t.Fatalf("SetString error = %v, want it to wrap ErrThrottled even without a hook", err)
	}
}
