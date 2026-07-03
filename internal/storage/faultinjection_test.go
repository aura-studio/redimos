package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This file holds the storage-seam half of the task 22.4 throttle fault-injection
// (requirement 18.8): it injects a RAW, unclassified DynamoDB
// ProvisionedThroughputExceededException at a chosen operation and drives it
// through the REAL throttle decorator, proving the decorator classifies it
// (IsThrottled), maps it to ErrThrottled for the command layer, and fires the
// OnThrottle alerting hook exactly once per throttled call — uniformly across the
// different KINDS of operation (a collection write and a keyspace scan), not just
// the String get/set that throttle_test.go already covers.

// faultInjectingStore embeds the Store interface (left nil) and overrides only the
// operations these tests drive, returning the injected backend error on each. It
// lets a raw throttling exception be injected at a chosen seam method and observed
// after the decorator wraps it. Every non-overridden method would panic if called,
// which is fine because these tests call only the overridden ones.
type faultInjectingStore struct {
	Store
	injected error
}

func (f faultInjectingStore) SAdd(context.Context, string, []string) (int, error) {
	return 0, f.injected
}

func (f faultInjectingStore) ScanKeys(context.Context, map[string]types.AttributeValue, int32, int64) ([]string, map[string]types.AttributeValue, error) {
	return nil, nil, f.injected
}

// TestFaultInjection_RawThrottleClassifiedAndAlerts injects a raw
// ProvisionedThroughputExceededException (the exact exception requirement 18.8
// names, NOT the pre-mapped sentinel) at a chosen operation and asserts the real
// throttle decorator (a) classifies it and returns ErrThrottled so the command
// layer can reply with the retryable "-ERR backend throttled, retry later", and
// (b) fires the OnThrottle alerting hook exactly once. It runs the injection on a
// collection write (SAdd) and a keyspace scan (ScanKeys) to show the throttle
// handling is applied uniformly at the seam regardless of the operation kind.
func TestFaultInjection_RawThrottleClassifiedAndAlerts(t *testing.T) {
	ctx := context.Background()
	raw := &types.ProvisionedThroughputExceededException{}

	cases := []struct {
		name string
		call func(Store) error
	}{
		{
			name: "collection write SAdd",
			call: func(s Store) error {
				_, err := s.SAdd(ctx, "0:k", []string{"m"})
				return err
			},
		},
		{
			name: "keyspace scan ScanKeys",
			call: func(s Store) error {
				_, _, err := s.ScanKeys(ctx, nil, 10, 0)
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fired := 0
			ts := newThrottleStore(faultInjectingStore{injected: raw}, func() { fired++ })

			err := tc.call(ts)
			if !errors.Is(err, ErrThrottled) {
				t.Fatalf("%s under throttle: err = %v, want it to wrap ErrThrottled", tc.name, err)
			}
			if fired != 1 {
				t.Fatalf("%s under throttle: OnThrottle fired %d times, want 1", tc.name, fired)
			}
		})
	}
}

// TestFaultInjection_NonThrottleFaultNotAlerted confirms fault injection of a
// NON-throttle backend error on the same chosen operation passes through
// unchanged and does NOT fire the alerting hook — the throttle path must not
// swallow or misclassify other backend failures (requirement 18.8 keeps only the
// throttle case retryable/alerted).
func TestFaultInjection_NonThrottleFaultNotAlerted(t *testing.T) {
	ctx := context.Background()
	other := errors.New("ValidationException: some other backend failure")

	fired := 0
	ts := newThrottleStore(faultInjectingStore{injected: other}, func() { fired++ })

	_, err := ts.SAdd(ctx, "0:k", []string{"m"})
	if !errors.Is(err, other) {
		t.Fatalf("non-throttle fault: err = %v, want it to pass through unchanged", err)
	}
	if errors.Is(err, ErrThrottled) {
		t.Fatal("non-throttle fault must not be mapped to ErrThrottled")
	}
	if fired != 0 {
		t.Fatalf("non-throttle fault: OnThrottle fired %d times, want 0", fired)
	}
}
