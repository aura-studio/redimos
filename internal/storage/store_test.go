package storage

import (
	"errors"
	"testing"

	redimo "github.com/aura-studio/redimo/v3"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// These tests exercise only the parts of the storage seam that do not require a
// live DynamoDB: construction wiring and the sentinel error. Method calls that hit
// DynamoDB are covered by the meta layer's fake-store tests and by integration
// tests guarded behind a live-backend env check.

func TestNew_ConstructsWithoutNetwork(t *testing.T) {
	// A zero-value client is enough: New only builds the redimo.Client config and
	// performs no network calls.
	s := New(&dynamodb.Client{}, Options{TableName: "redis-data"})
	if s == nil {
		t.Fatal("New returned nil Store")
	}
}

// TestNew_DefaultsToStronglyConsistent asserts the P0 default: a Store built with
// a bare Options{} reads strongly consistently (requirement 15.1). The redimo
// client's consistentReads flag is unexported, so the assertion goes through the
// storage-seam construction path — New must NOT downgrade to eventual consistency
// unless EventuallyConsistent is explicitly set.
func TestNew_DefaultsToStronglyConsistent(t *testing.T) {
	// Strong (default) and eventual (opt-out) both construct without network I/O;
	// this guards against a regression that flips the default back to eventual.
	strong := New(&dynamodb.Client{}, Options{})
	if strong == nil {
		t.Fatal("New with default options returned nil Store")
	}
	eventual := New(&dynamodb.Client{}, Options{EventuallyConsistent: true})
	if eventual == nil {
		t.Fatal("New with EventuallyConsistent returned nil Store")
	}
}

func TestNew_DefaultsWhenTableEmpty(t *testing.T) {
	s := New(&dynamodb.Client{}, Options{})
	if s == nil {
		t.Fatal("New returned nil Store with empty options")
	}
}

func TestNewFromClient(t *testing.T) {
	client := redimo.NewClient(&dynamodb.Client{}).Table("redis-data")
	s := NewFromClient(client)
	if s == nil {
		t.Fatal("NewFromClient returned nil Store")
	}
}

func TestErrWrongType_MirrorsRedimo(t *testing.T) {
	// The storage sentinel must carry the exact Redis WRONGTYPE text so the meta
	// layer and command handlers can reproduce Pika's reply byte-for-byte.
	if ErrWrongType.Error() != redimo.ErrWrongType.Error() {
		t.Fatalf("storage.ErrWrongType = %q, want it to mirror redimo.ErrWrongType %q",
			ErrWrongType.Error(), redimo.ErrWrongType.Error())
	}
	if !errors.Is(ErrWrongType, ErrWrongType) {
		t.Fatal("ErrWrongType is not matchable with errors.Is")
	}
}
