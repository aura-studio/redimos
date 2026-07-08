package meta

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aura-studio/redimos/internal/storage"
)

// fixedClock returns a now func pinned to t (epoch seconds) for deterministic
// expiry evaluation.
func fixedClock(t int64) func() int64 { return func() int64 { return t } }

// spyEnqueuer records the pks handed to it so tests can assert the read path
// enqueues an expired key exactly once (and never enqueues live/absent keys).
type spyEnqueuer struct {
	pks []string
}

func (s *spyEnqueuer) Enqueue(pk string) { s.pks = append(s.pks, pk) }

func TestReadPath_PresentUnexpired_ReturnsData(t *testing.T) {
	store := &fakeStore{
		loadFound: true,
		loadMeta:  storage.Meta{Type: "str", Exp: 0, Count: 1}, // exp=0 → never expires
	}
	enq := &spyEnqueuer{}
	r := NewReader(NewMetaStore(store, enq), fixedClock(100))

	readData := func(context.Context) (string, error) { return "hello", nil }

	val, found, err := ReadPath(context.Background(), r, "0:k", readData)
	if err != nil {
		t.Fatalf("ReadPath returned error: %v", err)
	}
	if !found {
		t.Fatalf("ReadPath found = false, want true for a present unexpired key")
	}
	if val != "hello" {
		t.Fatalf("ReadPath val = %q, want %q", val, "hello")
	}
	if len(enq.pks) != 0 {
		t.Fatalf("enqueued = %v, want none for a live key", enq.pks)
	}
}

func TestReadPath_FutureExp_ReturnsData(t *testing.T) {
	store := &fakeStore{
		loadFound: true,
		loadMeta:  storage.Meta{Type: "str", Exp: 200, Count: 1}, // exp>now → live
	}
	enq := &spyEnqueuer{}
	r := NewReader(NewMetaStore(store, enq), fixedClock(100))

	val, found, err := ReadPath(context.Background(), r, "0:k",
		func(context.Context) (string, error) { return "world", nil })
	if err != nil || !found || val != "world" {
		t.Fatalf("ReadPath = (%q, %v, %v), want (world, true, nil)", val, found, err)
	}
	if len(enq.pks) != 0 {
		t.Fatalf("enqueued = %v, want none for a live key", enq.pks)
	}
}

func TestReadPath_MetaAbsent_ReturnsNotFoundAndDoesNotEnqueue(t *testing.T) {
	store := &fakeStore{loadFound: false}
	enq := &spyEnqueuer{}
	r := NewReader(NewMetaStore(store, enq), fixedClock(100))

	readData := func(context.Context) (string, error) { return "should-be-ignored", nil }

	val, found, err := ReadPath(context.Background(), r, "0:missing", readData)
	if err != nil {
		t.Fatalf("ReadPath returned error: %v", err)
	}
	if found {
		t.Fatalf("ReadPath found = true, want false for an absent key")
	}
	if val != "" {
		t.Fatalf("ReadPath val = %q, want zero value for an absent key", val)
	}
	if len(enq.pks) != 0 {
		t.Fatalf("enqueued = %v, want none for an absent key", enq.pks)
	}
}

func TestReadPath_MetaAbsent_CancelsDataRead(t *testing.T) {
	store := &fakeStore{loadFound: false}
	r := NewReader(NewMetaStore(store, &spyEnqueuer{}), fixedClock(100))

	cancelled := make(chan struct{})
	// The data read blocks until its context is cancelled, letting us assert that
	// ReadPath signals cancellation when meta is absent.
	readData := func(ctx context.Context) (string, error) {
		<-ctx.Done()
		close(cancelled)
		return "", ctx.Err()
	}

	_, found, err := ReadPath(context.Background(), r, "0:missing", readData)
	if err != nil || found {
		t.Fatalf("ReadPath = (_, %v, %v), want (_, false, nil)", found, err)
	}

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("data read was not cancelled after meta was found absent")
	}
}

func TestReadPath_Expired_ReturnsNotFoundAndEnqueuesOnce(t *testing.T) {
	store := &fakeStore{
		loadFound: true,
		loadMeta:  storage.Meta{Type: "str", Exp: 100, Count: 1}, // exp==now → expired
	}
	enq := &spyEnqueuer{}
	r := NewReader(NewMetaStore(store, enq), fixedClock(100))

	readData := func(context.Context) (string, error) { return "should-be-ignored", nil }

	val, found, err := ReadPath(context.Background(), r, "0:k", readData)
	if err != nil {
		t.Fatalf("ReadPath returned error: %v", err)
	}
	if found {
		t.Fatalf("ReadPath found = true, want false for an expired key")
	}
	if val != "" {
		t.Fatalf("ReadPath val = %q, want zero value for an expired key", val)
	}
	if len(enq.pks) != 1 || enq.pks[0] != "0:k" {
		t.Fatalf("enqueued = %v, want exactly [0:k] for an expired key", enq.pks)
	}
}

func TestReadPath_ExpiredBoundary_ExpAfterNowIsLive(t *testing.T) {
	// One second past the boundary: exp = now+1 is NOT expired (IsExpired uses
	// exp <= now). This pins the boundary opposite TestReadPath_Expired.
	store := &fakeStore{
		loadFound: true,
		loadMeta:  storage.Meta{Type: "str", Exp: 101, Count: 1},
	}
	enq := &spyEnqueuer{}
	r := NewReader(NewMetaStore(store, enq), fixedClock(100))

	val, found, err := ReadPath(context.Background(), r, "0:k",
		func(context.Context) (string, error) { return "live", nil })
	if err != nil || !found || val != "live" {
		t.Fatalf("ReadPath = (%q, %v, %v), want (live, true, nil)", val, found, err)
	}
	if len(enq.pks) != 0 {
		t.Fatalf("enqueued = %v, want none for a key expiring in the future", enq.pks)
	}
}

func TestReadPath_MetaLoadError_Propagates(t *testing.T) {
	sentinel := errors.New("meta load failed")
	store := &fakeStore{loadFound: true, loadErr: sentinel}
	enq := &spyEnqueuer{}
	r := NewReader(NewMetaStore(store, enq), fixedClock(100))

	_, found, err := ReadPath(context.Background(), r, "0:k",
		func(context.Context) (string, error) { return "x", nil })
	if !errors.Is(err, sentinel) {
		t.Fatalf("ReadPath error = %v, want the underlying meta load error", err)
	}
	if found {
		t.Fatalf("ReadPath found = true on meta error, want false")
	}
	if len(enq.pks) != 0 {
		t.Fatalf("enqueued = %v, want none on a meta load error", enq.pks)
	}
}

func TestReadPath_DataReadError_Propagates(t *testing.T) {
	// Meta present and unexpired, but the data read fails: the error surfaces and
	// found is false.
	store := &fakeStore{loadFound: true, loadMeta: storage.Meta{Type: "str", Exp: 0}}
	r := NewReader(NewMetaStore(store, &spyEnqueuer{}), fixedClock(100))

	sentinel := errors.New("data read failed")
	_, found, err := ReadPath(context.Background(), r, "0:k",
		func(context.Context) (string, error) { return "", sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("ReadPath error = %v, want the underlying data read error", err)
	}
	if found {
		t.Fatalf("ReadPath found = true on data error, want false")
	}
}

func TestNewReader_NilClockUsesWallClock(t *testing.T) {
	// A nil clock must fall back to the wall clock without panicking. With exp=0
	// the key never expires, so the outcome is deterministic regardless of "now".
	store := &fakeStore{loadFound: true, loadMeta: storage.Meta{Type: "str", Exp: 0}}
	r := NewReader(NewMetaStore(store, &spyEnqueuer{}), nil)

	val, found, err := ReadPath(context.Background(), r, "0:k",
		func(context.Context) (string, error) { return "ok", nil })
	if err != nil || !found || val != "ok" {
		t.Fatalf("ReadPath = (%q, %v, %v), want (ok, true, nil)", val, found, err)
	}
}
