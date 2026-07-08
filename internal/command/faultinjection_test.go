package command

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/aura-studio/redimos/v2/internal/scan"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

// This file holds the task 22.4 capacity/fault-injection tests that drive the two
// backend-failure paths end-to-end through an in-process server and assert the
// exact wire reply a client would observe:
//
//   - a DynamoDB throttle (requirement 18.8): a write command whose store returns
//     a throttling error must surface the retryable
//     "-ERR backend throttled, retry later"; and
//   - an instance-kill cursor invalidation (requirement 13.5): a SCAN continuation
//     cursor minted by one instance, replayed after that instance is "killed" (its
//     in-memory cursor registry lost / replaced by a fresh one on a new instance),
//     must surface "-ERR invalid cursor".
//
// The storage seam that CLASSIFIES a throttle and fires the OnThrottle alerting
// hook is exercised in internal/storage (throttle_test.go and
// faultinjection_test.go); here the injected store returns the already-classified
// storage.ErrThrottled — exactly what reaches the command layer in production
// after the throttle decorator maps a ProvisionedThroughputExceededException — so
// this test pins the command-path mapping to the wire.

// throttlingStore is a fault-injecting wrapper over the fake in-memory Store that
// makes the two representative write primitives (SetString for SET, SAdd for
// SADD) fail as if DynamoDB throttled the request after the SDK's bounded
// retry/backoff. Every other operation (including the meta EnsureType the write
// paths call first) delegates to the embedded fake, so the command reaches the
// throttled data write on an otherwise-healthy key.
type throttlingStore struct {
	*fakeStringStore
}

// injectedThrottle mimics the error the throttle decorator hands the command
// layer: storage.ErrThrottled wrapping the originating backend detail, so
// writeStoreError's errors.Is(err, storage.ErrThrottled) branch fires.
func injectedThrottle() error {
	return fmt.Errorf("%w: ProvisionedThroughputExceededException", storage.ErrThrottled)
}

func (s *throttlingStore) SetString(context.Context, string, []byte) error {
	return injectedThrottle()
}

func (s *throttlingStore) SAdd(context.Context, string, []string) (int, error) {
	return 0, injectedThrottle()
}

// TestFaultInjection_ThrottlePropagatesRetryableError injects a DynamoDB throttle
// on the data write of two different write commands (a String SET and a Set SADD)
// and asserts the client receives the byte-for-byte retryable reply
// "-ERR backend throttled, retry later" for each. This is the command-path half of
// requirement 18.8: the storage seam has already classified the throttle and
// fired the alerting hook; the command layer must map storage.ErrThrottled to the
// retryable -ERR (never a fatal error, never a silent success).
func TestFaultInjection_ThrottlePropagatesRetryableError(t *testing.T) {
	conn, r := startScanServer(t, &throttlingStore{newFakeStringStore()}, fixedNow(1000))

	const want = "-ERR backend throttled, retry later"

	if got := sendRead(t, conn, r, "SET k v"); got != want {
		t.Errorf("SET under throttle = %q, want %q", got, want)
	}
	if got := sendRead(t, conn, r, "SADD s m"); got != want {
		t.Errorf("SADD under throttle = %q, want %q", got, want)
	}
}

// startScanServerReg boots an in-process server whose SCAN handler resolves
// continuation cursors against the supplied registry, using instID as both the
// server's connection instance id and (via the caller) the registry's owner. It
// returns a connected client. It mirrors startScanServer but lets the caller own
// the registry so a cursor can be minted on one instance and replayed against
// another.
func startScanServerReg(t *testing.T, store storage.Store, reg *scan.Registry, instID string, now func() int64) (net.Conn, *bufio.Reader) {
	t.Helper()

	router := NewRouterWithStorage(Config{MultiDB: true}, Storage{Store: store, Now: now, Scan: reg})
	s := server.New(server.Options{Addr: "127.0.0.1:0", InstID: instID}, router)

	signal := make(chan error, 1)
	go func() { _ = s.ListenServeAndSignal(signal) }()
	if err := <-signal; err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, bufio.NewReader(conn)
}

// TestFaultInjection_InstanceKillInvalidatesCursor reproduces requirement 13.5's
// instance-kill path at the command boundary. Instance A serves a paged SCAN and
// mints a real continuation cursor through the actual handler. The instance is
// then "killed": its in-memory cursor registry is gone, and a fresh instance B
// comes up with an empty registry and a different instance id. Replaying A's
// cursor against B — a cursor B's registry never handed out and does not own — is
// rejected with the byte-for-byte "-ERR invalid cursor", so the
// client restarts the scan from cursor 0 rather than silently losing or repeating
// the keyspace.
func TestFaultInjection_InstanceKillInvalidatesCursor(t *testing.T) {
	now := fixedNow(1000)

	// --- Instance A: mint a genuine continuation cursor through the SCAN handler.
	storeA := newFakeStringStore()
	regA := scan.New(scan.Config{InstID: "inst-A"})
	connA, rA := startScanServerReg(t, storeA, regA, "inst-A", now)

	// Populate enough keys that a small COUNT cannot drain the keyspace in one
	// page, forcing the handler to register a continuation cursor.
	for i := 0; i < 5; i++ {
		sendRead(t, connA, rA, fmt.Sprintf("SET key%d v", i))
	}
	send(t, connA, "SCAN 0 COUNT 2")
	cursorA, _ := readScanReply(t, rA)
	if cursorA == "0" {
		t.Fatalf("precondition: expected a non-terminating continuation cursor from instance A, got %q", cursorA)
	}

	// --- Instance A is killed: a brand-new instance B comes up with a fresh,
	// empty registry and a different instance id. (A fresh registry is exactly
	// what a restarted process has — the previous in-memory cursors are gone.)
	storeB := newFakeStringStore()
	regB := scan.New(scan.Config{InstID: "inst-B"})
	connB, rB := startScanServerReg(t, storeB, regB, "inst-B", now)

	// Replaying instance A's cursor against instance B is rejected: B's registry
	// never minted it and does not own it.
	const want = "-ERR invalid cursor"
	if got := sendRead(t, connB, rB, "SCAN "+cursorA); got != want {
		t.Errorf("SCAN <instance-A cursor> against instance B = %q, want %q", got, want)
	}

	// After the invalid-cursor error the client restarts from 0, which instance B
	// serves normally (an empty keyspace here) — the failure path is recoverable,
	// not fatal to the connection.
	send(t, connB, "SCAN 0")
	if cursor, keys := readScanReply(t, rB); cursor != "0" || len(keys) != 0 {
		t.Errorf("SCAN 0 restart on instance B = (cursor %q, keys %v), want (\"0\", [])", cursor, keys)
	}
}
