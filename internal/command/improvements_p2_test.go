package command

import (
	"bufio"
	"net"
	"testing"

	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

// This file covers the P2 handler-correctness fixes from the redimo/redimos audit
// (2026-07): SETRANGE-empty type check, the SET/SETNX foreign-type member reclaim
// (no orphan leak on a type overwrite), the LRANGE/ZRANGE effective-range result
// cap, and the constant-time AUTH compare. Each is a black-box wire test against a
// fake-backed router; the reclaim tests additionally inspect the fake's member
// maps to assert the old collection's items were physically removed.

// startStringServerCfg is startStringServer with an explicit Config (the default
// helper hardcodes Config{}); used here to exercise the collection result cap.
func startStringServerCfg(t *testing.T, cfg Config, store storage.Store, now func() int64) (net.Conn, *bufio.Reader) {
	t.Helper()
	r := NewRouterWithStorage(cfg, Storage{Store: store, Now: now})
	s := server.New(server.Options{Addr: "127.0.0.1:0"}, r)
	signal := make(chan error, 1)
	go func() { _ = s.ListenServeAndSignal(signal) }()
	if err := <-signal; err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, bufio.NewReader(conn)
}

// (readArray is defined in strings_test.go and reused here.)

// --- SETRANGE empty value must still type-check (WRONGTYPE) ------------------

func TestSetRangeEmptyValueChecksType(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	if got := sendRead(t, conn, r, "SADD k a"); got != ":1" {
		t.Fatalf("SADD k a = %q, want :1", got)
	}
	// Redis runs checkType before the empty-value short-circuit, so SETRANGE with an
	// empty value against a set replies WRONGTYPE — not length 0.
	if got := sendRead(t, conn, r, `SETRANGE k 0 ""`); got != "-"+"WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Fatalf("SETRANGE k 0 \"\" on a set = %q, want WRONGTYPE", got)
	}
}

// --- SET over any type reclaims the old collection's members ------------------

func TestSetOverwriteReclaimsForeignMembers(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))

	if got := sendRead(t, conn, r, "SADD k a b c"); got != ":3" {
		t.Fatalf("SADD k a b c = %q, want :3", got)
	}
	// A plain SET replaces a key of any type (Redis SET is type-agnostic).
	if got := sendRead(t, conn, r, "SET k hello"); got != "+OK" {
		t.Fatalf("SET k hello = %q, want +OK", got)
	}
	if got := sendRead(t, conn, r, "GET k"); got != "$hello" {
		t.Fatalf("GET k = %q, want $hello", got)
	}
	if got := sendRead(t, conn, r, "TYPE k"); got != "+string" {
		t.Fatalf("TYPE k = %q, want +string", got)
	}
	// The old set members must be physically reclaimed, not left as invisible orphans
	// (the async deleter's IsLive guard + SweepOrphans would both skip them once the
	// String meta lands, so the overwrite path must delete them synchronously).
	// This test's server runs in multi-db mode (startStringServer → Config{MultiDB:
	// true}), so the pk carries the "0:" prefix.
	pk := "0:k"
	if n := len(store.sets[pk]); n != 0 {
		t.Fatalf("SET over a set left %d orphan members under %q, want 0", n, pk)
	}
}

func TestSetNXOverExpiredForeignTypeReclaims(t *testing.T) {
	store := newFakeStringStore()
	now := int64(1000)
	conn, r := startStringServerCfg(t, Config{}, store, func() int64 { return now })

	if got := sendRead(t, conn, r, "SADD k a b c"); got != ":3" {
		t.Fatalf("SADD k a b c = %q, want :3", got)
	}
	if got := sendRead(t, conn, r, "EXPIRE k 10"); got != ":1" {
		t.Fatalf("EXPIRE k 10 = %q, want :1", got)
	}
	// Advance past the TTL: the set is now logically absent (expired) but its member
	// items still sit under pk until swept.
	now = 5000
	// SET NX claims the expired key as a fresh String.
	if got := sendRead(t, conn, r, "SET k v NX"); got != "+OK" {
		t.Fatalf("SET k v NX over expired set = %q, want +OK", got)
	}
	if got := sendRead(t, conn, r, "GET k"); got != "$v" {
		t.Fatalf("GET k = %q, want $v", got)
	}
	// This test's server runs in single-db mode (startStringServerCfg with Config{}),
	// so the pk is the raw key with no prefix.
	pk := "k"
	if n := len(store.sets[pk]); n != 0 {
		t.Fatalf("SET NX over an expired set left %d ghost members under %q, want 0", n, pk)
	}
}

// --- LRANGE / ZRANGE result cap on the effective range ----------------------

func TestLRangeResultCapOnEffectiveRange(t *testing.T) {
	store := newFakeStringStore()
	// Cap collection results at 3 members.
	conn, r := startStringServerCfg(t, Config{MaxCollectionResult: 3}, store, fixedNow(1000))

	if got := sendRead(t, conn, r, "RPUSH k a b c d e f"); got != ":6" {
		t.Fatalf("RPUSH = %q, want :6", got)
	}
	// The whole list (6 > cap 3) is rejected before materialization.
	if got := sendRead(t, conn, r, "LRANGE k 0 -1"); got != "-"+"ERR collection size exceeds the configured maximum result limit" {
		t.Fatalf("LRANGE k 0 -1 = %q, want collection-too-large", got)
	}
	// A bounded sub-range that selects only 3 elements still succeeds — the cap is on
	// the effective range width, not the whole-key size.
	got := readArrayAfter(t, conn, r, "LRANGE k 0 2")
	if len(got) != 3 || got[0] != "$a" || got[2] != "$c" {
		t.Fatalf("LRANGE k 0 2 = %v, want [a b c]", got)
	}
}

func TestZRangeResultCapOnEffectiveRange(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServerCfg(t, Config{MaxCollectionResult: 3}, store, fixedNow(1000))

	if got := sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c 4 d 5 e"); got != ":5" {
		t.Fatalf("ZADD = %q, want :5", got)
	}
	if got := sendRead(t, conn, r, "ZRANGE z 0 -1"); got != "-"+"ERR collection size exceeds the configured maximum result limit" {
		t.Fatalf("ZRANGE z 0 -1 = %q, want collection-too-large", got)
	}
	got := readArrayAfter(t, conn, r, "ZRANGE z 0 2")
	if len(got) != 3 || got[0] != "$a" || got[2] != "$c" {
		t.Fatalf("ZRANGE z 0 2 = %v, want [a b c]", got)
	}
	// ZREVRANGE selects the same number of ranks, so the cap fires identically.
	if got := sendRead(t, conn, r, "ZREVRANGE z 0 -1"); got != "-"+"ERR collection size exceeds the configured maximum result limit" {
		t.Fatalf("ZREVRANGE z 0 -1 = %q, want collection-too-large", got)
	}
}

// readArrayAfter sends cmd and reads the array reply that follows.
func readArrayAfter(t *testing.T, conn net.Conn, r *bufio.Reader, cmd string) []string {
	t.Helper()
	send(t, conn, cmd)
	return readArray(t, r)
}

// --- AUTH constant-time compare stays behaviourally correct ------------------

func TestAuthConstantTimeCompare(t *testing.T) {
	conn, r := startConnServer(t, Config{RequirePass: "s3cret"})

	// A wrong password of a different length must be rejected (ConstantTimeCompare
	// returns 0 on a length mismatch).
	if got := sendRead(t, conn, r, "AUTH nope"); got != "-ERR invalid password" {
		t.Fatalf("AUTH nope = %q, want -ERR invalid password", got)
	}
	// A wrong password of the SAME length must also be rejected.
	if got := sendRead(t, conn, r, "AUTH s3creT"); got != "-ERR invalid password" {
		t.Fatalf("AUTH s3creT = %q, want -ERR invalid password", got)
	}
	// The correct password authenticates.
	if got := sendRead(t, conn, r, "AUTH s3cret"); got != "+OK" {
		t.Fatalf("AUTH s3cret = %q, want +OK", got)
	}
}
