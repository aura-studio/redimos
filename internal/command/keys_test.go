package command

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

// spyEnqueuer records the pks handed to it by MetaStore.DeleteMeta so the Key
// tests can assert DEL wires member cleanup through the lazy-delete seam without a
// live background deleter.
type spyEnqueuer struct {
	pks []string
}

func (s *spyEnqueuer) Enqueue(pk string) { s.pks = append(s.pks, pk) }

var _ meta.DeletionEnqueuer = (*spyEnqueuer)(nil)

// startKeysServer boots an in-process server whose router is wired to the given
// fake store, clock and (optional) enqueuer, and returns a connected client. It
// mirrors startStringServer but threads the Storage.Enqueuer seam so DEL's async
// member cleanup can be observed.
func startKeysServer(t *testing.T, store storage.Store, now func() int64, enq meta.DeletionEnqueuer) (net.Conn, *bufio.Reader) {
	t.Helper()
	r := NewRouterWithStorage(Config{MultiDB: true}, Storage{Store: store, Now: now, Enqueuer: enq})
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
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	return conn, bufio.NewReader(conn)
}

// --- DEL (requirement 10.1) -------------------------------------------------

func TestDelExistingKey(t *testing.T) {
	store := newFakeStringStore()
	enq := &spyEnqueuer{}
	conn, r := startKeysServer(t, store, fixedNow(1000), enq)

	sendRead(t, conn, r, "SET k v")
	if got, want := sendRead(t, conn, r, "DEL k"), ":1"; got != want {
		t.Errorf("DEL k = %q, want %q", got, want)
	}
	// Key is immediately logically absent.
	if got, want := sendRead(t, conn, r, "GET k"), "$-1"; got != want {
		t.Errorf("GET k after DEL = %q, want %q", got, want)
	}
	// The pk was enqueued for async member cleanup.
	if len(enq.pks) != 1 || enq.pks[0] != "0:k" {
		t.Errorf("enqueued pks = %v, want [0:k]", enq.pks)
	}
}

func TestDelAbsentKeyCountsZero(t *testing.T) {
	enq := &spyEnqueuer{}
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), enq)

	if got, want := sendRead(t, conn, r, "DEL absent"), ":0"; got != want {
		t.Errorf("DEL absent = %q, want %q", got, want)
	}
	// No meta existed, so nothing is enqueued.
	if len(enq.pks) != 0 {
		t.Errorf("enqueued pks = %v, want []", enq.pks)
	}
}

func TestDelMultipleKeysCountsLiveOnly(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), &spyEnqueuer{})

	sendRead(t, conn, r, "SET a 1")
	sendRead(t, conn, r, "SET b 2")
	// a and b exist, c does not -> count 2.
	if got, want := sendRead(t, conn, r, "DEL a b c"), ":2"; got != want {
		t.Errorf("DEL a b c = %q, want %q", got, want)
	}
}

func TestDelExpiredKeyCountsZeroButCleansUp(t *testing.T) {
	store := newFakeStringStore()
	enq := &spyEnqueuer{}
	conn, r := startKeysServer(t, store, fixedNow(1000), enq)

	sendRead(t, conn, r, "SET k v")
	// Force expiry in the past.
	m := store.metas["0:k"]
	m.Exp = 500
	store.metas["0:k"] = m

	// An expired key does not count as existing (requirement 10.1 + 11.5)...
	if got, want := sendRead(t, conn, r, "DEL k"), ":0"; got != want {
		t.Errorf("DEL k (expired) = %q, want %q", got, want)
	}
	// ...but its meta is still removed and its members enqueued for cleanup.
	if len(enq.pks) != 1 || enq.pks[0] != "0:k" {
		t.Errorf("enqueued pks = %v, want [0:k]", enq.pks)
	}
	if store.live["0:k"] {
		t.Error("meta should have been removed by DEL")
	}
}

// --- EXISTS (requirement 10.2) ----------------------------------------------

func TestExistsLiveKey(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)

	if got, want := sendRead(t, conn, r, "EXISTS k"), ":0"; got != want {
		t.Errorf("EXISTS k (absent) = %q, want %q", got, want)
	}
	sendRead(t, conn, r, "SET k v")
	if got, want := sendRead(t, conn, r, "EXISTS k"), ":1"; got != want {
		t.Errorf("EXISTS k = %q, want %q", got, want)
	}
}

func TestExistsCountsRepeatsAndMultiple(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)

	sendRead(t, conn, r, "SET a 1")
	sendRead(t, conn, r, "SET b 2")
	// a exists (x2 via repeat), b exists, missing does not -> 3.
	if got, want := sendRead(t, conn, r, "EXISTS a a b missing"), ":3"; got != want {
		t.Errorf("EXISTS a a b missing = %q, want %q", got, want)
	}
}

func TestExistsExpiredKeyIsAbsent(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startKeysServer(t, store, fixedNow(1000), nil)

	sendRead(t, conn, r, "SET k v")
	m := store.metas["0:k"]
	m.Exp = 500 // in the past relative to now=1000
	store.metas["0:k"] = m

	if got, want := sendRead(t, conn, r, "EXISTS k"), ":0"; got != want {
		t.Errorf("EXISTS k (expired) = %q, want %q", got, want)
	}
}

// --- TYPE (requirement 10.3) ------------------------------------------------

func TestTypeString(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)

	sendRead(t, conn, r, "SET k v")
	if got, want := sendRead(t, conn, r, "TYPE k"), "+string"; got != want {
		t.Errorf("TYPE k = %q, want %q", got, want)
	}
}

func TestTypeAbsentReturnsNone(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)

	if got, want := sendRead(t, conn, r, "TYPE absent"), "+none"; got != want {
		t.Errorf("TYPE absent = %q, want %q", got, want)
	}
}

func TestTypeExpiredReturnsNone(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startKeysServer(t, store, fixedNow(1000), nil)

	sendRead(t, conn, r, "SET k v")
	m := store.metas["0:k"]
	m.Exp = 500
	store.metas["0:k"] = m

	if got, want := sendRead(t, conn, r, "TYPE k"), "+none"; got != want {
		t.Errorf("TYPE k (expired) = %q, want %q", got, want)
	}
}

func TestTypeCollectionKinds(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startKeysServer(t, store, fixedNow(1000), nil)

	cases := map[string]struct {
		kind meta.KeyType
		want string
	}{
		"0:h": {meta.TypeHash, "+hash"},
		"0:l": {meta.TypeList, "+list"},
		"0:s": {meta.TypeSet, "+set"},
		"0:z": {meta.TypeZSet, "+zset"},
	}
	for pk, tc := range cases {
		store.metas[pk] = storage.Meta{Type: string(tc.kind)}
		store.live[pk] = true
	}
	for pk, tc := range cases {
		key := pk[len("0:"):]
		if got := sendRead(t, conn, r, "TYPE "+key); got != tc.want {
			t.Errorf("TYPE %s = %q, want %q", key, got, tc.want)
		}
	}
}

// --- arity (requirement 3.2) ------------------------------------------------

func TestKeysArityErrors(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)
	cases := map[string]string{
		"DEL":      "-ERR wrong number of arguments for 'del' command",
		"EXISTS":   "-ERR wrong number of arguments for 'exists' command",
		"TYPE":     "-ERR wrong number of arguments for 'type' command",
		"TYPE a b": "-ERR wrong number of arguments for 'type' command",
	}
	for cmd, want := range cases {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// --- EXPIRE family (requirements 10.4, 10.5, 10.11) -------------------------

func TestExpireSetsExpAndReturnsOne(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startKeysServer(t, store, fixedNow(1000), nil)

	sendRead(t, conn, r, "SET k v")
	if got, want := sendRead(t, conn, r, "EXPIRE k 60"), ":1"; got != want {
		t.Errorf("EXPIRE k 60 = %q, want %q", got, want)
	}
	if got, want := store.metas["0:k"].Exp, int64(1060); got != want {
		t.Errorf("meta.exp = %d, want %d (now+60)", got, want)
	}
}

func TestExpireAbsentKeyReturnsZero(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)
	if got, want := sendRead(t, conn, r, "EXPIRE absent 60"), ":0"; got != want {
		t.Errorf("EXPIRE absent 60 = %q, want %q", got, want)
	}
}

func TestExpireExpiredKeyReturnsZero(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startKeysServer(t, store, fixedNow(1000), nil)
	sendRead(t, conn, r, "SET k v")
	m := store.metas["0:k"]
	m.Exp = 500 // already expired relative to now=1000
	store.metas["0:k"] = m

	if got, want := sendRead(t, conn, r, "EXPIRE k 60"), ":0"; got != want {
		t.Errorf("EXPIRE k 60 (expired) = %q, want %q", got, want)
	}
}

func TestExpireAtSetsAbsoluteExp(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startKeysServer(t, store, fixedNow(1000), nil)
	sendRead(t, conn, r, "SET k v")
	if got, want := sendRead(t, conn, r, "EXPIREAT k 5000"), ":1"; got != want {
		t.Errorf("EXPIREAT k 5000 = %q, want %q", got, want)
	}
	if got, want := store.metas["0:k"].Exp, int64(5000); got != want {
		t.Errorf("meta.exp = %d, want %d (absolute)", got, want)
	}
}

func TestPExpireTruncatesToSeconds(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startKeysServer(t, store, fixedNow(1000), nil)
	sendRead(t, conn, r, "SET k v")
	// 65500ms after now=1000s => (1000*1000+65500)/1000 = 1065s.
	if got, want := sendRead(t, conn, r, "PEXPIRE k 65500"), ":1"; got != want {
		t.Errorf("PEXPIRE k 65500 = %q, want %q", got, want)
	}
	if got, want := store.metas["0:k"].Exp, int64(1065); got != want {
		t.Errorf("meta.exp = %d, want %d (second-truncated)", got, want)
	}
}

func TestPExpireAtTruncatesToSeconds(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startKeysServer(t, store, fixedNow(1000), nil)
	sendRead(t, conn, r, "SET k v")
	// Absolute 5000500ms => 5000s.
	if got, want := sendRead(t, conn, r, "PEXPIREAT k 5000500"), ":1"; got != want {
		t.Errorf("PEXPIREAT k 5000500 = %q, want %q", got, want)
	}
	if got, want := store.metas["0:k"].Exp, int64(5000); got != want {
		t.Errorf("meta.exp = %d, want %d (second-truncated)", got, want)
	}
}

func TestExpireNonIntegerReturnsError(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)
	want := "-ERR value is not an integer or out of range"
	for _, cmd := range []string{"EXPIRE k abc", "EXPIREAT k x", "PEXPIRE k 1.5", "PEXPIREAT k +5"} {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// --- past-expiry semantics (requirement 10.4) -------------------------------

func TestExpirePastDeletesLiveKeyAndReturnsOne(t *testing.T) {
	store := newFakeStringStore()
	enq := &spyEnqueuer{}
	conn, r := startKeysServer(t, store, fixedNow(1000), enq)
	sendRead(t, conn, r, "SET k v")

	// Negative TTL resolves to a past expiry: delete the live key, reply :1.
	if got, want := sendRead(t, conn, r, "EXPIRE k -5"), ":1"; got != want {
		t.Errorf("EXPIRE k -5 = %q, want %q", got, want)
	}
	// Key is immediately logically absent on next access.
	if got, want := sendRead(t, conn, r, "GET k"), "$-1"; got != want {
		t.Errorf("GET k after past EXPIRE = %q, want %q", got, want)
	}
	if store.live["0:k"] {
		t.Error("meta should have been removed by past EXPIRE")
	}
	// The pk was enqueued for async member cleanup.
	if len(enq.pks) != 1 || enq.pks[0] != "0:k" {
		t.Errorf("enqueued pks = %v, want [0:k]", enq.pks)
	}
}

func TestExpireAtPastDeletesLiveKey(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startKeysServer(t, store, fixedNow(1000), nil)
	sendRead(t, conn, r, "SET k v")
	// Timestamp in the past (< now) deletes and returns :1.
	if got, want := sendRead(t, conn, r, "EXPIREAT k 500"), ":1"; got != want {
		t.Errorf("EXPIREAT k 500 (past) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "EXISTS k"), ":0"; got != want {
		t.Errorf("EXISTS k after past EXPIREAT = %q, want %q", got, want)
	}
}

func TestExpireAtZeroDeletesLiveKey(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startKeysServer(t, store, fixedNow(1000), nil)
	sendRead(t, conn, r, "SET k v")
	// EXPIREAT 0 must delete (not be interpreted as never-expiring exp=0).
	if got, want := sendRead(t, conn, r, "EXPIREAT k 0"), ":1"; got != want {
		t.Errorf("EXPIREAT k 0 = %q, want %q", got, want)
	}
	if store.live["0:k"] {
		t.Error("EXPIREAT k 0 should have deleted the key, not set exp=0")
	}
}

func TestExpirePastAbsentKeyReturnsZero(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)
	if got, want := sendRead(t, conn, r, "EXPIRE absent -5"), ":0"; got != want {
		t.Errorf("EXPIRE absent -5 = %q, want %q", got, want)
	}
}

// EXPIRE applies to any key type, including collections (requirement 10.11).
func TestExpireOnCollectionKey(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startKeysServer(t, store, fixedNow(1000), nil)
	store.metas["0:h"] = storage.Meta{Type: string(meta.TypeHash)}
	store.live["0:h"] = true

	if got, want := sendRead(t, conn, r, "EXPIRE h 60"), ":1"; got != want {
		t.Errorf("EXPIRE h 60 (hash) = %q, want %q", got, want)
	}
	if got, want := store.metas["0:h"].Exp, int64(1060); got != want {
		t.Errorf("hash meta.exp = %d, want %d", got, want)
	}
}

// --- TTL / PTTL (requirements 10.6, 10.11) ----------------------------------

func TestTTLAbsentReturnsMinusTwo(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)
	if got, want := sendRead(t, conn, r, "TTL absent"), ":-2"; got != want {
		t.Errorf("TTL absent = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "PTTL absent"), ":-2"; got != want {
		t.Errorf("PTTL absent = %q, want %q", got, want)
	}
}

func TestTTLExpiredReturnsMinusTwo(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startKeysServer(t, store, fixedNow(1000), nil)
	sendRead(t, conn, r, "SET k v")
	m := store.metas["0:k"]
	m.Exp = 500
	store.metas["0:k"] = m
	if got, want := sendRead(t, conn, r, "TTL k"), ":-2"; got != want {
		t.Errorf("TTL k (expired) = %q, want %q", got, want)
	}
}

func TestTTLNoExpiryReturnsMinusOne(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)
	sendRead(t, conn, r, "SET k v")
	if got, want := sendRead(t, conn, r, "TTL k"), ":-1"; got != want {
		t.Errorf("TTL k (no expiry) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "PTTL k"), ":-1"; got != want {
		t.Errorf("PTTL k (no expiry) = %q, want %q", got, want)
	}
}

func TestTTLReturnsRemainingSeconds(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)
	sendRead(t, conn, r, "SET k v")
	sendRead(t, conn, r, "EXPIRE k 60")
	if got, want := sendRead(t, conn, r, "TTL k"), ":60"; got != want {
		t.Errorf("TTL k = %q, want %q", got, want)
	}
	// PTTL is remaining seconds * 1000 (second-precision storage).
	if got, want := sendRead(t, conn, r, "PTTL k"), ":60000"; got != want {
		t.Errorf("PTTL k = %q, want %q", got, want)
	}
}

// --- PERSIST (requirement 10.7) ---------------------------------------------

func TestPersistRemovesExpiry(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startKeysServer(t, store, fixedNow(1000), nil)
	sendRead(t, conn, r, "SET k v")
	sendRead(t, conn, r, "EXPIRE k 60")

	if got, want := sendRead(t, conn, r, "PERSIST k"), ":1"; got != want {
		t.Errorf("PERSIST k = %q, want %q", got, want)
	}
	if got := store.metas["0:k"].Exp; got != 0 {
		t.Errorf("meta.exp after PERSIST = %d, want 0", got)
	}
	// TTL now reports no expiry.
	if got, want := sendRead(t, conn, r, "TTL k"), ":-1"; got != want {
		t.Errorf("TTL k after PERSIST = %q, want %q", got, want)
	}
}

func TestPersistNoExpiryReturnsZero(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)
	sendRead(t, conn, r, "SET k v") // live but no TTL
	if got, want := sendRead(t, conn, r, "PERSIST k"), ":0"; got != want {
		t.Errorf("PERSIST k (no TTL) = %q, want %q", got, want)
	}
}

func TestPersistAbsentReturnsZero(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)
	if got, want := sendRead(t, conn, r, "PERSIST absent"), ":0"; got != want {
		t.Errorf("PERSIST absent = %q, want %q", got, want)
	}
}

func TestPersistExpiredReturnsZero(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startKeysServer(t, store, fixedNow(1000), nil)
	sendRead(t, conn, r, "SET k v")
	m := store.metas["0:k"]
	m.Exp = 500
	store.metas["0:k"] = m
	if got, want := sendRead(t, conn, r, "PERSIST k"), ":0"; got != want {
		t.Errorf("PERSIST k (expired) = %q, want %q", got, want)
	}
}

// --- arity for TTL family (requirement 3.2) ---------------------------------

func TestTTLFamilyArityErrors(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)
	cases := map[string]string{
		"EXPIRE k":     "-ERR wrong number of arguments for 'expire' command",
		"EXPIRE k 1 2": "-ERR wrong number of arguments for 'expire' command",
		"EXPIREAT k":   "-ERR wrong number of arguments for 'expireat' command",
		"PEXPIRE k":    "-ERR wrong number of arguments for 'pexpire' command",
		"PEXPIREAT k":  "-ERR wrong number of arguments for 'pexpireat' command",
		"TTL":          "-ERR wrong number of arguments for 'ttl' command",
		"TTL a b":      "-ERR wrong number of arguments for 'ttl' command",
		"PTTL":         "-ERR wrong number of arguments for 'pttl' command",
		"PERSIST":      "-ERR wrong number of arguments for 'persist' command",
		"PERSIST a b":  "-ERR wrong number of arguments for 'persist' command",
	}
	for cmd, want := range cases {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// --- KEYS rejection (requirement 10.9) --------------------------------------

// KEYS is an operations-only, guarded command: any client-issued KEYS <pattern>
// (including the classic full-scan KEYS *) is declined with a descriptive error
// pointing at SCAN, rather than triggering an unbounded full-table Scan.
func TestKeysRejectedAsOpsOnly(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)

	want := "-" + errKeysOpsOnly
	for _, cmd := range []string{"KEYS *", "KEYS user:*", "KEYS foo"} {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// A bare KEYS (arity 2 not satisfied) still gets the standard
// wrong-number-of-arguments reply before the ops-only rejection (requirement 3.2).
func TestKeysArityError(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)
	cases := map[string]string{
		"KEYS":       "-ERR wrong number of arguments for 'keys' command",
		"KEYS a b":   "-ERR wrong number of arguments for 'keys' command",
		"KEYS a b c": "-ERR wrong number of arguments for 'keys' command",
	}
	for cmd, want := range cases {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// --- RENAME / RENAMENX rejection (requirement 10.10) ------------------------

// RENAME and RENAMENX are not supported in P0 (whole-collection copy is too
// costly); both are declined with a descriptive error.
func TestRenameFamilyRejected(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)

	sendRead(t, conn, r, "SET k v")
	want := "-" + errRenameUnsupported
	for _, cmd := range []string{"RENAME k k2", "RENAMENX k k2"} {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// Malformed RENAME/RENAMENX still get the standard wrong-number-of-arguments
// reply before the unsupported rejection (requirement 3.2).
func TestRenameFamilyArityErrors(t *testing.T) {
	conn, r := startKeysServer(t, newFakeStringStore(), fixedNow(1000), nil)
	cases := map[string]string{
		"RENAME":         "-ERR wrong number of arguments for 'rename' command",
		"RENAME k":       "-ERR wrong number of arguments for 'rename' command",
		"RENAME k a b":   "-ERR wrong number of arguments for 'rename' command",
		"RENAMENX":       "-ERR wrong number of arguments for 'renamenx' command",
		"RENAMENX k":     "-ERR wrong number of arguments for 'renamenx' command",
		"RENAMENX k a b": "-ERR wrong number of arguments for 'renamenx' command",
	}
	for cmd, want := range cases {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}
