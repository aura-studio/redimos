package command

import (
	"bufio"
	"context"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// fakeStringStore is a stateful in-memory storage.Store double covering the meta
// primitives and the String data operations, so the String command handlers can
// be exercised over a real in-process server without a live DynamoDB. Each key is
// modelled by an optional meta item (type/exp/cnt + presence) and an optional
// String value item, mirroring the two-item layout the proxy uses.
type fakeStringStore struct {
	metas map[string]storage.Meta
	live  map[string]bool // meta presence for pk
	data  map[string][]byte
	has   map[string]bool // value-item presence for pk

	// hashes models each Hash key's field items: pk -> field -> value. It is
	// independent of the meta item (the count lives in metas[pk].Count, maintained
	// by the command handlers via EnsureType), mirroring the per-field item layout.
	hashes map[string]map[string][]byte

	// sets models each Set key's member items: pk -> member -> present. Like
	// hashes it is independent of the meta item (cardinality lives in
	// metas[pk].Count, maintained by the handlers via EnsureType), mirroring the
	// per-member item layout the proxy uses.
	sets map[string]map[string]struct{}

	// zsets models each Sorted Set key's member items: pk -> member -> score.
	// Like sets it is independent of the meta item (the member count lives in
	// metas[pk].Count, maintained by the handlers via EnsureType); the range/rank
	// reads sort it with storage.SortZMembers so the fake ranks members identically
	// to the redimo-backed store's score index.
	zsets map[string]map[string]float64

	// lists models each List key's element items as an ordered slice, head at
	// index 0. Like the other collections it is independent of the meta item (the
	// length lives in metas[pk].Count, maintained by the handlers via EnsureType),
	// so LLEN reads meta.cnt for O(1) while these ops mutate the element order.
	lists map[string][][]byte
}

func newFakeStringStore() *fakeStringStore {
	return &fakeStringStore{
		metas:  make(map[string]storage.Meta),
		live:   make(map[string]bool),
		data:   make(map[string][]byte),
		has:    make(map[string]bool),
		hashes: make(map[string]map[string][]byte),
		sets:   make(map[string]map[string]struct{}),
		zsets:  make(map[string]map[string]float64),
		lists:  make(map[string][][]byte),
	}
}

func (s *fakeStringStore) EnsureTypeExpiring(ctx context.Context, pk, expected string, cntDelta, nowEpoch int64) (int64, bool, error) {
	c, err := s.EnsureType(ctx, pk, expected, cntDelta)
	return c, false, err
}

func (s *fakeStringStore) EnsureType(_ context.Context, pk, expected string, cntDelta int64) (int64, error) {
	m := s.metas[pk]
	if s.live[pk] && m.Type != expected {
		return 0, storage.ErrWrongType
	}
	m.Type = expected
	m.Count += cntDelta
	s.metas[pk] = m
	s.live[pk] = true
	return m.Count, nil
}

func (s *fakeStringStore) DeleteMetaIfEmpty(_ context.Context, pk string) (bool, error) {
	m := s.metas[pk]
	if !s.live[pk] || m.Count > 0 {
		return false, nil
	}
	existed := s.live[pk]
	delete(s.live, pk)
	delete(s.metas, pk)
	return existed, nil
}

func (s *fakeStringStore) CreateTypeIfAbsent(_ context.Context, pk, expected string, cntDelta, nowEpoch int64) (bool, error) {
	m := s.metas[pk]
	live := s.live[pk] && !(m.Exp > 0 && m.Exp <= nowEpoch)
	if live {
		return false, nil
	}
	// Claim: reset the meta (count assigned, not added; expiry cleared).
	s.metas[pk] = storage.Meta{Type: expected, Count: cntDelta}
	s.live[pk] = true
	return true, nil
}

func (s *fakeStringStore) LoadMeta(_ context.Context, pk string) (storage.Meta, bool, error) {
	if !s.live[pk] {
		return storage.Meta{}, false, nil
	}
	return s.metas[pk], true, nil
}

func (s *fakeStringStore) SetExpire(_ context.Context, pk string, expEpoch int64) (bool, error) {
	if !s.live[pk] {
		return false, nil
	}
	m := s.metas[pk]
	m.Exp = expEpoch
	s.metas[pk] = m
	return true, nil
}

func (s *fakeStringStore) Persist(_ context.Context, pk string) (bool, error) {
	if !s.live[pk] {
		return false, nil
	}
	m := s.metas[pk]
	m.Exp = 0
	s.metas[pk] = m
	return true, nil
}

func (s *fakeStringStore) DeleteMeta(_ context.Context, pk string) (bool, error) {
	existed := s.live[pk]
	delete(s.live, pk)
	delete(s.metas, pk)
	return existed, nil
}

func (s *fakeStringStore) DeleteMembers(_ context.Context, pk string) (int, error) {
	// Reclaim every data-member item under pk regardless of type (the String
	// value item and any Hash/Set/SortedSet/List members), mirroring the
	// redimo-backed store's "delete everything except the meta item" contract.
	// This is what lets the *STORE commands replace a destination of any prior
	// type before writing the fresh result set.
	n := 0
	if s.has[pk] {
		n++
		delete(s.has, pk)
		delete(s.data, pk)
	}
	n += len(s.hashes[pk])
	delete(s.hashes, pk)
	n += len(s.sets[pk])
	delete(s.sets, pk)
	n += len(s.zsets[pk])
	delete(s.zsets, pk)
	n += len(s.lists[pk])
	delete(s.lists, pk)
	return n, nil
}

func (s *fakeStringStore) SweepOrphans(_ context.Context) (int, error) { return 0, nil }

func (s *fakeStringStore) GetString(_ context.Context, pk string) ([]byte, bool, error) {
	if !s.has[pk] {
		return nil, false, nil
	}
	return s.data[pk], true, nil
}

func (s *fakeStringStore) MGetStrings(_ context.Context, pks []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(pks))
	for _, pk := range pks {
		if s.has[pk] {
			out[pk] = s.data[pk]
		}
	}
	return out, nil
}

func (s *fakeStringStore) SetString(_ context.Context, pk string, val []byte) error {
	s.data[pk] = val
	s.has[pk] = true
	return nil
}

func (s *fakeStringStore) GetSetString(_ context.Context, pk string, val []byte) ([]byte, bool, error) {
	old, existed := s.data[pk], s.has[pk]
	s.data[pk] = val
	s.has[pk] = true
	return old, existed, nil
}

// SetStringIfEquals models the redimo-backed store's compare-and-set: the write
// lands only if the value item is still exactly as the caller last read it
// (oldExists must match presence, and when present the bytes must be equal),
// otherwise it reports ok=false with no write so the command layer retries.
func (s *fakeStringStore) SetStringIfEquals(_ context.Context, pk string, newVal, oldVal []byte, oldExists bool) (bool, error) {
	curVal, curHas := s.data[pk], s.has[pk]
	if curHas != oldExists {
		return false, nil
	}
	if oldExists && !bytesEqual(curVal, oldVal) {
		return false, nil
	}

	s.data[pk] = newVal
	s.has[pk] = true

	return true, nil
}

// bytesEqual reports whether two byte slices hold the same bytes (a local helper
// so the fake avoids pulling in the bytes package).
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// HScan pages the in-memory hash model for pk, mirroring the redimo-backed store's
// partition Query so the HSCAN handler can be exercised end-to-end. Fields are
// iterated in a stable (sorted) order and the pagination token is an index-based
// LEK ({"i": <next offset>}): a nil lek starts at offset 0, limit bounds the page
// size, and a non-nil nextLEK carries the offset to resume from until the hash is
// exhausted (then nextLEK is nil and the handler reports the terminating cursor 0).
func (s *fakeStringStore) HScan(_ context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]storage.HField, map[string]types.AttributeValue, error) {
	h := s.hashes[pk]
	names := make([]string, 0, len(h))
	for f := range h {
		names = append(names, f)
	}
	sort.Strings(names)

	start := 0
	if lek != nil {
		if av, ok := lek["i"].(*types.AttributeValueMemberN); ok {
			if n, err := strconv.Atoi(av.Value); err == nil {
				start = n
			}
		}
	}
	if start > len(names) {
		start = len(names)
	}

	end := len(names)
	if limit > 0 && start+int(limit) < end {
		end = start + int(limit)
	}

	out := make([]storage.HField, 0, end-start)
	for _, f := range names[start:end] {
		out = append(out, storage.HField{Field: f, Value: h[f]})
	}

	var nextLEK map[string]types.AttributeValue
	if end < len(names) {
		nextLEK = map[string]types.AttributeValue{"i": &types.AttributeValueMemberN{Value: strconv.Itoa(end)}}
	}

	return out, nextLEK, nil
}

var _ storage.Store = (*fakeStringStore)(nil)

// IncrBy mirrors the redimo-backed store's read-modify-write reconciliation:
// parse the current value bytes as a Redis integer, apply the delta, and store
// the decimal result back as the same bytes GetString reads. A non-integer target
// or an out-of-range result returns the storage sentinel the command layer maps
// to the not-an-integer / overflow reply.
func (s *fakeStringStore) IncrBy(_ context.Context, pk string, delta int64) (int64, error) {
	var cur int64
	if s.has[pk] {
		v, err := strconv.ParseInt(string(s.data[pk]), 10, 64)
		if err != nil {
			return 0, storage.ErrNotInteger
		}
		cur = v
	}
	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, storage.ErrIncrOverflow
	}
	next := cur + delta
	s.data[pk] = []byte(strconv.FormatInt(next, 10))
	s.has[pk] = true
	return next, nil
}

// IncrByFloat mirrors the redimo-backed store's float read-modify-write: parse,
// add, format Redis-style, store back as the same bytes.
func (s *fakeStringStore) IncrByFloat(_ context.Context, pk string, delta float64) ([]byte, error) {
	var cur float64
	if s.has[pk] {
		v, err := strconv.ParseFloat(string(s.data[pk]), 64)
		if err != nil || math.IsNaN(v) {
			return nil, storage.ErrNotFloat
		}
		cur = v
	}
	next := cur + delta
	if math.IsNaN(next) || math.IsInf(next, 0) {
		return nil, storage.ErrIncrNaNOrInfinity
	}
	out := []byte(strconv.FormatFloat(next, 'f', -1, 64))
	s.data[pk] = out
	s.has[pk] = true
	return out, nil
}

// startStringServer boots an in-process server whose router is wired to the given
// fake store and clock, and returns a connected client. The clock lets expiry-
// sensitive tests pin "now".
func startStringServer(t *testing.T, store storage.Store, now func() int64) (net.Conn, *bufio.Reader) {
	t.Helper()
	r := NewRouterWithStorage(Config{MultiDB: true}, Storage{Store: store, Now: now})
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

// fixedNow returns a clock pinned to n epoch seconds.
func fixedNow(n int64) func() int64 { return func() int64 { return n } }

// --- GET / SET (requirement 5.1, 5.2) ---------------------------------------

func TestSetThenGet(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	if got, want := sendRead(t, conn, r, "SET k v1"), "+OK"; got != want {
		t.Errorf("SET k v1 = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$v1"; got != want {
		t.Errorf("GET k = %q, want %q", got, want)
	}
}

func TestGetMissingIsNullBulk(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "GET absent"), "$-1"; got != want {
		t.Errorf("GET absent = %q, want %q (null bulk)", got, want)
	}
}

func TestSetOverwrites(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k first")
	if got, want := sendRead(t, conn, r, "SET k second"), "+OK"; got != want {
		t.Errorf("SET k second = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$second"; got != want {
		t.Errorf("GET k = %q, want %q", got, want)
	}
}

// --- SET EX/PX (requirement 5.2) --------------------------------------------

func TestSetEXWritesExpiryInSeconds(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))

	if got, want := sendRead(t, conn, r, "SET k v EX 60"), "+OK"; got != want {
		t.Errorf("SET k v EX 60 = %q, want %q", got, want)
	}
	if got, want := store.metas["0:k"].Exp, int64(1060); got != want {
		t.Errorf("meta.exp = %d, want %d (now+60)", got, want)
	}
}

func TestSetPXTruncatesToSeconds(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))

	// 65500ms after now=1000s => absolute 1000*1000+65500 = 1065500ms => 1065s.
	if got, want := sendRead(t, conn, r, "SET k v PX 65500"), "+OK"; got != want {
		t.Errorf("SET k v PX 65500 = %q, want %q", got, want)
	}
	if got, want := store.metas["0:k"].Exp, int64(1065); got != want {
		t.Errorf("meta.exp = %d, want %d (second-truncated)", got, want)
	}
}

func TestSetClearsExistingTTL(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))

	sendRead(t, conn, r, "SET k v EX 60")
	if store.metas["0:k"].Exp == 0 {
		t.Fatalf("precondition: expected a TTL to be set")
	}
	// A plain SET must clear the TTL (Redis/Pika semantics).
	sendRead(t, conn, r, "SET k v2")
	if got := store.metas["0:k"].Exp; got != 0 {
		t.Errorf("meta.exp after plain SET = %d, want 0 (TTL cleared)", got)
	}
}

func TestSetInvalidExpireTime(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR invalid expire time in set"
	if got := sendRead(t, conn, r, "SET k v EX 0"); got != want {
		t.Errorf("SET k v EX 0 = %q, want %q", got, want)
	}
}

func TestSetNonIntegerExpire(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR value is not an integer or out of range"
	if got := sendRead(t, conn, r, "SET k v EX abc"); got != want {
		t.Errorf("SET k v EX abc = %q, want %q", got, want)
	}
}

func TestSetSyntaxErrors(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR syntax error"
	for _, cmd := range []string{"SET k v NX XX", "SET k v EX", "SET k v FOO", "SET k v EX 10 PX 10"} {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// --- SET NX / XX (requirement 5.3, 5.4) -------------------------------------

func TestSetNXRejectsExisting(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k v1")
	// NX on an existing key: no write, null bulk (requirement 5.3).
	if got, want := sendRead(t, conn, r, "SET k v2 NX"), "$-1"; got != want {
		t.Errorf("SET k v2 NX = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$v1"; got != want {
		t.Errorf("GET k after NX reject = %q, want %q (unchanged)", got, want)
	}
}

func TestSetNXCreatesWhenAbsent(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "SET k v NX"), "+OK"; got != want {
		t.Errorf("SET k v NX (absent) = %q, want %q", got, want)
	}
}

func TestSetXXRejectsAbsent(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// XX on an absent key: no write, null bulk (requirement 5.4).
	if got, want := sendRead(t, conn, r, "SET k v XX"), "$-1"; got != want {
		t.Errorf("SET k v XX (absent) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$-1"; got != want {
		t.Errorf("GET k after XX reject = %q, want %q (not created)", got, want)
	}
}

func TestSetXXUpdatesExisting(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k v1")
	if got, want := sendRead(t, conn, r, "SET k v2 XX"), "+OK"; got != want {
		t.Errorf("SET k v2 XX (existing) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$v2"; got != want {
		t.Errorf("GET k = %q, want %q", got, want)
	}
}

func TestSetNXOnExpiredKeySucceeds(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	// Seed an expired string key (exp in the past).
	sendRead(t, conn, r, "SET k old EX 1") // exp = 1001
	// Advance the clock past expiry by rebuilding the server on a later clock is
	// overkill; instead seed exp directly to a past value.
	m := store.metas["0:k"]
	m.Exp = 500 // already in the past relative to now=1000
	store.metas["0:k"] = m

	// NX should treat the expired key as absent and succeed.
	if got, want := sendRead(t, conn, r, "SET k new NX"), "+OK"; got != want {
		t.Errorf("SET k new NX (expired) = %q, want %q", got, want)
	}
}

// --- SETNX (requirement 5.5) ------------------------------------------------

func TestSetNXCommand(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "SETNX k v1"), ":1"; got != want {
		t.Errorf("SETNX k v1 (absent) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SETNX k v2"), ":0"; got != want {
		t.Errorf("SETNX k v2 (existing) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$v1"; got != want {
		t.Errorf("GET k = %q, want %q (unchanged by rejected SETNX)", got, want)
	}
}

// --- SETEX / PSETEX (requirement 5.5) ---------------------------------------

func TestSetEX(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	if got, want := sendRead(t, conn, r, "SETEX k 60 v"), "+OK"; got != want {
		t.Errorf("SETEX k 60 v = %q, want %q", got, want)
	}
	if got, want := store.metas["0:k"].Exp, int64(1060); got != want {
		t.Errorf("meta.exp = %d, want %d", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$v"; got != want {
		t.Errorf("GET k = %q, want %q", got, want)
	}
}

func TestSetEXInvalidExpire(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR invalid expire time in setex"
	if got := sendRead(t, conn, r, "SETEX k 0 v"); got != want {
		t.Errorf("SETEX k 0 v = %q, want %q", got, want)
	}
}

func TestPSetEX(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	// 60000ms => 60s => exp 1060.
	if got, want := sendRead(t, conn, r, "PSETEX k 60000 v"), "+OK"; got != want {
		t.Errorf("PSETEX k 60000 v = %q, want %q", got, want)
	}
	if got, want := store.metas["0:k"].Exp, int64(1060); got != want {
		t.Errorf("meta.exp = %d, want %d", got, want)
	}
}

func TestPSetEXInvalidExpire(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR invalid expire time in psetex"
	if got := sendRead(t, conn, r, "PSETEX k -5 v"); got != want {
		t.Errorf("PSETEX k -5 v = %q, want %q", got, want)
	}
}

// --- GETSET (requirement 5.5) -----------------------------------------------

func TestGetSetReturnsOldValue(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k old")
	if got, want := sendRead(t, conn, r, "GETSET k new"), "$old"; got != want {
		t.Errorf("GETSET k new = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$new"; got != want {
		t.Errorf("GET k = %q, want %q", got, want)
	}
}

func TestGetSetMissingReturnsNullBulk(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "GETSET k v"), "$-1"; got != want {
		t.Errorf("GETSET k v (absent) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$v"; got != want {
		t.Errorf("GET k = %q, want %q (set by GETSET)", got, want)
	}
}

// --- WRONGTYPE type-check on writes (requirement 3.6) -----------------------

// TestSetOverwritesAnyType pins the Redis semantics that plain SET / SET XX /
// SETEX / PSETEX are destructive, type-agnostic writes: they overwrite a key of
// ANY type and reply "+OK". GETSET, which reads the previous value as a string,
// still returns WRONGTYPE on a non-string key.
func TestSetOverwritesAnyType(t *testing.T) {
	newHashKey := func() *fakeStringStore {
		s := newFakeStringStore()
		s.metas["0:k"] = storage.Meta{Type: string(meta.TypeHash)}
		s.live["0:k"] = true
		return s
	}

	// Plain SET overwrites the hash and the key becomes a string.
	conn, r := startStringServer(t, newHashKey(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "SET k v"), "+OK"; got != want {
		t.Errorf("SET k v (hash key) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$v"; got != want {
		t.Errorf("GET k after SET overwrite = %q, want %q", got, want)
	}

	// SET XX overwrites (XX passes because the key exists, regardless of type).
	conn, r = startStringServer(t, newHashKey(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "SET k v XX"), "+OK"; got != want {
		t.Errorf("SET k v XX (hash key) = %q, want %q", got, want)
	}

	// SETEX overwrites too.
	conn, r = startStringServer(t, newHashKey(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "SETEX k 100 v"), "+OK"; got != want {
		t.Errorf("SETEX k 100 v (hash key) = %q, want %q", got, want)
	}

	// GETSET on a non-string key is still WRONGTYPE.
	conn, r = startStringServer(t, newHashKey(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "GETSET k v"),
		"-WRONGTYPE Operation against a key holding the wrong kind of value"; got != want {
		t.Errorf("GETSET k v (hash key) = %q, want %q", got, want)
	}
}

func TestSetNXOnWrongTypeIsRejectionNotWrongType(t *testing.T) {
	store := newFakeStringStore()
	store.metas["0:k"] = storage.Meta{Type: string(meta.TypeHash)}
	store.live["0:k"] = true

	conn, r := startStringServer(t, store, fixedNow(1000))
	// SET NX / SETNX on an existing key of any type is a rejection, not WRONGTYPE.
	if got, want := sendRead(t, conn, r, "SET k v NX"), "$-1"; got != want {
		t.Errorf("SET k v NX (hash key) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "SETNX k v"), ":0"; got != want {
		t.Errorf("SETNX k (hash key) = %q, want %q", got, want)
	}
}

// --- arity (requirement 3.2) ------------------------------------------------

func TestStringArityErrors(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	cases := map[string]string{
		"GET":        "-ERR wrong number of arguments for 'get' command",
		"SET k":      "-ERR wrong number of arguments for 'set' command",
		"SETNX k":    "-ERR wrong number of arguments for 'setnx' command",
		"SETEX k 1":  "-ERR wrong number of arguments for 'setex' command",
		"PSETEX k 1": "-ERR wrong number of arguments for 'psetex' command",
		"GETSET k":   "-ERR wrong number of arguments for 'getset' command",
	}
	for cmd, want := range cases {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// TestEncodePK documents the mode-aware pk encoding used by every data-command
// family: single-db (raw keys) vs multi-db (uniform "{db}:" prefix).
func TestEncodePK(t *testing.T) {
	// Multi-DB mode: pk = "{db}:{key}".
	rm := &Router{Config: Config{MultiDB: true}}
	if got, want := rm.encodePK(0, []byte("foo")), "0:foo"; got != want {
		t.Errorf("multi-db encodePK(0, foo) = %q, want %q", got, want)
	}
	if got, want := rm.encodePK(3, []byte("foo")), "3:foo"; got != want {
		t.Errorf("multi-db encodePK(3, foo) = %q, want %q", got, want)
	}

	// Single-DB mode: pk = raw key, no prefix; the db argument is ignored (every DB
	// aliases to the one shared keyspace).
	rs := &Router{Config: Config{MultiDB: false}}
	if got, want := rs.encodePK(0, []byte("foo")), "foo"; got != want {
		t.Errorf("single-db encodePK(0, foo) = %q, want %q", got, want)
	}
	if got, want := rs.encodePK(3, []byte("foo")), "foo"; got != want {
		t.Errorf("single-db encodePK(3, foo) = %q, want %q (db ignored)", got, want)
	}
}

// TestConnectionOnlyRouterHasNoStringCommands verifies NewRouter(cfg) stays
// connection-only so the existing handshake tests are unaffected by the storage
// wiring.
func TestConnectionOnlyRouterHasNoStringCommands(t *testing.T) {
	r := NewRouter(Config{})
	if _, ok := r.Table.Lookup("GET"); ok {
		t.Error("connection-only router should not register GET")
	}
}

// readArray reads a single RESP2 array reply and renders each element the same
// way readReply renders scalars (e.g. "$foo", "$-1"). It handles the bulk-string
// and null-bulk elements MGET produces.
func readArray(t *testing.T, r *bufio.Reader) []string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read array header: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 || line[0] != '*' {
		t.Fatalf("expected array header, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		t.Fatalf("bad array header %q: %v", line, err)
	}
	if n < 0 {
		return nil // null array "*-1"
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = readReply(t, r)
	}
	return out
}

// --- MGET (requirement 5.6) --------------------------------------------------

func TestMGetReturnsValuesInRequestOrder(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET a 1")
	sendRead(t, conn, r, "SET b 2")
	sendRead(t, conn, r, "SET c 3")

	// Request a permuted order; the reply must follow the request order.
	send(t, conn, "MGET c a b")
	got := readArray(t, r)
	want := []string{"$3", "$1", "$2"}
	if len(got) != len(want) {
		t.Fatalf("MGET returned %d elems, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("MGET[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestMGetMissingKeyIsNullBulk(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET a hello")

	send(t, conn, "MGET a absent")
	got := readArray(t, r)
	want := []string{"$hello", "$-1"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("MGET[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestMGetWrongTypeIsNullBulk(t *testing.T) {
	store := newFakeStringStore()
	// Seed a non-string (hash) key: live meta with a non-str type.
	store.metas["0:h"] = storage.Meta{Type: string(meta.TypeHash)}
	store.live["0:h"] = true

	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "SET s value")

	send(t, conn, "MGET s h")
	got := readArray(t, r)
	// The hash key must surface as null bulk, not any value.
	want := []string{"$value", "$-1"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("MGET[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestMGetExpiredKeyIsNullBulk(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "SET a v")
	// Make the key expired relative to now=1000.
	m := store.metas["0:a"]
	m.Exp = 500
	store.metas["0:a"] = m

	send(t, conn, "MGET a")
	got := readArray(t, r)
	if len(got) != 1 || got[0] != "$-1" {
		t.Errorf("MGET a (expired) = %v, want [$-1]", got)
	}
}

func TestMGetSingleKey(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k v")
	send(t, conn, "MGET k")
	got := readArray(t, r)
	if len(got) != 1 || got[0] != "$v" {
		t.Errorf("MGET k = %v, want [$v]", got)
	}
}

func TestMGetArity(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// MGET with no keys is an arity error (arity -2 requires >=2 args).
	want := "-ERR wrong number of arguments for 'mget' command"
	if got := sendRead(t, conn, r, "MGET"); got != want {
		t.Errorf("MGET = %q, want %q", got, want)
	}
}

// --- MSET (requirement 5.7) --------------------------------------------------

func TestMSetMultiplePairs(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "MSET a 1 b 2 c 3"), "+OK"; got != want {
		t.Errorf("MSET = %q, want %q", got, want)
	}
	for k, want := range map[string]string{"GET a": "$1", "GET b": "$2", "GET c": "$3"} {
		if got := sendRead(t, conn, r, k); got != want {
			t.Errorf("%q = %q, want %q", k, got, want)
		}
	}
}

func TestMSetOddArgsError(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// When the arity is satisfied but the pairs do not match up (an ODD count), Redis 3.2's
	// msetGenericCommand replies this exact literal — uppercase "MSET", unquoted, no
	// " command" suffix — for BOTH MSET and MSETNX (they share the function). This is
	// distinct from the too-few-args arity error ("...for 'mset' command"). Verified
	// against the live redis:3.2 oracle.
	want := "-ERR wrong number of arguments for MSET"
	if got := sendRead(t, conn, r, "MSET a 1 b"); got != want {
		t.Errorf("MSET a 1 b = %q, want %q", got, want)
	}
	if got := sendRead(t, conn, r, "MSETNX a 1 b"); got != want {
		t.Errorf("MSETNX a 1 b = %q, want %q (MSETNX reports MSET too)", got, want)
	}
}

func TestMSetClearsTTL(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "SET a v EX 60") // a has a TTL
	if store.metas["0:a"].Exp == 0 {
		t.Fatal("precondition: expected a TTL on key a")
	}
	// MSET overwrites and clears the TTL (like plain SET).
	sendRead(t, conn, r, "MSET a v2")
	if got := store.metas["0:a"].Exp; got != 0 {
		t.Errorf("meta.exp after MSET = %d, want 0 (TTL cleared)", got)
	}
}

// TestMSetBatchingBoundary writes more keys than a single <=25-key batch to
// exercise the batch-splitting loop across the boundary; every key must still be
// written (batches are applied sequentially, not atomically across batches).
func TestMSetBatchingBoundary(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))

	// 26 pairs => spans two batches (25 + 1), crossing the msetBatchKeys boundary.
	const n = 26
	var sb strings.Builder
	sb.WriteString("MSET")
	for i := 0; i < n; i++ {
		sb.WriteString(" k")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(" v")
		sb.WriteString(strconv.Itoa(i))
	}
	if got, want := sendRead(t, conn, r, sb.String()), "+OK"; got != want {
		t.Fatalf("MSET (%d pairs) = %q, want %q", n, got, want)
	}

	for i := 0; i < n; i++ {
		key := "k" + strconv.Itoa(i)
		want := "$v" + strconv.Itoa(i)
		if got := sendRead(t, conn, r, "GET "+key); got != want {
			t.Errorf("GET %s = %q, want %q", key, got, want)
		}
	}
}

func TestMSetOverwritesAnyType(t *testing.T) {
	store := newFakeStringStore()
	// Seed a hash key; MSET is type-agnostic in Redis and overwrites it.
	store.metas["0:h"] = storage.Meta{Type: string(meta.TypeHash)}
	store.live["0:h"] = true

	conn, r := startStringServer(t, store, fixedNow(1000))
	if got, want := sendRead(t, conn, r, "MSET h v"), "+OK"; got != want {
		t.Errorf("MSET h v (hash key) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET h"), "$v"; got != want {
		t.Errorf("GET h after MSET overwrite = %q, want %q", got, want)
	}
}

func TestMSetArity(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// MSET with fewer than 3 args is an arity error (arity -3).
	want := "-ERR wrong number of arguments for 'mset' command"
	if got := sendRead(t, conn, r, "MSET k"); got != want {
		t.Errorf("MSET k = %q, want %q", got, want)
	}
}

// --- INCR / DECR / INCRBY / DECRBY / INCRBYFLOAT (requirements 5.8, 5.9) -----

func TestIncrCreatesKeyFromZero(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// INCR on a missing key initialises it to 0 then increments to 1.
	if got, want := sendRead(t, conn, r, "INCR n"), ":1"; got != want {
		t.Errorf("INCR n (absent) = %q, want %q", got, want)
	}
	// The value must read back as the decimal string "1" (encoding reconciliation).
	if got, want := sendRead(t, conn, r, "GET n"), "$1"; got != want {
		t.Errorf("GET n after INCR = %q, want %q", got, want)
	}
}

func TestIncrOnExistingSetValue(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// SET a value that parses as an integer, then INCR/GET must reconcile.
	sendRead(t, conn, r, "SET k 5")
	if got, want := sendRead(t, conn, r, "INCR k"), ":6"; got != want {
		t.Errorf("INCR k (=5) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$6"; got != want {
		t.Errorf("GET k after INCR = %q, want %q", got, want)
	}
}

func TestDecr(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k 10")
	if got, want := sendRead(t, conn, r, "DECR k"), ":9"; got != want {
		t.Errorf("DECR k (=10) = %q, want %q", got, want)
	}
	// DECR below zero yields a negative value.
	sendRead(t, conn, r, "SET z 0")
	if got, want := sendRead(t, conn, r, "DECR z"), ":-1"; got != want {
		t.Errorf("DECR z (=0) = %q, want %q", got, want)
	}
}

func TestIncrBy(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "INCRBY k 7"), ":7"; got != want {
		t.Errorf("INCRBY k 7 (absent) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "INCRBY k 3"), ":10"; got != want {
		t.Errorf("INCRBY k 3 (=7) = %q, want %q", got, want)
	}
	// A negative amount decrements.
	if got, want := sendRead(t, conn, r, "INCRBY k -4"), ":6"; got != want {
		t.Errorf("INCRBY k -4 (=10) = %q, want %q", got, want)
	}
}

func TestDecrBy(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k 20")
	if got, want := sendRead(t, conn, r, "DECRBY k 5"), ":15"; got != want {
		t.Errorf("DECRBY k 5 (=20) = %q, want %q", got, want)
	}
}

func TestIncrByNonIntegerAmount(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR value is not an integer or out of range"
	if got := sendRead(t, conn, r, "INCRBY k abc"); got != want {
		t.Errorf("INCRBY k abc = %q, want %q", got, want)
	}
	if got := sendRead(t, conn, r, "DECRBY k 1.5"); got != want {
		t.Errorf("DECRBY k 1.5 = %q, want %q", got, want)
	}
}

func TestIncrNonIntegerTarget(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// A string value that does not parse as an integer errors on INCR.
	sendRead(t, conn, r, "SET k hello")
	want := "-ERR value is not an integer or out of range"
	if got := sendRead(t, conn, r, "INCR k"); got != want {
		t.Errorf("INCR k (=hello) = %q, want %q", got, want)
	}
	// The value must be left unchanged by the failed INCR.
	if got, want := sendRead(t, conn, r, "GET k"), "$hello"; got != want {
		t.Errorf("GET k after failed INCR = %q, want %q", got, want)
	}
}

func TestDecrByMostNegativeIsValueBased(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// Redis 3.2 negates the decrement in C, where -MinInt64 wraps back to MinInt64, so
	// DECRBY by the most-negative int64 is INCRBY by it — decided by the current value,
	// not rejected outright. A missing key starts at 0, so the result is MinInt64.
	if got, want := sendRead(t, conn, r, "DECRBY k -9223372036854775808"), ":-9223372036854775808"; got != want {
		t.Errorf("DECRBY fresh k MinInt64 = %q, want %q", got, want)
	}
	// From a negative value it genuinely underflows and replies the shared overflow error.
	sendRead(t, conn, r, "SET n -1")
	if got, want := sendRead(t, conn, r, "DECRBY n -9223372036854775808"), "-ERR increment or decrement would overflow"; got != want {
		t.Errorf("DECRBY -1 MinInt64 = %q, want %q", got, want)
	}
}

func TestIncrWrongType(t *testing.T) {
	store := newFakeStringStore()
	store.metas["0:h"] = storage.Meta{Type: string(meta.TypeHash)}
	store.live["0:h"] = true

	conn, r := startStringServer(t, store, fixedNow(1000))
	want := "-WRONGTYPE Operation against a key holding the wrong kind of value"
	if got := sendRead(t, conn, r, "INCR h"); got != want {
		t.Errorf("INCR h (hash key) = %q, want %q", got, want)
	}
	if got := sendRead(t, conn, r, "INCRBYFLOAT h 1.0"); got != want {
		t.Errorf("INCRBYFLOAT h 1.0 (hash key) = %q, want %q", got, want)
	}
}

func TestIncrByFloat(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// A missing key starts at 0; the reply is a bulk string, trailing zeros trimmed.
	if got, want := sendRead(t, conn, r, "INCRBYFLOAT k 3.14"), "$3.14"; got != want {
		t.Errorf("INCRBYFLOAT k 3.14 (absent) = %q, want %q", got, want)
	}
	// 3.14 + 1.86 = 5, formatted as "5" (no trailing ".0").
	if got, want := sendRead(t, conn, r, "INCRBYFLOAT k 1.86"), "$5"; got != want {
		t.Errorf("INCRBYFLOAT k 1.86 (=3.14) = %q, want %q", got, want)
	}
	// The stored value reads back verbatim through GET.
	if got, want := sendRead(t, conn, r, "GET k"), "$5"; got != want {
		t.Errorf("GET k after INCRBYFLOAT = %q, want %q", got, want)
	}
}

func TestIncrByFloatOnIntegerSetValue(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k 10")
	if got, want := sendRead(t, conn, r, "INCRBYFLOAT k 0.1"), "$10.1"; got != want {
		t.Errorf("INCRBYFLOAT k 0.1 (=10) = %q, want %q", got, want)
	}
}

func TestIncrByFloatNonFloatAmount(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR value is not a valid float"
	if got := sendRead(t, conn, r, "INCRBYFLOAT k notafloat"); got != want {
		t.Errorf("INCRBYFLOAT k notafloat = %q, want %q", got, want)
	}
}

func TestIncrByFloatNonFloatTarget(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k hello")
	want := "-ERR value is not a valid float"
	if got := sendRead(t, conn, r, "INCRBYFLOAT k 1.0"); got != want {
		t.Errorf("INCRBYFLOAT k 1.0 (=hello) = %q, want %q", got, want)
	}
}

func TestIncrFamilyArity(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	cases := map[string]string{
		"INCR":          "-ERR wrong number of arguments for 'incr' command",
		"DECR":          "-ERR wrong number of arguments for 'decr' command",
		"INCRBY k":      "-ERR wrong number of arguments for 'incrby' command",
		"DECRBY k":      "-ERR wrong number of arguments for 'decrby' command",
		"INCRBYFLOAT k": "-ERR wrong number of arguments for 'incrbyfloat' command",
	}
	for cmd, want := range cases {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// --- APPEND (requirements 5.10, 16.4) ---------------------------------------

func TestAppendCreatesKeyWhenAbsent(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// APPEND on a missing key creates it with the value; reply is the new length.
	if got, want := sendRead(t, conn, r, "APPEND k hello"), ":5"; got != want {
		t.Errorf("APPEND k hello (absent) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$hello"; got != want {
		t.Errorf("GET k after APPEND = %q, want %q", got, want)
	}
}

func TestAppendToExistingConcatenates(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k hello")
	// APPEND returns the new total length (5 + 6 = 11).
	if got, want := sendRead(t, conn, r, "APPEND k _world"), ":11"; got != want {
		t.Errorf("APPEND k _world (=hello) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$hello_world"; got != want {
		t.Errorf("GET k after APPEND = %q, want %q", got, want)
	}
}

func TestAppendPreservesTTL(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "SET k hi EX 60") // exp = 1060
	if got := store.metas["0:k"].Exp; got != 1060 {
		t.Fatalf("precondition: meta.exp = %d, want 1060", got)
	}
	// APPEND must not clear the existing TTL (Redis semantics).
	sendRead(t, conn, r, "APPEND k there")
	if got := store.metas["0:k"].Exp; got != 1060 {
		t.Errorf("meta.exp after APPEND = %d, want 1060 (TTL preserved)", got)
	}
}

func TestAppendOnExpiredKeyStartsFresh(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "SET k stale")
	// Force the key expired relative to now=1000.
	m := store.metas["0:k"]
	m.Exp = 500
	store.metas["0:k"] = m

	// The expired value must not contribute to the append base.
	if got, want := sendRead(t, conn, r, "APPEND k new"), ":3"; got != want {
		t.Errorf("APPEND k new (expired) = %q, want %q", got, want)
	}
}

func TestAppendWrongType(t *testing.T) {
	store := newFakeStringStore()
	store.metas["0:h"] = storage.Meta{Type: string(meta.TypeHash)}
	store.live["0:h"] = true

	conn, r := startStringServer(t, store, fixedNow(1000))
	want := "-WRONGTYPE Operation against a key holding the wrong kind of value"
	if got := sendRead(t, conn, r, "APPEND h v"); got != want {
		t.Errorf("APPEND h v (hash key) = %q, want %q", got, want)
	}
}

func TestAppendArity(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR wrong number of arguments for 'append' command"
	if got := sendRead(t, conn, r, "APPEND k"); got != want {
		t.Errorf("APPEND k = %q, want %q", got, want)
	}
}

// --- STRLEN (requirement 5.11) ----------------------------------------------

func TestStrlenPresent(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k hello")
	if got, want := sendRead(t, conn, r, "STRLEN k"), ":5"; got != want {
		t.Errorf("STRLEN k (=hello) = %q, want %q", got, want)
	}
}

func TestStrlenAbsentIsZero(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "STRLEN absent"), ":0"; got != want {
		t.Errorf("STRLEN absent = %q, want %q", got, want)
	}
}

func TestStrlenExpiredIsZero(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "SET k hello")
	m := store.metas["0:k"]
	m.Exp = 500 // expired relative to now=1000
	store.metas["0:k"] = m
	if got, want := sendRead(t, conn, r, "STRLEN k"), ":0"; got != want {
		t.Errorf("STRLEN k (expired) = %q, want %q", got, want)
	}
}

func TestStrlenWrongType(t *testing.T) {
	store := newFakeStringStore()
	store.metas["0:h"] = storage.Meta{Type: string(meta.TypeHash)}
	store.live["0:h"] = true

	conn, r := startStringServer(t, store, fixedNow(1000))
	want := "-WRONGTYPE Operation against a key holding the wrong kind of value"
	if got := sendRead(t, conn, r, "STRLEN h"); got != want {
		t.Errorf("STRLEN h (hash key) = %q, want %q", got, want)
	}
}

func TestStrlenArity(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR wrong number of arguments for 'strlen' command"
	if got := sendRead(t, conn, r, "STRLEN"); got != want {
		t.Errorf("STRLEN = %q, want %q", got, want)
	}
}

// --- SETRANGE (requirements 5.10, 16.4) -------------------------------------

func TestSetRangeExtendsExisting(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k Hello_World")
	// Overwrite starting at offset 6: "Hello_World" -> "Hello_Redis"; length 11.
	if got, want := sendRead(t, conn, r, "SETRANGE k 6 Redis"), ":11"; got != want {
		t.Errorf("SETRANGE k 6 Redis = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "GET k"), "$Hello_Redis"; got != want {
		t.Errorf("GET k after SETRANGE = %q, want %q", got, want)
	}
}

func TestSetRangeZeroPadsWhenOffsetBeyondEnd(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	// SETRANGE on an absent key at offset 5 zero-pads [0,5) then writes value.
	if got, want := sendRead(t, conn, r, "SETRANGE k 5 hello"), ":10"; got != want {
		t.Errorf("SETRANGE k 5 hello (absent) = %q, want %q", got, want)
	}
	// The stored bytes are 5 NULs followed by "hello".
	want := []byte("\x00\x00\x00\x00\x00hello")
	if got := store.data["0:k"]; string(got) != string(want) {
		t.Errorf("stored value = %q, want %q", got, want)
	}
	// GET reads the zero-padded value back verbatim.
	if got, w := sendRead(t, conn, r, "GET k"), "$"+string(want); got != w {
		t.Errorf("GET k = %q, want %q", got, w)
	}
}

func TestSetRangeNegativeOffsetErrors(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR offset is out of range"
	if got := sendRead(t, conn, r, "SETRANGE k -1 v"); got != want {
		t.Errorf("SETRANGE k -1 v = %q, want %q", got, want)
	}
}

func TestSetRangeNonIntegerOffset(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR value is not an integer or out of range"
	if got := sendRead(t, conn, r, "SETRANGE k abc v"); got != want {
		t.Errorf("SETRANGE k abc v = %q, want %q", got, want)
	}
}

func TestSetRangeWrongType(t *testing.T) {
	store := newFakeStringStore()
	store.metas["0:h"] = storage.Meta{Type: string(meta.TypeHash)}
	store.live["0:h"] = true

	conn, r := startStringServer(t, store, fixedNow(1000))
	want := "-WRONGTYPE Operation against a key holding the wrong kind of value"
	if got := sendRead(t, conn, r, "SETRANGE h 0 v"); got != want {
		t.Errorf("SETRANGE h 0 v (hash key) = %q, want %q", got, want)
	}
}

func TestSetRangeArity(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR wrong number of arguments for 'setrange' command"
	if got := sendRead(t, conn, r, "SETRANGE k 0"); got != want {
		t.Errorf("SETRANGE k 0 = %q, want %q", got, want)
	}
}

// --- GETRANGE (requirement 5.11) --------------------------------------------

func TestGetRangePositiveIndices(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k Hello_World")
	// [0,4] -> "Hello".
	if got, want := sendRead(t, conn, r, "GETRANGE k 0 4"), "$Hello"; got != want {
		t.Errorf("GETRANGE k 0 4 = %q, want %q", got, want)
	}
	// [6,10] -> "World".
	if got, want := sendRead(t, conn, r, "GETRANGE k 6 10"), "$World"; got != want {
		t.Errorf("GETRANGE k 6 10 = %q, want %q", got, want)
	}
	// [0,-1] -> whole string.
	if got, want := sendRead(t, conn, r, "GETRANGE k 0 -1"), "$Hello_World"; got != want {
		t.Errorf("GETRANGE k 0 -1 = %q, want %q", got, want)
	}
}

func TestGetRangeNegativeIndices(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k Hello_World")
	// [-5,-1] -> "World".
	if got, want := sendRead(t, conn, r, "GETRANGE k -5 -1"), "$World"; got != want {
		t.Errorf("GETRANGE k -5 -1 = %q, want %q", got, want)
	}
	// A start below the string start clamps to 0.
	if got, want := sendRead(t, conn, r, "GETRANGE k -100 4"), "$Hello"; got != want {
		t.Errorf("GETRANGE k -100 4 = %q, want %q", got, want)
	}
}

func TestGetRangeOutOfRangeIsEmpty(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k hello")
	// start > end after resolution yields the empty string.
	if got, want := sendRead(t, conn, r, "GETRANGE k 10 20"), "$"; got != want {
		t.Errorf("GETRANGE k 10 20 = %q, want %q (empty)", got, want)
	}
	// An end past the string is clamped to the last index.
	if got, want := sendRead(t, conn, r, "GETRANGE k 0 100"), "$hello"; got != want {
		t.Errorf("GETRANGE k 0 100 = %q, want %q", got, want)
	}
}

func TestGetRangeMissingKeyIsEmpty(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// Missing key returns the empty string (not the null bulk).
	if got, want := sendRead(t, conn, r, "GETRANGE absent 0 10"), "$"; got != want {
		t.Errorf("GETRANGE absent 0 10 = %q, want %q (empty string)", got, want)
	}
}

func TestGetRangeNonIntegerIndex(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "SET k hello")
	want := "-ERR value is not an integer or out of range"
	if got := sendRead(t, conn, r, "GETRANGE k x 4"); got != want {
		t.Errorf("GETRANGE k x 4 = %q, want %q", got, want)
	}
	if got := sendRead(t, conn, r, "GETRANGE k 0 y"); got != want {
		t.Errorf("GETRANGE k 0 y = %q, want %q", got, want)
	}
}

func TestGetRangeArity(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR wrong number of arguments for 'getrange' command"
	if got := sendRead(t, conn, r, "GETRANGE k 0"); got != want {
		t.Errorf("GETRANGE k 0 = %q, want %q", got, want)
	}
}

// --- Hash operations on the fake store (task 13.1) --------------------------
//
// The fake models each Hash key as pk -> field -> value, independent of the meta
// item; the command handlers keep metas[pk].Count in step via EnsureType, so
// meta.Count tracks len(hashes[pk]) exactly as the real store does. HGet/HSet use
// binary values verbatim (no reconciliation needed in the fake).

func (s *fakeStringStore) hashMap(pk string) map[string][]byte {
	h := s.hashes[pk]
	if h == nil {
		h = make(map[string][]byte)
		s.hashes[pk] = h
	}
	return h
}

func (s *fakeStringStore) HSet(_ context.Context, pk string, fields []storage.HField) (int, error) {
	h := s.hashMap(pk)
	added := 0
	for _, f := range fields {
		if _, ok := h[f.Field]; !ok {
			added++
		}
		h[f.Field] = f.Value
	}
	return added, nil
}

func (s *fakeStringStore) HSetNX(_ context.Context, pk, field string, val []byte) (bool, error) {
	h := s.hashMap(pk)
	if _, ok := h[field]; ok {
		return false, nil
	}
	h[field] = val
	return true, nil
}

func (s *fakeStringStore) HGet(_ context.Context, pk, field string) ([]byte, bool, error) {
	v, ok := s.hashes[pk][field]
	return v, ok, nil
}

func (s *fakeStringStore) HMGet(_ context.Context, pk string, fields []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(fields))
	h := s.hashes[pk]
	for _, f := range fields {
		if v, ok := h[f]; ok {
			out[f] = v
		}
	}
	return out, nil
}

func (s *fakeStringStore) HGetAll(_ context.Context, pk string) ([]storage.HField, error) {
	h := s.hashes[pk]
	out := make([]storage.HField, 0, len(h))
	for f, v := range h {
		out = append(out, storage.HField{Field: f, Value: v})
	}
	return out, nil
}

func (s *fakeStringStore) HKeys(_ context.Context, pk string) ([]string, error) {
	h := s.hashes[pk]
	out := make([]string, 0, len(h))
	for f := range h {
		out = append(out, f)
	}
	return out, nil
}

func (s *fakeStringStore) HVals(_ context.Context, pk string) ([][]byte, error) {
	h := s.hashes[pk]
	out := make([][]byte, 0, len(h))
	for _, v := range h {
		out = append(out, v)
	}
	return out, nil
}

func (s *fakeStringStore) HDel(_ context.Context, pk string, fields []string) (int, error) {
	h := s.hashes[pk]
	removed := 0
	for _, f := range fields {
		if _, ok := h[f]; ok {
			delete(h, f)
			removed++
		}
	}
	if len(h) == 0 {
		delete(s.hashes, pk)
	}
	return removed, nil
}

func (s *fakeStringStore) HExists(_ context.Context, pk, field string) (bool, error) {
	_, ok := s.hashes[pk][field]
	return ok, nil
}

func (s *fakeStringStore) HStrlen(_ context.Context, pk, field string) (int, error) {
	v, ok := s.hashes[pk][field]
	if !ok {
		return 0, nil
	}
	return len(v), nil
}

func (s *fakeStringStore) HIncrBy(_ context.Context, pk, field string, delta int64) (int64, bool, error) {
	h := s.hashMap(pk)
	v, existed := h[field]
	var cur int64
	if existed {
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, false, storage.ErrHashNotInteger
		}
		cur = n
	}
	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, false, storage.ErrIncrOverflow
	}
	next := cur + delta
	h[field] = []byte(strconv.FormatInt(next, 10))
	return next, !existed, nil
}

func (s *fakeStringStore) HIncrByFloat(_ context.Context, pk, field string, delta float64) ([]byte, bool, error) {
	h := s.hashMap(pk)
	v, existed := h[field]
	var cur float64
	if existed {
		f, err := strconv.ParseFloat(string(v), 64)
		if err != nil || math.IsNaN(f) {
			return nil, false, storage.ErrHashNotFloat
		}
		cur = f
	}
	next := cur + delta
	if math.IsNaN(next) || math.IsInf(next, 0) {
		return nil, false, storage.ErrIncrNaNOrInfinity
	}
	out := []byte(strconv.FormatFloat(next, 'f', -1, 64))
	h[field] = out
	return out, !existed, nil
}

// --- Set data operations (task 14.1) ----------------------------------------
//
// Members are modelled as a per-pk set of strings, independent of the meta item
// (cardinality lives in metas[pk].Count, maintained by the command handlers via
// EnsureType). SPop/SRandMember select in-process, mirroring the redimo-backed
// store's behaviour; SPop removes, SRandMember does not.

func (s *fakeStringStore) setMap(pk string) map[string]struct{} {
	m := s.sets[pk]
	if m == nil {
		m = make(map[string]struct{})
		s.sets[pk] = m
	}
	return m
}

func (s *fakeStringStore) SAdd(_ context.Context, pk string, members []string) (int, error) {
	m := s.setMap(pk)
	added := 0
	for _, member := range members {
		if _, ok := m[member]; !ok {
			m[member] = struct{}{}
			added++
		}
	}
	return added, nil
}

func (s *fakeStringStore) SRem(_ context.Context, pk string, members []string) (int, error) {
	m := s.sets[pk]
	removed := 0
	for _, member := range members {
		if _, ok := m[member]; ok {
			delete(m, member)
			removed++
		}
	}
	return removed, nil
}

func (s *fakeStringStore) SIsMember(_ context.Context, pk, member string) (bool, error) {
	_, ok := s.sets[pk][member]
	return ok, nil
}

func (s *fakeStringStore) SMembers(_ context.Context, pk string) ([]string, error) {
	m := s.sets[pk]
	out := make([]string, 0, len(m))
	for member := range m {
		out = append(out, member)
	}
	return out, nil
}

func (s *fakeStringStore) SPop(ctx context.Context, pk string, count int) ([]string, error) {
	if count <= 0 {
		return nil, nil
	}
	members, _ := s.SMembers(ctx, pk)
	if count > len(members) {
		count = len(members)
	}
	chosen := members[:count]
	m := s.sets[pk]
	for _, member := range chosen {
		delete(m, member)
	}
	return chosen, nil
}

func (s *fakeStringStore) SRandMember(ctx context.Context, pk string, count int) ([]string, error) {
	members, _ := s.SMembers(ctx, pk)
	if len(members) == 0 {
		return nil, nil
	}
	if count < 0 {
		n := -count
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, members[i%len(members)])
		}
		return out, nil
	}
	if count > len(members) {
		count = len(members)
	}
	return members[:count], nil
}

// SScan pages the in-memory set model for pk, mirroring the redimo-backed store's
// partition Query so the SSCAN handler can be exercised end-to-end. Members are
// iterated in a stable (sorted) order and the pagination token is an index-based
// LEK ({"i": <next offset>}): a nil lek starts at offset 0, limit bounds the page
// size, and a non-nil nextLEK carries the offset to resume from until the set is
// exhausted (then nextLEK is nil and the handler reports the terminating cursor 0).
func (s *fakeStringStore) SScan(_ context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]string, map[string]types.AttributeValue, error) {
	set := s.sets[pk]
	names := make([]string, 0, len(set))
	for member := range set {
		names = append(names, member)
	}
	sort.Strings(names)

	start := 0
	if lek != nil {
		if av, ok := lek["i"].(*types.AttributeValueMemberN); ok {
			if n, err := strconv.Atoi(av.Value); err == nil {
				start = n
			}
		}
	}
	if start > len(names) {
		start = len(names)
	}

	end := len(names)
	if limit > 0 && start+int(limit) < end {
		end = start + int(limit)
	}

	out := append([]string(nil), names[start:end]...)

	var nextLEK map[string]types.AttributeValue
	if end < len(names) {
		nextLEK = map[string]types.AttributeValue{"i": &types.AttributeValueMemberN{Value: strconv.Itoa(end)}}
	}

	return out, nextLEK, nil
}

// --- Sorted Set data operations (task 15.1) ---------------------------------
//
// Members are modelled as a per-pk map member->score, independent of the meta
// item (the member count lives in metas[pk].Count, maintained by the command
// handlers via EnsureType). The range/rank reads sort the members with
// storage.SortZMembers and reuse storage.ZReverse / ZNormalizeRankRange /
// ZScoreInRange, the same helpers the redimo-backed store uses, so the fake ranks
// and filters members identically to production.

func (s *fakeStringStore) zsetMap(pk string) map[string]float64 {
	m := s.zsets[pk]
	if m == nil {
		m = make(map[string]float64)
		s.zsets[pk] = m
	}
	return m
}

// zAscending returns the sorted set at pk in ascending score order (ties by
// member), mirroring the store's ordered read.
func (s *fakeStringStore) zAscending(pk string) []storage.ZMember {
	m := s.zsets[pk]
	out := make([]storage.ZMember, 0, len(m))
	for member, score := range m {
		out = append(out, storage.ZMember{Member: member, Score: score})
	}
	storage.SortZMembers(out)
	return out
}

func (s *fakeStringStore) ZAdd(_ context.Context, pk string, members []storage.ZMember) (int, error) {
	m := s.zsetMap(pk)
	added := 0
	for _, zm := range members {
		if _, ok := m[zm.Member]; !ok {
			added++
		}
		m[zm.Member] = zm.Score
	}
	return added, nil
}

func (s *fakeStringStore) ZRem(_ context.Context, pk string, members []string) (int, error) {
	m := s.zsets[pk]
	removed := 0
	for _, member := range members {
		if _, ok := m[member]; ok {
			delete(m, member)
			removed++
		}
	}
	return removed, nil
}

func (s *fakeStringStore) ZScore(_ context.Context, pk, member string) (float64, bool, error) {
	score, ok := s.zsets[pk][member]
	return score, ok, nil
}

func (s *fakeStringStore) ZIncrBy(_ context.Context, pk, member string, delta float64) (float64, bool, error) {
	m := s.zsetMap(pk)
	cur, existed := m[member]
	next := cur + delta
	m[member] = next
	return next, !existed, nil
}

func (s *fakeStringStore) ZRangeByRank(_ context.Context, pk string, start, stop int, rev bool) ([]storage.ZMember, error) {
	ordered := s.zAscending(pk)
	if rev {
		ordered = storage.ZReverse(ordered)
	}
	lo, hi, ok := storage.ZNormalizeRankRange(len(ordered), start, stop)
	if !ok {
		return []storage.ZMember{}, nil
	}
	return append([]storage.ZMember(nil), ordered[lo:hi+1]...), nil
}

func (s *fakeStringStore) ZRangeByScore(_ context.Context, pk string, min, max storage.ScoreBound, rev bool) ([]storage.ZMember, error) {
	asc := s.zAscending(pk)
	filtered := make([]storage.ZMember, 0, len(asc))
	for _, m := range asc {
		if storage.ZScoreInRange(m.Score, min, max) {
			filtered = append(filtered, m)
		}
	}
	if rev {
		filtered = storage.ZReverse(filtered)
	}
	return filtered, nil
}

func (s *fakeStringStore) ZCount(_ context.Context, pk string, min, max storage.ScoreBound) (int, error) {
	count := 0
	for _, m := range s.zAscending(pk) {
		if storage.ZScoreInRange(m.Score, min, max) {
			count++
		}
	}
	return count, nil
}

func (s *fakeStringStore) ZRank(_ context.Context, pk, member string, rev bool) (int, bool, error) {
	asc := s.zAscending(pk)
	for i, m := range asc {
		if m.Member == member {
			if rev {
				return len(asc) - 1 - i, true, nil
			}
			return i, true, nil
		}
	}
	return 0, false, nil
}

func (s *fakeStringStore) ZRemRangeByRank(ctx context.Context, pk string, start, stop int) (int, error) {
	victims, _ := s.ZRangeByRank(ctx, pk, start, stop, false)
	return s.zRemMembers(pk, victims), nil
}

func (s *fakeStringStore) ZRemRangeByScore(ctx context.Context, pk string, min, max storage.ScoreBound) (int, error) {
	victims, _ := s.ZRangeByScore(ctx, pk, min, max, false)
	return s.zRemMembers(pk, victims), nil
}

func (s *fakeStringStore) zRemMembers(pk string, victims []storage.ZMember) int {
	m := s.zsets[pk]
	removed := 0
	for _, v := range victims {
		if _, ok := m[v.Member]; ok {
			delete(m, v.Member)
			removed++
		}
	}
	return removed
}

// ZScan pages the in-memory sorted-set model for pk, mirroring the redimo-backed
// store's single-partition Query so the ZSCAN handler can be exercised end-to-end.
// Members are iterated in a stable (sorted-by-member) order — ZSCAN makes no score
// ordering guarantee — and the pagination token is the same index-based LEK
// ({"i": <next offset>}) the Hash/Set fakes use: a nil lek starts at offset 0,
// limit bounds the page size, and a non-nil nextLEK carries the offset to resume
// from until the set is exhausted (then nextLEK is nil and the handler reports the
// terminating cursor 0). Each returned ZMember pairs the member with its score.
func (s *fakeStringStore) ZScan(_ context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]storage.ZMember, map[string]types.AttributeValue, error) {
	zs := s.zsets[pk]
	names := make([]string, 0, len(zs))
	for member := range zs {
		names = append(names, member)
	}
	sort.Strings(names)

	start := 0
	if lek != nil {
		if av, ok := lek["i"].(*types.AttributeValueMemberN); ok {
			if n, err := strconv.Atoi(av.Value); err == nil {
				start = n
			}
		}
	}
	if start > len(names) {
		start = len(names)
	}

	end := len(names)
	if limit > 0 && start+int(limit) < end {
		end = start + int(limit)
	}

	out := make([]storage.ZMember, 0, end-start)
	for _, m := range names[start:end] {
		out = append(out, storage.ZMember{Member: m, Score: zs[m]})
	}

	var nextLEK map[string]types.AttributeValue
	if end < len(names) {
		nextLEK = map[string]types.AttributeValue{"i": &types.AttributeValueMemberN{Value: strconv.Itoa(end)}}
	}

	return out, nextLEK, nil
}

// --- List data operations (task 16.1) --------------------------------------
//
// Elements are modelled as an ordered slice with the head at index 0, mirroring
// the redimo-backed store: LPush prepends in argument order (so LPUSH a b c ends
// head-to-tail as c, b, a), RPush appends, LPop/RPop take from the head/tail, and
// LRange/LIndex apply Redis' negative-index semantics via the same
// storage.ZNormalizeRankRange helper the redimo-backed store uses.

func (s *fakeStringStore) LPush(_ context.Context, pk string, elements [][]byte) (int, error) {
	l := s.lists[pk]
	for _, e := range elements {
		l = append([][]byte{append([]byte(nil), e...)}, l...)
	}
	s.lists[pk] = l
	return len(elements), nil
}

func (s *fakeStringStore) RPush(_ context.Context, pk string, elements [][]byte) (int, error) {
	l := s.lists[pk]
	for _, e := range elements {
		l = append(l, append([]byte(nil), e...))
	}
	s.lists[pk] = l
	return len(elements), nil
}

func (s *fakeStringStore) LPop(_ context.Context, pk string) ([]byte, bool, error) {
	l := s.lists[pk]
	if len(l) == 0 {
		return nil, false, nil
	}
	v := l[0]
	s.lists[pk] = l[1:]
	return v, true, nil
}

func (s *fakeStringStore) RPop(_ context.Context, pk string) ([]byte, bool, error) {
	l := s.lists[pk]
	if len(l) == 0 {
		return nil, false, nil
	}
	v := l[len(l)-1]
	s.lists[pk] = l[:len(l)-1]
	return v, true, nil
}

func (s *fakeStringStore) LRange(_ context.Context, pk string, start, stop int) ([][]byte, error) {
	l := s.lists[pk]
	lo, hi, ok := storage.ZNormalizeRankRange(len(l), start, stop)
	if !ok {
		return [][]byte{}, nil
	}
	return append([][]byte(nil), l[lo:hi+1]...), nil
}

func (s *fakeStringStore) LIndex(_ context.Context, pk string, index int) ([]byte, bool, error) {
	l := s.lists[pk]
	n := len(l)
	if index < 0 {
		index += n
	}
	if index < 0 || index >= n {
		return nil, false, nil
	}
	return l[index], true, nil
}

// LRangeAll returns a copy of the whole element slice in head-to-tail order, the
// read half of the LSET/LTRIM/LREM/LINSERT combined implementation.
func (s *fakeStringStore) LRangeAll(_ context.Context, pk string) ([][]byte, error) {
	return append([][]byte(nil), s.lists[pk]...), nil
}

// LReplaceAll rewrites the element slice to exactly elements (copying each so the
// caller's buffers are not aliased), mirroring the redimo-backed store's
// clear-then-repush contract: an empty slice removes the element items entirely.
// It returns the new length; the count counter is maintained by the handlers via
// EnsureType, exactly as the redimo-backed store leaves the meta item to the
// caller.
func (s *fakeStringStore) LReplaceAll(_ context.Context, pk string, elements [][]byte) (int, error) {
	if len(elements) == 0 {
		delete(s.lists, pk)
		return 0, nil
	}
	cp := make([][]byte, len(elements))
	for i, e := range elements {
		cp[i] = append([]byte(nil), e...)
	}
	s.lists[pk] = cp
	return len(elements), nil
}

// ScanKeys models the storage scan primitive for the SCAN command tests. It
// mirrors the redimo-backed store's contract: it returns the pks of LIVE,
// non-expired meta items (the FilterExpression `sk = "#meta" AND (未过期)`),
// pages them with a COUNT-sized limit, and hands back an opaque continuation
// token when more remain. The keyspace is sorted so paging is deterministic, and
// the token is a simple index encoded the way DynamoDB would carry a
// LastEvaluatedKey attribute (an N attribute under "idx"). A nil token means the
// scan reached the end (SCAN then reports the terminating cursor 0).
func (s *fakeStringStore) ScanKeys(_ context.Context, lek map[string]types.AttributeValue, limit int32, now int64) ([]string, map[string]types.AttributeValue, error) {
	pks := make([]string, 0, len(s.live))
	for pk, present := range s.live {
		if !present {
			continue
		}
		m := s.metas[pk]
		if m.Exp > 0 && m.Exp <= now {
			continue // expired: never surfaced, matching the read-path filter.
		}
		pks = append(pks, pk)
	}
	sort.Strings(pks)

	// Resolve the start index from the pagination token; a nil token starts fresh.
	start := 0
	if lek != nil {
		if v, ok := lek["idx"].(*types.AttributeValueMemberN); ok {
			if n, err := strconv.Atoi(v.Value); err == nil {
				start = n
			}
		}
	}
	if start > len(pks) {
		start = len(pks)
	}

	// One page. limit <= 0 leaves the page size to the store; model that as the
	// whole remainder so a COUNT-less SCAN returns everything in a single page.
	end := len(pks)
	if limit > 0 && start+int(limit) < end {
		end = start + int(limit)
	}

	var next map[string]types.AttributeValue
	if end < len(pks) {
		next = map[string]types.AttributeValue{
			"idx": &types.AttributeValueMemberN{Value: strconv.Itoa(end)},
		}
	}
	return pks[start:end], next, nil
}
