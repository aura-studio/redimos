package command

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aura-studio/redimos/internal/server"
	"github.com/aura-studio/redimos/internal/storage"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This file is the command-layer half of task 20.2 (concurrency semantics,
// requirements 16.3, 16.4, 5.8). Where the storage-layer test
// (internal/storage/concurrency_test.go) proves the casRetry + SETCAS loop is
// lost-update-safe, these tests drive the WHOLE proxy — many concurrent client
// connections through the in-process redcon server and command router onto a
// shared store — and assert the observable Redis semantics hold under contention:
//
//   - INCR from many connections converges to the exact count with no duplicate or
//     lost values (requirements 16.3, 16.4, 5.8);
//   - concurrent HSET of the SAME field keeps HLEN at 1 and reports the field
//     created exactly once (the meta counter stays consistent, requirement 16.3);
//   - EXPIRE racing writes on one key never corrupts the key (it stays live, the
//     right type, with a coherent TTL — requirement 16.3);
//   - concurrent SPOP never hands the same member to two connections — each member
//     is popped at most once (requirement 16.3).
//
// The proxy serves each connection serially but many connections hit the shared
// store concurrently, so the test store must be safe under concurrent access.
// syncStore below wraps the package's fakeStringStore with a single mutex and
// serializes every method, giving each Store operation atomic, well-defined
// semantics — the same guarantee a single DynamoDB item operation provides — so
// what these tests measure is the COMMAND LAYER's behaviour under contention, not
// a race in the test double. (fakeStringStore itself is not mutex-guarded and is
// shared by the serial single-connection tests; wrapping it keeps those untouched.)

// syncStore serializes every fakeStringStore method behind one mutex so the store
// is safe to share across the concurrent client connections these tests open. It
// holds the lock for the whole delegated call; the embedded fake is itself
// lock-free, so the fake's internal sibling calls (e.g. SRandMember -> SMembers)
// never re-enter the lock and cannot deadlock.
type syncStore struct {
	mu    sync.Mutex
	inner *fakeStringStore
}

func newSyncStore() *syncStore { return &syncStore{inner: newFakeStringStore()} }

var _ storage.Store = (*syncStore)(nil)

// --- meta primitives ---------------------------------------------------------

func (s *syncStore) EnsureType(ctx context.Context, pk, expected string, cntDelta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.EnsureType(ctx, pk, expected, cntDelta)
}

func (s *syncStore) EnsureTypeExpiring(ctx context.Context, pk, expected string, cntDelta, nowEpoch int64) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.EnsureTypeExpiring(ctx, pk, expected, cntDelta, nowEpoch)
}

func (s *syncStore) CreateTypeIfAbsent(ctx context.Context, pk, expected string, cntDelta, nowEpoch int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.CreateTypeIfAbsent(ctx, pk, expected, cntDelta, nowEpoch)
}

func (s *syncStore) LoadMeta(ctx context.Context, pk string) (storage.Meta, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.LoadMeta(ctx, pk)
}

func (s *syncStore) SetExpire(ctx context.Context, pk string, expEpoch int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.SetExpire(ctx, pk, expEpoch)
}

func (s *syncStore) Persist(ctx context.Context, pk string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.Persist(ctx, pk)
}

func (s *syncStore) DeleteMeta(ctx context.Context, pk string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.DeleteMeta(ctx, pk)
}

func (s *syncStore) DeleteMetaIfEmpty(ctx context.Context, pk string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.DeleteMetaIfEmpty(ctx, pk)
}

func (s *syncStore) DeleteMembers(ctx context.Context, pk string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.DeleteMembers(ctx, pk)
}

func (s *syncStore) SweepOrphans(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.SweepOrphans(ctx)
}

// --- String -----------------------------------------------------------------

func (s *syncStore) GetString(ctx context.Context, pk string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.GetString(ctx, pk)
}

func (s *syncStore) MGetStrings(ctx context.Context, pks []string) (map[string][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.MGetStrings(ctx, pks)
}

func (s *syncStore) SetString(ctx context.Context, pk string, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.SetString(ctx, pk, val)
}

func (s *syncStore) GetSetString(ctx context.Context, pk string, val []byte) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.GetSetString(ctx, pk, val)
}

func (s *syncStore) SetStringIfEquals(ctx context.Context, pk string, newVal, oldVal []byte, oldExists bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.SetStringIfEquals(ctx, pk, newVal, oldVal, oldExists)
}

func (s *syncStore) IncrBy(ctx context.Context, pk string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.IncrBy(ctx, pk, delta)
}

func (s *syncStore) IncrByFloat(ctx context.Context, pk string, delta float64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.IncrByFloat(ctx, pk, delta)
}

// --- Hash --------------------------------------------------------------------

func (s *syncStore) HSet(ctx context.Context, pk string, fields []storage.HField) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.HSet(ctx, pk, fields)
}

func (s *syncStore) HSetNX(ctx context.Context, pk, field string, val []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.HSetNX(ctx, pk, field, val)
}

func (s *syncStore) HGet(ctx context.Context, pk, field string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.HGet(ctx, pk, field)
}

func (s *syncStore) HMGet(ctx context.Context, pk string, fields []string) (map[string][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.HMGet(ctx, pk, fields)
}

func (s *syncStore) HGetAll(ctx context.Context, pk string) ([]storage.HField, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.HGetAll(ctx, pk)
}

func (s *syncStore) HKeys(ctx context.Context, pk string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.HKeys(ctx, pk)
}

func (s *syncStore) HVals(ctx context.Context, pk string) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.HVals(ctx, pk)
}

func (s *syncStore) HDel(ctx context.Context, pk string, fields []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.HDel(ctx, pk, fields)
}

func (s *syncStore) HExists(ctx context.Context, pk, field string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.HExists(ctx, pk, field)
}

func (s *syncStore) HStrlen(ctx context.Context, pk, field string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.HStrlen(ctx, pk, field)
}

func (s *syncStore) HIncrBy(ctx context.Context, pk, field string, delta int64) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.HIncrBy(ctx, pk, field, delta)
}

func (s *syncStore) HIncrByFloat(ctx context.Context, pk, field string, delta float64) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.HIncrByFloat(ctx, pk, field, delta)
}

// --- Set ---------------------------------------------------------------------

func (s *syncStore) SAdd(ctx context.Context, pk string, members []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.SAdd(ctx, pk, members)
}

func (s *syncStore) SRem(ctx context.Context, pk string, members []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.SRem(ctx, pk, members)
}

func (s *syncStore) SIsMember(ctx context.Context, pk, member string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.SIsMember(ctx, pk, member)
}

func (s *syncStore) SMembers(ctx context.Context, pk string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.SMembers(ctx, pk)
}

func (s *syncStore) SPop(ctx context.Context, pk string, count int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.SPop(ctx, pk, count)
}

func (s *syncStore) SRandMember(ctx context.Context, pk string, count int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.SRandMember(ctx, pk, count)
}

func (s *syncStore) SScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]string, map[string]types.AttributeValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.SScan(ctx, pk, lek, limit)
}

// --- Sorted Set --------------------------------------------------------------

func (s *syncStore) ZAdd(ctx context.Context, pk string, members []storage.ZMember) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.ZAdd(ctx, pk, members)
}

func (s *syncStore) ZRem(ctx context.Context, pk string, members []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.ZRem(ctx, pk, members)
}

func (s *syncStore) ZScore(ctx context.Context, pk, member string) (float64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.ZScore(ctx, pk, member)
}

func (s *syncStore) ZIncrBy(ctx context.Context, pk, member string, delta float64) (float64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.ZIncrBy(ctx, pk, member, delta)
}

func (s *syncStore) ZRangeByRank(ctx context.Context, pk string, start, stop int, rev bool) ([]storage.ZMember, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.ZRangeByRank(ctx, pk, start, stop, rev)
}

func (s *syncStore) ZRangeByScore(ctx context.Context, pk string, min, max storage.ScoreBound, rev bool) ([]storage.ZMember, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.ZRangeByScore(ctx, pk, min, max, rev)
}

func (s *syncStore) ZCount(ctx context.Context, pk string, min, max storage.ScoreBound) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.ZCount(ctx, pk, min, max)
}

func (s *syncStore) ZRank(ctx context.Context, pk, member string, rev bool) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.ZRank(ctx, pk, member, rev)
}

func (s *syncStore) ZRemRangeByRank(ctx context.Context, pk string, start, stop int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.ZRemRangeByRank(ctx, pk, start, stop)
}

func (s *syncStore) ZRemRangeByScore(ctx context.Context, pk string, min, max storage.ScoreBound) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.ZRemRangeByScore(ctx, pk, min, max)
}

func (s *syncStore) ZScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]storage.ZMember, map[string]types.AttributeValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.ZScan(ctx, pk, lek, limit)
}

// --- List --------------------------------------------------------------------

func (s *syncStore) LPush(ctx context.Context, pk string, elements [][]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.LPush(ctx, pk, elements)
}

func (s *syncStore) RPush(ctx context.Context, pk string, elements [][]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.RPush(ctx, pk, elements)
}

func (s *syncStore) LPop(ctx context.Context, pk string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.LPop(ctx, pk)
}

func (s *syncStore) RPop(ctx context.Context, pk string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.RPop(ctx, pk)
}

func (s *syncStore) LRange(ctx context.Context, pk string, start, stop int) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.LRange(ctx, pk, start, stop)
}

func (s *syncStore) LIndex(ctx context.Context, pk string, index int) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.LIndex(ctx, pk, index)
}

func (s *syncStore) LRangeAll(ctx context.Context, pk string) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.LRangeAll(ctx, pk)
}

func (s *syncStore) LReplaceAll(ctx context.Context, pk string, elements [][]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.LReplaceAll(ctx, pk, elements)
}

// --- scan --------------------------------------------------------------------

func (s *syncStore) ScanKeys(ctx context.Context, lek map[string]types.AttributeValue, limit int32, now int64) ([]string, map[string]types.AttributeValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.ScanKeys(ctx, lek, limit, now)
}

func (s *syncStore) HScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]storage.HField, map[string]types.AttributeValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.HScan(ctx, pk, lek, limit)
}

// --- concurrency test harness ------------------------------------------------

// startConcurrentServer boots an in-process storage-backed server on the given
// store and returns its address so each goroutine can open its own connection.
// Unlike startStringServer it does not dial or set a per-connection deadline,
// leaving connection management to the concurrent test.
func startConcurrentServer(t *testing.T, store storage.Store) string {
	t.Helper()

	r := NewRouterWithStorage(Config{MultiDB: true}, Storage{Store: store, Now: fixedNow(1000)})
	s := server.New(server.Options{Addr: "127.0.0.1:0"}, r)
	signal := make(chan error, 1)
	go func() { _ = s.ListenServeAndSignal(signal) }()
	if err := <-signal; err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	return s.Addr().String()
}

// dialConn opens one client connection to addr with a generous deadline.
func dialConn(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	return conn, bufio.NewReader(conn)
}

// reply is a goroutine-safe parsed RESP2 value. It never calls t.Fatalf (illegal
// off the test goroutine); callers inspect Err instead.
type reply struct {
	Kind byte     // '+' simple, '-' error, ':' int, '$' bulk (Null when absent), '*' array
	Str  string   // payload for '+'/'-'/':'/'$'
	Null bool     // true for a null bulk ($-1) or null array (*-1)
	Arr  []string // bulk payloads for '*'; a null element is rendered "" with no marker (unused here)
	Err  error
}

// readValueConcurrent parses one RESP2 reply without touching *testing.T, so it is
// safe to call from worker goroutines. It handles the shapes the tested commands
// return: simple string, error, integer, bulk (incl. null) and flat bulk arrays.
func readValueConcurrent(r *bufio.Reader) reply {
	line, err := r.ReadString('\n')
	if err != nil {
		return reply{Err: err}
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return reply{Err: fmt.Errorf("empty reply line")}
	}

	switch line[0] {
	case '+', '-', ':':
		return reply{Kind: line[0], Str: line[1:]}
	case '$':
		n, cerr := strconv.Atoi(line[1:])
		if cerr != nil {
			return reply{Err: fmt.Errorf("bad bulk header %q: %w", line, cerr)}
		}
		if n < 0 {
			return reply{Kind: '$', Null: true}
		}
		buf := make([]byte, n+2)
		if _, rerr := io.ReadFull(r, buf); rerr != nil {
			return reply{Err: rerr}
		}
		return reply{Kind: '$', Str: string(buf[:n])}
	case '*':
		n, cerr := strconv.Atoi(line[1:])
		if cerr != nil {
			return reply{Err: fmt.Errorf("bad array header %q: %w", line, cerr)}
		}
		if n < 0 {
			return reply{Kind: '*', Null: true}
		}
		arr := make([]string, 0, n)
		for i := 0; i < n; i++ {
			el := readValueConcurrent(r)
			if el.Err != nil {
				return reply{Err: el.Err}
			}
			if el.Null {
				arr = append(arr, "")
				continue
			}
			arr = append(arr, el.Str)
		}
		return reply{Kind: '*', Arr: arr}
	default:
		return reply{Err: fmt.Errorf("unexpected reply prefix in %q", line)}
	}
}

// sendReadConcurrent writes one inline command and reads a full reply, returning
// an error rather than failing the test, so it is safe inside worker goroutines.
func sendReadConcurrent(conn net.Conn, r *bufio.Reader, cmd string) reply {
	if _, err := conn.Write([]byte(cmd + "\r\n")); err != nil {
		return reply{Err: err}
	}
	return readValueConcurrent(r)
}

// --- INCR concurrency (requirements 16.3, 16.4, 5.8) -------------------------

// TestConcurrentINCRConvergesTo1000 fires 1000 INCRs of one key spread across many
// concurrent connections and asserts the counter converges to exactly 1000 with
// every intermediate reply distinct — no increment is lost and none is counted
// twice. It drives the full command path (EnsureType + atomic IncrBy) so it
// validates the proxy's INCR semantics under contention, not just the storage loop.
func TestConcurrentINCRConvergesTo1000(t *testing.T) {
	const (
		conns        = 50
		incrsPerConn = 20
		wantTotal    = conns * incrsPerConn // 1000
	)

	store := newSyncStore()
	addr := startConcurrentServer(t, store)

	var mu sync.Mutex
	seen := make(map[int64]int, wantTotal)

	var wg sync.WaitGroup
	errCh := make(chan error, conns)

	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cn, err := net.Dial("tcp", addr)
			if err != nil {
				errCh <- err
				return
			}
			defer func() { _ = cn.Close() }()
			_ = cn.SetDeadline(time.Now().Add(30 * time.Second))
			r := bufio.NewReader(cn)

			for i := 0; i < incrsPerConn; i++ {
				rep := sendReadConcurrent(cn, r, "INCR counter")
				if rep.Err != nil {
					errCh <- rep.Err
					return
				}
				if rep.Kind != ':' {
					errCh <- fmt.Errorf("INCR reply kind %q (%q), want integer", rep.Kind, rep.Str)
					return
				}
				v, perr := strconv.ParseInt(rep.Str, 10, 64)
				if perr != nil {
					errCh <- fmt.Errorf("INCR reply %q not an int: %w", rep.Str, perr)
					return
				}
				mu.Lock()
				seen[v]++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent INCR worker failed: %v", err)
	}

	// Final value via a fresh connection.
	conn, r := dialConn(t, addr)
	final := sendReadConcurrent(conn, r, "GET counter")
	if final.Err != nil {
		t.Fatalf("GET counter: %v", final.Err)
	}
	if final.Kind != '$' || final.Str != strconv.Itoa(wantTotal) {
		t.Fatalf("GET counter = %q (kind %q), want %d", final.Str, final.Kind, wantTotal)
	}

	// Every integer 1..1000 must have been returned exactly once: no lost update
	// (a gap) and no double count (a duplicate).
	if len(seen) != wantTotal {
		t.Fatalf("saw %d distinct INCR results, want %d", len(seen), wantTotal)
	}
	for v := int64(1); v <= int64(wantTotal); v++ {
		if seen[v] != 1 {
			t.Fatalf("INCR result %d appeared %d times, want exactly 1 (lost or duplicated update)", v, seen[v])
		}
	}
}

// --- HSET same-field race (requirement 16.3) ---------------------------------

// TestConcurrentHSetSameFieldKeepsCountConsistent has many connections HSET the
// SAME field concurrently. Redis counts a field once no matter how many writers
// set it, so HLEN must be exactly 1 and exactly one writer may report the field as
// newly created (:1); the rest must report :0. This proves the meta counter stays
// consistent when concurrent writes race on one field (requirement 16.3).
func TestConcurrentHSetSameFieldKeepsCountConsistent(t *testing.T) {
	const conns = 200

	store := newSyncStore()
	addr := startConcurrentServer(t, store)

	var created int64 // number of HSETs that reported the field newly created
	var mu sync.Mutex

	var wg sync.WaitGroup
	errCh := make(chan error, conns)

	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cn, err := net.Dial("tcp", addr)
			if err != nil {
				errCh <- err
				return
			}
			defer func() { _ = cn.Close() }()
			_ = cn.SetDeadline(time.Now().Add(30 * time.Second))
			r := bufio.NewReader(cn)

			rep := sendReadConcurrent(cn, r, fmt.Sprintf("HSET h field val-%d", id))
			if rep.Err != nil {
				errCh <- rep.Err
				return
			}
			if rep.Kind != ':' {
				errCh <- fmt.Errorf("HSET reply kind %q (%q), want integer", rep.Kind, rep.Str)
				return
			}
			switch rep.Str {
			case "1":
				mu.Lock()
				created++
				mu.Unlock()
			case "0":
			default:
				errCh <- fmt.Errorf("HSET reply :%s, want :1 or :0", rep.Str)
			}
		}(c)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent HSET worker failed: %v", err)
	}

	if created != 1 {
		t.Fatalf("%d HSETs reported the field newly created, want exactly 1", created)
	}

	conn, r := dialConn(t, addr)
	hlen := sendReadConcurrent(conn, r, "HLEN h")
	if hlen.Err != nil {
		t.Fatalf("HLEN h: %v", hlen.Err)
	}
	if hlen.Kind != ':' || hlen.Str != "1" {
		t.Fatalf("HLEN h = %q (kind %q), want :1 after concurrent same-field HSET", hlen.Str, hlen.Kind)
	}
}

// --- EXPIRE vs write race (requirement 16.3) ---------------------------------

// TestConcurrentExpireAndWriteStaysConsistent races EXPIRE against overwriting
// writes (SET) on one key. Whichever wins, the key must never end up corrupt: it
// stays live, keeps type string, GET returns one of the written values, and its
// TTL is coherent (either no expiry, or a positive TTL within the window we set) —
// never the -2 of a missing key. This exercises concurrent writers touching a
// key's value and its meta.exp without losing type/consistency (requirement 16.3).
func TestConcurrentExpireAndWriteStaysConsistent(t *testing.T) {
	t.Skip("v1 line: EXPIRE/TTL/TYPE are gated on redimo v1.6.1 (no TTL, no type tag)")
	const writers = 100

	store := newSyncStore()
	addr := startConcurrentServer(t, store)

	// Seed the key so EXPIRE has something to act on.
	seed, sr := dialConn(t, addr)
	if rep := sendReadConcurrent(seed, sr, "SET k seed"); rep.Err != nil || rep.Kind != '+' {
		t.Fatalf("seed SET k = %+v", rep)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, writers*2)

	launch := func(cmd string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cn, err := net.Dial("tcp", addr)
			if err != nil {
				errCh <- err
				return
			}
			defer func() { _ = cn.Close() }()
			_ = cn.SetDeadline(time.Now().Add(30 * time.Second))
			r := bufio.NewReader(cn)

			rep := sendReadConcurrent(cn, r, cmd)
			if rep.Err != nil {
				errCh <- rep.Err
				return
			}
			if rep.Kind == '-' {
				errCh <- fmt.Errorf("%q returned error %q", cmd, rep.Str)
			}
		}()
	}

	// clock is pinned at 1000 (fixedNow); EXPIRE k 100 sets exp=1100, so a live key
	// must report TTL in (0, 100].
	for i := 0; i < writers; i++ {
		launch("EXPIRE k 100")
		launch(fmt.Sprintf("SET k v-%d", i))
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent EXPIRE/SET worker failed: %v", err)
	}

	conn, r := dialConn(t, addr)

	if rep := sendReadConcurrent(conn, r, "EXISTS k"); rep.Err != nil || rep.Kind != ':' || rep.Str != "1" {
		t.Fatalf("EXISTS k = %+v, want :1 (key must survive the race)", rep)
	}
	if rep := sendReadConcurrent(conn, r, "TYPE k"); rep.Err != nil || rep.Kind != '+' || rep.Str != "string" {
		t.Fatalf("TYPE k = %+v, want +string (type must stay consistent)", rep)
	}
	if rep := sendReadConcurrent(conn, r, "GET k"); rep.Err != nil || rep.Kind != '$' || rep.Null {
		t.Fatalf("GET k = %+v, want a non-null bulk value", rep)
	}
	ttl := sendReadConcurrent(conn, r, "TTL k")
	if ttl.Err != nil || ttl.Kind != ':' {
		t.Fatalf("TTL k = %+v, want an integer", ttl)
	}
	tv, perr := strconv.ParseInt(ttl.Str, 10, 64)
	if perr != nil {
		t.Fatalf("TTL k reply %q not an int: %v", ttl.Str, perr)
	}
	// -1 = no expiry (a SET landed last and cleared it, or EXPIRE never won),
	// (0,100] = an EXPIRE is the current state. -2 (missing) or anything else is a
	// corrupt outcome.
	if tv != -1 && !(tv > 0 && tv <= 100) {
		t.Fatalf("TTL k = %d, want -1 or a value in (0,100] — got an inconsistent TTL", tv)
	}
}

// --- SPOP uniqueness (requirement 16.3) --------------------------------------

// TestConcurrentSPopUniqueness pre-seeds a set with N distinct members, then has
// many connections concurrently SPOP until the set is empty, and asserts every
// member is handed out at most once across all poppers (no member popped twice)
// and that popped ∪ remaining reconstructs the original set with none lost. This
// is the set analogue of the no-lost/no-duplicate INCR property (requirement 16.3).
func TestConcurrentSPopUniqueness(t *testing.T) {
	const members = 500

	store := newSyncStore()
	addr := startConcurrentServer(t, store)

	// Seed the set on one connection (SADD in chunks to keep the command line sane).
	seed, sr := dialConn(t, addr)
	const chunk = 50
	for start := 0; start < members; start += chunk {
		var b strings.Builder
		b.WriteString("SADD s")
		for i := start; i < start+chunk && i < members; i++ {
			fmt.Fprintf(&b, " m%d", i)
		}
		if rep := sendReadConcurrent(seed, sr, b.String()); rep.Err != nil || rep.Kind != ':' {
			t.Fatalf("seed SADD = %+v", rep)
		}
	}

	const poppers = 40
	var mu sync.Mutex
	popped := make(map[string]int, members)

	var wg sync.WaitGroup
	errCh := make(chan error, poppers)

	for p := 0; p < poppers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cn, err := net.Dial("tcp", addr)
			if err != nil {
				errCh <- err
				return
			}
			defer func() { _ = cn.Close() }()
			_ = cn.SetDeadline(time.Now().Add(30 * time.Second))
			r := bufio.NewReader(cn)

			for {
				rep := sendReadConcurrent(cn, r, "SPOP s")
				if rep.Err != nil {
					errCh <- rep.Err
					return
				}
				if rep.Kind == '$' && rep.Null {
					return // set drained
				}
				if rep.Kind != '$' {
					errCh <- fmt.Errorf("SPOP reply kind %q (%q), want bulk", rep.Kind, rep.Str)
					return
				}
				mu.Lock()
				popped[rep.Str]++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent SPOP worker failed: %v", err)
	}

	// No member handed out more than once.
	for m, n := range popped {
		if n != 1 {
			t.Fatalf("member %q popped %d times, want at most 1 (SPOP handed a member to two connections)", m, n)
		}
	}

	// The set must be fully drained: SCARD 0 and every seeded member popped once.
	conn, r := dialConn(t, addr)
	if rep := sendReadConcurrent(conn, r, "SCARD s"); rep.Err != nil || rep.Kind != ':' || rep.Str != "0" {
		t.Fatalf("SCARD s = %+v, want :0 after draining", rep)
	}
	if len(popped) != members {
		t.Fatalf("popped %d distinct members, want %d (some members were lost)", len(popped), members)
	}
	for i := 0; i < members; i++ {
		if popped[fmt.Sprintf("m%d", i)] != 1 {
			t.Fatalf("member m%d was not popped exactly once", i)
		}
	}
}

func (s *syncStore) KeyType(_ context.Context, _ string) (string, bool, error) {
	return "", false, nil
}
