package command

import (
	"bufio"
	"sort"
	"strings"
	"testing"

	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

// Unit tests for the Hash command family (task 13.1). They run the real
// in-process server + router over the stateful fakeStringStore (which models the
// per-field item layout and the meta counter), so the handlers, meta counting and
// RESP encoding are all exercised end-to-end. Order-insensitive replies
// (HGETALL/HKEYS/HVALS) are compared as sets/maps because the field order is
// unspecified.

// stripBulk removes the "$" prefix readArray/readReply put on a bulk-string
// element, yielding the raw payload.
func stripBulk(s string) string { return strings.TrimPrefix(s, "$") }

// readHashMap reads a flat HGETALL-style array (field, value, field, value, ...)
// into a map so it can be compared independent of field order.
func readHashMap(t *testing.T, r *bufio.Reader) map[string]string {
	t.Helper()
	arr := readArray(t, r)
	if len(arr)%2 != 0 {
		t.Fatalf("HGETALL array has odd length %d: %v", len(arr), arr)
	}
	m := make(map[string]string, len(arr)/2)
	for i := 0; i+1 < len(arr); i += 2 {
		m[stripBulk(arr[i])] = stripBulk(arr[i+1])
	}
	return m
}

// readSortedBulk reads an array of bulk strings and returns the payloads sorted,
// for order-independent comparison of HKEYS/HVALS.
func readSortedBulk(t *testing.T, r *bufio.Reader) []string {
	t.Helper()
	arr := readArray(t, r)
	out := make([]string, len(arr))
	for i, e := range arr {
		out[i] = stripBulk(e)
	}
	sort.Strings(out)
	return out
}

// --- HSET new/update + HLEN (requirements 6.1, 6.2, 6.4) --------------------

func TestHSetNewAndUpdateWithHLen(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	// Redis 3.2 HSET writes a single field/value pair (multi-field is 4.0+). Two new
	// fields → each replies 1 (field created), HLEN 2.
	if got, want := sendRead(t, conn, r, "HSET h f1 v1"), ":1"; got != want {
		t.Errorf("HSET (new f1) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HSET h f2 v2"), ":1"; got != want {
		t.Errorf("HSET (new f2) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HLEN h"), ":2"; got != want {
		t.Errorf("HLEN = %q, want %q", got, want)
	}

	// Update f1 → reply 0 (no new field); new f3 → reply 1, HLEN 3.
	if got, want := sendRead(t, conn, r, "HSET h f1 v1b"), ":0"; got != want {
		t.Errorf("HSET (update f1) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HSET h f3 v3"), ":1"; got != want {
		t.Errorf("HSET (new f3) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HLEN h"), ":3"; got != want {
		t.Errorf("HLEN after update = %q, want %q", got, want)
	}

	// The updated value must be readable.
	if got, want := sendRead(t, conn, r, "HGET h f1"), "$v1b"; got != want {
		t.Errorf("HGET h f1 = %q, want %q", got, want)
	}
}

func TestHLenAbsentIsZero(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "HLEN absent"), ":0"; got != want {
		t.Errorf("HLEN absent = %q, want %q", got, want)
	}
}

// --- HGET / HMGET / HGETALL (requirement 6.1) -------------------------------

func TestHGetMissing(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// Missing key.
	if got, want := sendRead(t, conn, r, "HGET absent f"), "$-1"; got != want {
		t.Errorf("HGET absent = %q, want %q", got, want)
	}
	// Existing key, missing field.
	sendRead(t, conn, r, "HSET h f1 v1")
	if got, want := sendRead(t, conn, r, "HGET h nope"), "$-1"; got != want {
		t.Errorf("HGET h nope = %q, want %q", got, want)
	}
}

func TestHMGetInRequestOrder(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "HMSET h a 1 b 2 c 3")

	send(t, conn, "HMGET h c absent a")
	got := readArray(t, r)
	want := []string{"$3", "$-1", "$1"}
	if len(got) != len(want) {
		t.Fatalf("HMGET len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("HMGET[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestHMGetAbsentKeyAllNull(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	send(t, conn, "HMGET absent a b")
	got := readArray(t, r)
	want := []string{"$-1", "$-1"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("HMGET absent[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestHGetAll(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "HMSET h a 1 b 2 c 3")

	send(t, conn, "HGETALL h")
	got := readHashMap(t, r)
	want := map[string]string{"a": "1", "b": "2", "c": "3"}
	if len(got) != len(want) {
		t.Fatalf("HGETALL = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("HGETALL[%s] = %q, want %q", k, got[k], v)
		}
	}
}

func TestHGetAllAbsentIsEmptyArray(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	send(t, conn, "HGETALL absent")
	arr := readArray(t, r)
	if len(arr) != 0 {
		t.Errorf("HGETALL absent = %v, want empty array", arr)
	}
}

// --- HDEL + count maintenance (requirements 6.1, 6.4) -----------------------

func TestHDelMaintainsCount(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "HMSET h a 1 b 2 c 3")

	// Delete two existing + one absent → reply 2, HLEN 1.
	if got, want := sendRead(t, conn, r, "HDEL h a b zzz"), ":2"; got != want {
		t.Errorf("HDEL = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HLEN h"), ":1"; got != want {
		t.Errorf("HLEN after HDEL = %q, want %q", got, want)
	}
}

func TestHDelAbsentKey(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "HDEL absent f"), ":0"; got != want {
		t.Errorf("HDEL absent = %q, want %q", got, want)
	}
}

// TestHDelLastFieldRemovesKey verifies removing the final field deletes the key
// (an empty hash does not exist in Redis): EXISTS/TYPE/HLEN all report it gone.
func TestHDelLastFieldRemovesKey(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "HSET h only v")

	if got, want := sendRead(t, conn, r, "HDEL h only"), ":1"; got != want {
		t.Errorf("HDEL last = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "EXISTS h"), ":0"; got != want {
		t.Errorf("EXISTS after emptying = %q, want %q (empty hash must not exist)", got, want)
	}
	if got, want := sendRead(t, conn, r, "TYPE h"), "+none"; got != want {
		t.Errorf("TYPE after emptying = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HLEN h"), ":0"; got != want {
		t.Errorf("HLEN after emptying = %q, want %q", got, want)
	}
}

// --- HEXISTS (requirement 6.1) ----------------------------------------------

func TestHExists(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "HSET h f v")
	if got, want := sendRead(t, conn, r, "HEXISTS h f"), ":1"; got != want {
		t.Errorf("HEXISTS present = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HEXISTS h nope"), ":0"; got != want {
		t.Errorf("HEXISTS missing field = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HEXISTS absent f"), ":0"; got != want {
		t.Errorf("HEXISTS absent key = %q, want %q", got, want)
	}
}

// --- HKEYS / HVALS (requirement 6.1) ----------------------------------------

func TestHKeysAndHVals(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "HMSET h a 1 b 2 c 3")

	send(t, conn, "HKEYS h")
	if got, want := readSortedBulk(t, r), []string{"a", "b", "c"}; !equalStrings(got, want) {
		t.Errorf("HKEYS = %v, want %v", got, want)
	}

	send(t, conn, "HVALS h")
	if got, want := readSortedBulk(t, r), []string{"1", "2", "3"}; !equalStrings(got, want) {
		t.Errorf("HVALS = %v, want %v", got, want)
	}
}

func TestHKeysAbsentIsEmptyArray(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	send(t, conn, "HKEYS absent")
	if arr := readArray(t, r); len(arr) != 0 {
		t.Errorf("HKEYS absent = %v, want empty array", arr)
	}
}

// --- HSETNX (requirement 6.1) -----------------------------------------------

func TestHSetNX(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "HSETNX h f v1"), ":1"; got != want {
		t.Errorf("HSETNX new = %q, want %q", got, want)
	}
	// Field already exists → no write, :0.
	if got, want := sendRead(t, conn, r, "HSETNX h f v2"), ":0"; got != want {
		t.Errorf("HSETNX existing = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HGET h f"), "$v1"; got != want {
		t.Errorf("HGET after rejected HSETNX = %q, want %q (unchanged)", got, want)
	}
	if got, want := sendRead(t, conn, r, "HLEN h"), ":1"; got != want {
		t.Errorf("HLEN = %q, want %q", got, want)
	}
}

// --- HINCRBY / HINCRBYFLOAT (requirement 6.1) -------------------------------

func TestHIncrBy(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// New field starts at 0 and is created (count bumped).
	if got, want := sendRead(t, conn, r, "HINCRBY h n 5"), ":5"; got != want {
		t.Errorf("HINCRBY new = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HLEN h"), ":1"; got != want {
		t.Errorf("HLEN after HINCRBY new = %q, want %q", got, want)
	}
	// Existing field increments in place (no count change).
	if got, want := sendRead(t, conn, r, "HINCRBY h n -2"), ":3"; got != want {
		t.Errorf("HINCRBY existing = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HLEN h"), ":1"; got != want {
		t.Errorf("HLEN unchanged = %q, want %q", got, want)
	}
}

func TestHIncrByNonIntegerAmount(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR value is not an integer or out of range"
	if got := sendRead(t, conn, r, "HINCRBY h n abc"); got != want {
		t.Errorf("HINCRBY h n abc = %q, want %q", got, want)
	}
}

func TestHIncrByNonIntegerField(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "HSET h f hello")
	want := "-ERR hash value is not an integer"
	if got := sendRead(t, conn, r, "HINCRBY h f 1"); got != want {
		t.Errorf("HINCRBY on non-integer field = %q, want %q", got, want)
	}
}

func TestHIncrByFloat(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "HINCRBYFLOAT h n 3.14"), "$3.14"; got != want {
		t.Errorf("HINCRBYFLOAT new = %q, want %q", got, want)
	}
	// 3.14 + 1.86 = 5, formatted as "5".
	if got, want := sendRead(t, conn, r, "HINCRBYFLOAT h n 1.86"), "$5"; got != want {
		t.Errorf("HINCRBYFLOAT = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HLEN h"), ":1"; got != want {
		t.Errorf("HLEN after HINCRBYFLOAT = %q, want %q", got, want)
	}
}

func TestHIncrByFloatNonFloatField(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "HSET h f hello")
	want := "-ERR hash value is not a valid float"
	if got := sendRead(t, conn, r, "HINCRBYFLOAT h f 1.0"); got != want {
		t.Errorf("HINCRBYFLOAT on non-float field = %q, want %q", got, want)
	}
}

// --- HSTRLEN (requirement 6.1) ----------------------------------------------

func TestHStrlen(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "HSET h f hello")
	if got, want := sendRead(t, conn, r, "HSTRLEN h f"), ":5"; got != want {
		t.Errorf("HSTRLEN = %q, want %q", got, want)
	}
	// Missing field / key → 0.
	if got, want := sendRead(t, conn, r, "HSTRLEN h nope"), ":0"; got != want {
		t.Errorf("HSTRLEN missing field = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HSTRLEN absent f"), ":0"; got != want {
		t.Errorf("HSTRLEN absent key = %q, want %q", got, want)
	}
}

// --- WRONGTYPE (requirement 6.1) --------------------------------------------

func TestHashWrongType(t *testing.T) {
	store := newFakeStringStore()
	// Seed a String key; every Hash command against it must reply WRONGTYPE.
	store.metas["0:s"] = storage.Meta{Type: string(meta.TypeString)}
	store.live["0:s"] = true

	conn, r := startStringServer(t, store, fixedNow(1000))
	want := "-WRONGTYPE Operation against a key holding the wrong kind of value"

	cmds := []string{
		"HSET s f v",
		"HSETNX s f v",
		"HGET s f",
		"HMGET s f",
		"HGETALL s",
		"HDEL s f",
		"HEXISTS s f",
		"HKEYS s",
		"HVALS s",
		"HLEN s",
		"HINCRBY s f 1",
		"HINCRBYFLOAT s f 1.0",
		"HSTRLEN s f",
		"HMSET s f v",
	}
	for _, cmd := range cmds {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// --- HMSET (requirement 6.1) ------------------------------------------------

func TestHMSet(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "HMSET h a 1 b 2"), "+OK"; got != want {
		t.Errorf("HMSET = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HLEN h"), ":2"; got != want {
		t.Errorf("HLEN after HMSET = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "HGET h a"), "$1"; got != want {
		t.Errorf("HGET h a = %q, want %q", got, want)
	}
}

// --- arity (requirement 3.2) ------------------------------------------------

func TestHashArityErrors(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	cases := map[string]string{
		"HSET h f":     "-ERR wrong number of arguments for 'hset' command",
		"HSET h f v x": "-ERR wrong number of arguments for 'hset' command",
		"HGET h":       "-ERR wrong number of arguments for 'hget' command",
		"HSETNX h f":   "-ERR wrong number of arguments for 'hsetnx' command",
		"HDEL h":       "-ERR wrong number of arguments for 'hdel' command",
		"HMGET h":      "-ERR wrong number of arguments for 'hmget' command",
		"HGETALL":      "-ERR wrong number of arguments for 'hgetall' command",
		"HLEN":         "-ERR wrong number of arguments for 'hlen' command",
		"HINCRBY h f":  "-ERR wrong number of arguments for 'hincrby' command",
		"HMSET h f":    "-ERR wrong number of arguments for 'hmset' command",
		"HSTRLEN h":    "-ERR wrong number of arguments for 'hstrlen' command",
	}
	for cmd, want := range cases {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// TestHSetRejectsMultiField verifies that Redis 3.2's exact arity 4 is honored: any
// HSET with more than a single field/value pair (a Redis 4.0+ extension) is rejected
// by the arity gate, whether the extra args pair up evenly or oddly.
func TestHSetRejectsMultiField(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR wrong number of arguments for 'hset' command"
	for _, cmd := range []string{"HSET h f1 v1 f2", "HSET h f1 v1 f2 v2"} {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// equalStrings reports whether two string slices are element-wise equal.
func equalStrings(a, b []string) bool {
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
