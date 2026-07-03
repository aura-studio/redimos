package command

import (
	"testing"

	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

// Unit tests for the List command family (task 16.1). They run the real
// in-process server + router over the stateful fakeStringStore (which models the
// ordered element slice and the meta counter), so the handlers, meta counting and
// RESP encoding are exercised end-to-end. List replies (LRANGE) are order-
// sensitive, so they are compared in wire order via readArrayPayloads.

// --- LPUSH / RPUSH order + LLEN (requirements 7.1, 7.2, 7.7) -----------------

// TestLPushOrderAndLLen verifies LPUSH prepends in argument order (LPUSH a b c ->
// head-to-tail c, b, a), returns the growing length, and LLEN reads it O(1).
func TestLPushOrderAndLLen(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	if got, want := sendRead(t, conn, r, "LPUSH l a b c"), ":3"; got != want {
		t.Errorf("LPUSH l a b c = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN l"), ":3"; got != want {
		t.Errorf("LLEN = %q, want %q", got, want)
	}

	send(t, conn, "LRANGE l 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"c", "b", "a"}; !equalStrings(got, want) {
		t.Errorf("LRANGE after LPUSH = %v, want %v", got, want)
	}

	// A second LPUSH prepends again: LPUSH l x y -> y, x in front of c, b, a.
	if got, want := sendRead(t, conn, r, "LPUSH l x y"), ":5"; got != want {
		t.Errorf("LPUSH l x y = %q, want %q", got, want)
	}
	send(t, conn, "LRANGE l 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"y", "x", "c", "b", "a"}; !equalStrings(got, want) {
		t.Errorf("LRANGE after 2nd LPUSH = %v, want %v", got, want)
	}
}

// TestRPushOrderAndLLen verifies RPUSH appends in argument order (RPUSH a b c ->
// head-to-tail a, b, c).
func TestRPushOrderAndLLen(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	if got, want := sendRead(t, conn, r, "RPUSH l a b c"), ":3"; got != want {
		t.Errorf("RPUSH l a b c = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN l"), ":3"; got != want {
		t.Errorf("LLEN = %q, want %q", got, want)
	}

	send(t, conn, "LRANGE l 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"a", "b", "c"}; !equalStrings(got, want) {
		t.Errorf("LRANGE after RPUSH = %v, want %v", got, want)
	}
}

func TestLLenAbsentIsZero(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "LLEN absent"), ":0"; got != want {
		t.Errorf("LLEN absent = %q, want %q", got, want)
	}
}

// --- LPUSHX / RPUSHX (requirements 7.1, 7.7) --------------------------------

// TestLPushXAbsentReturnsZero verifies LPUSHX/RPUSHX on an absent key push nothing
// and reply ":0" (and do not create the key).
func TestPushXAbsentReturnsZero(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	if got, want := sendRead(t, conn, r, "LPUSHX l a"), ":0"; got != want {
		t.Errorf("LPUSHX absent = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "RPUSHX l a"), ":0"; got != want {
		t.Errorf("RPUSHX absent = %q, want %q", got, want)
	}
	// Neither created the key.
	if got, want := sendRead(t, conn, r, "EXISTS l"), ":0"; got != want {
		t.Errorf("EXISTS after PUSHX absent = %q, want %q (must not create)", got, want)
	}
}

// TestPushXPresentPushes verifies LPUSHX/RPUSHX push onto an existing list and
// report the new length.
func TestPushXPresentPushes(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a")

	if got, want := sendRead(t, conn, r, "LPUSHX l head"), ":2"; got != want {
		t.Errorf("LPUSHX present = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "RPUSHX l tail"), ":3"; got != want {
		t.Errorf("RPUSHX present = %q, want %q", got, want)
	}

	send(t, conn, "LRANGE l 0 -1")
	if got, want := readArrayPayloads(t, r), []string{"head", "a", "tail"}; !equalStrings(got, want) {
		t.Errorf("LRANGE after PUSHX = %v, want %v", got, want)
	}
}

// --- LPOP / RPOP (requirements 7.3, 7.7) ------------------------------------

// TestLPopRPopOrderAndCount verifies LPOP takes from the head, RPOP from the tail,
// and each decrements the length.
func TestLPopRPopOrderAndCount(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b c") // head-to-tail a, b, c

	if got, want := sendRead(t, conn, r, "LPOP l"), "$a"; got != want {
		t.Errorf("LPOP = %q, want %q (head)", got, want)
	}
	if got, want := sendRead(t, conn, r, "RPOP l"), "$c"; got != want {
		t.Errorf("RPOP = %q, want %q (tail)", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN l"), ":1"; got != want {
		t.Errorf("LLEN after pops = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "LINDEX l 0"), "$b"; got != want {
		t.Errorf("remaining element = %q, want %q", got, want)
	}
}

func TestLPopEmptyIsNullBulk(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "LPOP absent"), "$-1"; got != want {
		t.Errorf("LPOP absent = %q, want %q (null bulk)", got, want)
	}
	if got, want := sendRead(t, conn, r, "RPOP absent"), "$-1"; got != want {
		t.Errorf("RPOP absent = %q, want %q (null bulk)", got, want)
	}
}

// TestLPopLastElementRemovesKey verifies popping the final element deletes the key
// (an empty list does not exist in Redis).
func TestLPopLastElementRemovesKey(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l only")

	if got, want := sendRead(t, conn, r, "LPOP l"), "$only"; got != want {
		t.Errorf("LPOP last = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "EXISTS l"), ":0"; got != want {
		t.Errorf("EXISTS after emptying = %q, want %q (empty list must not exist)", got, want)
	}
	if got, want := sendRead(t, conn, r, "TYPE l"), "+none"; got != want {
		t.Errorf("TYPE after emptying = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "LLEN l"), ":0"; got != want {
		t.Errorf("LLEN after emptying = %q, want %q", got, want)
	}
}

// TestRPopLastElementRemovesKey mirrors the above for RPOP.
func TestRPopLastElementRemovesKey(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l only")

	if got, want := sendRead(t, conn, r, "RPOP l"), "$only"; got != want {
		t.Errorf("RPOP last = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "EXISTS l"), ":0"; got != want {
		t.Errorf("EXISTS after emptying = %q, want %q", got, want)
	}
}

// --- LRANGE (requirement 7.1) -----------------------------------------------

func TestLRangeVariants(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b c d e") // indices 0..4

	cases := []struct {
		cmd  string
		want []string
	}{
		{"LRANGE l 0 -1", []string{"a", "b", "c", "d", "e"}},     // whole list
		{"LRANGE l 0 2", []string{"a", "b", "c"}},                // positive subrange
		{"LRANGE l 1 3", []string{"b", "c", "d"}},                // positive subrange
		{"LRANGE l -3 -1", []string{"c", "d", "e"}},              // negative indices
		{"LRANGE l -100 100", []string{"a", "b", "c", "d", "e"}}, // clamped out-of-range
		{"LRANGE l 2 1", nil},                                    // empty (start > stop)
		{"LRANGE l 5 10", nil},                                   // empty (start past end)
	}
	for _, tc := range cases {
		send(t, conn, tc.cmd)
		got := readArrayPayloads(t, r)
		if len(tc.want) == 0 {
			if len(got) != 0 {
				t.Errorf("%q = %v, want empty array", tc.cmd, got)
			}
			continue
		}
		if !equalStrings(got, tc.want) {
			t.Errorf("%q = %v, want %v", tc.cmd, got, tc.want)
		}
	}
}

func TestLRangeAbsentIsEmptyArray(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	send(t, conn, "LRANGE absent 0 -1")
	if arr := readArray(t, r); len(arr) != 0 {
		t.Errorf("LRANGE absent = %v, want empty array", arr)
	}
}

func TestLRangeNonIntegerBounds(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR value is not an integer or out of range"
	if got := sendRead(t, conn, r, "LRANGE l x 1"); got != want {
		t.Errorf("LRANGE l x 1 = %q, want %q", got, want)
	}
	if got := sendRead(t, conn, r, "LRANGE l 0 y"); got != want {
		t.Errorf("LRANGE l 0 y = %q, want %q", got, want)
	}
}

// --- LINDEX (requirement 7.1) -----------------------------------------------

func TestLIndexVariants(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "RPUSH l a b c") // 0:a 1:b 2:c

	cases := map[string]string{
		"LINDEX l 0":   "$a",
		"LINDEX l 2":   "$c",
		"LINDEX l -1":  "$c",  // last
		"LINDEX l -3":  "$a",  // first
		"LINDEX l 3":   "$-1", // out of range (positive)
		"LINDEX l -4":  "$-1", // out of range (negative)
		"LINDEX l 100": "$-1",
	}
	for cmd, want := range cases {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

func TestLIndexAbsentIsNullBulk(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "LINDEX absent 0"), "$-1"; got != want {
		t.Errorf("LINDEX absent = %q, want %q", got, want)
	}
}

func TestLIndexNonInteger(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR value is not an integer or out of range"
	if got := sendRead(t, conn, r, "LINDEX l abc"); got != want {
		t.Errorf("LINDEX l abc = %q, want %q", got, want)
	}
}

// --- WRONGTYPE (requirement 7.1) --------------------------------------------

func TestListWrongType(t *testing.T) {
	store := newFakeStringStore()
	// Seed a String key; every List command against it must reply WRONGTYPE.
	store.metas["0:k"] = storage.Meta{Type: string(meta.TypeString)}
	store.live["0:k"] = true

	conn, r := startStringServer(t, store, fixedNow(1000))
	want := "-WRONGTYPE Operation against a key holding the wrong kind of value"

	cmds := []string{
		"LPUSH k v",
		"RPUSH k v",
		"LPUSHX k v",
		"RPUSHX k v",
		"LPOP k",
		"RPOP k",
		"LRANGE k 0 -1",
		"LINDEX k 0",
		"LLEN k",
	}
	for _, cmd := range cmds {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// --- arity (requirement 3.2) ------------------------------------------------

func TestListArityErrors(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	cases := map[string]string{
		"LPUSH l":    "-ERR wrong number of arguments for 'lpush' command",
		"RPUSH l":    "-ERR wrong number of arguments for 'rpush' command",
		"LPUSHX l":   "-ERR wrong number of arguments for 'lpushx' command",
		"RPUSHX l":   "-ERR wrong number of arguments for 'rpushx' command",
		"LPOP":       "-ERR wrong number of arguments for 'lpop' command",
		"LPOP l x":   "-ERR wrong number of arguments for 'lpop' command",
		"RPOP":       "-ERR wrong number of arguments for 'rpop' command",
		"LRANGE l 0": "-ERR wrong number of arguments for 'lrange' command",
		"LINDEX l":   "-ERR wrong number of arguments for 'lindex' command",
		"LLEN":       "-ERR wrong number of arguments for 'llen' command",
		"LLEN l x":   "-ERR wrong number of arguments for 'llen' command",
	}
	for cmd, want := range cases {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}
