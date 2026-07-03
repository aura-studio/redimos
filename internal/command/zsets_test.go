package command

import (
	"testing"

	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

// These tests exercise the Sorted Set command family end-to-end over the
// in-process server + fakeStringStore double, mirroring the Hash/Set family
// tests. They cover the score-ordered reads (requirement 9.1), the O(1) ZCARD
// counter from meta.cnt (requirements 9.2, 9.7), and the atomic cnt maintenance /
// empty-key deletion the collection write path guarantees (requirement 9.7).

// --- ZADD / ZCARD (requirements 9.1, 9.2, 9.7) ------------------------------

func TestZAddNewMembersAndZCard(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))

	// Three new members => reply 3 (all newly added).
	if got, want := sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c"), ":3"; got != want {
		t.Errorf("ZADD z (new) = %q, want %q", got, want)
	}
	// ZCARD reads meta.cnt in O(1).
	if got, want := sendRead(t, conn, r, "ZCARD z"), ":3"; got != want {
		t.Errorf("ZCARD z = %q, want %q", got, want)
	}
	if got, want := store.metas["0:z"].Count, int64(3); got != want {
		t.Errorf("meta.cnt = %d, want %d", got, want)
	}
}

func TestZAddUpdateScoreDoesNotCount(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))

	sendRead(t, conn, r, "ZADD z 1 a 2 b")
	// Re-adding an existing member with a new score returns 0 (no NEW members) and
	// leaves cnt unchanged, but the score is updated.
	if got, want := sendRead(t, conn, r, "ZADD z 5 a 9 new"), ":1"; got != want {
		t.Errorf("ZADD z (1 update, 1 new) = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "ZSCORE z a"), "$5"; got != want {
		t.Errorf("ZSCORE z a (updated) = %q, want %q", got, want)
	}
	if got, want := store.metas["0:z"].Count, int64(3); got != want {
		t.Errorf("meta.cnt = %d, want %d", got, want)
	}
}

func TestZAddInvalidScore(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR value is not a valid float"
	if got := sendRead(t, conn, r, "ZADD z notanumber a"); got != want {
		t.Errorf("ZADD z notanumber a = %q, want %q", got, want)
	}
}

func TestZAddOddArgsSyntaxError(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR syntax error"
	if got := sendRead(t, conn, r, "ZADD z 1 a 2"); got != want {
		t.Errorf("ZADD z 1 a 2 (odd) = %q, want %q", got, want)
	}
}

func TestZAddSupportsInfinity(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z +inf hi -inf lo")
	if got, want := sendRead(t, conn, r, "ZSCORE z hi"), "$inf"; got != want {
		t.Errorf("ZSCORE z hi = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "ZSCORE z lo"), "$-inf"; got != want {
		t.Errorf("ZSCORE z lo = %q, want %q", got, want)
	}
}

// --- ZSCORE (requirement 9.1) -----------------------------------------------

func TestZScoreMissingMemberAndKey(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// Absent key => null bulk.
	if got, want := sendRead(t, conn, r, "ZSCORE absent m"), "$-1"; got != want {
		t.Errorf("ZSCORE absent m = %q, want %q", got, want)
	}
	sendRead(t, conn, r, "ZADD z 1 a")
	// Present key, absent member => null bulk.
	if got, want := sendRead(t, conn, r, "ZSCORE z nope"), "$-1"; got != want {
		t.Errorf("ZSCORE z nope = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "ZSCORE z a"), "$1"; got != want {
		t.Errorf("ZSCORE z a = %q, want %q", got, want)
	}
}

// --- ZINCRBY (requirements 9.1, 9.7) ----------------------------------------

func TestZIncrBy(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))

	// Incrementing a missing member creates it from 0 and bumps cnt.
	if got, want := sendRead(t, conn, r, "ZINCRBY z 5 a"), "$5"; got != want {
		t.Errorf("ZINCRBY z 5 a (new) = %q, want %q", got, want)
	}
	if got, want := store.metas["0:z"].Count, int64(1); got != want {
		t.Errorf("meta.cnt after new member = %d, want %d", got, want)
	}
	// Incrementing an existing member does not change cnt.
	if got, want := sendRead(t, conn, r, "ZINCRBY z 2.5 a"), "$7.5"; got != want {
		t.Errorf("ZINCRBY z 2.5 a (=5) = %q, want %q", got, want)
	}
	if got, want := store.metas["0:z"].Count, int64(1); got != want {
		t.Errorf("meta.cnt after existing increment = %d, want %d", got, want)
	}
}

func TestZIncrByInvalidIncrement(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR value is not a valid float"
	if got := sendRead(t, conn, r, "ZINCRBY z abc a"); got != want {
		t.Errorf("ZINCRBY z abc a = %q, want %q", got, want)
	}
}

// --- ZRANGE / ZREVRANGE (requirement 9.1) -----------------------------------

func TestZRangeAscendingAndWithScores(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c")

	send(t, conn, "ZRANGE z 0 -1")
	got := readArray(t, r)
	want := []string{"$a", "$b", "$c"}
	assertArray(t, "ZRANGE z 0 -1", got, want)

	send(t, conn, "ZRANGE z 0 -1 WITHSCORES")
	got = readArray(t, r)
	want = []string{"$a", "$1", "$b", "$2", "$c", "$3"}
	assertArray(t, "ZRANGE z 0 -1 WITHSCORES", got, want)
}

func TestZRangeNegativeIndices(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c 4 d")

	// Last two members.
	send(t, conn, "ZRANGE z -2 -1")
	assertArray(t, "ZRANGE z -2 -1", readArray(t, r), []string{"$c", "$d"})

	// A start past stop yields an empty array.
	send(t, conn, "ZRANGE z 3 1")
	assertArray(t, "ZRANGE z 3 1", readArray(t, r), []string{})
}

func TestZRevRange(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c")

	send(t, conn, "ZREVRANGE z 0 -1")
	assertArray(t, "ZREVRANGE z 0 -1", readArray(t, r), []string{"$c", "$b", "$a"})

	send(t, conn, "ZREVRANGE z 0 1 WITHSCORES")
	assertArray(t, "ZREVRANGE z 0 1 WITHSCORES", readArray(t, r), []string{"$c", "$3", "$b", "$2"})
}

func TestZRangeTiesOrderedByMember(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	// Equal scores => members ordered lexicographically.
	sendRead(t, conn, r, "ZADD z 1 c 1 a 1 b")
	send(t, conn, "ZRANGE z 0 -1")
	assertArray(t, "ZRANGE z 0 -1 (ties)", readArray(t, r), []string{"$a", "$b", "$c"})
}

func TestZRangeAbsentKeyIsEmptyArray(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	send(t, conn, "ZRANGE absent 0 -1")
	assertArray(t, "ZRANGE absent", readArray(t, r), []string{})
}

func TestZRangeSyntaxError(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a")
	want := "-ERR syntax error"
	if got := sendRead(t, conn, r, "ZRANGE z 0 -1 BOGUS"); got != want {
		t.Errorf("ZRANGE z 0 -1 BOGUS = %q, want %q", got, want)
	}
}

// --- ZRANGEBYSCORE / ZREVRANGEBYSCORE (requirement 9.1) ----------------------

func TestZRangeByScoreInclusive(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c 4 d")

	send(t, conn, "ZRANGEBYSCORE z 2 3")
	assertArray(t, "ZRANGEBYSCORE z 2 3", readArray(t, r), []string{"$b", "$c"})
}

func TestZRangeByScoreExclusive(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c 4 d")

	// Exclusive on both ends: (1,(4 => scores 2 and 3.
	send(t, conn, "ZRANGEBYSCORE z (1 (4")
	assertArray(t, "ZRANGEBYSCORE z (1 (4", readArray(t, r), []string{"$b", "$c"})
}

func TestZRangeByScoreInfinities(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c")

	send(t, conn, "ZRANGEBYSCORE z -inf +inf WITHSCORES")
	assertArray(t, "ZRANGEBYSCORE z -inf +inf WITHSCORES", readArray(t, r),
		[]string{"$a", "$1", "$b", "$2", "$c", "$3"})
}

func TestZRevRangeByScore(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c 4 d")

	// ZREVRANGEBYSCORE takes (max, min); descending order.
	send(t, conn, "ZREVRANGEBYSCORE z 3 2")
	assertArray(t, "ZREVRANGEBYSCORE z 3 2", readArray(t, r), []string{"$c", "$b"})
}

func TestZRangeByScoreBadBound(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	want := "-ERR min or max is not a float"
	if got := sendRead(t, conn, r, "ZRANGEBYSCORE z foo 3"); got != want {
		t.Errorf("ZRANGEBYSCORE z foo 3 = %q, want %q", got, want)
	}
}

// --- ZRANK / ZREVRANK (requirement 9.1) -------------------------------------

func TestZRankAndZRevRank(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c")

	if got, want := sendRead(t, conn, r, "ZRANK z a"), ":0"; got != want {
		t.Errorf("ZRANK z a = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "ZRANK z c"), ":2"; got != want {
		t.Errorf("ZRANK z c = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "ZREVRANK z c"), ":0"; got != want {
		t.Errorf("ZREVRANK z c = %q, want %q", got, want)
	}
	// Missing member => null bulk.
	if got, want := sendRead(t, conn, r, "ZRANK z absent"), "$-1"; got != want {
		t.Errorf("ZRANK z absent = %q, want %q", got, want)
	}
}

// --- ZCOUNT (requirement 9.1) -----------------------------------------------

func TestZCount(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c 4 d")

	if got, want := sendRead(t, conn, r, "ZCOUNT z 2 3"), ":2"; got != want {
		t.Errorf("ZCOUNT z 2 3 = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "ZCOUNT z (2 4"), ":2"; got != want {
		t.Errorf("ZCOUNT z (2 4 = %q, want %q", got, want)
	}
	if got, want := sendRead(t, conn, r, "ZCOUNT z -inf +inf"), ":4"; got != want {
		t.Errorf("ZCOUNT z -inf +inf = %q, want %q", got, want)
	}
	// Absent key => 0.
	if got, want := sendRead(t, conn, r, "ZCOUNT absent 0 10"), ":0"; got != want {
		t.Errorf("ZCOUNT absent = %q, want %q", got, want)
	}
}

// --- ZREM + count maintenance + empty-key deletion (requirements 9.1, 9.7) ---

func TestZRemMaintainsCount(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c")

	if got, want := sendRead(t, conn, r, "ZREM z a x"), ":1"; got != want {
		t.Errorf("ZREM z a x = %q, want %q (only a existed)", got, want)
	}
	if got, want := sendRead(t, conn, r, "ZCARD z"), ":2"; got != want {
		t.Errorf("ZCARD z after ZREM = %q, want %q", got, want)
	}
	if got, want := store.metas["0:z"].Count, int64(2); got != want {
		t.Errorf("meta.cnt = %d, want %d", got, want)
	}
}

func TestZRemLastMemberDeletesKey(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a")

	sendRead(t, conn, r, "ZREM z a")
	// The key must be gone (an empty sorted set does not exist).
	if store.live["0:z"] {
		t.Error("key 0:z should be deleted after its last member is removed")
	}
	if got, want := sendRead(t, conn, r, "ZCARD z"), ":0"; got != want {
		t.Errorf("ZCARD z after emptying = %q, want %q", got, want)
	}
}

func TestZRemAbsentKeyIsZero(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	if got, want := sendRead(t, conn, r, "ZREM absent m"), ":0"; got != want {
		t.Errorf("ZREM absent m = %q, want %q", got, want)
	}
}

// --- ZREMRANGEBYRANK / ZREMRANGEBYSCORE (requirements 9.1, 9.7) --------------

func TestZRemRangeByRank(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c 4 d")

	// Remove the two lowest-ranked members.
	if got, want := sendRead(t, conn, r, "ZREMRANGEBYRANK z 0 1"), ":2"; got != want {
		t.Errorf("ZREMRANGEBYRANK z 0 1 = %q, want %q", got, want)
	}
	if got, want := store.metas["0:z"].Count, int64(2); got != want {
		t.Errorf("meta.cnt = %d, want %d", got, want)
	}
	send(t, conn, "ZRANGE z 0 -1")
	assertArray(t, "ZRANGE z 0 -1 after ZREMRANGEBYRANK", readArray(t, r), []string{"$c", "$d"})
}

func TestZRemRangeByScore(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a 2 b 3 c 4 d")

	// Remove scores in [2,3].
	if got, want := sendRead(t, conn, r, "ZREMRANGEBYSCORE z 2 3"), ":2"; got != want {
		t.Errorf("ZREMRANGEBYSCORE z 2 3 = %q, want %q", got, want)
	}
	send(t, conn, "ZRANGE z 0 -1")
	assertArray(t, "ZRANGE z 0 -1 after ZREMRANGEBYSCORE", readArray(t, r), []string{"$a", "$d"})
}

func TestZRemRangeByScoreDeletesEmptiedKey(t *testing.T) {
	store := newFakeStringStore()
	conn, r := startStringServer(t, store, fixedNow(1000))
	sendRead(t, conn, r, "ZADD z 1 a 2 b")

	sendRead(t, conn, r, "ZREMRANGEBYSCORE z -inf +inf")
	if store.live["0:z"] {
		t.Error("key 0:z should be deleted after ZREMRANGEBYSCORE removes all members")
	}
}

// --- WRONGTYPE (requirement 3.6) --------------------------------------------

func TestZSetWrongType(t *testing.T) {
	store := newFakeStringStore()
	// Seed a hash key so every ZSet command must reply WRONGTYPE.
	store.metas["0:k"] = storage.Meta{Type: string(meta.TypeHash)}
	store.live["0:k"] = true

	conn, r := startStringServer(t, store, fixedNow(1000))
	want := "-WRONGTYPE Operation against a key holding the wrong kind of value"
	cmds := []string{
		"ZADD k 1 a",
		"ZREM k a",
		"ZSCORE k a",
		"ZINCRBY k 1 a",
		"ZCARD k",
		"ZCOUNT k 0 1",
		"ZRANGE k 0 -1",
		"ZREVRANGE k 0 -1",
		"ZRANGEBYSCORE k 0 1",
		"ZREVRANGEBYSCORE k 1 0",
		"ZRANK k a",
		"ZREVRANK k a",
		"ZREMRANGEBYRANK k 0 -1",
		"ZREMRANGEBYSCORE k 0 1",
	}
	for _, cmd := range cmds {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// --- arity (requirement 3.2) ------------------------------------------------

func TestZSetArityErrors(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	cases := map[string]string{
		"ZADD z 1":             "-ERR wrong number of arguments for 'zadd' command",
		"ZREM z":               "-ERR wrong number of arguments for 'zrem' command",
		"ZSCORE z":             "-ERR wrong number of arguments for 'zscore' command",
		"ZINCRBY z 1":          "-ERR wrong number of arguments for 'zincrby' command",
		"ZCARD":                "-ERR wrong number of arguments for 'zcard' command",
		"ZCOUNT z 0":           "-ERR wrong number of arguments for 'zcount' command",
		"ZRANGE z 0":           "-ERR wrong number of arguments for 'zrange' command",
		"ZRANK z":              "-ERR wrong number of arguments for 'zrank' command",
		"ZREMRANGEBYRANK z 0":  "-ERR wrong number of arguments for 'zremrangebyrank' command",
		"ZREMRANGEBYSCORE z 0": "-ERR wrong number of arguments for 'zremrangebyscore' command",
	}
	for cmd, want := range cases {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// assertArray compares a rendered RESP array against the expected elements.
func assertArray(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s returned %d elems %v, want %d %v", label, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s[%d] = %q, want %q", label, i, got[i], want[i])
		}
	}
}
