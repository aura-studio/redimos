package integration

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Property-based invariant tests: drive randomized command sequences at the real proxy
// and, after every mutation, assert the observable state matches an in-test reference
// model. A fixed seed keeps failures reproducible (printed on failure). These exercise the
// full proxy -> redimo -> DynamoDB path, so they are gated on REDIMOS_PROXY_ADDR like the
// other integration tests.

const propSeed = 20260705

// parseBulkArray decodes a RESP2 array-of-bulk-strings reply into a []string. A nil
// element ($-1) decodes to "". It fatals on a non-array reply.
func parseBulkArray(t *testing.T, reply []byte) []string {
	t.Helper()
	r := bufio.NewReader(bytes.NewReader(reply))

	line, err := r.ReadString('\n')
	if err != nil || len(line) == 0 || line[0] != '*' {
		t.Fatalf("expected array reply, got %q", reply)
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		t.Fatalf("bad array header %q: %v", line, err)
	}
	if n < 0 {
		return nil
	}

	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil || len(hdr) == 0 || hdr[0] != '$' {
			t.Fatalf("expected bulk header, got %q (reply %q)", hdr, reply)
		}
		l, err := strconv.Atoi(strings.TrimSpace(hdr[1:]))
		if err != nil {
			t.Fatalf("bad bulk header %q: %v", hdr, err)
		}
		if l < 0 {
			out = append(out, "")
			continue
		}
		buf := make([]byte, l+2) // include trailing CRLF
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatalf("short bulk body: %v", err)
		}
		out = append(out, string(buf[:l]))
	}

	return out
}

// TestPropertyListOrder: after any sequence of RPUSH/LPUSH/LPOP/RPOP/LSET, LRANGE 0 -1
// must equal the reference list — i.e. redimo/redimos preserve list order exactly.
func TestPropertyListOrder(t *testing.T) {
	p := dial(t, proxyAddr(t))
	rng := rand.New(rand.NewSource(propSeed))
	key := bs(fmt.Sprintf("prop:list:%s", nonce()))
	p.do(bs("DEL"), key)

	var model []string
	var trace []string

	for i := 0; i < 300; i++ {
		var op string
		switch rng.Intn(5) {
		case 0: // RPUSH
			v := fmt.Sprintf("v%d", rng.Intn(40))
			op = "RPUSH " + v
			p.do(bs("RPUSH"), key, bs(v))
			model = append(model, v)
		case 1: // LPUSH
			v := fmt.Sprintf("v%d", rng.Intn(40))
			op = "LPUSH " + v
			p.do(bs("LPUSH"), key, bs(v))
			model = append([]string{v}, model...)
		case 2: // LPOP
			op = "LPOP"
			p.do(bs("LPOP"), key)
			if len(model) > 0 {
				model = model[1:]
			}
		case 3: // RPOP
			op = "RPOP"
			p.do(bs("RPOP"), key)
			if len(model) > 0 {
				model = model[:len(model)-1]
			}
		case 4: // LSET (only when non-empty)
			if len(model) > 0 {
				idx := rng.Intn(len(model))
				v := fmt.Sprintf("s%d", rng.Intn(40))
				op = fmt.Sprintf("LSET %d %s", idx, v)
				p.do(bs("LSET"), key, bs(strconv.Itoa(idx)), bs(v))
				model[idx] = v
			} else {
				op = "LSET(skip-empty)"
			}
		}
		trace = append(trace, op)

		got := parseBulkArray(t, p.do(bs("LRANGE"), key, bs("0"), bs("-1")))
		if !equalStrings(got, model) {
			from := 0
			if len(trace) > 8 {
				from = len(trace) - 8
			}
			t.Fatalf("op %d (seed %d): LRANGE = %v, want %v\nrecent ops: %v", i, propSeed, got, model, trace[from:])
		}
	}
}

// TestPropertySetUniqueness: after any sequence of SADD/SREM, SMEMBERS (sorted) must equal
// the reference set (sorted) — membership is exact and never duplicated.
func TestPropertySetUniqueness(t *testing.T) {
	p := dial(t, proxyAddr(t))
	rng := rand.New(rand.NewSource(propSeed + 1))
	key := bs(fmt.Sprintf("prop:set:%s", nonce()))
	p.do(bs("DEL"), key)

	model := map[string]struct{}{}

	for i := 0; i < 300; i++ {
		v := fmt.Sprintf("m%d", rng.Intn(30))
		if rng.Intn(2) == 0 {
			p.do(bs("SADD"), key, bs(v))
			model[v] = struct{}{}
		} else {
			p.do(bs("SREM"), key, bs(v))
			delete(model, v)
		}

		got := parseBulkArray(t, p.do(bs("SMEMBERS"), key))
		sort.Strings(got)
		want := make([]string, 0, len(model))
		for m := range model {
			want = append(want, m)
		}
		sort.Strings(want)

		if !equalStrings(got, want) {
			t.Fatalf("op %d (seed %d): SMEMBERS = %v, want %v", i, propSeed+1, got, want)
		}
	}
}

// TestPropertyZSetScoreOrder: after any sequence of ZADD/ZREM, ZRANGE 0 -1 must list
// members in ascending (score, member) order and contain exactly the reference members.
func TestPropertyZSetScoreOrder(t *testing.T) {
	p := dial(t, proxyAddr(t))
	rng := rand.New(rand.NewSource(propSeed + 2))
	key := bs(fmt.Sprintf("prop:zset:%s", nonce()))
	p.do(bs("DEL"), key)

	scores := map[string]int{}

	for i := 0; i < 300; i++ {
		v := fmt.Sprintf("z%d", rng.Intn(30))
		if rng.Intn(2) == 0 {
			s := rng.Intn(10)
			p.do(bs("ZADD"), key, bs(strconv.Itoa(s)), bs(v))
			scores[v] = s
		} else {
			p.do(bs("ZREM"), key, bs(v))
			delete(scores, v)
		}

		got := parseBulkArray(t, p.do(bs("ZRANGE"), key, bs("0"), bs("-1")))

		want := make([]string, 0, len(scores))
		for m := range scores {
			want = append(want, m)
		}
		sort.Slice(want, func(a, b int) bool {
			if scores[want[a]] != scores[want[b]] {
				return scores[want[a]] < scores[want[b]]
			}
			return want[a] < want[b] // score tie broken by member, matching Redis
		})

		if !equalStrings(got, want) {
			t.Fatalf("op %d (seed %d): ZRANGE = %v, want %v", i, propSeed+2, got, want)
		}
	}
}

// nonce returns a per-run-unique key suffix so a test never observes members left
// by a prior run (DEL reclaims members asynchronously, so a reused key can briefly
// retain stale data). The op sequence stays deterministic via the fixed seed.
func nonce() string { return strconv.FormatInt(time.Now().UnixNano(), 36) }

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
