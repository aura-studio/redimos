package redimos_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/aura-studio/redimos"
)

// TestInProcessClient_V1Introspection locks in the redimo v1.7.0 read-only
// introspection surface end-to-end through the in-process go-redis client: SCAN
// enumerates the seeded keys, TYPE returns the correct type for each (set-vs-zset via
// the skN heuristic), TTL replies -1 for a live key / -2 for a missing one (the v1
// line has no expiry), and HSCAN/SSCAN/ZSCAN list a collection's members. Env-gated
// (REDIMOS_DDB_ENDPOINT + REDIMOS_DDB_TABLE) exactly like the other integration tests,
// so offline `go test ./...` stays green.
func TestInProcessClient_V1Introspection(t *testing.T) {
	endpoint, table := ddbEndpoint(t)
	ddb := newDDB(t, endpoint)

	client, closer, err := redimos.NewInProcessClient(ddb, redimos.Options{Table: table})
	if err != nil {
		t.Fatalf("NewInProcessClient: %v", err)
	}
	defer closer.Close()
	ctx := context.Background()

	ns := "v1intro:" + time.Now().Format("150405.000") + ":"
	k := func(s string) string { return ns + s }

	client.Do(ctx, "SET", k("str"), "hello")
	client.Do(ctx, "HMSET", k("hash"), "f1", "v1", "f2", "v2") // 3.2 HSET is single-field; HMSET is multi-field
	client.Do(ctx, "RPUSH", k("list"), "a", "b", "c")
	client.Do(ctx, "SADD", k("set"), "m1", "m2", "m3")
	client.Do(ctx, "ZADD", k("zset"), "1", "one", "2.5", "two")
	defer client.Do(ctx, "DEL", k("str"), k("hash"), k("list"), k("set"), k("zset"))

	// TYPE — correct for every type (set-vs-zset resolved by the skN heuristic).
	for suffix, want := range map[string]string{
		"str": "string", "hash": "hash", "list": "list", "set": "set", "zset": "zset",
	} {
		got, err := client.Type(ctx, k(suffix)).Result()
		if err != nil {
			t.Fatalf("TYPE %s: %v", suffix, err)
		}
		if got != want {
			t.Errorf("TYPE %s = %q, want %q", suffix, got, want)
		}
	}

	// SCAN enumerates every seeded key.
	found := map[string]bool{}
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, ns+"*", 100).Result()
		if err != nil {
			t.Fatalf("SCAN: %v", err)
		}
		for _, key := range keys {
			found[key] = true
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	var missing []string
	for _, s := range []string{"str", "hash", "list", "set", "zset"} {
		if !found[k(s)] {
			missing = append(missing, k(s))
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("SCAN did not enumerate: %v", missing)
	}

	// TTL: -1 for a live key (no expiry on the v1 line), -2 for a missing key.
	if ttl, _ := client.Do(ctx, "TTL", k("str")).Int(); ttl != -1 {
		t.Errorf("TTL live = %d, want -1", ttl)
	}
	if ttl, _ := client.Do(ctx, "TTL", k("absent")).Int(); ttl != -2 {
		t.Errorf("TTL missing = %d, want -2", ttl)
	}

	// Collection scans return members (flattened field/value and member/score pairs).
	if hf, _, err := client.HScan(ctx, k("hash"), 0, "", 100).Result(); err != nil || len(hf) != 4 {
		t.Errorf("HSCAN = %v (len %d), err %v", hf, len(hf), err)
	}
	if sm, _, err := client.SScan(ctx, k("set"), 0, "", 100).Result(); err != nil || len(sm) != 3 {
		t.Errorf("SSCAN = %v (len %d), err %v", sm, len(sm), err)
	}
	if zm, _, err := client.ZScan(ctx, k("zset"), 0, "", 100).Result(); err != nil || len(zm) != 4 {
		t.Errorf("ZSCAN = %v (len %d), err %v", zm, len(zm), err)
	}
}
