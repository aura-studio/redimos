package redimos_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aura-studio/redimos"
)

// TestCommandCoverage probes, via the in-process go-redis client, which Redis
// commands are USABLE vs NOT-USABLE on this redimos build. It is env-gated exactly
// like the embedding tests (ddbEndpoint skips unless REDIMOS_DDB_ENDPOINT +
// REDIMOS_DDB_TABLE are set), so `go test ./...` stays green offline.
//
// Method: build the in-process client, seed correctly-typed keys so a genuine command
// never trips on wrong-arity/wrong-type, send each probe via client.Do, and classify
// the reply. NOT-USABLE iff the reply is an error whose text matches (case-insensitive)
// a "not implemented / by-design rejected" marker (see classify); anything else —
// success, or a benign per-value error like WRONGTYPE/syntax — is USABLE.
//
// The two lists (grouped by family, NOT-USABLE annotated with the exact reply error)
// are emitted with t.Logf; they are the source for doc/command-coverage.md.
func TestCommandCoverage(t *testing.T) {
	endpoint, table := ddbEndpoint(t)
	ddb := newDDB(t, endpoint)

	client, closer, err := redimos.NewInProcessClient(ddb, redimos.Options{Table: table, MultiDB: true})
	if err != nil {
		t.Fatalf("NewInProcessClient: %v", err)
	}
	defer closer.Close()

	ctx := context.Background()
	// A unique run nonce so the probe is idempotent and never collides with real data.
	ns := "cov:" + time.Now().Format("20060102150405.000") + ":"
	k := func(suffix string) string { return ns + suffix }

	// --- seed correctly-typed keys so genuine commands don't fail on type/arity -----
	// Best-effort: seeding errors are ignored (a gated command's seed simply no-ops),
	// the probe still classifies from the command's own reply.
	seed := func(args ...interface{}) { _ = client.Do(ctx, args...).Err() }
	seed("SET", k("str"), "hello")
	seed("SET", k("str2"), "world")
	seed("SET", k("num"), "10")
	seed("SET", k("floatnum"), "3.14")
	seed("SET", k("bits"), "foobar")
	seed("HSET", k("hash"), "f1", "v1")
	seed("HSET", k("hash"), "f2", "v2")
	seed("RPUSH", k("list"), "a", "b", "c")
	seed("RPUSH", k("list2"), "x")
	seed("SADD", k("set"), "m1", "m2", "m3")
	seed("SADD", k("set2"), "m2", "m3", "m4")
	seed("ZADD", k("zset"), "1", "one", "2", "two", "3", "three")
	seed("ZADD", k("zset2"), "1", "two", "2", "four")
	seed("GEOADD", k("geo"), "13.361389", "38.115556", "Palermo")
	seed("GEOADD", k("geo"), "15.087269", "37.502669", "Catania")
	seed("PFADD", k("hll"), "a", "b", "c")
	seed("PFADD", k("hll2"), "c", "d", "e")
	seed("SET", k("del1"), "v")
	seed("SET", k("del2"), "v")
	seed("SET", k("ttlk"), "v")
	seed("SET", k("expk"), "v")
	seed("SET", k("perk"), "v")
	seed("SET", k("renk"), "v")
	seed("SET", k("copysrc"), "v")
	seed("SET", k("typek"), "v")

	type probe struct {
		family string
		name   string
		args   []interface{}
	}

	probes := []probe{
		// --- strings ---
		{"strings", "SET", []interface{}{"SET", k("s.set"), "v"}},
		{"strings", "GET", []interface{}{"GET", k("str")}},
		{"strings", "GETSET", []interface{}{"GETSET", k("s.getset"), "v"}},
		{"strings", "SETNX", []interface{}{"SETNX", k("s.setnx"), "v"}},
		{"strings", "SETEX", []interface{}{"SETEX", k("s.setex"), "100", "v"}},
		{"strings", "PSETEX", []interface{}{"PSETEX", k("s.psetex"), "100000", "v"}},
		{"strings", "MSET", []interface{}{"MSET", k("s.mset1"), "v1", k("s.mset2"), "v2"}},
		{"strings", "MSETNX", []interface{}{"MSETNX", k("s.msetnx1"), "v1", k("s.msetnx2"), "v2"}},
		{"strings", "MGET", []interface{}{"MGET", k("str"), k("str2")}},
		{"strings", "APPEND", []interface{}{"APPEND", k("s.append"), "abc"}},
		{"strings", "STRLEN", []interface{}{"STRLEN", k("str")}},
		{"strings", "SETRANGE", []interface{}{"SETRANGE", k("s.setrange"), "0", "abc"}},
		{"strings", "GETRANGE", []interface{}{"GETRANGE", k("str"), "0", "2"}},
		{"strings", "SUBSTR", []interface{}{"SUBSTR", k("str"), "0", "2"}},
		{"strings", "SETBIT", []interface{}{"SETBIT", k("s.setbit"), "7", "1"}},
		{"strings", "GETBIT", []interface{}{"GETBIT", k("bits"), "0"}},
		{"strings", "BITCOUNT", []interface{}{"BITCOUNT", k("bits")}},
		{"strings", "BITPOS", []interface{}{"BITPOS", k("bits"), "1"}},
		{"strings", "BITOP", []interface{}{"BITOP", "AND", k("s.bitop"), k("bits"), k("str")}},
		{"strings", "BITFIELD", []interface{}{"BITFIELD", k("s.bitfield"), "GET", "u8", "0"}},
		{"strings", "INCR", []interface{}{"INCR", k("s.incr")}},
		{"strings", "DECR", []interface{}{"DECR", k("s.decr")}},
		{"strings", "INCRBY", []interface{}{"INCRBY", k("s.incrby"), "5"}},
		{"strings", "DECRBY", []interface{}{"DECRBY", k("s.decrby"), "5"}},
		{"strings", "INCRBYFLOAT", []interface{}{"INCRBYFLOAT", k("s.incrbyfloat"), "1.5"}},
		{"strings", "GETDEL", []interface{}{"GETDEL", k("str2")}},
		{"strings", "GETEX", []interface{}{"GETEX", k("str")}},

		// --- hashes ---
		{"hashes", "HSET", []interface{}{"HSET", k("h.hset"), "f", "v"}},
		{"hashes", "HGET", []interface{}{"HGET", k("hash"), "f1"}},
		{"hashes", "HMSET", []interface{}{"HMSET", k("h.hmset"), "f1", "v1", "f2", "v2"}},
		{"hashes", "HMGET", []interface{}{"HMGET", k("hash"), "f1", "f2"}},
		{"hashes", "HGETALL", []interface{}{"HGETALL", k("hash")}},
		{"hashes", "HKEYS", []interface{}{"HKEYS", k("hash")}},
		{"hashes", "HVALS", []interface{}{"HVALS", k("hash")}},
		{"hashes", "HLEN", []interface{}{"HLEN", k("hash")}},
		{"hashes", "HDEL", []interface{}{"HDEL", k("hash"), "nope"}},
		{"hashes", "HEXISTS", []interface{}{"HEXISTS", k("hash"), "f1"}},
		{"hashes", "HINCRBY", []interface{}{"HINCRBY", k("h.hincrby"), "n", "3"}},
		{"hashes", "HINCRBYFLOAT", []interface{}{"HINCRBYFLOAT", k("h.hincrbyfloat"), "n", "1.5"}},
		{"hashes", "HSETNX", []interface{}{"HSETNX", k("h.hsetnx"), "f", "v"}},
		{"hashes", "HSTRLEN", []interface{}{"HSTRLEN", k("hash"), "f1"}},
		{"hashes", "HSCAN", []interface{}{"HSCAN", k("hash"), "0"}},
		{"hashes", "HRANDFIELD", []interface{}{"HRANDFIELD", k("hash")}},

		// --- lists ---
		{"lists", "LPUSH", []interface{}{"LPUSH", k("l.lpush"), "a"}},
		{"lists", "RPUSH", []interface{}{"RPUSH", k("l.rpush"), "a"}},
		{"lists", "LPUSHX", []interface{}{"LPUSHX", k("list"), "z"}},
		{"lists", "RPUSHX", []interface{}{"RPUSHX", k("list"), "z"}},
		{"lists", "LPOP", []interface{}{"LPOP", k("l.lpop.seed")}},
		{"lists", "RPOP", []interface{}{"RPOP", k("l.rpop.seed")}},
		{"lists", "LLEN", []interface{}{"LLEN", k("list")}},
		{"lists", "LINDEX", []interface{}{"LINDEX", k("list"), "0"}},
		{"lists", "LRANGE", []interface{}{"LRANGE", k("list"), "0", "-1"}},
		{"lists", "LSET", []interface{}{"LSET", k("list"), "0", "A"}},
		{"lists", "LTRIM", []interface{}{"LTRIM", k("list"), "0", "-1"}},
		{"lists", "LREM", []interface{}{"LREM", k("list"), "0", "nope"}},
		{"lists", "LINSERT", []interface{}{"LINSERT", k("list"), "BEFORE", "b", "bb"}},
		{"lists", "RPOPLPUSH", []interface{}{"RPOPLPUSH", k("list2"), k("l.rpoplpush.dst")}},
		{"lists", "LMOVE", []interface{}{"LMOVE", k("list"), k("l.lmove.dst"), "LEFT", "RIGHT"}},
		{"lists", "BLPOP", []interface{}{"BLPOP", k("list"), "0"}},
		{"lists", "BRPOP", []interface{}{"BRPOP", k("list"), "0"}},

		// --- sets ---
		{"sets", "SADD", []interface{}{"SADD", k("se.sadd"), "a"}},
		{"sets", "SREM", []interface{}{"SREM", k("set"), "nope"}},
		{"sets", "SCARD", []interface{}{"SCARD", k("set")}},
		{"sets", "SISMEMBER", []interface{}{"SISMEMBER", k("set"), "m1"}},
		{"sets", "SMEMBERS", []interface{}{"SMEMBERS", k("set")}},
		{"sets", "SPOP", []interface{}{"SPOP", k("se.spop.seed")}},
		{"sets", "SRANDMEMBER", []interface{}{"SRANDMEMBER", k("set")}},
		{"sets", "SMOVE", []interface{}{"SMOVE", k("set"), k("se.smove.dst"), "m1"}},
		{"sets", "SINTER", []interface{}{"SINTER", k("set"), k("set2")}},
		{"sets", "SINTERSTORE", []interface{}{"SINTERSTORE", k("se.inter.dst"), k("set"), k("set2")}},
		{"sets", "SUNION", []interface{}{"SUNION", k("set"), k("set2")}},
		{"sets", "SUNIONSTORE", []interface{}{"SUNIONSTORE", k("se.union.dst"), k("set"), k("set2")}},
		{"sets", "SDIFF", []interface{}{"SDIFF", k("set"), k("set2")}},
		{"sets", "SDIFFSTORE", []interface{}{"SDIFFSTORE", k("se.diff.dst"), k("set"), k("set2")}},
		{"sets", "SSCAN", []interface{}{"SSCAN", k("set"), "0"}},
		{"sets", "SMISMEMBER", []interface{}{"SMISMEMBER", k("set"), "m1", "nope"}},

		// --- zsets ---
		{"zsets", "ZADD", []interface{}{"ZADD", k("z.zadd"), "1", "a"}},
		{"zsets", "ZREM", []interface{}{"ZREM", k("zset"), "nope"}},
		{"zsets", "ZCARD", []interface{}{"ZCARD", k("zset")}},
		{"zsets", "ZSCORE", []interface{}{"ZSCORE", k("zset"), "one"}},
		{"zsets", "ZINCRBY", []interface{}{"ZINCRBY", k("zset"), "1", "one"}},
		{"zsets", "ZCOUNT", []interface{}{"ZCOUNT", k("zset"), "-inf", "+inf"}},
		{"zsets", "ZRANK", []interface{}{"ZRANK", k("zset"), "one"}},
		{"zsets", "ZREVRANK", []interface{}{"ZREVRANK", k("zset"), "one"}},
		{"zsets", "ZRANGE", []interface{}{"ZRANGE", k("zset"), "0", "-1"}},
		{"zsets", "ZREVRANGE", []interface{}{"ZREVRANGE", k("zset"), "0", "-1"}},
		{"zsets", "ZRANGEBYSCORE", []interface{}{"ZRANGEBYSCORE", k("zset"), "-inf", "+inf"}},
		{"zsets", "ZREVRANGEBYSCORE", []interface{}{"ZREVRANGEBYSCORE", k("zset"), "+inf", "-inf"}},
		{"zsets", "ZRANGEBYLEX", []interface{}{"ZRANGEBYLEX", k("zset"), "-", "+"}},
		{"zsets", "ZREVRANGEBYLEX", []interface{}{"ZREVRANGEBYLEX", k("zset"), "+", "-"}},
		{"zsets", "ZLEXCOUNT", []interface{}{"ZLEXCOUNT", k("zset"), "-", "+"}},
		{"zsets", "ZREMRANGEBYRANK", []interface{}{"ZREMRANGEBYRANK", k("z.remrank"), "0", "0"}},
		{"zsets", "ZREMRANGEBYSCORE", []interface{}{"ZREMRANGEBYSCORE", k("z.remscore"), "-inf", "-inf"}},
		{"zsets", "ZREMRANGEBYLEX", []interface{}{"ZREMRANGEBYLEX", k("z.remlex"), "[nope", "[nope"}},
		{"zsets", "ZPOPMIN", []interface{}{"ZPOPMIN", k("z.popmin.seed")}},
		{"zsets", "ZPOPMAX", []interface{}{"ZPOPMAX", k("z.popmax.seed")}},
		{"zsets", "ZUNIONSTORE", []interface{}{"ZUNIONSTORE", k("z.union.dst"), "2", k("zset"), k("zset2")}},
		{"zsets", "ZINTERSTORE", []interface{}{"ZINTERSTORE", k("z.inter.dst"), "2", k("zset"), k("zset2")}},
		{"zsets", "ZSCAN", []interface{}{"ZSCAN", k("zset"), "0"}},
		{"zsets", "ZMSCORE", []interface{}{"ZMSCORE", k("zset"), "one", "nope"}},
		{"zsets", "ZRANDMEMBER", []interface{}{"ZRANDMEMBER", k("zset")}},

		// --- geo ---
		{"geo", "GEOADD", []interface{}{"GEOADD", k("g.geoadd"), "13.361389", "38.115556", "P"}},
		{"geo", "GEODIST", []interface{}{"GEODIST", k("geo"), "Palermo", "Catania"}},
		{"geo", "GEOPOS", []interface{}{"GEOPOS", k("geo"), "Palermo"}},
		{"geo", "GEOHASH", []interface{}{"GEOHASH", k("geo"), "Palermo"}},
		{"geo", "GEORADIUS", []interface{}{"GEORADIUS", k("geo"), "15", "37", "200", "km"}},
		{"geo", "GEORADIUSBYMEMBER", []interface{}{"GEORADIUSBYMEMBER", k("geo"), "Palermo", "200", "km"}},
		{"geo", "GEOSEARCH", []interface{}{"GEOSEARCH", k("geo"), "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "ASC"}},

		// --- generic / keys ---
		{"generic", "DEL", []interface{}{"DEL", k("del1")}},
		{"generic", "EXISTS", []interface{}{"EXISTS", k("del2")}},
		{"generic", "EXPIRE", []interface{}{"EXPIRE", k("expk"), "100"}},
		{"generic", "PEXPIRE", []interface{}{"PEXPIRE", k("expk"), "100000"}},
		{"generic", "EXPIREAT", []interface{}{"EXPIREAT", k("expk"), "9999999999"}},
		{"generic", "PEXPIREAT", []interface{}{"PEXPIREAT", k("expk"), "99999999999999"}},
		{"generic", "TTL", []interface{}{"TTL", k("ttlk")}},
		{"generic", "PTTL", []interface{}{"PTTL", k("ttlk")}},
		{"generic", "PERSIST", []interface{}{"PERSIST", k("perk")}},
		{"generic", "TYPE", []interface{}{"TYPE", k("typek")}},
		{"generic", "RENAME", []interface{}{"RENAME", k("renk"), k("g.rename.dst")}},
		{"generic", "RENAMENX", []interface{}{"RENAMENX", k("renk"), k("g.renamenx.dst")}},
		{"generic", "MOVE", []interface{}{"MOVE", k("typek"), "1"}},
		{"generic", "KEYS", []interface{}{"KEYS", ns + "*"}},
		{"generic", "SCAN", []interface{}{"SCAN", "0"}},
		{"generic", "RANDOMKEY", []interface{}{"RANDOMKEY"}},
		{"generic", "DUMP", []interface{}{"DUMP", k("typek")}},
		{"generic", "RESTORE", []interface{}{"RESTORE", k("g.restore.dst"), "0", "payload"}},
		{"generic", "OBJECT", []interface{}{"OBJECT", "ENCODING", k("typek")}},
		{"generic", "TOUCH", []interface{}{"TOUCH", k("typek")}},
		{"generic", "UNLINK", []interface{}{"UNLINK", k("g.unlink.seed")}},
		{"generic", "COPY", []interface{}{"COPY", k("copysrc"), k("g.copy.dst")}},

		// --- hyperloglog ---
		{"hll", "PFADD", []interface{}{"PFADD", k("hll.pfadd"), "a"}},
		{"hll", "PFCOUNT", []interface{}{"PFCOUNT", k("hll")}},
		{"hll", "PFMERGE", []interface{}{"PFMERGE", k("hll.merge.dst"), k("hll"), k("hll2")}},

		// --- connection / server ---
		{"connection", "PING", []interface{}{"PING"}},
		{"connection", "ECHO", []interface{}{"ECHO", "hi"}},
		{"connection", "SELECT", []interface{}{"SELECT", "0"}},
		{"connection", "COMMAND", []interface{}{"COMMAND", "COUNT"}},
		{"connection", "INFO", []interface{}{"INFO"}},
		{"connection", "CLIENT", []interface{}{"CLIENT", "SETNAME", "probe"}},
		{"connection", "DBSIZE", []interface{}{"DBSIZE"}},
		{"connection", "FLUSHDB", []interface{}{"FLUSHDB"}},
		{"connection", "FLUSHALL", []interface{}{"FLUSHALL"}},
		{"connection", "CONFIG", []interface{}{"CONFIG", "GET", "maxmemory"}},
		{"connection", "TIME", []interface{}{"TIME"}},
		{"connection", "HELLO", []interface{}{"HELLO"}},

		// --- pubsub / txn / scripting (expected mostly not-usable) ---
		{"pubsub/txn/script", "SUBSCRIBE", []interface{}{"SUBSCRIBE", k("ch")}},
		{"pubsub/txn/script", "PUBLISH", []interface{}{"PUBLISH", k("ch"), "msg"}},
		{"pubsub/txn/script", "MULTI", []interface{}{"MULTI"}},
		{"pubsub/txn/script", "EXEC", []interface{}{"EXEC"}},
		{"pubsub/txn/script", "DISCARD", []interface{}{"DISCARD"}},
		{"pubsub/txn/script", "WATCH", []interface{}{"WATCH", k("str")}},
		{"pubsub/txn/script", "EVAL", []interface{}{"EVAL", "return 1", "0"}},
		{"pubsub/txn/script", "SCRIPT", []interface{}{"SCRIPT", "LOAD", "return 1"}},
	}

	// families preserves the emission order.
	familyOrder := []string{
		"strings", "hashes", "lists", "sets", "zsets", "geo",
		"generic", "hll", "connection", "pubsub/txn/script",
	}

	type result struct {
		name     string
		usable   bool
		reason   string // classification bucket for NOT-USABLE
		errText  string // raw reply error, if any
	}
	byFamily := map[string][]result{}

	for _, p := range probes {
		errText := ""
		if e := client.Do(ctx, p.args...).Err(); e != nil {
			errText = e.Error()
		}
		usable, reason := classify(errText)
		byFamily[p.family] = append(byFamily[p.family], result{
			name: p.name, usable: usable, reason: reason, errText: errText,
		})
	}

	// --- emit the two lists, grouped by family --------------------------------
	var usableB, notB strings.Builder
	usableTotal, notTotal := 0, 0
	perFamilyUsable := map[string]int{}
	perFamilyNot := map[string]int{}

	for _, fam := range familyOrder {
		rs := byFamily[fam]
		sort.Slice(rs, func(i, j int) bool { return rs[i].name < rs[j].name })
		var us, ns2 []string
		for _, r := range rs {
			if r.usable {
				us = append(us, r.name)
				usableTotal++
				perFamilyUsable[fam]++
			} else {
				ns2 = append(ns2, fmt.Sprintf("%s [%s: %s]", r.name, r.reason, r.errText))
				notTotal++
				perFamilyNot[fam]++
			}
		}
		if len(us) > 0 {
			fmt.Fprintf(&usableB, "  %-18s (%d): %s\n", fam, len(us), strings.Join(us, ", "))
		}
		if len(ns2) > 0 {
			fmt.Fprintf(&notB, "  %s (%d):\n", fam, len(ns2))
			for _, s := range ns2 {
				fmt.Fprintf(&notB, "    - %s\n", s)
			}
		}
	}

	t.Logf("COMMAND COVERAGE for table=%q (endpoint=%q)", table, endpoint)
	t.Logf("USABLE (%d):\n%s", usableTotal, usableB.String())
	t.Logf("NOT-USABLE (%d):\n%s", notTotal, notB.String())

	// --- best-effort cleanup of probe keys ------------------------------------
	// SCAN the namespace and DEL what we can. Cleanup failures are non-fatal.
	cleanupKeys := []interface{}{"DEL"}
	if keys, err := client.Keys(ctx, ns+"*").Result(); err == nil {
		for _, key := range keys {
			cleanupKeys = append(cleanupKeys, key)
		}
	}
	if len(cleanupKeys) > 1 {
		_ = client.Do(ctx, cleanupKeys...).Err()
	}
}

// classify decides USABLE vs NOT-USABLE from a reply's error text. An empty errText
// (the command succeeded) is USABLE. A non-empty errText is NOT-USABLE only when it
// matches a "not implemented / by-design rejected" marker; any other error (e.g.
// WRONGTYPE, a syntax quibble on an otherwise-supported command) counts as USABLE
// because the command IS routed and enforced — it just disliked these specific args.
func classify(errText string) (usable bool, reason string) {
	if errText == "" {
		return true, ""
	}
	low := strings.ToLower(errText)
	switch {
	case strings.Contains(low, "unknown command"):
		return false, "unknown command"
	case strings.Contains(low, "not supported"):
		return false, "by-design"
	case strings.Contains(low, "is disabled"):
		return false, "by-design"
	case strings.Contains(low, "invalid db index"):
		return false, "by-design"
	default:
		// A real, routed command that merely rejected these args (WRONGTYPE / syntax /
		// value error). It is usable.
		return true, ""
	}
}
