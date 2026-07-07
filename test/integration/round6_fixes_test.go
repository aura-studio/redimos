package integration

import (
	"strings"
	"testing"
)

// Regression tests for the 2026-07-07 round-6 adversarial pass — a regression audit of
// the round-4/5 fixes plus connection/formatting depth. Each byte-diffs against the
// live redis:3.2 oracle. (The HELLO-under-requirepass fix needs a requirepass instance
// and is covered by a unit test in internal/command instead.)

// TestFixIncrByFloatFormatting: INCRBYFLOAT/HINCRBYFLOAT format their reply (and the
// stored value) with Redis' ld2string(LD_STR_HUMAN) = %.17f + trim — 17 FIXED decimal
// places, not the shortest round-tripping form. Tiny magnitudes therefore collapse
// (1e-20 -> "0") instead of printing 20 decimal places.
func TestFixIncrByFloatFormatting(t *testing.T) {
	d := newDiffer(t)
	d.eq("INCRBYFLOAT 1e-20 -> 0", bs("INCRBYFLOAT"), d.k("f1"), bs("1e-20"))
	d.eq("INCRBYFLOAT 9e-18 -> 17-place round", bs("INCRBYFLOAT"), d.k("f2"), bs("9e-18"))
	d.eq("INCRBYFLOAT 1.5e-17", bs("INCRBYFLOAT"), d.k("f3"), bs("1.5e-17"))
	d.eq("HINCRBYFLOAT 1e-20 -> 0", bs("HINCRBYFLOAT"), d.k("f4"), bs("x"), bs("1e-20"))
	// Ordinary magnitudes are unchanged by the formatting swap.
	d.eq("INCRBYFLOAT 0.5", bs("INCRBYFLOAT"), d.k("f5"), bs("0.5"))
	d.eq("INCRBYFLOAT 3.0 -> 3", bs("INCRBYFLOAT"), d.k("f6"), bs("3.0"))
	// The stored value round-trips through GET identically.
	k := d.k("f7")
	d.eq("INCRBYFLOAT 1e-20 store", bs("INCRBYFLOAT"), k, bs("1e-20"))
	d.eq("GET stored", bs("GET"), k)
	t.Logf("compared %d INCRBYFLOAT-format replies vs Redis 3.2", d.n)
}

// TestFixOversizedFieldMemberNoop: HGET/HMGET/HDEL/HEXISTS/HSTRLEN and
// ZSCORE/ZRANK/ZREVRANK/ZREM of a field/member too large to be stored (sort key past
// the DynamoDB limit) report it absent (nil / :0), matching Redis — extending the
// round-4 set-family short-circuit to hashes and zsets.
func TestFixOversizedFieldMemberNoop(t *testing.T) {
	d := newDiffer(t)
	big := bs(strings.Repeat("a", 2000))

	h := d.k("h")
	d.eq("HSET seed", bs("HSET"), h, bs("realf"), bs("v"))
	d.eq("HGET oversized -> nil", bs("HGET"), h, big)
	d.eq("HMGET oversized -> [nil]", bs("HMGET"), h, big)
	d.eq("HDEL oversized -> :0", bs("HDEL"), h, big)
	d.eq("HEXISTS oversized -> :0", bs("HEXISTS"), h, big)
	d.eq("HSTRLEN oversized -> :0", bs("HSTRLEN"), h, big)
	d.eq("HGET real field still works", bs("HGET"), h, bs("realf"))

	z := d.k("z")
	d.eq("ZADD seed", bs("ZADD"), z, bs("1"), bs("realm"))
	d.eq("ZSCORE oversized -> nil", bs("ZSCORE"), z, big)
	d.eq("ZRANK oversized -> nil", bs("ZRANK"), z, big)
	d.eq("ZREVRANK oversized -> nil", bs("ZREVRANK"), z, big)
	d.eq("ZREM oversized -> :0", bs("ZREM"), z, big)
	d.eq("ZSCORE real member still works", bs("ZSCORE"), z, bs("realm"))
	t.Logf("compared %d oversized-field/member replies vs Redis 3.2", d.n)
}

// TestFixTypeCheckBeforeSizeGuard: a live wrong-type key replies WRONGTYPE even when
// the value argument is oversized — Redis checks the type right after lookup, before
// any size consideration (which it does not have anyway).
func TestFixTypeCheckBeforeSizeGuard(t *testing.T) {
	d := newDiffer(t)
	big := bs(strings.Repeat("x", 400*1024))

	k1 := d.k("wt1")
	d.eq("SET string", bs("SET"), k1, bs("v"))
	d.eq("LPUSH wrong-type oversized -> WRONGTYPE", bs("LPUSH"), k1, big)
	d.eq("RPUSH wrong-type oversized -> WRONGTYPE", bs("RPUSH"), k1, big)

	k2 := d.k("wt2")
	d.eq("RPUSH list", bs("RPUSH"), k2, bs("a"))
	d.eq("SETRANGE wrong-type oversized -> WRONGTYPE", bs("SETRANGE"), k2, bs("400000"), bs("x"))
	d.eq("APPEND wrong-type oversized -> WRONGTYPE", bs("APPEND"), k2, big)
	t.Logf("compared %d type-before-size replies vs Redis 3.2", d.n)
}

// TestFixStubSubcommandErrors: SLOWLOG / COMMAND / CLIENT / CONFIG reject unknown
// subcommands and wrong per-subcommand arities with Redis 3.2's exact errors, while
// the benign probe stubs (CONFIG GET maxmemory, CLIENT LIST, COMMAND COUNT) survive.
func TestFixStubSubcommandErrors(t *testing.T) {
	d := newDiffer(t)

	d.eq("SLOWLOG HELP", bs("SLOWLOG"), bs("HELP"))
	d.eq("SLOWLOG BADSUB", bs("SLOWLOG"), bs("BADSUB"))
	d.eq("SLOWLOG LEN extra", bs("SLOWLOG"), bs("LEN"), bs("extra"))
	d.eq("SLOWLOG RESET extra", bs("SLOWLOG"), bs("RESET"), bs("extra"))
	d.eq("SLOWLOG RESET (valid)", bs("SLOWLOG"), bs("RESET"))

	d.eq("COMMAND BADSUB", bs("COMMAND"), bs("BADSUB"))
	d.eq("COMMAND COUNT extra", bs("COMMAND"), bs("COUNT"), bs("extra"))

	d.eq("CLIENT FOO", bs("CLIENT"), bs("FOO"))
	d.eq("CLIENT ID (5.0+ cmd)", bs("CLIENT"), bs("ID"))
	d.eq("CLIENT GETNAME extra", bs("CLIENT"), bs("GETNAME"), bs("extra"))
	// GETNAME runs BEFORE any successful SETNAME: redimos discards the name
	// (accepted probe stub), so a post-SETNAME GETNAME would legitimately differ.
	d.eq("CLIENT GETNAME (no name) -> nil", bs("CLIENT"), bs("GETNAME"))
	d.eq("CLIENT SETNAME with space", bs("CLIENT"), bs("SETNAME"), bs("a b"))
	d.eq("CLIENT SETNAME valid", bs("CLIENT"), bs("SETNAME"), bs("myname"))

	d.eq("CONFIG GET (no param)", bs("CONFIG"), bs("GET"))
	d.eq("CONFIG GET extra param", bs("CONFIG"), bs("GET"), bs("a"), bs("b"))
	d.eq("CONFIG SET missing value", bs("CONFIG"), bs("SET"), bs("maxmemory"))
	d.eq("CONFIG RESETSTAT extra", bs("CONFIG"), bs("RESETSTAT"), bs("x"))
	d.eq("CONFIG GET maxmemory (stub survives)", bs("CONFIG"), bs("GET"), bs("maxmemory"))
	t.Logf("compared %d stub-subcommand replies vs Redis 3.2", d.n)
}
