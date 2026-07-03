package command

import (
	"strings"
	"testing"

	"github.com/aura-studio/redimos/v2/internal/resp"
)

// unsupported_test.go is the guard for task 18.1 / requirement 4.1–4.8: the
// command families redimos deliberately does not support (see unsupported.go)
// must be explicitly REJECTED with an error and never silently accepted.
//
// It exercises the real command router over an in-process server + TCP
// connection (the same seam the String/Key tests use) with a fully wired,
// storage-backed router, so any command that were accidentally registered would
// reach its handler and produce a non-error reply — which these tests would
// catch.

// representativeArgs returns a plausible argument line for cmd so the command is
// sent in a realistic shape (arguments and all). The reply must be identical
// regardless of the arguments — dispatch fails the table lookup before it ever
// looks at arity — but sending real arguments proves the proxy does not, for
// example, quietly accept a well-formed SETBIT/PUBLISH/EVAL. An empty string
// means "send the bare command name".
func representativeArgs(cmd string) string {
	switch cmd {
	// Pub/Sub.
	case "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE":
		return "chan"
	case "PUBLISH":
		return "chan msg"
	case "PUBSUB":
		return "CHANNELS"
	// Lua.
	case "EVAL":
		return "return 1 0"
	case "EVALSHA":
		return "abc 0"
	case "SCRIPT":
		return "LOAD return"
	// Transactions.
	case "WATCH":
		return "k"
	// Blocking pops.
	case "BLPOP", "BRPOP":
		return "k 0"
	case "BRPOPLPUSH":
		return "src dst 0"
	// Bit ops.
	case "SETBIT":
		return "k 7 1"
	case "GETBIT":
		return "k 7"
	case "BITCOUNT":
		return "k"
	case "BITOP":
		return "AND dest k"
	case "BITPOS":
		return "k 1"
	// HyperLogLog.
	case "PFADD":
		return "hll a"
	case "PFCOUNT":
		return "hll"
	case "PFMERGE":
		return "dst src"
	// GEO.
	case "GEOADD":
		return "geo 13.36 38.11 palermo"
	case "GEODIST":
		return "geo a b"
	case "GEOPOS", "GEOHASH":
		return "geo a"
	case "GEORADIUS":
		return "geo 15 37 200 km"
	case "GEORADIUSBYMEMBER":
		return "geo a 200 km"
	// Streams.
	case "XADD":
		return "s * f v"
	case "XRANGE", "XREVRANGE":
		return "s - +"
	case "XREAD":
		return "COUNT 2 STREAMS s 0"
	case "XDEL":
		return "s 1-1"
	case "XTRIM":
		return "s MAXLEN 10"
	case "XINFO":
		return "STREAM s"
	// FLUSHALL / FLUSHDB and anything else take no args.
	default:
		return ""
	}
}

// TestUnsupportedCommandsRejectedWithUnknownCommand asserts every command in
// UnsupportedCommands is rejected with the exact byte-for-byte unknown-command
// reply — "-ERR unknown command '<name>'" with the name echoed verbatim — and is
// therefore never silently accepted (requirement 4.1–4.8). The command is sent
// both bare and with representative arguments to show the rejection does not
// depend on argument shape.
func TestUnsupportedCommandsRejectedWithUnknownCommand(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	for _, name := range UnsupportedCommands {
		// The name is echoed exactly as sent (requirement 3.3 / 4.8).
		want := "-" + resp.ErrUnknownCommand(name)

		// Bare command name.
		if got := sendRead(t, conn, r, name); got != want {
			t.Errorf("%q = %q, want %q", name, got, want)
		}

		// With representative arguments: the reply must be identical and must
		// still be an error (never a silent success/downgrade).
		if args := representativeArgs(name); args != "" {
			line := name + " " + args
			got := sendRead(t, conn, r, line)
			if got != want {
				t.Errorf("%q = %q, want %q", line, got, want)
			}
			if !strings.HasPrefix(got, "-ERR") {
				t.Errorf("%q = %q, want an -ERR reply (must not be silently accepted)", line, got)
			}
		}
	}
}

// TestUnsupportedCommandsNotRegistered locks in the design decision (see
// unsupported.go): none of the unsupported command families may be registered on
// a fully wired, storage-backed router. If a future change accidentally registers
// one (e.g. implementing SETBIT), this fails loudly rather than letting an
// unsupported command slip through the table and bypass the unknown-command
// rejection.
func TestUnsupportedCommandsNotRegistered(t *testing.T) {
	r := NewRouterWithStorage(Config{}, Storage{Store: newFakeStringStore(), Now: fixedNow(1000)})

	for _, name := range UnsupportedCommands {
		if spec, ok := r.Table.Lookup(name); ok {
			t.Errorf("unsupported command %q is registered (as %q); it must fall through to the unknown-command rejection", name, spec.Name)
		}
	}
}

// TestUnsupportedCommandsAreErrorRepliesNotDowngrade is a focused restatement of
// the core requirement: for a representative command from each declined family,
// the reply is an error and is NOT any success shape (+OK, an integer, a bulk
// value, or an array). This guards specifically against "silent downgrade"
// (requirement 4.1–4.7) — e.g. FLUSHALL must not reply "+OK", PUBLISH must not
// reply ":0", GETBIT must not reply ":0".
func TestUnsupportedCommandsAreErrorRepliesNotDowngrade(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	// One representative command per family (requirement clause in parens).
	perFamily := map[string]string{
		"SUBSCRIBE": "chan",        // 4.1 Pub/Sub
		"PUBLISH":   "chan msg",    // 4.1 Pub/Sub
		"EVAL":      "return 1 0",  // 4.2 Lua
		"MULTI":     "",            // 4.3 transactions
		"BLPOP":     "k 0",         // 4.4 blocking
		"SETBIT":    "k 7 1",       // 4.5 bit ops
		"PFADD":     "hll a",       // 4.6 HyperLogLog
		"GEOADD":    "geo 13 38 m", // 4.6 GEO
		"XADD":      "s * f v",     // 4.6 Streams
		"FLUSHALL":  "",            // 4.7 flush
		"FLUSHDB":   "",            // 4.7 flush
	}

	for name, args := range perFamily {
		line := name
		if args != "" {
			line = name + " " + args
		}
		got := sendRead(t, conn, r, line)
		if !strings.HasPrefix(got, "-") {
			t.Errorf("%q = %q, want an error reply (must not silently downgrade)", line, got)
		}
		if want := "-" + resp.ErrUnknownCommand(name); got != want {
			t.Errorf("%q = %q, want %q", line, got, want)
		}
	}
}
