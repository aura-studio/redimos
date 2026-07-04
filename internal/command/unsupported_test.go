package command

import (
	"strings"
	"testing"

	"github.com/aura-studio/redimos/v2/internal/resp"
)

// unsupported_test.go guards how redimos disposes of the commands it does not serve
// (see unsupported.go): the Streams family (absent from Redis 3.2) must reach the
// generic unknown-command reply, while the deliberately-declined real-3.2 families
// (Pub/Sub, Lua, transactions, blocking pops) must return their first-class proxy
// rejections and never silently succeed.
//
// The tests exercise the real command router over an in-process server + TCP
// connection with a fully wired, storage-backed router, so a command that were
// accidentally accepted would reach its handler and produce a non-error reply —
// which these tests would catch.

// representativeArgs returns a plausible, correctly-shaped argument line for cmd so
// the command reaches its handler (past the arity check) rather than tripping a
// "wrong number of arguments" error. An empty string means "send the bare name".
func representativeArgs(cmd string) string {
	switch cmd {
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
	default:
		return ""
	}
}

// TestUnsupportedCommandsRejectedWithUnknownCommand asserts every command in
// UnsupportedCommands (the Streams family, absent from Redis 3.2) is rejected with
// the exact byte-for-byte unknown-command reply — "-ERR unknown command '<name>'"
// with the name echoed verbatim — matching what a real Redis 3.2 server replies.
func TestUnsupportedCommandsRejectedWithUnknownCommand(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	for _, name := range UnsupportedCommands {
		want := "-" + resp.ErrUnknownCommand(name)

		if got := sendRead(t, conn, r, name); got != want {
			t.Errorf("%q = %q, want %q", name, got, want)
		}

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

// TestUnsupportedCommandsNotRegistered locks in that the unknown-command families
// (Streams) stay unregistered on a fully wired router, so they keep reaching the
// oracle-correct unknown-command rejection.
func TestUnsupportedCommandsNotRegistered(t *testing.T) {
	r := NewRouterWithStorage(Config{}, Storage{Store: newFakeStringStore(), Now: fixedNow(1000)})

	for _, name := range UnsupportedCommands {
		if spec, ok := r.Table.Lookup(name); ok {
			t.Errorf("unsupported command %q is registered (as %q); it must fall through to the unknown-command rejection", name, spec.Name)
		}
	}
}

// rejectCase is one deliberately-declined real-Redis-3.2 command: a correctly-shaped
// invocation and the dedicated proxy-rejection message it must return.
type rejectCase struct {
	line string
	want string
}

// TestRejectedFamiliesReturnDedicatedError asserts the Pub/Sub, Lua, transaction and
// blocking-pop families are declined with their FIRST-CLASS proxy rejection (not the
// generic unknown-command reply and never a silent success). These commands exist in
// Redis 3.2, so a clear "not supported on this proxy" message is more honest than
// "unknown command". Args are correctly shaped so dispatch reaches the reject handler.
func TestRejectedFamiliesReturnDedicatedError(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	cases := []rejectCase{
		// Pub/Sub.
		{"SUBSCRIBE ch", errPubSubUnsupported},
		{"UNSUBSCRIBE", errPubSubUnsupported},
		{"PSUBSCRIBE ch", errPubSubUnsupported},
		{"PUNSUBSCRIBE", errPubSubUnsupported},
		{"PUBLISH ch msg", errPubSubUnsupported},
		{"PUBSUB CHANNELS", errPubSubUnsupported},
		// Lua.
		{"EVAL script 0", errScriptUnsupported},
		{"EVALSHA abc 0", errScriptUnsupported},
		{"SCRIPT LOAD x", errScriptUnsupported},
		// Transactions.
		{"MULTI", errTxnUnsupported},
		{"EXEC", errTxnUnsupported},
		{"DISCARD", errTxnUnsupported},
		{"WATCH k", errTxnUnsupported},
		{"UNWATCH", errTxnUnsupported},
		// Blocking pops.
		{"BLPOP k 0", errBlockingUnsupported},
		{"BRPOP k 0", errBlockingUnsupported},
		{"BRPOPLPUSH src dst 0", errBlockingUnsupported},
		// Individual declined commands (each with its own message).
		{"SHUTDOWN", errShutdownUnsupported},
		{"SHUTDOWN NOSAVE", errShutdownUnsupported},
		{"ASKING", errAskingUnsupported},
		{"READONLY", errReadOnlyUnsupported},
	}

	for _, tc := range cases {
		got := sendRead(t, conn, r, tc.line)
		if want := "-" + tc.want; got != want {
			t.Errorf("%q = %q, want %q", tc.line, got, want)
		}
	}
}

// TestRejectedFamiliesRegistered confirms the reject families ARE registered (so they
// take the dedicated-rejection path, not the unknown-command path) with their real
// Redis 3.2 arities, so a mis-shaped call still returns the standard arity error.
func TestRejectedFamiliesRegistered(t *testing.T) {
	r := NewRouterWithStorage(Config{}, Storage{Store: newFakeStringStore(), Now: fixedNow(1000)})

	for _, name := range []string{
		"SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PUBLISH", "PUBSUB",
		"EVAL", "EVALSHA", "SCRIPT",
		"MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
		"BLPOP", "BRPOP", "BRPOPLPUSH",
		"SHUTDOWN", "ASKING", "READONLY",
	} {
		if _, ok := r.Table.Lookup(name); !ok {
			t.Errorf("reject family command %q is not registered; it would fall through to unknown-command instead of the dedicated rejection", name)
		}
	}
}

// TestFlushRejected verifies FLUSHALL / FLUSHDB are declined with a first-class
// proxy rejection (not the generic unknown-command reply and never a silent
// "+OK"): flushing would wipe the whole shared DynamoDB table.
func TestFlushRejected(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	for _, cmd := range []string{"FLUSHALL", "FLUSHDB"} {
		got := sendRead(t, conn, r, cmd)
		if want := "-" + errFlushDisabled; got != want {
			t.Errorf("%s = %q, want %q", cmd, got, want)
		}
	}
}
