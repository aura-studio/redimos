package command

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/aura-studio/redimos/internal/server"
)

// buildTestTable registers a small set of commands exercising exact and
// negative arity plus a write flag, with handlers that emit a distinguishable
// reply so tests can confirm the handler actually ran.
func buildTestTable() Table {
	tbl := NewTable()
	// Exact arity: GET requires exactly 2 args (name + key).
	tbl.Register("GET", 2, false, func(_ context.Context, c *server.Conn, args [][]byte) {
		c.Redcon().WriteString("GOT:" + string(args[1]))
	})
	// Negative arity: MSET requires at least 3 args (name + >=1 key/value pair).
	tbl.Register("MSET", -3, true, func(_ context.Context, c *server.Conn, _ [][]byte) {
		c.Redcon().WriteString("OK")
	})
	return tbl
}

// startRouterServer boots a server on an ephemeral port using tbl as the
// dispatcher and returns a connected client reader/writer.
func startRouterServer(t *testing.T, tbl Table) (net.Conn, *bufio.Reader) {
	t.Helper()
	s := server.New(server.Options{Addr: "127.0.0.1:0"}, tbl)
	signal := make(chan error, 1)
	go func() { _ = s.ListenServeAndSignal(signal) }()
	if err := <-signal; err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	return conn, bufio.NewReader(conn)
}

// sendLine writes one inline command and reads back a single RESP line
// (everything up to and including the terminating CRLF for simple/error/int
// replies).
func sendLine(t *testing.T, conn net.Conn, r *bufio.Reader, cmd string) string {
	t.Helper()
	if _, err := conn.Write([]byte(cmd + "\r\n")); err != nil {
		t.Fatalf("write %q: %v", cmd, err)
	}
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read reply for %q: %v", cmd, err)
	}
	return strings.TrimRight(line, "\r\n")
}

func TestDispatchLookupRunsHandler(t *testing.T) {
	tbl := buildTestTable()
	conn, r := startRouterServer(t, tbl)

	if got, want := sendLine(t, conn, r, "GET foo"), "+GOT:foo"; got != want {
		t.Errorf("GET foo = %q, want %q", got, want)
	}
}

func TestDispatchLookupIsCaseInsensitive(t *testing.T) {
	tbl := buildTestTable()
	conn, r := startRouterServer(t, tbl)

	// Requirement 3.1: lookup by lowercased command name regardless of casing.
	if got, want := sendLine(t, conn, r, "get bar"), "+GOT:bar"; got != want {
		t.Errorf("get bar = %q, want %q", got, want)
	}
	if got, want := sendLine(t, conn, r, "GeT baz"), "+GOT:baz"; got != want {
		t.Errorf("GeT baz = %q, want %q", got, want)
	}
}

func TestDispatchUnknownCommand(t *testing.T) {
	tbl := buildTestTable()
	conn, r := startRouterServer(t, tbl)

	// Requirement 3.3: unknown command, name echoed verbatim (case preserved).
	if got, want := sendLine(t, conn, r, "FOOBAR x"), "-ERR unknown command 'FOOBAR'"; got != want {
		t.Errorf("FOOBAR x = %q, want %q", got, want)
	}
}

func TestDispatchArityExactMismatch(t *testing.T) {
	tbl := buildTestTable()
	conn, r := startRouterServer(t, tbl)

	// GET has exact arity 2; sending just "GET" (1 arg) is a mismatch.
	// Requirement 3.2: lowercase command name in the error.
	if got, want := sendLine(t, conn, r, "GET"), "-ERR wrong number of arguments for 'get' command"; got != want {
		t.Errorf("GET (no key) = %q, want %q", got, want)
	}
	// Too many args is also a mismatch for exact arity.
	if got, want := sendLine(t, conn, r, "GET a b"), "-ERR wrong number of arguments for 'get' command"; got != want {
		t.Errorf("GET a b = %q, want %q", got, want)
	}
}

func TestDispatchArityNegativeMismatch(t *testing.T) {
	tbl := buildTestTable()
	conn, r := startRouterServer(t, tbl)

	// MSET has arity -3 (at least 3 args). "MSET k" is 2 args -> mismatch.
	if got, want := sendLine(t, conn, r, "MSET k"), "-ERR wrong number of arguments for 'mset' command"; got != want {
		t.Errorf("MSET k = %q, want %q", got, want)
	}
	// At the minimum (3 args) the handler runs.
	if got, want := sendLine(t, conn, r, "MSET k v"), "+OK"; got != want {
		t.Errorf("MSET k v = %q, want %q", got, want)
	}
	// Above the minimum also runs.
	if got, want := sendLine(t, conn, r, "MSET k v k2 v2"), "+OK"; got != want {
		t.Errorf("MSET k v k2 v2 = %q, want %q", got, want)
	}
}
