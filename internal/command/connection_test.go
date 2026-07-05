package command

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aura-studio/redimos/v2/internal/server"
)

// startConnServer boots a server on an ephemeral port using a Router built with
// cfg as the dispatcher, and returns a connected client reader/writer. The
// Router registers the handshake/connection commands under test.
func startConnServer(t *testing.T, cfg Config) (net.Conn, *bufio.Reader) {
	t.Helper()
	r := NewRouter(cfg)
	s := server.New(server.Options{Addr: "127.0.0.1:0"}, r)
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

// send writes one inline command terminated by CRLF.
func send(t *testing.T, conn net.Conn, cmd string) {
	t.Helper()
	if _, err := conn.Write([]byte(cmd + "\r\n")); err != nil {
		t.Fatalf("write %q: %v", cmd, err)
	}
}

// readReply reads a single RESP2 reply and renders it as a comparable string:
//   - "+OK"            simple string
//   - "-ERR ..."       error
//   - ":1"             integer
//   - "$-1"            null bulk string
//   - "$foo"           bulk string with payload "foo"
//
// Only the reply shapes produced by the connection commands are handled.
func readReply(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		t.Fatalf("empty reply line")
	}
	switch line[0] {
	case '+', '-', ':':
		return line
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			t.Fatalf("bad bulk header %q: %v", line, err)
		}
		if n < 0 {
			return "$-1"
		}
		buf := make([]byte, n+2) // payload + CRLF
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatalf("read bulk payload: %v", err)
		}
		return "$" + string(buf[:n])
	default:
		t.Fatalf("unexpected reply prefix in %q", line)
		return ""
	}
}

// sendRead is a convenience for the common send-then-read-one-reply pattern.
func sendRead(t *testing.T, conn net.Conn, r *bufio.Reader, cmd string) string {
	t.Helper()
	send(t, conn, cmd)
	return readReply(t, r)
}

// --- HELLO (requirement 2.1) -------------------------------------------------

func TestHelloRepliesUnknownCommand(t *testing.T) {
	conn, r := startConnServer(t, Config{})

	// Requirement 2.1: HELLO must reply the unknown-command error so go-redis v9 /
	// redis-py 5+ fall back to RESP2. The command name is echoed exactly as the
	// client cased it (like the generic unknown-command path and real Redis).
	cases := map[string]string{
		"HELLO":                   "-ERR unknown command 'HELLO'",
		"hello 3":                 "-ERR unknown command 'hello'",
		"HELLO 3 AUTH user pass":  "-ERR unknown command 'HELLO'",
	}
	for cmd, want := range cases {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

// --- PING (requirement 2.2, 2.3) ---------------------------------------------

func TestPingNoArgRepliesPong(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	if got, want := sendRead(t, conn, r, "PING"), "+PONG"; got != want {
		t.Errorf("PING = %q, want %q", got, want)
	}
}

func TestPingWithArgEchoesBulk(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	// Requirement 2.3: a single argument is echoed back as a bulk string.
	if got, want := sendRead(t, conn, r, "PING hello"), "$hello"; got != want {
		t.Errorf("PING hello = %q, want %q", got, want)
	}
}

func TestPingTooManyArgsIsArityError(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	want := "-ERR wrong number of arguments for 'ping' command"
	if got := sendRead(t, conn, r, "PING a b"); got != want {
		t.Errorf("PING a b = %q, want %q", got, want)
	}
}

// --- ECHO (requirement 2.4) --------------------------------------------------

func TestEchoEchoesArgument(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	if got, want := sendRead(t, conn, r, "ECHO world"), "$world"; got != want {
		t.Errorf("ECHO world = %q, want %q", got, want)
	}
}

func TestEchoWrongArity(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	want := "-ERR wrong number of arguments for 'echo' command"
	if got := sendRead(t, conn, r, "ECHO"); got != want {
		t.Errorf("ECHO (no arg) = %q, want %q", got, want)
	}
}

// --- AUTH + NOAUTH gating (requirement 2.5, 2.6) -----------------------------

func TestAuthSuccessMarksAuthed(t *testing.T) {
	conn, r := startConnServer(t, Config{RequirePass: "s3cret"})

	// Before AUTH, a business command is rejected with NOAUTH (requirement 2.6).
	if got, want := sendRead(t, conn, r, "ECHO x"), "-NOAUTH Authentication required."; got != want {
		t.Errorf("pre-auth ECHO = %q, want %q", got, want)
	}
	// Correct password authenticates (requirement 2.5).
	if got, want := sendRead(t, conn, r, "AUTH s3cret"), "+OK"; got != want {
		t.Errorf("AUTH s3cret = %q, want %q", got, want)
	}
	// After AUTH, the same connection may run business commands.
	if got, want := sendRead(t, conn, r, "ECHO x"), "$x"; got != want {
		t.Errorf("post-auth ECHO x = %q, want %q", got, want)
	}
}

func TestAuthWrongPassword(t *testing.T) {
	conn, r := startConnServer(t, Config{RequirePass: "s3cret"})
	if got, want := sendRead(t, conn, r, "AUTH nope"), "-ERR invalid password"; got != want {
		t.Errorf("AUTH nope = %q, want %q", got, want)
	}
	// A failed AUTH must not authenticate the connection.
	if got, want := sendRead(t, conn, r, "ECHO x"), "-NOAUTH Authentication required."; got != want {
		t.Errorf("post-failed-auth ECHO = %q, want %q", got, want)
	}
}

func TestAuthWithoutRequirepassConfigured(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	want := "-ERR Client sent AUTH, but no password is set"
	if got := sendRead(t, conn, r, "AUTH whatever"); got != want {
		t.Errorf("AUTH whatever = %q, want %q", got, want)
	}
}

func TestNoAuthGateExemptsAuthAndQuit(t *testing.T) {
	conn, r := startConnServer(t, Config{RequirePass: "pw"})
	// QUIT is exempt from the gate: it replies +OK and closes the connection.
	if got, want := sendRead(t, conn, r, "QUIT"), "+OK"; got != want {
		t.Errorf("QUIT = %q, want %q", got, want)
	}
}

func TestNoGateWhenRequirepassEmpty(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	// With no requirepass, business commands run without AUTH.
	if got, want := sendRead(t, conn, r, "ECHO x"), "$x"; got != want {
		t.Errorf("ECHO x (no requirepass) = %q, want %q", got, want)
	}
}

// --- SELECT (requirement 2.7, 2.8, 2.9) --------------------------------------

func TestSelectZeroIsOK(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	if got, want := sendRead(t, conn, r, "SELECT 0"), "+OK"; got != want {
		t.Errorf("SELECT 0 = %q, want %q", got, want)
	}
}

func TestSelectNonZeroWithoutMultiDBRejected(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	// Requirement 2.8: non-zero SELECT with multi-DB disabled is rejected.
	want := "-ERR invalid DB index"
	for _, cmd := range []string{"SELECT 1", "SELECT 5"} {
		if got := sendRead(t, conn, r, cmd); got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
}

func TestSelectNonZeroWithMultiDBEnabled(t *testing.T) {
	conn, r := startConnServer(t, Config{MultiDB: true})
	// Requirement 2.9: with multi-DB enabled, a non-zero SELECT is accepted.
	if got, want := sendRead(t, conn, r, "SELECT 3"), "+OK"; got != want {
		t.Errorf("SELECT 3 (multi-DB) = %q, want %q", got, want)
	}
}

func TestSelectOutOfRangeWithMultiDB(t *testing.T) {
	conn, r := startConnServer(t, Config{MultiDB: true}) // default 16 DBs
	// Redis 3.2.12 replies the SAME "invalid DB index" text for a numeric-but-out-of-range
	// index (negative, or >= databases) as for a non-numeric one — verified against the
	// live oracle. What differs from the without-multi-DB case is only the accepted range.
	for _, cmd := range []string{"SELECT -1", "SELECT 16", "SELECT 999"} {
		if got, want := sendRead(t, conn, r, cmd), "-ERR invalid DB index"; got != want {
			t.Errorf("%q = %q, want %q", cmd, got, want)
		}
	}
	// The last in-range index (databases-1) is accepted.
	if got, want := sendRead(t, conn, r, "SELECT 15"), "+OK"; got != want {
		t.Errorf("SELECT 15 = %q, want %q", got, want)
	}
}

func TestSelectNonIntegerIndex(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	// Redis 3.2 reports a non-numeric SELECT argument as "invalid DB index".
	want := "-ERR invalid DB index"
	if got := sendRead(t, conn, r, "SELECT abc"); got != want {
		t.Errorf("SELECT abc = %q, want %q", got, want)
	}
}
