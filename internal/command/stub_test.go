package command

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"testing"
)

// readValue reads one full RESP2 reply and renders it as a comparable string.
// It extends readReply (connection_test.go) with array support so the stub
// replies (COMMAND -> *0, CONFIG GET -> *2, TIME -> *2) can be asserted:
//
//   - "+OK"                 simple string
//   - "-ERR ..."            error
//   - ":0"                  integer
//   - "$-1"                 null bulk string
//   - "$foo"                bulk string with payload "foo"
//   - "*-1"                 null array
//   - "[e1 e2 ...]"         array with each rendered element joined by a space
func readValue(t *testing.T, r *bufio.Reader) string {
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
	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			t.Fatalf("bad array header %q: %v", line, err)
		}
		if n < 0 {
			return "*-1"
		}
		elems := make([]string, n)
		for i := 0; i < n; i++ {
			elems[i] = readValue(t, r)
		}
		return "[" + strings.Join(elems, " ") + "]"
	default:
		t.Fatalf("unexpected reply prefix in %q", line)
		return ""
	}
}

// sendReadValue sends one inline command and reads a full RESP2 reply, including
// arrays.
func sendReadValue(t *testing.T, r *bufio.Reader, conn interface{ Write([]byte) (int, error) }, cmd string) string {
	t.Helper()
	if _, err := conn.Write([]byte(cmd + "\r\n")); err != nil {
		t.Fatalf("write %q: %v", cmd, err)
	}
	return readValue(t, r)
}

// --- COMMAND / COMMAND COUNT (requirement 19.1) ------------------------------

func TestCommandRepliesEmptyArray(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	if got, want := sendReadValue(t, r, conn, "COMMAND"), "[]"; got != want {
		t.Errorf("COMMAND = %q, want %q (empty array *0)", got, want)
	}
}

func TestCommandCountRepliesZero(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	if got, want := sendReadValue(t, r, conn, "COMMAND COUNT"), ":0"; got != want {
		t.Errorf("COMMAND COUNT = %q, want %q", got, want)
	}
}

// --- CLIENT SETNAME / GETNAME (requirement 19.2) -----------------------------

func TestClientSetnameRepliesOK(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	if got, want := sendReadValue(t, r, conn, "CLIENT SETNAME myconn"), "+OK"; got != want {
		t.Errorf("CLIENT SETNAME = %q, want %q", got, want)
	}
}

func TestClientGetnameRepliesNullBulk(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	if got, want := sendReadValue(t, r, conn, "CLIENT GETNAME"), "$-1"; got != want {
		t.Errorf("CLIENT GETNAME = %q, want %q", got, want)
	}
}

// --- CONFIG GET (requirement 19.3) -------------------------------------------

func TestConfigGetMaxmemoryReturnsDefault(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	// CONFIG GET maxmemory -> *2 ["maxmemory", "0"].
	if got, want := sendReadValue(t, r, conn, "CONFIG GET maxmemory"), "[$maxmemory $0]"; got != want {
		t.Errorf("CONFIG GET maxmemory = %q, want %q", got, want)
	}
}

func TestConfigGetUnknownReturnsEmptyArray(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	if got, want := sendReadValue(t, r, conn, "CONFIG GET nonesuch"), "[]"; got != want {
		t.Errorf("CONFIG GET nonesuch = %q, want %q (empty array)", got, want)
	}
}

func TestConfigSetRepliesOK(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	if got, want := sendReadValue(t, r, conn, "CONFIG SET maxmemory 100mb"), "+OK"; got != want {
		t.Errorf("CONFIG SET = %q, want %q", got, want)
	}
}

// --- DBSIZE (requirement 19.4) -----------------------------------------------

func TestDBSizeRepliesInteger(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	got := sendReadValue(t, r, conn, "DBSIZE")
	if !strings.HasPrefix(got, ":") {
		t.Fatalf("DBSIZE = %q, want an integer reply", got)
	}
	if _, err := strconv.ParseInt(got[1:], 10, 64); err != nil {
		t.Errorf("DBSIZE integer payload %q not numeric: %v", got, err)
	}
}

// --- TIME (requirement 19.5) -------------------------------------------------

func TestTimeRepliesTwoNumericElements(t *testing.T) {
	conn, r := startConnServer(t, Config{})
	got := sendReadValue(t, r, conn, "TIME")
	// Expect "[$<seconds> $<micros>]" with both elements numeric.
	if !strings.HasPrefix(got, "[") || !strings.HasSuffix(got, "]") {
		t.Fatalf("TIME = %q, want a 2-element array", got)
	}
	inner := strings.TrimSuffix(strings.TrimPrefix(got, "["), "]")
	parts := strings.Split(inner, " ")
	if len(parts) != 2 {
		t.Fatalf("TIME = %q, want exactly 2 elements, got %d", got, len(parts))
	}
	for i, p := range parts {
		if !strings.HasPrefix(p, "$") {
			t.Errorf("TIME element %d = %q, want a bulk string", i, p)
			continue
		}
		if _, err := strconv.ParseInt(p[1:], 10, 64); err != nil {
			t.Errorf("TIME element %d payload %q not numeric: %v", i, p, err)
		}
	}
}
