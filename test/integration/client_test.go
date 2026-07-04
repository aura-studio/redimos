// Package integration holds redimos' end-to-end tests that exercise the real
// redimo -> DynamoDB path (not the in-memory fakes) and, where an oracle is
// configured, compare the proxy byte-for-byte against a live Redis 3.2.
//
// They are gated on environment so a bare `go test ./...` skips them cleanly, and
// they RUN under the Docker harness that provides the endpoints:
//
//	REDIMOS_PROXY_ADDR   host:port of a running redimos proxy (required; else skip)
//	REDIMOS_REDIS_ORACLE host:port of a real Redis 3.2 (optional; differential only)
//
// Three properties are covered, one file each:
//   - charset_test.go      — every value/member/key is byte-for-byte binary-safe
//     across all redimo-backed command families (all 256 byte values).
//   - atomicity_test.go    — concurrent SETNX / INCR are atomic (matching Redis'
//     single-threaded semantics) on the redimo-backed store.
//   - differential_test.go — redimo-backed commands reply byte-for-byte identically
//     to a live Redis 3.2 oracle.
package integration

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

// proxyAddr returns REDIMOS_PROXY_ADDR or skips the test when it is unset.
func proxyAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("REDIMOS_PROXY_ADDR")
	if addr == "" {
		t.Skip("REDIMOS_PROXY_ADDR not set; skipping integration test")
	}
	return addr
}

// oracleAddr returns REDIMOS_REDIS_ORACLE or skips (used by the differential test).
func oracleAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("REDIMOS_REDIS_ORACLE")
	if addr == "" {
		t.Skip("REDIMOS_REDIS_ORACLE not set; skipping differential test")
	}
	return addr
}

// respConn is a minimal binary-safe RESP2 client. Unlike a parsing client it
// returns the RAW bytes of each reply, which is exactly what byte-for-byte
// differential and binary-safety assertions need.
type respConn struct {
	conn net.Conn
	r    *bufio.Reader
}

func dial(t *testing.T, addr string) *respConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &respConn{conn: conn, r: bufio.NewReader(conn)}
}

// do sends a command as a RESP array of bulk strings and returns the raw bytes of
// the single reply. Arguments are binary-safe.
func (c *respConn) do(args ...[]byte) []byte {
	_ = c.conn.SetDeadline(time.Now().Add(10 * time.Second))
	var b []byte
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(args)), 10)
	b = append(b, '\r', '\n')
	for _, a := range args {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(a)), 10)
		b = append(b, '\r', '\n')
		b = append(b, a...)
		b = append(b, '\r', '\n')
	}
	if _, err := c.conn.Write(b); err != nil {
		return []byte("WRITE-ERR: " + err.Error())
	}
	reply, err := readReply(c.r)
	if err != nil {
		return []byte("READ-ERR: " + err.Error())
	}
	return reply
}

// bulkPayload extracts the payload of a `$<n>\r\n<payload>\r\n` bulk-string reply,
// or reports !ok for a null bulk ($-1) or a non-bulk reply. It is how the charset
// test recovers the exact bytes a GET/LINDEX/HGET returned.
func bulkPayload(reply []byte) (payload []byte, ok bool) {
	if len(reply) == 0 || reply[0] != '$' {
		return nil, false
	}
	i := 1
	for i < len(reply) && reply[i] != '\r' {
		i++
	}
	n, err := strconv.Atoi(string(reply[1:i]))
	if err != nil || n < 0 {
		return nil, false
	}
	start := i + 2 // skip \r\n
	if start+n > len(reply) {
		return nil, false
	}
	return reply[start : start+n], true
}

// readReply reads one complete RESP2 reply and returns all of its raw bytes.
func readReply(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 1 {
		return nil, fmt.Errorf("empty reply line")
	}
	switch line[0] {
	case '+', '-', ':':
		return line, nil
	case '$':
		n, perr := replyLen(line)
		if perr != nil {
			return nil, perr
		}
		if n < 0 { // null bulk $-1
			return line, nil
		}
		body := make([]byte, n+2) // payload + CRLF
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
		return append(line, body...), nil
	case '*':
		n, perr := replyLen(line)
		if perr != nil {
			return nil, perr
		}
		out := append([]byte(nil), line...)
		if n < 0 { // null array *-1
			return out, nil
		}
		for i := 0; i < n; i++ {
			el, err := readReply(r)
			if err != nil {
				return nil, err
			}
			out = append(out, el...)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unexpected reply prefix %q", line[0])
	}
}

func replyLen(line []byte) (int, error) {
	// line is "<prefix><number>\r\n"
	end := len(line)
	for end > 0 && (line[end-1] == '\n' || line[end-1] == '\r') {
		end--
	}
	return strconv.Atoi(string(line[1:end]))
}

// bs is a terse []byte literal helper.
func bs(s string) []byte { return []byte(s) }
