// Package difftest implements the differential-testing harness that verifies
// redimos is byte-for-byte compatible with a Pika v3.2.2 oracle (Requirements
// 1.6 and 4.8).
//
// The harness sends an identical RESP2 command sequence to two endpoints (the
// Pika oracle and redimos) and compares the raw RESP replies byte-for-byte.
// It provides three building blocks:
//
//  1. A raw RESP reader (ReadReply) that captures the exact reply bytes,
//     including type prefixes and CRLF terminators, so differences in error
//     text, null encodings ($-1 vs *0 vs *-1), and integer boundaries are
//     caught at the byte level.
//  2. A command-matrix driven entry point (see Matrix in harness.go).
//  3. A random-sequence fuzz entry point built on testing/quick (see
//     GenerateSequence in harness.go and the fuzz test in difftest_test.go).
//
// The live-endpoint entry points are guarded: they skip cleanly when the
// PIKA_ADDR / REDIMOS_ADDR environment variables are unset, so `go test ./...`
// passes without any infrastructure. The RESP reader and command encoder are
// exercised by pure in-memory unit and property tests that always run.
package difftest

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// EncodeCommand serializes a command and its arguments as a RESP2 array of
// bulk strings, matching what a real Redis client sends on the wire:
//
//	*<n>\r\n$<len>\r\n<arg>\r\n ...
//
// It accepts raw byte arguments so tests can exercise binary-safe values and
// integer-boundary strings without lossy conversions.
func EncodeCommand(args ...[]byte) []byte {
	var b bytes.Buffer
	b.WriteByte('*')
	b.WriteString(strconv.Itoa(len(args)))
	b.WriteString("\r\n")
	for _, a := range args {
		b.WriteByte('$')
		b.WriteString(strconv.Itoa(len(a)))
		b.WriteString("\r\n")
		b.Write(a)
		b.WriteString("\r\n")
	}
	return b.Bytes()
}

// EncodeCommandStrings is a convenience wrapper around EncodeCommand for the
// common case of string arguments.
func EncodeCommandStrings(args ...string) []byte {
	raw := make([][]byte, len(args))
	for i, a := range args {
		raw[i] = []byte(a)
	}
	return EncodeCommand(raw...)
}

// ReadReply reads exactly one complete RESP2 reply from r and returns its raw
// bytes verbatim, including the type prefix and all CRLF terminators. Arrays
// are read recursively so the returned slice is the full, unmodified reply as
// it appeared on the wire.
//
// Capturing raw bytes (rather than a decoded value) is what makes the harness
// able to assert byte-for-byte equality against the Pika oracle: two endpoints
// that decode to the "same" logical value but serialize it differently (for
// example `$-1` vs `*-1` vs `*0`, or a differently worded error) will produce
// different raw bytes and be flagged.
func ReadReply(r *bufio.Reader) ([]byte, error) {
	var buf bytes.Buffer
	if err := readReplyInto(r, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func readReplyInto(r *bufio.Reader, buf *bytes.Buffer) error {
	prefix, err := r.ReadByte()
	if err != nil {
		return err
	}
	buf.WriteByte(prefix)

	line, err := readLine(r, buf)
	if err != nil {
		return err
	}

	switch prefix {
	case '+', '-', ':':
		// Simple string, error, integer: the line is the whole payload.
		return nil
	case '$':
		return readBulkBody(r, buf, line)
	case '*':
		return readArrayBody(r, buf, line)
	default:
		return fmt.Errorf("difftest: unknown RESP type prefix %q", prefix)
	}
}

// readLine consumes bytes up to and including the terminating CRLF, appends
// them to buf, and returns the line content WITHOUT the trailing CRLF.
func readLine(r *bufio.Reader, buf *bytes.Buffer) ([]byte, error) {
	// ReadBytes('\n') captures through the newline; RESP always uses CRLF.
	line, err := r.ReadBytes('\n')
	if err != nil {
		// Still record whatever was read to aid debugging of truncated frames.
		buf.Write(line)
		return nil, err
	}
	buf.Write(line)
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("difftest: malformed RESP line, expected CRLF: %q", line)
	}
	return line[:len(line)-2], nil
}

func readBulkBody(r *bufio.Reader, buf *bytes.Buffer, header []byte) error {
	n, err := strconv.Atoi(string(header))
	if err != nil {
		return fmt.Errorf("difftest: invalid bulk length %q: %w", header, err)
	}
	if n < 0 {
		// Null bulk string ($-1). No body follows.
		return nil
	}
	// Read exactly n bytes of body plus the trailing CRLF.
	body := make([]byte, n+2)
	if _, err := io.ReadFull(r, body); err != nil {
		buf.Write(body)
		return err
	}
	if body[n] != '\r' || body[n+1] != '\n' {
		return fmt.Errorf("difftest: bulk body not CRLF-terminated")
	}
	buf.Write(body)
	return nil
}

func readArrayBody(r *bufio.Reader, buf *bytes.Buffer, header []byte) error {
	n, err := strconv.Atoi(string(header))
	if err != nil {
		return fmt.Errorf("difftest: invalid array length %q: %w", header, err)
	}
	if n < 0 {
		// Null array (*-1). No elements follow.
		return nil
	}
	// Empty array (*0) and populated arrays both fall through here; a *0 simply
	// reads zero elements.
	for i := 0; i < n; i++ {
		if err := readReplyInto(r, buf); err != nil {
			return err
		}
	}
	return nil
}
