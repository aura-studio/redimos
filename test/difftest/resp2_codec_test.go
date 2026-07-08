package difftest

import (
	"bufio"
	"bytes"
	"strconv"
	"testing"
	"testing/quick"
	"time"

	"github.com/aura-studio/redimos/internal/resp"
)

// resp2_codec_test.go is the task 4.3 RESP2 codec differential test.
//
// It has two halves:
//
//  1. Always-run, no-infrastructure byte-level assertions. These treat the
//     resp.Append* encoders as the redimos side of the codec and assert their
//     output equals the canonical Pika v3.2.2 wire bytes for all five RESP2
//     types and the three null-value conventions ($-1 vs *0 vs *-1). Each
//     encoded reply is also fed through the harness's own ReadReply -- the very
//     reader used to capture oracle replies -- to prove the encoder output is a
//     well-formed frame captured verbatim. This is a genuine codec differential
//     (encoder vs wire reader) that runs under `go test ./...` with zero infra.
//
//  2. An env-guarded live differential (TestDiffRESP2Codec) that replays the
//     RESP2CodecSequences against a real Pika oracle and redimos, comparing raw
//     replies byte-for-byte. It skips cleanly when PIKA_ADDR / REDIMOS_ADDR are
//     unset.
//
// Property 6: error-text consistency.
// Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.6.

// canonicalReply pairs a logical RESP2 reply, produced via the resp encoders,
// with the exact Pika v3.2.2 wire bytes it must equal.
type canonicalReply struct {
	name    string
	encoded []byte // produced by the resp.Append* encoders (redimos side)
	wire    string // canonical Pika v3.2.2 wire form (oracle side)
}

// canonicalReplies enumerates one representative reply for each RESP2 type plus
// the three null conventions. The encoded column is what redimos emits; the
// wire column is the byte-for-byte oracle truth.
func canonicalReplies() []canonicalReply {
	return []canonicalReply{
		// Simple String (+) -- Requirement 1.1.
		{"simple-ok", resp.AppendSimpleString(nil, "OK"), "+OK\r\n"},
		{"simple-pong", resp.AppendSimpleString(nil, "PONG"), "+PONG\r\n"},

		// Error (-) -- Requirement 1.1; error text is Property 6.
		{"error-syntax", resp.AppendError(nil, resp.ErrSyntax), "-ERR syntax error\r\n"},
		{"error-wrongtype", resp.AppendError(nil, resp.ErrWrongType),
			"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},

		// Integer (:) -- Requirement 1.1.
		{"int-zero", resp.AppendInt(nil, 0), ":0\r\n"},
		{"int-one", resp.AppendInt(nil, 1), ":1\r\n"},
		{"int-neg-one", resp.AppendInt(nil, -1), ":-1\r\n"},
		{"int-neg-two", resp.AppendInt(nil, -2), ":-2\r\n"},
		{"int-max", resp.AppendInt(nil, 9223372036854775807), ":9223372036854775807\r\n"},
		{"int-min", resp.AppendInt(nil, -9223372036854775808), ":-9223372036854775808\r\n"},

		// Bulk String ($) -- Requirement 1.1.
		{"bulk-hello", resp.AppendBulkString(nil, []byte("hello")), "$5\r\nhello\r\n"},
		{"bulk-empty", resp.AppendBulkString(nil, []byte("")), "$0\r\n\r\n"},

		// Array (*) -- Requirement 1.1.
		{"array-two", resp.AppendBulkArray(nil, [][]byte{[]byte("a"), []byte("bc")}),
			"*2\r\n$1\r\na\r\n$2\r\nbc\r\n"},

		// Null conventions -- Requirements 1.2, 1.3, 1.4.
		{"null-bulk", resp.AppendNullBulk(nil), "$-1\r\n"},
		{"empty-array", resp.AppendEmptyArray(nil), "*0\r\n"},
		{"null-array", resp.AppendNullArray(nil), "*-1\r\n"},
	}
}

// TestRESP2EncoderMatchesOracleWire is the core always-run codec differential:
// for every RESP2 type and null convention, the encoder output must equal the
// canonical Pika wire bytes exactly. Requirements 1.1, 1.2, 1.3, 1.4.
func TestRESP2EncoderMatchesOracleWire(t *testing.T) {
	for _, c := range canonicalReplies() {
		if string(c.encoded) != c.wire {
			t.Errorf("%s: encoder emitted %q, want oracle wire %q",
				c.name, visualize(c.encoded), visualize([]byte(c.wire)))
		}
	}
}

// TestRESP2EncoderFramesRoundTrip feeds every encoded reply through the
// harness's ReadReply -- the same reader that captures oracle replies -- and
// asserts it is captured verbatim as exactly one frame. This ties the encoder
// to the differential comparison path used against the live oracle.
// Requirement 1.6.
func TestRESP2EncoderFramesRoundTrip(t *testing.T) {
	for _, c := range canonicalReplies() {
		r := bufio.NewReader(bytes.NewReader(c.encoded))
		got, err := ReadReply(r)
		if err != nil {
			t.Errorf("%s: ReadReply(%q) error: %v", c.name, visualize(c.encoded), err)
			continue
		}
		if !bytes.Equal(got, c.encoded) {
			t.Errorf("%s: ReadReply captured %q, want verbatim %q",
				c.name, visualize(got), visualize(c.encoded))
		}
		// No trailing bytes should remain: the encoder emits exactly one frame.
		if _, err := r.ReadByte(); err == nil {
			t.Errorf("%s: encoder emitted more than one frame", c.name)
		}
	}
}

// TestRESP2NullConventionsDistinct locks the three null-ish encodings as
// mutually distinct byte frames, the distinction clients disambiguate.
// Requirements 1.2, 1.3, 1.4.
func TestRESP2NullConventionsDistinct(t *testing.T) {
	nullBulk := resp.AppendNullBulk(nil)
	emptyArr := resp.AppendEmptyArray(nil)
	nullArr := resp.AppendNullArray(nil)

	if bytes.Equal(nullBulk, emptyArr) || bytes.Equal(nullBulk, nullArr) || bytes.Equal(emptyArr, nullArr) {
		t.Fatalf("null encodings must be distinct: $-1=%q *0=%q *-1=%q",
			nullBulk, emptyArr, nullArr)
	}

	// And each must round-trip through ReadReply as its own distinct frame.
	for _, want := range []string{"$-1\r\n", "*0\r\n", "*-1\r\n"} {
		got := mustRead(t, want)
		if string(got) != want {
			t.Fatalf("null frame %q captured as %q", want, visualize(got))
		}
	}
}

// TestRESP2ErrorTextConsistency is Property 6: every error the router can emit
// renders to the exact Pika v3.2.2 wire line. This covers the fixed error
// constants plus the two constructed error texts. Requirements 1.1, 1.6.
func TestRESP2ErrorTextConsistency(t *testing.T) {
	cases := []struct {
		name string
		body string
		wire string
	}{
		{"wrongtype", resp.ErrWrongType,
			"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{"not-integer", resp.ErrNotInteger, "-ERR value is not an integer or out of range\r\n"},
		{"syntax", resp.ErrSyntax, "-ERR syntax error\r\n"},
		{"noauth", resp.ErrNoAuth, "-NOAUTH Authentication required.\r\n"},
		{"backend-limit", resp.ErrValueExceedsBackendLimit, "-ERR value exceeds backend limit (400KB)\r\n"},
		{"invalid-cursor", resp.ErrInvalidCursor, "-ERR invalid cursor\r\n"},
		{"wrong-args-get", resp.ErrWrongNumberOfArgs("GET"),
			"-ERR wrong number of arguments for 'get' command\r\n"},
		{"wrong-args-hset-upper", resp.ErrWrongNumberOfArgs("HSET"),
			"-ERR wrong number of arguments for 'hset' command\r\n"},
		{"unknown-hello", resp.ErrUnknownCommand("HELLO"), "-ERR unknown command 'HELLO'\r\n"},
		{"unknown-foo", resp.ErrUnknownCommand("FOO"), "-ERR unknown command 'FOO'\r\n"},
	}
	for _, c := range cases {
		got := resp.AppendError(nil, c.body)
		if string(got) != c.wire {
			t.Errorf("%s: error wire = %q, want %q", c.name, visualize(got), visualize([]byte(c.wire)))
		}
		// The rendered error must also be a single, verbatim-captured frame.
		if rt := mustRead(t, string(got)); !bytes.Equal(rt, got) {
			t.Errorf("%s: error frame not captured verbatim: got %q want %q",
				c.name, visualize(rt), visualize(got))
		}
	}
}

// TestRESP2CodecRoundTripProperty is the property-based half of task 4.3:
// for arbitrary Integer and Bulk String payloads, the resp encoder output is
// captured verbatim by ReadReply as exactly one frame. This asserts the codec
// and the differential wire reader agree across the whole input space, not just
// the enumerated examples. Requirement 1.6.
//
// Property 6: error-text consistency (the error arm below).
// Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.6.
func TestRESP2CodecRoundTripProperty(t *testing.T) {
	// Integer arm: any int64 encodes and round-trips verbatim.
	intProp := func(n int64) bool {
		enc := resp.AppendInt(nil, n)
		if string(enc) != ":"+strconv.FormatInt(n, 10)+"\r\n" {
			return false
		}
		return frameRoundTrips(enc)
	}
	if err := quick.Check(intProp, &quick.Config{MaxCount: 5000}); err != nil {
		t.Fatalf("integer codec round-trip property failed: %v", err)
	}

	// Bulk arm: any byte slice (binary-safe, may contain CRLF) encodes as a
	// length-prefixed bulk and round-trips verbatim.
	bulkProp := func(b []byte) bool {
		enc := resp.AppendBulkString(nil, b)
		var want bytes.Buffer
		want.WriteByte('$')
		want.WriteString(strconv.Itoa(len(b)))
		want.WriteString("\r\n")
		want.Write(b)
		want.WriteString("\r\n")
		if !bytes.Equal(enc, want.Bytes()) {
			return false
		}
		return frameRoundTrips(enc)
	}
	if err := quick.Check(bulkProp, &quick.Config{MaxCount: 5000}); err != nil {
		t.Fatalf("bulk codec round-trip property failed: %v", err)
	}

	// Array arm: any slice of bulk elements round-trips; a nil slice must encode
	// as the null array *-1 and a non-nil empty slice as the empty array *0.
	arrProp := func(elems [][]byte) bool {
		enc := resp.AppendBulkArray(nil, elems)
		if elems == nil {
			if string(enc) != "*-1\r\n" {
				return false
			}
		} else if len(elems) == 0 {
			if string(enc) != "*0\r\n" {
				return false
			}
		}
		return frameRoundTrips(enc)
	}
	if err := quick.Check(arrProp, &quick.Config{MaxCount: 3000}); err != nil {
		t.Fatalf("array codec round-trip property failed: %v", err)
	}

	// Error arm (Property 6): any error body renders to a single verbatim frame.
	errProp := func(body string) bool {
		// RESP error bodies are single-line; reject generated bodies containing
		// CR or LF as those are not valid error payloads (out of input space).
		if bytes.ContainsAny([]byte(body), "\r\n") {
			return true
		}
		enc := resp.AppendError(nil, body)
		if string(enc) != "-"+body+"\r\n" {
			return false
		}
		return frameRoundTrips(enc)
	}
	if err := quick.Check(errProp, &quick.Config{MaxCount: 5000}); err != nil {
		t.Fatalf("error codec round-trip property failed: %v", err)
	}
}

// frameRoundTrips reports whether enc is captured verbatim by ReadReply as
// exactly one frame with no trailing bytes.
func frameRoundTrips(enc []byte) bool {
	r := bufio.NewReader(bytes.NewReader(enc))
	got, err := ReadReply(r)
	if err != nil {
		return false
	}
	if !bytes.Equal(got, enc) {
		return false
	}
	_, err = r.ReadByte()
	return err != nil // must be EOF: exactly one frame
}

// --- Env-guarded live differential -----------------------------------------

// TestDiffRESP2Codec replays the RESP2-codec sequences against a live Pika
// oracle and redimos, comparing raw replies byte-for-byte. It skips cleanly
// when PIKA_ADDR / REDIMOS_ADDR are unset so `go test ./...` needs no infra.
// Requirement 1.6; Property 6.
func TestDiffRESP2Codec(t *testing.T) {
	ep := endpointsFromEnv(t)
	if ep.Timeout == 0 {
		ep.Timeout = 5 * time.Second
	}
	t.Logf("resp2 codec differential: %d sequences %v",
		len(RESP2CodecSequences()), RESP2CodecSequenceNames())

	for _, seq := range RESP2CodecSequences() {
		seq := seq
		t.Run(seq.Name, func(t *testing.T) {
			diffs, err := CompareSequence(ep, seq)
			if err != nil {
				t.Fatalf("sequence %q could not run: %v", seq.Name, err)
			}
			for _, d := range diffs {
				t.Errorf("%s", d)
			}
		})
	}
}
