package difftest

import (
	"bufio"
	"bytes"
	"math/rand"
	"strconv"
	"testing"
	"testing/quick"
)

// --- Unit tests: EncodeCommand ---------------------------------------------

func TestEncodeCommand(t *testing.T) {
	got := EncodeCommandStrings("SET", "k", "v")
	want := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
	if string(got) != want {
		t.Fatalf("EncodeCommand mismatch:\n got %q\nwant %q", got, want)
	}
}

func TestEncodeCommandBinarySafe(t *testing.T) {
	// A value containing CRLF must be length-prefixed, not terminated early.
	val := []byte("a\r\nb")
	got := EncodeCommand([]byte("SET"), []byte("k"), val)
	want := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\r\nb\r\n"
	if string(got) != want {
		t.Fatalf("binary-safe encode mismatch:\n got %q\nwant %q", got, want)
	}
}

// --- Unit tests: ReadReply on each RESP2 type ------------------------------

func TestReadReplyTypes(t *testing.T) {
	cases := []struct {
		name  string
		frame string
	}{
		{"simple-string", "+OK\r\n"},
		{"pong", "+PONG\r\n"},
		{"error", "-ERR unknown command 'FOO'\r\n"},
		{"wrongtype", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{"integer", ":12345\r\n"},
		{"negative-integer", ":-2\r\n"},
		{"bulk", "$5\r\nhello\r\n"},
		{"empty-bulk", "$0\r\n\r\n"},
		{"null-bulk", "$-1\r\n"},
		{"empty-array", "*0\r\n"},
		{"null-array", "*-1\r\n"},
		{"array", "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"},
		{"nested-array", "*2\r\n:1\r\n*2\r\n$1\r\na\r\n$-1\r\n"},
		{"bulk-with-crlf", "$4\r\na\r\nb\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := bufio.NewReader(bytes.NewReader([]byte(tc.frame)))
			got, err := ReadReply(r)
			if err != nil {
				t.Fatalf("ReadReply(%q) error: %v", tc.frame, err)
			}
			if string(got) != tc.frame {
				t.Fatalf("ReadReply did not capture exact bytes:\n got %q\nwant %q", got, tc.frame)
			}
		})
	}
}

// TestReadReplyDistinguishesNulls is the crux of task 2.2: $-1, *0 and *-1 must
// be captured as distinct byte frames, never collapsed together.
func TestReadReplyDistinguishesNulls(t *testing.T) {
	nullBulk := mustRead(t, "$-1\r\n")
	emptyArr := mustRead(t, "*0\r\n")
	nullArr := mustRead(t, "*-1\r\n")

	if bytes.Equal(nullBulk, emptyArr) || bytes.Equal(nullBulk, nullArr) || bytes.Equal(emptyArr, nullArr) {
		t.Fatalf("null encodings must be distinct: $-1=%q *0=%q *-1=%q", nullBulk, emptyArr, nullArr)
	}
}

func mustRead(t *testing.T, frame string) []byte {
	t.Helper()
	r := bufio.NewReader(bytes.NewReader([]byte(frame)))
	got, err := ReadReply(r)
	if err != nil {
		t.Fatalf("ReadReply(%q) error: %v", frame, err)
	}
	return got
}

// TestReadReplyStopsAtFrameBoundary verifies the reader consumes exactly one
// reply and leaves subsequent frames intact (needed for pipelining).
func TestReadReplyStopsAtFrameBoundary(t *testing.T) {
	stream := "+OK\r\n:42\r\n$3\r\nabc\r\n"
	r := bufio.NewReader(bytes.NewReader([]byte(stream)))

	want := []string{"+OK\r\n", ":42\r\n", "$3\r\nabc\r\n"}
	for i, w := range want {
		got, err := ReadReply(r)
		if err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
		if string(got) != w {
			t.Fatalf("reply %d: got %q want %q", i, got, w)
		}
	}
}

func TestReadReplyErrors(t *testing.T) {
	bad := []string{
		"",             // empty stream
		"?bogus\r\n",   // unknown type prefix
		"$5\r\nhi\r\n", // bulk length longer than body
		"*1\r\n",       // array promises an element that never arrives
	}
	for _, frame := range bad {
		r := bufio.NewReader(bytes.NewReader([]byte(frame)))
		if _, err := ReadReply(r); err == nil {
			t.Fatalf("ReadReply(%q) expected error, got nil", frame)
		}
	}
}

// --- Property test: ReadReply captures exact frame bytes -------------------

// randomFrame builds a random, well-formed RESP2 reply frame and returns its
// raw bytes. depth bounds recursion for arrays.
func randomFrame(r *rand.Rand, depth int) []byte {
	kind := r.Intn(5)
	if depth <= 0 {
		kind = r.Intn(4) // no arrays at max depth
	}
	switch kind {
	case 0: // simple string
		return []byte("+" + randToken(r) + "\r\n")
	case 1: // error
		return []byte("-ERR " + randToken(r) + "\r\n")
	case 2: // integer
		return []byte(":" + randIntStr(r) + "\r\n")
	case 3: // bulk (including null and empty)
		n := r.Intn(8) - 1 // -1 => null bulk
		if n < 0 {
			return []byte("$-1\r\n")
		}
		body := make([]byte, n)
		for i := range body {
			body[i] = byte(r.Intn(256)) // binary-safe, may include CRLF
		}
		var b bytes.Buffer
		b.WriteString("$")
		b.WriteString(strconv.Itoa(n))
		b.WriteString("\r\n")
		b.Write(body)
		b.WriteString("\r\n")
		return b.Bytes()
	default: // array (including null and empty)
		n := r.Intn(5) - 1 // -1 => null array, 0 => empty array
		if n < 0 {
			return []byte("*-1\r\n")
		}
		var b bytes.Buffer
		b.WriteString("*")
		b.WriteString(strconv.Itoa(n))
		b.WriteString("\r\n")
		for i := 0; i < n; i++ {
			b.Write(randomFrame(r, depth-1))
		}
		return b.Bytes()
	}
}

func randToken(r *rand.Rand) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789 "
	n := r.Intn(10)
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[r.Intn(len(alphabet))]
	}
	return string(b)
}

// TestReadReplyRoundTripProperty asserts that for any two concatenated
// well-formed frames, ReadReply returns the first frame's exact bytes and then
// the second frame's exact bytes. This is the invariant the differential
// engine relies on: raw replies are captured verbatim and frame boundaries are
// respected.
func TestReadReplyRoundTripProperty(t *testing.T) {
	f := func(seed int64) bool {
		r := rand.New(rand.NewSource(seed))
		f1 := randomFrame(r, 3)
		f2 := randomFrame(r, 3)
		stream := append(append([]byte{}, f1...), f2...)

		br := bufio.NewReader(bytes.NewReader(stream))
		got1, err := ReadReply(br)
		if err != nil {
			t.Logf("frame1 %q err %v", f1, err)
			return false
		}
		got2, err := ReadReply(br)
		if err != nil {
			t.Logf("frame2 %q err %v", f2, err)
			return false
		}
		return bytes.Equal(got1, f1) && bytes.Equal(got2, f2)
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 2000}); err != nil {
		t.Fatalf("round-trip property failed: %v", err)
	}
}
