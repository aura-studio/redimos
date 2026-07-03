package resp

import "testing"

func TestAppendSimpleString(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"OK", "+OK\r\n"},
		{"PONG", "+PONG\r\n"},
		{"", "+\r\n"},
	}
	for _, c := range cases {
		if got := string(AppendSimpleString(nil, c.in)); got != c.want {
			t.Errorf("AppendSimpleString(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestAppendError(t *testing.T) {
	got := string(AppendError(nil, ErrSyntax))
	want := "-ERR syntax error\r\n"
	if got != want {
		t.Errorf("AppendError = %q, want %q", got, want)
	}
}

func TestAppendInt(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, ":0\r\n"},
		{1, ":1\r\n"},
		{-1, ":-1\r\n"},
		{-2, ":-2\r\n"},
		{9223372036854775807, ":9223372036854775807\r\n"},
		{-9223372036854775808, ":-9223372036854775808\r\n"},
	}
	for _, c := range cases {
		if got := string(AppendInt(nil, c.in)); got != c.want {
			t.Errorf("AppendInt(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestAppendBulkString(t *testing.T) {
	cases := []struct {
		in   []byte
		want string
	}{
		{[]byte("hello"), "$5\r\nhello\r\n"},
		{[]byte(""), "$0\r\n\r\n"},             // empty (non-null) bulk
		{[]byte("a\r\nb"), "$4\r\na\r\nb\r\n"}, // embedded CRLF is length-prefixed
	}
	for _, c := range cases {
		if got := string(AppendBulkString(nil, c.in)); got != c.want {
			t.Errorf("AppendBulkString(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestNullConventions locks in the three distinct null-ish encodings required by
// Requirements 1.2, 1.3, 1.4.
func TestNullConventions(t *testing.T) {
	if got := string(AppendNullBulk(nil)); got != "$-1\r\n" {
		t.Errorf("AppendNullBulk = %q, want %q", got, "$-1\r\n")
	}
	if got := string(AppendEmptyArray(nil)); got != "*0\r\n" {
		t.Errorf("AppendEmptyArray = %q, want %q", got, "*0\r\n")
	}
	if got := string(AppendNullArray(nil)); got != "*-1\r\n" {
		t.Errorf("AppendNullArray = %q, want %q", got, "*-1\r\n")
	}
}

func TestAppendArrayHeader(t *testing.T) {
	cases := []struct {
		in   int
		want string
	}{
		{0, "*0\r\n"},
		{1, "*1\r\n"},
		{3, "*3\r\n"},
	}
	for _, c := range cases {
		if got := string(AppendArrayHeader(nil, c.in)); got != c.want {
			t.Errorf("AppendArrayHeader(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestAppendBulkArray(t *testing.T) {
	// nil slice -> null array
	if got := string(AppendBulkArray(nil, nil)); got != "*-1\r\n" {
		t.Errorf("AppendBulkArray(nil) = %q, want %q", got, "*-1\r\n")
	}
	// non-nil empty slice -> empty array
	if got := string(AppendBulkArray(nil, [][]byte{})); got != "*0\r\n" {
		t.Errorf("AppendBulkArray(empty) = %q, want %q", got, "*0\r\n")
	}
	// populated array
	got := string(AppendBulkArray(nil, [][]byte{[]byte("a"), []byte("bc")}))
	want := "*2\r\n$1\r\na\r\n$2\r\nbc\r\n"
	if got != want {
		t.Errorf("AppendBulkArray = %q, want %q", got, want)
	}
}

// TestAppendReusesBuffer verifies the Append* helpers append to an existing
// buffer rather than overwriting it, which the Writer relies on.
func TestAppendReusesBuffer(t *testing.T) {
	buf := AppendSimpleString(nil, "OK")
	buf = AppendInt(buf, 5)
	if got, want := string(buf), "+OK\r\n:5\r\n"; got != want {
		t.Errorf("chained append = %q, want %q", got, want)
	}
}

// TestErrorConstants pins the byte-for-byte error text to the design's
// "错误文案逐字对齐" section (Requirements 2.6, 3.4-3.8, 11.2, 14.1-14.2).
func TestErrorConstants(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"WRONGTYPE", ErrWrongType, "WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"not integer", ErrNotInteger, "ERR value is not an integer or out of range"},
		{"syntax", ErrSyntax, "ERR syntax error"},
		{"noauth", ErrNoAuth, "NOAUTH Authentication required."},
		{"backend limit", ErrValueExceedsBackendLimit, "ERR value exceeds backend limit (400KB)"},
		{"invalid cursor", ErrInvalidCursor, "ERR invalid cursor, restart scan"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}
}

func TestErrWrongNumberOfArgs(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"GET", "ERR wrong number of arguments for 'get' command"},
		{"get", "ERR wrong number of arguments for 'get' command"},
		{"HSET", "ERR wrong number of arguments for 'hset' command"},
	}
	for _, c := range cases {
		if got := ErrWrongNumberOfArgs(c.in); got != c.want {
			t.Errorf("ErrWrongNumberOfArgs(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestErrUnknownCommand(t *testing.T) {
	// Case is preserved exactly as the client sent it.
	cases := []struct {
		in   string
		want string
	}{
		{"HELLO", "ERR unknown command 'HELLO'"},
		{"FOO", "ERR unknown command 'FOO'"},
		{"foobar", "ERR unknown command 'foobar'"},
	}
	for _, c := range cases {
		if got := ErrUnknownCommand(c.in); got != c.want {
			t.Errorf("ErrUnknownCommand(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestErrorWireForm confirms constants render to the correct full wire line
// when passed through AppendError.
func TestErrorWireForm(t *testing.T) {
	got := string(AppendError(nil, ErrWrongType))
	want := "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	if got != want {
		t.Errorf("wire form = %q, want %q", got, want)
	}
}
