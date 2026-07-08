package command

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"

	"github.com/aura-studio/redimos/internal/resp"
	"github.com/aura-studio/redimos/internal/server"
)

// TestParseIntValid covers accepted integers including the int64 boundaries.
func TestParseIntValid(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"0", 0},
		{"1", 1},
		{"-1", -1},
		{"10", 10},
		{"-10", -10},
		{"123456789", 123456789},
		{"-123456789", -123456789},
		{strconv.FormatInt(math.MaxInt64, 10), math.MaxInt64}, // 9223372036854775807
		{strconv.FormatInt(math.MinInt64, 10), math.MinInt64}, // -9223372036854775808
	}
	for _, tc := range cases {
		got, err := ParseInt([]byte(tc.in))
		if err != nil {
			t.Errorf("ParseInt(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseInt(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// TestParseIntInvalid covers rejected inputs: non-numeric, empty, whitespace,
// leading '+'/zeros, and int64 overflow in both directions.
func TestParseIntInvalid(t *testing.T) {
	cases := []string{
		"",                     // empty
		"-",                    // bare sign
		"+5",                   // leading plus rejected (Redis accepts only '-')
		"007",                  // leading zero on multi-digit value
		"01",                   // leading zero
		" 5",                   // leading whitespace
		"5 ",                   // trailing whitespace
		"1 2",                  // embedded whitespace
		"abc",                  // non-numeric
		"3.14",                 // float
		"12a",                  // trailing junk
		"9223372036854775808",  // MaxInt64 + 1 (overflow)
		"-9223372036854775809", // MinInt64 - 1 (overflow)
		"99999999999999999999", // far beyond uint64
	}
	for _, in := range cases {
		got, err := ParseInt([]byte(in))
		if !errors.Is(err, ErrNotInteger) {
			t.Errorf("ParseInt(%q) err = %v, want ErrNotInteger", in, err)
		}
		if got != 0 {
			t.Errorf("ParseInt(%q) value = %d, want 0 on error", in, got)
		}
	}
}

// TestSentinelErrorTextMatchesWire ensures the sentinels carry the exact RESP2
// message body so WriteArgError and direct mapping stay byte-for-byte correct.
func TestSentinelErrorTextMatchesWire(t *testing.T) {
	if ErrNotInteger.Error() != resp.ErrNotInteger {
		t.Errorf("ErrNotInteger text = %q, want %q", ErrNotInteger.Error(), resp.ErrNotInteger)
	}
	if ErrSyntax.Error() != resp.ErrSyntax {
		t.Errorf("ErrSyntax text = %q, want %q", ErrSyntax.Error(), resp.ErrSyntax)
	}
}

// TestWriteArgErrorUnknown verifies an unrecognized error is reported as
// unhandled without writing anything (safe with a nil connection). The
// recognized/wrapped-sentinel paths are exercised over a real connection in
// TestArgHelpersOverConnection.
func TestWriteArgErrorUnknown(t *testing.T) {
	if WriteArgError(nil, errors.New("boom")) {
		t.Error("WriteArgError reported handling an unknown error")
	}
	if WriteArgError(nil, nil) {
		t.Error("WriteArgError reported handling a nil error")
	}
}

type wrapErr struct{ inner error }

func (w *wrapErr) Error() string { return "wrapped: " + w.inner.Error() }
func (w *wrapErr) Unwrap() error { return w.inner }

// TestArgHelpersOverConnection drives ParseIntReply, WriteNotInteger and
// WriteSyntaxError through the real server + router path and asserts the exact
// bytes returned to the client.
func TestArgHelpersOverConnection(t *testing.T) {
	tbl := NewTable()
	// INCRBY-style: parse args[2] as an integer, echo it back on success.
	tbl.Register("PARSEINT", 3, false, func(_ context.Context, c *server.Conn, args [][]byte) {
		n, ok := ParseIntReply(c, args[2])
		if !ok {
			return
		}
		c.Redcon().WriteInt64(n)
	})
	// A handler that always reports a syntax error.
	tbl.Register("SYNTAX", 1, false, func(_ context.Context, c *server.Conn, _ [][]byte) {
		WriteSyntaxError(c)
	})
	// A handler that always reports not-an-integer directly.
	tbl.Register("NOTINT", 1, false, func(_ context.Context, c *server.Conn, _ [][]byte) {
		WriteNotInteger(c)
	})
	// A handler that maps a wrapped sentinel via WriteArgError.
	tbl.Register("WRAPSYN", 1, false, func(_ context.Context, c *server.Conn, _ [][]byte) {
		if !WriteArgError(c, &wrapErr{ErrSyntax}) {
			// Fall back to a distinguishable reply so the test fails loudly.
			c.Redcon().WriteString("UNHANDLED")
		}
	})

	conn, r := startRouterServer(t, tbl)

	if got, want := sendLine(t, conn, r, "PARSEINT k 42"), ":42"; got != want {
		t.Errorf("PARSEINT k 42 = %q, want %q", got, want)
	}
	if got, want := sendLine(t, conn, r, "PARSEINT k notnum"), "-"+resp.ErrNotInteger; got != want {
		t.Errorf("PARSEINT k notnum = %q, want %q", got, want)
	}
	if got, want := sendLine(t, conn, r, "PARSEINT k 9223372036854775808"), "-"+resp.ErrNotInteger; got != want {
		t.Errorf("PARSEINT overflow = %q, want %q", got, want)
	}
	if got, want := sendLine(t, conn, r, "SYNTAX"), "-"+resp.ErrSyntax; got != want {
		t.Errorf("SYNTAX = %q, want %q", got, want)
	}
	if got, want := sendLine(t, conn, r, "NOTINT"), "-"+resp.ErrNotInteger; got != want {
		t.Errorf("NOTINT = %q, want %q", got, want)
	}
	// Wrapped sentinels must still be recognized via errors.Is and mapped to
	// the exact wire text.
	if got, want := sendLine(t, conn, r, "WRAPSYN"), "-"+resp.ErrSyntax; got != want {
		t.Errorf("WRAPSYN = %q, want %q", got, want)
	}
}
