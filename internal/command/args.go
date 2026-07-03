package command

import (
	"errors"
	"math"

	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// This file holds the reusable argument parsing and validation helpers shared
// by the per-family command handlers. They centralize the two generic
// argument-error replies whose wire text must match Pika v3.2.2 byte-for-byte
// (design.md "错误文案逐字对齐"):
//
//   - non-integer / out-of-range integer -> "-ERR value is not an integer or out of range" (requirement 3.4, 5.9)
//   - illegal optional-argument combination -> "-ERR syntax error"                          (requirement 3.5)
//
// Handlers can use these in two styles:
//
//  1. Sentinel errors returned from a pure parse step and later mapped to the
//     wire with WriteArgError — convenient when a handler validates several
//     arguments and wants a single error-writing site.
//  2. The Write* helpers (and ParseIntReply) that emit the reply directly via
//     the same resp.Writer path as the router's writeError, so a handler can
//     validate-and-reply then return early.

// Sentinel argument-validation errors. Their Error() text is exactly the RESP2
// error message body (without the leading '-' or trailing CRLF), so they map
// one-to-one to the resp package constants and can be handed to WriteArgError.
var (
	// ErrNotInteger signals that an argument the command required to be an
	// integer was non-numeric or did not fit in a signed 64-bit integer.
	// Requirement 3.4, 5.9.
	ErrNotInteger = errors.New(resp.ErrNotInteger)

	// ErrSyntax signals an illegal optional-argument combination (e.g. mutually
	// exclusive flags, an unknown option token, or a missing option value).
	// Requirement 3.5.
	ErrSyntax = errors.New(resp.ErrSyntax)
)

// minInt64Abs is the absolute value of math.MinInt64 (2^63), which does not fit
// in an int64 but does in a uint64. maxInt64Abs is math.MaxInt64 (2^63 - 1).
const (
	maxInt64Abs = uint64(math.MaxInt64)
	minInt64Abs = uint64(math.MaxInt64) + 1
)

// ParseInt parses a command argument as a strict base-10 signed 64-bit integer,
// following Redis' string2ll semantics so replies match the Pika v3.2.2 oracle:
//
//   - the empty string is rejected;
//   - a lone "0" is 0, but multi-digit values may not have a leading zero;
//   - a leading '+' is rejected (Redis accepts only an optional leading '-');
//   - surrounding or embedded whitespace is rejected;
//   - the value must fit in int64 — anything larger (or a bare "-") is rejected.
//
// On any violation it returns ErrNotInteger, whose text is the exact
// "-ERR value is not an integer or out of range" wire message. Requirement 3.4,
// 5.9.
func ParseInt(arg []byte) (int64, error) {
	n := len(arg)
	if n == 0 {
		return 0, ErrNotInteger
	}
	// A single "0" is the only value permitted to start with '0'.
	if n == 1 && arg[0] == '0' {
		return 0, nil
	}

	i := 0
	negative := false
	if arg[0] == '-' {
		negative = true
		i = 1
		if i == n { // just "-"
			return 0, ErrNotInteger
		}
	}

	// The first significant digit must be 1-9: this rejects a leading '+', a
	// leading zero on a multi-digit number, and any non-digit first byte.
	if arg[i] < '1' || arg[i] > '9' {
		return 0, ErrNotInteger
	}

	var v uint64 = uint64(arg[i] - '0')
	i++
	for ; i < n; i++ {
		c := arg[i]
		if c < '0' || c > '9' {
			return 0, ErrNotInteger
		}
		d := uint64(c - '0')
		// Reject before v*10+d would overflow uint64; the tighter int64 range
		// check below then rejects anything past the signed limits.
		if v > (math.MaxUint64-d)/10 {
			return 0, ErrNotInteger
		}
		v = v*10 + d
	}

	if negative {
		if v > minInt64Abs {
			return 0, ErrNotInteger
		}
		if v == minInt64Abs {
			return math.MinInt64, nil
		}
		return -int64(v), nil
	}
	if v > maxInt64Abs {
		return 0, ErrNotInteger
	}
	return int64(v), nil
}

// ParseIntReply parses arg as an integer with ParseInt. On success it returns
// the value and ok=true. On failure it writes the
// "-ERR value is not an integer or out of range" reply to the connection and
// returns ok=false, so a handler can simply `return` after a false result.
// Requirement 3.4, 5.9.
func ParseIntReply(c *server.Conn, arg []byte) (int64, bool) {
	v, err := ParseInt(arg)
	if err != nil {
		WriteNotInteger(c)
		return 0, false
	}
	return v, true
}

// WriteNotInteger writes the RESP2 "-ERR value is not an integer or out of
// range" reply. Requirement 3.4, 5.9.
func WriteNotInteger(c *server.Conn) {
	writeError(c, resp.ErrNotInteger)
}

// WriteSyntaxError writes the RESP2 "-ERR syntax error" reply, used when a
// command's optional-argument combination is illegal. Requirement 3.5.
func WriteSyntaxError(c *server.Conn) {
	writeError(c, resp.ErrSyntax)
}

// WriteArgError maps a sentinel argument-validation error to its byte-for-byte
// wire text and writes it via the same resp.Writer path as the router's
// writeError. It reports whether err was a recognized argument sentinel
// (ErrNotInteger or ErrSyntax); callers pass any other error through their own
// handling. Requirement 3.4, 3.5.
func WriteArgError(c *server.Conn, err error) bool {
	switch {
	case errors.Is(err, ErrNotInteger):
		writeError(c, resp.ErrNotInteger)
	case errors.Is(err, ErrSyntax):
		writeError(c, resp.ErrSyntax)
	default:
		return false
	}
	return true
}
