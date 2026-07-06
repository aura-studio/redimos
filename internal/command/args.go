package command

import (
	"errors"
	"math"
	"strconv"
	"strings"

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

	// ErrNotFloat signals that an argument the command required to be a float was
	// not a valid float (or was NaN). Its text is the exact
	// "-ERR value is not a valid float" wire message. Requirement 6.1.
	ErrNotFloat = errors.New(resp.ErrNotValidFloat)
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

// ParseFloat parses a command argument as a base-10 float64 (Redis' "value is a valid
// float" semantics): the whole string must be a finite-or-infinite number and NaN is
// rejected. It is the float companion of ParseInt. It does NOT accept the score-specific
// "inf"/"+inf"/"-inf" spellings or a leading "(" exclusive marker — those are handled by
// parseScore / parseScoreBound, which layer their rules on top of this. On violation it
// returns ErrNotFloat. Requirement 6.1.
func ParseFloat(arg []byte) (float64, error) {
	s := string(arg)
	// Redis' getDoubleFromObject is strtod-based, and strtod does NOT accept Go's
	// underscore digit separators ("1_000"). Go's strconv.ParseFloat DOES, so reject any
	// '_' up front to avoid accepting a value Redis would reject as "not a valid float".
	if strings.IndexByte(s, '_') >= 0 {
		return 0, ErrNotFloat
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		// strtod also parses a hex integer constant WITHOUT the binary 'p' exponent that
		// Go requires ("0x10" -> 16, "0x1f" -> 31), so redimos would otherwise reject hex
		// numbers Redis accepts. Retry those with a zero exponent appended.
		if hf, ok := parseHexNoExp(s); ok {
			f = hf
		} else {
			return 0, ErrNotFloat
		}
	}
	if math.IsNaN(f) {
		return 0, ErrNotFloat
	}
	return f, nil
}

// parseHexNoExp accepts a C-strtod-style hex INTEGER constant that lacks the binary 'p'
// exponent Go's ParseFloat requires (e.g. "0x1f", "-0x10") by appending "p0" and
// re-parsing. It returns ok=false for anything that is not such a constant — including hex
// values that already have an exponent or a fractional '.' — leaving ParseFloat's normal
// rejection in place.
func parseHexNoExp(s string) (float64, bool) {
	body := s
	if len(body) > 0 && (body[0] == '+' || body[0] == '-') {
		body = body[1:]
	}
	if len(body) < 3 || body[0] != '0' || (body[1] != 'x' && body[1] != 'X') {
		return 0, false
	}
	if strings.ContainsAny(body, "pP.") {
		return 0, false
	}
	f, err := strconv.ParseFloat(s+"p0", 64)
	return f, err == nil
}

// ParseFloatReply is the float analogue of ParseIntReply: on failure it writes the
// "-ERR value is not a valid float" reply and returns ok=false. Requirement 6.1.
func ParseFloatReply(c *server.Conn, arg []byte) (float64, bool) {
	f, err := ParseFloat(arg)
	if err != nil {
		WriteNotFloat(c)
		return 0, false
	}
	return f, true
}

// WriteNotFloat writes the RESP2 "-ERR value is not a valid float" reply. Requirement 6.1.
func WriteNotFloat(c *server.Conn) {
	writeError(c, resp.ErrNotValidFloat)
}

// Args is a thin typed view over a command's raw arguments for handlers that prefer
// index-based, self-documenting access instead of manual args[i] indexing plus strconv.
// Bounds are guaranteed by the router's arity validation before dispatch, so At does not
// re-check; the typed accessors delegate to the canonical ParseInt/ParseFloat so error
// text stays byte-for-byte identical everywhere. Adoption is incremental — handlers can be
// migrated to it over time without changing behavior.
type Args [][]byte

// Len reports the argument count (including the command name at index 0).
func (a Args) Len() int { return len(a) }

// At returns the raw bytes at index i (caller ensures i is within the validated arity).
func (a Args) At(i int) []byte { return a[i] }

// Str returns the argument at index i as a string.
func (a Args) Str(i int) string { return string(a[i]) }

// Int parses the argument at index i as a strict signed 64-bit integer (ParseInt).
func (a Args) Int(i int) (int64, error) { return ParseInt(a[i]) }

// Float parses the argument at index i as a float64 (ParseFloat).
func (a Args) Float(i int) (float64, error) { return ParseFloat(a[i]) }

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
	case errors.Is(err, ErrNotFloat):
		writeError(c, resp.ErrNotValidFloat)
	case errors.Is(err, ErrSyntax):
		writeError(c, resp.ErrSyntax)
	default:
		return false
	}
	return true
}
