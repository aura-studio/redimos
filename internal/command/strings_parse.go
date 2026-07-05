package command

import (
	"math"

	"github.com/aura-studio/redimos/v2/internal/resp"
)

// setOptions holds the parsed SET optional arguments.
type setOptions struct {
	nx bool
	xx bool

	// expSet is true when EX or PX was supplied; expEpoch is then the absolute
	// expiry in epoch seconds to write to meta.exp. When expSet is false the SET
	// clears any existing TTL (Redis/Pika SET semantics).
	expSet   bool
	expEpoch int64
}

// parseSetOptions parses the optional SET arguments following "SET key value".
// now is the current epoch seconds, used to turn a relative EX/PX interval into
// the absolute meta.exp. It returns the parsed options and an empty errMsg on
// success; on failure errMsg is the RESP2 error body to reply (syntax error,
// not-an-integer, or invalid-expire-time) and the options are unusable.
//
// Recognized tokens (case-insensitive): EX <seconds>, PX <milliseconds>, NX, XX.
// EX and PX are mutually exclusive, as are NX and XX; a repeated or conflicting
// option, an unknown token, or a missing EX/PX value is a syntax error. A
// non-integer EX/PX value is the not-an-integer error; a non-positive value is
// the invalid-expire-time error.
func parseSetOptions(opts [][]byte, now int64) (setOptions, string) {
	var o setOptions

	for i := 0; i < len(opts); i++ {
		switch toLower(string(opts[i])) {
		case "nx":
			if o.xx || o.nx {
				return setOptions{}, resp.ErrSyntax
			}
			o.nx = true
		case "xx":
			if o.nx || o.xx {
				return setOptions{}, resp.ErrSyntax
			}
			o.xx = true
		case "ex", "px":
			isMillis := toLower(string(opts[i])) == "px"
			if o.expSet || i+1 >= len(opts) {
				return setOptions{}, resp.ErrSyntax
			}
			n, err := ParseInt(opts[i+1])
			if err != nil {
				return setOptions{}, resp.ErrNotInteger
			}
			if n <= 0 {
				return setOptions{}, resp.ErrInvalidExpireTime("set")
			}
			o.expSet = true
			if isMillis {
				// Absolute expiry in epoch seconds, truncating sub-second
				// precision (Pika v3.2.2 has no millisecond precision).
				o.expEpoch = (now*1000 + n) / 1000
			} else {
				o.expEpoch = now + n
			}
			i++ // consume the value argument.
		default:
			return setOptions{}, resp.ErrSyntax
		}
	}

	return o, ""
}

// parseFloatArg parses an INCRBYFLOAT / HINCRBYFLOAT increment with Redis' semantics:
// a FINITE decimal/exponent, whole string consumed. ok is false when the argument is
// not a valid float. Redis rejects a non-finite increment ("inf", "1e400", ...) at
// parse time with "value is not a valid float" (string2ld rejects inf/nan); the shared
// ParseFloat only rejects NaN (it must still accept ±inf for ZADD scores), so also
// reject ±Inf here so the increment path matches Redis rather than deferring to the
// store's "increment would produce NaN or Infinity".
func parseFloatArg(arg []byte) (float64, bool) {
	f, err := ParseFloat(arg)
	if err != nil || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}
