package command

import (
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
				// Absolute expiry in epoch seconds. Sub-second precision is not
				// stored (Pika v3.2.2 has none), but a positive sub-second PX must
				// not instant-delete the key, and a huge PX must not overflow into a
				// bogus permanent/negative-TTL key — msExpiryEpoch handles both.
				o.expEpoch = msExpiryEpoch(now, n)
			} else {
				o.expEpoch = secExpiryEpoch(now, n)
			}
			i++ // consume the value argument.
		default:
			return setOptions{}, resp.ErrSyntax
		}
	}

	return o, ""
}

// parseFloatArg parses an INCRBYFLOAT / HINCRBYFLOAT increment with Redis 3.2's exact
// semantics, and ok is false when the argument is not a valid increment. The subtle part
// is ±Inf, which Redis treats in two different ways depending on how it is spelled — and
// ParseFloat already reproduces the split:
//
//   - the LITERAL "inf"/"+inf"/"-inf" is ACCEPTED at parse (Redis' string2ld only rejects
//     an overflow-to-HUGE_VAL, not a representable infinity); the command then fails on the
//     non-finite RESULT with "increment would produce NaN or Infinity". strconv.ParseFloat
//     likewise returns (+Inf, nil) for the literal, so ParseFloat returns it and we defer
//     to the store's inf/NaN-result guard — verified against the live oracle.
//   - an OVERFLOWING magnitude like "1e400" is REJECTED at parse with "value is not a valid
//     float" (string2ld sees errno==ERANGE with value==HUGE_VAL); strconv returns ErrRange
//     for it, so ParseFloat returns ErrNotFloat and we reject here.
//
// NaN is rejected at parse by ParseFloat, matching Redis.
func parseFloatArg(arg []byte) (float64, bool) {
	f, err := ParseFloat(arg)
	return f, err == nil
}
