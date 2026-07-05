// Package resp provides RESP2 (Redis serialization protocol v2) write helpers
// and the byte-for-byte error text constants that must match Pika v3.2.2.
//
// The Append* functions are the source of truth for the exact wire bytes. They
// give full control over the three distinct null-ish encodings that Redis
// clients disambiguate:
//
//   - null bulk string   -> "$-1\r\n"
//   - empty array        -> "*0\r\n"
//   - null (nil) array   -> "*-1\r\n"
//
// redcon's own Conn.WriteNull collapses these into a single form, so encoders
// that need precise control build bytes with the Append* helpers and hand them
// to redcon via Conn.WriteRaw (see Writer below). This keeps redcon wired for
// connection management while guaranteeing byte-for-byte oracle parity.
package resp

import "strconv"

// crlf terminates every RESP2 element.
const crlf = "\r\n"

// AppendSimpleString appends a RESP2 Simple String ("+<s>\r\n").
//
// Simple Strings must not contain CR or LF; callers pass short, controlled
// tokens such as "OK" or "PONG".
func AppendSimpleString(dst []byte, s string) []byte {
	dst = append(dst, '+')
	dst = append(dst, s...)
	return append(dst, crlf...)
}

// AppendError appends a RESP2 Error ("-<msg>\r\n").
//
// msg is the error text without the leading '-' or trailing CRLF, e.g. one of
// the ErrXxx constants or the result of ErrWrongNumberOfArgs/ErrUnknownCommand.
func AppendError(dst []byte, msg string) []byte {
	dst = append(dst, '-')
	dst = append(dst, msg...)
	return append(dst, crlf...)
}

// AppendInt appends a RESP2 Integer (":<n>\r\n").
func AppendInt(dst []byte, n int64) []byte {
	dst = append(dst, ':')
	dst = strconv.AppendInt(dst, n, 10)
	return append(dst, crlf...)
}

// AppendBulkString appends a RESP2 Bulk String ("$<len>\r\n<bytes>\r\n").
//
// A non-nil but empty slice encodes as an empty bulk string ("$0\r\n\r\n"),
// which is distinct from the null bulk string produced by AppendNullBulk.
func AppendBulkString(dst []byte, b []byte) []byte {
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(len(b)), 10)
	dst = append(dst, crlf...)
	dst = append(dst, b...)
	return append(dst, crlf...)
}

// AppendNullBulk appends the RESP2 null bulk string ("$-1\r\n").
//
// This is the canonical "value does not exist" reply for commands such as GET
// and for SET NX/XX rejections.
func AppendNullBulk(dst []byte) []byte {
	return append(dst, "$-1\r\n"...)
}

// AppendArrayHeader appends a RESP2 Array header ("*<n>\r\n"). The caller must
// then append exactly n elements.
//
// Passing n == 0 yields the empty array "*0\r\n"; use AppendNullArray for the
// distinct null array "*-1\r\n".
func AppendArrayHeader(dst []byte, n int) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(n), 10)
	return append(dst, crlf...)
}

// AppendEmptyArray appends the RESP2 empty array ("*0\r\n").
func AppendEmptyArray(dst []byte) []byte {
	return append(dst, "*0\r\n"...)
}

// AppendNullArray appends the RESP2 null array ("*-1\r\n"), used where Pika
// v3.2.2 replies with a nil array rather than an empty one.
func AppendNullArray(dst []byte) []byte {
	return append(dst, "*-1\r\n"...)
}

// AppendBulkArray appends an array of bulk strings. A nil elems slice encodes
// as the null array "*-1\r\n"; a non-nil empty slice encodes as "*0\r\n".
func AppendBulkArray(dst []byte, elems [][]byte) []byte {
	if elems == nil {
		return AppendNullArray(dst)
	}
	dst = AppendArrayHeader(dst, len(elems))
	for _, e := range elems {
		dst = AppendBulkString(dst, e)
	}
	return dst
}

// AppendOptBulkArray appends a RESP2 array whose element i is a bulk string when
// present[i] is true and the null bulk string "$-1" otherwise. It is the exact
// wire shape MGET (and later HMGET) needs: present values interleaved with null
// entries for missing / wrong-type / expired keys. The array length is len(present);
// values[i] is read only when present[i] is true, so a shorter or nil values slice
// is fine for all-null positions.
func AppendOptBulkArray(dst []byte, values [][]byte, present []bool) []byte {
	dst = AppendArrayHeader(dst, len(present))
	for i := range present {
		if present[i] {
			dst = AppendBulkString(dst, values[i])
		} else {
			dst = AppendNullBulk(dst)
		}
	}
	return dst
}

// Byte-for-byte error text constants (message body only, without the leading
// '-' RESP error prefix or trailing CRLF). These must match Pika v3.2.2
// verbatim; see design.md "错误文案逐字对齐".
const (
	// ErrWrongType is returned when a key holds a value of a different type
	// than the command expects. Requirement 3.6, 11.2.
	ErrWrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"

	// ErrNotInteger is returned when an integer argument is not an integer or
	// is out of range. Requirement 3.4, 5.9.
	ErrNotInteger = "ERR value is not an integer or out of range"

	// ErrSyntax is returned for an illegal optional-argument combination.
	// Requirement 3.5.
	ErrSyntax = "ERR syntax error"

	// ErrMSetOddArgs is returned by MSET and MSETNX when the arity is satisfied but the
	// key/value arguments do not pair up (an odd count). Redis 3.2's msetGenericCommand
	// hard-codes this exact literal — note the UPPERCASE "MSET" with no quotes and no
	// " command" suffix, and that MSETNX reports "MSET" too because it shares the same
	// function. It is distinct from the generic too-few-args arity error, which does use
	// the quoted lowercased 'mset'/'msetnx' form.
	ErrMSetOddArgs = "ERR wrong number of arguments for MSET"

	// ErrNotValidFloat is returned when a float argument (or the target value of
	// INCRBYFLOAT) is not a valid floating-point number. Requirement 5.9.
	ErrNotValidFloat = "ERR value is not a valid float"

	// ErrIncrDecrOverflow is returned when an INCR/DECR/INCRBY/DECRBY would push
	// the value past the signed 64-bit range. Requirement 5.8.
	ErrIncrDecrOverflow = "ERR increment or decrement would overflow"

	// ErrDecrOverflow is returned when a DECRBY amount is exactly the most
	// negative int64, whose negation would itself overflow. Requirement 5.8.
	ErrDecrOverflow = "ERR decrement would overflow"

	// ErrIncrNaNOrInfinity is returned when an INCRBYFLOAT would produce a NaN or
	// infinite result. Requirement 5.8.
	ErrIncrNaNOrInfinity = "ERR increment would produce NaN or Infinity"

	// ErrHashNotInteger is returned when an HINCRBY targets a field whose value is
	// not an integer. Requirement 6.1.
	ErrHashNotInteger = "ERR hash value is not an integer"

	// ErrHashNotFloat is returned when an HINCRBYFLOAT targets a field whose value
	// is not a valid float. Requirement 6.1. Matches Redis 3.2 byte-for-byte
	// ("is not a valid float", including the word "valid").
	ErrHashNotFloat = "ERR hash value is not a valid float"

	// ErrNoAuth is returned for business commands on an unauthenticated
	// connection when requirepass is configured. Requirement 2.6.
	// Note the trailing period, which is part of the Redis wire text.
	ErrNoAuth = "NOAUTH Authentication required."

	// ErrValueExceedsBackendLimit is returned when a key/member name exceeds
	// 1KB or a value exceeds 390KB. The prefix stays "ERR" and the parenthetical
	// reports the 400KB backend item limit. Requirement 3.7, 14.1, 14.2.
	ErrValueExceedsBackendLimit = "ERR value exceeds backend limit (400KB)"

	// ErrBackendThrottled is returned when DynamoDB throttles the request
	// (ProvisionedThroughputExceededException / a throttling APIError) and the AWS
	// SDK's bounded retry/backoff has been exhausted. The prefix stays a plain
	// "ERR" — not a distinct error class — so the client keeps retryable semantics
	// and can simply retry the command after backing off, matching the design's
	// "传播为 -ERR ...（保留可重试语义）" contract. Requirement 18.8.
	ErrBackendThrottled = "ERR backend throttled, retry later"

	// ErrBackendError is the generic reply for any storage/AWS-SDK error the command
	// layer does not map to a specific Redis error. The raw backend text (DynamoDB
	// validation/condition/attribute details) is NEVER echoed to the client — it is a
	// reconnaissance surface and non-Redis noise — so the real cause is logged
	// server-side and the client gets a fixed retryable "ERR".
	ErrBackendError = "ERR backend error, retry later"

	// ErrRMWMaxRetries is returned when a read-modify-write command (APPEND/SETRANGE/
	// INCR reconciliation) exhausts its bounded optimistic-concurrency retry budget under
	// sustained hot-key contention. The prefix stays a plain "ERR" so the client keeps
	// retryable semantics. It is a meaningful redimos-semantic error (not backend
	// internals), so it is surfaced verbatim rather than via the generic ErrBackendError.
	ErrRMWMaxRetries = "ERR read-modify-write exceeded retry limit under contention"

	// ErrCollectionTooLarge is returned when a whole-collection reply or *STORE operand
	// would materialize more members than the configured --max-collection-result cap.
	// It is a redimos-specific protective limit (Redis has none), in the same spirit as
	// ErrValueExceedsBackendLimit, guarding the proxy against a single-command OOM.
	ErrCollectionTooLarge = "ERR collection size exceeds the configured maximum result limit"

	// ErrOffsetOutOfRange is returned when a SETRANGE offset is negative
	// (Redis/Pika reject a negative offset before touching the value).
	// Requirement 5.10.
	ErrOffsetOutOfRange = "ERR offset is out of range"

	// ErrInvalidCursor is returned when a SCAN cursor is unknown (LRU eviction,
	// instance restart, or cross-instance reuse). Requirement 3.8, 13.5. Matches
	// Redis 3.2 byte-for-byte ("ERR invalid cursor").
	ErrInvalidCursor = "ERR invalid cursor"

	// ErrInvalidDBIndex is returned by SELECT for EVERY invalid index: a non-numeric
	// argument, a non-zero SELECT while multi-DB is disabled, AND a numeric index outside
	// [0, databases). The live redis:3.2 oracle replies this SAME text for all three cases
	// (verified: `SELECT 16`, `SELECT -1`, and `SELECT abc` all yield "ERR invalid DB
	// index"), so — unlike mainline Redis 3.2, whose selectDb path emits a distinct "DB
	// index is out of range" — we deliberately use one message throughout. Requirement 2.8.
	ErrInvalidDBIndex = "ERR invalid DB index"

	// ErrNoSuchKey is returned by LSET when the target key does not exist (an
	// empty/absent list cannot have an element set). Requirement 7.4.
	ErrNoSuchKey = "ERR no such key"

	// ErrIndexOutOfRange is returned by LSET when the index is outside the current
	// list bounds. Requirement 7.4.
	ErrIndexOutOfRange = "ERR index out of range"

	// ErrNoPasswordSet is returned when a client sends AUTH but no requirepass
	// is configured (matching Pika v3.2.2 / Redis 3.2). Requirement 2.5.
	ErrNoPasswordSet = "ERR Client sent AUTH, but no password is set"

	// ErrInvalidPassword is returned when AUTH is given a password that does
	// not match the configured requirepass (matching Pika v3.2.2). Requirement 2.5.
	ErrInvalidPassword = "ERR invalid password"
)

// ErrWrongNumberOfArgs builds the arity-mismatch error text for cmd. The
// command name is lowercased to match Redis/Pika, e.g. ErrWrongNumberOfArgs("GET")
// yields "ERR wrong number of arguments for 'get' command". Requirement 3.2.
func ErrWrongNumberOfArgs(cmd string) string {
	return "ERR wrong number of arguments for '" + toLower(cmd) + "' command"
}

// ErrUnknownCommand builds the unknown-command error text for name. The name is
// echoed exactly as the client sent it (case preserved), matching Redis/Pika,
// e.g. ErrUnknownCommand("HELLO") yields "ERR unknown command 'HELLO'".
// Requirement 2.1, 3.3.
func ErrUnknownCommand(name string) string {
	return "ERR unknown command '" + name + "'"
}

// ErrInvalidExpireTime builds the invalid-expire-time error text for cmd, used
// when a SET/SETEX/PSETEX expire argument is not strictly positive. Matches Redis
// 3.2 byte-for-byte: a bare lowercased command name with no quotes or " command"
// suffix, e.g. ErrInvalidExpireTime("setex") -> "ERR invalid expire time in setex".
func ErrInvalidExpireTime(cmd string) string {
	return "ERR invalid expire time in " + toLower(cmd)
}

// toLower lowercases ASCII command names without pulling in the strings package
// or allocating when the input is already lowercase.
func toLower(s string) string {
	hasUpper := false
	for i := 0; i < len(s); i++ {
		if c := s[i]; c >= 'A' && c <= 'Z' {
			hasUpper = true
			break
		}
	}
	if !hasUpper {
		return s
	}
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}
