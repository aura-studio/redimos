package difftest

// strings_diff.go supplies the String-command differential sequences used by
// task 9.5. Where matrix.go and resp2_codec.go sweep the byte-level hot spots
// broadly, these sequences exercise the full String command surface
// (requirements 5.1–5.11) and its boundaries, so a failure points directly at
// which String command or edge case diverged from the Pika v3.2.2 oracle:
//
//   - GET/SET plus the null bulk ($-1) for a missing key (5.1).
//   - SET with the EX/PX/NX/XX option combinations, including NX/XX rejection
//     which replies the null bulk ($-1) (5.2, 5.3, 5.4).
//   - SETNX / SETEX / PSETEX / GETSET conditional writes (5.5).
//   - MGET / MSET, including the null bulk MGET yields for a missing or
//     non-String key, and the odd-arity MSET error (5.6, 5.7).
//   - INCR / DECR / INCRBY / DECRBY at the int64 boundaries (overflow at
//     int64 max, DECR into negatives) (5.8, 5.9).
//   - INCRBYFLOAT decimal formatting (5.8, 5.9).
//   - APPEND / STRLEN / SETRANGE / GETRANGE lengths and ranges, including
//     negative-index GETRANGE and NUL zero-padding SETRANGE (5.10, 5.11).
//   - Error text (Property 6): WRONGTYPE, not-an-integer, syntax error, and
//     invalid expire time.
//
// They are consumed by the env-guarded TestDiffStrings entry point, which skips
// cleanly when PIKA_ADDR / REDIMOS_ADDR are unset so `go test ./...` needs no
// live infrastructure. Each sequence uses a unique key prefix and brackets its
// work with DEL so runs are independent even against a persistent oracle.
//
// Requirements 5.1–5.11; Property 6 (error-text consistency).

// StringDiffSequences returns the differential sequences that cover the full
// String command set and its boundaries.
func StringDiffSequences() []Sequence {
	return []Sequence{
		stringGetSetNullSequence(),
		stringSetOptionsSequence(),
		stringConditionalWriteSequence(),
		stringMGetMSetSequence(),
		stringIntegerBoundarySequence(),
		stringIncrByFloatSequence(),
		stringRangeSequence(),
		stringErrorTextSequence(),
	}
}

// stringGetSetNullSequence covers plain GET/SET and the null bulk ($-1) a
// missing key yields, contrasted with a present (including empty) value.
// Requirement 5.1.
func stringGetSetNullSequence() Sequence {
	k := "difftest:str:getset"
	return Sequence{
		Name: "string-get-set-null",
		Commands: []Command{
			Cmd("DEL", k),
			// GET on a missing key -> null bulk ($-1).
			Cmd("GET", k),
			Cmd("SET", k, "hello"), // +OK
			Cmd("GET", k),          // $5\r\nhello
			// Overwrite, then read the new value.
			Cmd("SET", k, "world"),
			Cmd("GET", k),
			// Empty value is a present, non-null bulk ($0), distinct from $-1.
			Cmd("SET", k, ""),
			Cmd("GET", k),
			Cmd("STRLEN", k), // :0
			// DEL then GET returns to the null bulk.
			Cmd("DEL", k),
			Cmd("GET", k),
			Cmd("DEL", k),
		},
	}
}

// stringSetOptionsSequence covers the SET EX/PX/NX/XX option combinations,
// including the NX/XX rejections that reply the null bulk ($-1). It avoids
// byte-comparing an exact TTL right after EX/PX (that value can drift across a
// second boundary); instead it confirms the written value with GET and the TTL
// sentinels via PERSIST + TTL. Requirements 5.2, 5.3, 5.4.
func stringSetOptionsSequence() Sequence {
	k := "difftest:str:setopt"
	return Sequence{
		Name: "string-set-options",
		Commands: []Command{
			Cmd("DEL", k),
			// XX on a missing key -> rejected, null bulk ($-1); key stays absent.
			Cmd("SET", k, "v", "XX"),
			Cmd("GET", k), // $-1
			// NX on a missing key -> written, +OK.
			Cmd("SET", k, "v1", "NX"),
			Cmd("GET", k), // $3\r\nv1... -> "v1"
			// NX on an existing key -> rejected, null bulk ($-1); value unchanged.
			Cmd("SET", k, "v2", "NX"),
			Cmd("GET", k), // still "v1"
			// XX on an existing key -> written, +OK.
			Cmd("SET", k, "v3", "XX"),
			Cmd("GET", k), // "v3"
			// EX writes the value and sets a TTL; verify value + that a TTL exists
			// is cleared by PERSIST back to -1 (sentinel compare, not exact secs).
			Cmd("SET", k, "v4", "EX", "1000"),
			Cmd("GET", k), // "v4"
			Cmd("PERSIST", k),
			Cmd("TTL", k), // :-1 (no expire after PERSIST)
			// PX likewise (milliseconds truncated to seconds by Pika v3.2.2).
			Cmd("SET", k, "v5", "PX", "1000000"),
			Cmd("GET", k), // "v5"
			Cmd("PERSIST", k),
			Cmd("TTL", k), // :-1
			// NX combined with EX on a missing key writes with a TTL.
			Cmd("DEL", k),
			Cmd("SET", k, "v6", "NX", "EX", "1000"),
			Cmd("GET", k), // "v6"
			Cmd("PERSIST", k),
			Cmd("TTL", k), // :-1
			Cmd("DEL", k),
		},
	}
}

// stringConditionalWriteSequence covers SETNX/SETEX/PSETEX/GETSET conditional
// writes and their return values. Requirement 5.5.
func stringConditionalWriteSequence() Sequence {
	k := "difftest:str:cond"
	return Sequence{
		Name: "string-conditional-write",
		Commands: []Command{
			Cmd("DEL", k),
			// SETNX on a missing key -> :1 (set); on an existing key -> :0.
			Cmd("SETNX", k, "a"),
			Cmd("GET", k), // "a"
			Cmd("SETNX", k, "b"),
			Cmd("GET", k), // still "a"
			// GETSET returns the previous value and installs the new one.
			Cmd("GETSET", k, "c"), // "a"
			Cmd("GET", k),         // "c"
			// GETSET on a missing key returns the null bulk ($-1).
			Cmd("DEL", k),
			Cmd("GETSET", k, "d"), // $-1
			Cmd("GET", k),         // "d"
			// SETEX sets value with a second-precision TTL; PERSIST + TTL sentinel.
			Cmd("SETEX", k, "1000", "e"),
			Cmd("GET", k), // "e"
			Cmd("PERSIST", k),
			Cmd("TTL", k), // :-1
			// PSETEX sets value with a millisecond TTL (truncated to seconds).
			Cmd("PSETEX", k, "1000000", "f"),
			Cmd("GET", k), // "f"
			Cmd("PERSIST", k),
			Cmd("TTL", k), // :-1
			Cmd("DEL", k),
		},
	}
}

// stringMGetMSetSequence covers MGET/MSET, including the null bulk MGET yields
// for a missing or non-String key, and the odd-arity MSET error. Requirements
// 5.6, 5.7.
func stringMGetMSetSequence() Sequence {
	k := "difftest:str:multi"
	k1, k2, k3 := k+":1", k+":2", k+":3"
	hk := k + ":hash"
	return Sequence{
		Name: "string-mget-mset",
		Commands: []Command{
			Cmd("DEL", k1, k2, k3, hk),
			// MGET before any writes -> array of three null bulks.
			Cmd("MGET", k1, k2, k3),
			// MSET writes all pairs in one command -> +OK.
			Cmd("MSET", k1, "v1", k2, "v2", k3, "v3"),
			Cmd("MGET", k1, k2, k3), // ["v1","v2","v3"]
			// MGET with a mix of present, missing, and a non-String key -> the
			// missing and non-String slots are null bulks.
			Cmd("HSET", hk, "f", "x"), // hk is a hash
			Cmd("MGET", k1, k+":absent", hk, k3),
			// Odd number of MSET arguments -> wrong-number-of-arguments error.
			Cmd("MSET", k1, "v1", k2),
			Cmd("DEL", k1, k2, k3, hk),
		},
	}
}

// stringIntegerBoundarySequence covers INCR/DECR/INCRBY/DECRBY at the int64
// boundaries: overflow at int64 max, walking DECR into negatives, and the
// not-an-integer / overflow error text. Requirements 5.8, 5.9.
func stringIntegerBoundarySequence() Sequence {
	k := "difftest:str:int"
	maxInt64 := "9223372036854775807"
	minInt64 := "-9223372036854775808"
	return Sequence{
		Name: "string-integer-boundary",
		Commands: []Command{
			Cmd("DEL", k),
			// A fresh key is treated as 0: INCR -> :1, DECR walks negative.
			Cmd("INCR", k),         // :1
			Cmd("INCR", k),         // :2
			Cmd("DECR", k),         // :1
			Cmd("DECR", k),         // :0
			Cmd("DECR", k),         // :-1
			Cmd("INCRBY", k, "10"), // :9
			Cmd("DECRBY", k, "20"), // :-11
			Cmd("DECRBY", k, "-5"), // :-6 (double negative)
			// Overflow at int64 max: INCR must error, value unchanged.
			Cmd("SET", k, maxInt64),
			Cmd("INCR", k),        // -ERR ... overflow
			Cmd("INCRBY", k, "1"), // -ERR ... overflow
			Cmd("GET", k),         // still int64 max
			// Underflow at int64 min: DECR must error, value unchanged.
			Cmd("SET", k, minInt64),
			Cmd("DECR", k),        // -ERR ... overflow
			Cmd("DECRBY", k, "1"), // -ERR ... overflow
			Cmd("GET", k),         // still int64 min
			// A non-integer value -> not-an-integer error on INCR.
			Cmd("SET", k, "notanumber"),
			Cmd("INCR", k),        // -ERR value is not an integer or out of range
			Cmd("INCRBY", k, "1"), // same
			// A non-integer increment argument -> not-an-integer error.
			Cmd("SET", k, "10"),
			Cmd("INCRBY", k, "3.14"),   // -ERR value is not an integer or out of range
			Cmd("INCRBY", k, "notint"), // same
			Cmd("DEL", k),
		},
	}
}

// stringIncrByFloatSequence covers INCRBYFLOAT decimal formatting: shortest
// decimal with no exponent and trailing zeros trimmed, plus the not-a-valid-float
// error. Requirements 5.8, 5.9.
func stringIncrByFloatSequence() Sequence {
	k := "difftest:str:float"
	return Sequence{
		Name: "string-incrbyfloat",
		Commands: []Command{
			Cmd("DEL", k),
			// Fresh key starts at 0.
			Cmd("INCRBYFLOAT", k, "10.5"), // "10.5"
			Cmd("INCRBYFLOAT", k, "0.1"),  // "10.6"
			Cmd("INCRBYFLOAT", k, "-5.0"), // "5.6" (trailing zero trimmed)
			Cmd("INCRBYFLOAT", k, "4.4"),  // "10" (integer result, no decimals)
			Cmd("GET", k),                 // "10"
			// Exponent-form input is accepted and formatted without exponent.
			Cmd("SET", k, "3.0"),
			Cmd("INCRBYFLOAT", k, "1.0e2"), // "103"
			// A non-float increment -> not-a-valid-float error.
			Cmd("INCRBYFLOAT", k, "notafloat"),
			// A non-float target value -> not-a-valid-float error.
			Cmd("SET", k, "notanumber"),
			Cmd("INCRBYFLOAT", k, "1.0"),
			Cmd("DEL", k),
		},
	}
}

// stringRangeSequence covers APPEND/STRLEN/SETRANGE/GETRANGE lengths and ranges,
// including negative-index GETRANGE and NUL zero-padding SETRANGE beyond the
// current length. Requirements 5.10, 5.11.
func stringRangeSequence() Sequence {
	k := "difftest:str:range"
	return Sequence{
		Name: "string-range",
		Commands: []Command{
			Cmd("DEL", k),
			// APPEND to a missing key creates it and returns the new length.
			Cmd("APPEND", k, "Hello"),  // :5
			Cmd("APPEND", k, " World"), // :11
			Cmd("GET", k),              // "Hello World"
			Cmd("STRLEN", k),           // :11
			// STRLEN on a missing key -> :0.
			Cmd("STRLEN", k+":missing"),
			// GETRANGE: positive, negative, and empty ranges.
			Cmd("GETRANGE", k, "0", "4"),     // "Hello"
			Cmd("GETRANGE", k, "6", "10"),    // "World"
			Cmd("GETRANGE", k, "-5", "-1"),   // "World"
			Cmd("GETRANGE", k, "0", "-1"),    // "Hello World"
			Cmd("GETRANGE", k, "10", "5"),    // "" (start > end)
			Cmd("GETRANGE", k, "100", "200"), // "" (out of range)
			// GETRANGE on a missing key -> empty string.
			Cmd("GETRANGE", k+":missing", "0", "-1"),
			// SETRANGE within bounds overwrites and returns the length.
			Cmd("SET", k, "Hello World"),
			Cmd("SETRANGE", k, "6", "Redis"), // :11
			Cmd("GET", k),                    // "Hello Redis"
			// SETRANGE beyond the current length zero-pads with NUL bytes.
			Cmd("DEL", k),
			Cmd("SETRANGE", k, "5", "abc"), // :8 (5 NULs + "abc")
			Cmd("STRLEN", k),               // :8
			// SETRANGE with an empty value performs no write; returns current len.
			Cmd("SETRANGE", k, "0", ""), // :8
			// A negative SETRANGE offset -> offset-out-of-range error.
			Cmd("SETRANGE", k, "-1", "x"),
			Cmd("DEL", k),
		},
	}
}

// stringErrorTextSequence pins the String-command error text byte-for-byte
// (Property 6): WRONGTYPE against a wrong-type key, not-an-integer, syntax error
// from bad SET options, and invalid expire time. Requirements 5.2, 5.3, 5.5, 5.9.
func stringErrorTextSequence() Sequence {
	k := "difftest:str:err"
	return Sequence{
		Name: "string-error-text",
		Commands: []Command{
			Cmd("DEL", k),

			// --- WRONGTYPE: a String read/write against a non-String key ---
			// SET itself replaces any type, so it is deliberately NOT probed here;
			// only the type-checked String read/RMW commands must reply WRONGTYPE.
			Cmd("RPUSH", k, "x"),          // k is now a list
			Cmd("GET", k),                 // -WRONGTYPE ...
			Cmd("APPEND", k, "y"),         // -WRONGTYPE ...
			Cmd("STRLEN", k),              // -WRONGTYPE ...
			Cmd("GETRANGE", k, "0", "-1"), // -WRONGTYPE ...
			Cmd("INCR", k),                // -WRONGTYPE ...
			Cmd("DEL", k),

			// --- not-an-integer (5.9) ---
			Cmd("SET", k, "abc"),
			Cmd("INCR", k), // -ERR value is not an integer or out of range
			Cmd("SET", k, "10"),
			Cmd("INCRBY", k, "notint"), // same

			// --- syntax error from bad SET options (5.2, 5.3) ---
			Cmd("SET", k, "v", "BOGUS"),     // -ERR syntax error
			Cmd("SET", k, "v", "NX", "XX"),  // mutually exclusive -> syntax error
			Cmd("SET", k, "v", "EX"),        // EX with no value -> syntax error
			Cmd("SET", k, "v", "EX", "xyz"), // EX with non-integer -> not-an-integer

			// --- invalid expire time (5.5) ---
			Cmd("SET", k, "v", "EX", "0"),  // -ERR invalid expire time in 'set' command
			Cmd("SETEX", k, "0", "v"),      // -ERR invalid expire time in 'setex' command
			Cmd("SETEX", k, "-1", "v"),     // same
			Cmd("PSETEX", k, "0", "v"),     // -ERR invalid expire time in 'psetex' command
			Cmd("SETEX", k, "notint", "v"), // -ERR value is not an integer or out of range

			Cmd("DEL", k),
		},
	}
}

// StringDiffSequenceNames returns the sequence names for logging and subtest
// naming.
func StringDiffSequenceNames() []string {
	seqs := StringDiffSequences()
	names := make([]string, len(seqs))
	for i, s := range seqs {
		names[i] = s.Name
	}
	return names
}
