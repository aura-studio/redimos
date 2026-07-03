package difftest

import "fmt"

// Matrix returns the curated command-matrix sequences used to drive the
// differential assertion engine. The matrix deliberately targets the three
// byte-level compatibility hot spots called out by task 2.2:
//
//   - Error text: wrong-arity, unknown command, WRONGTYPE, not-an-integer,
//     syntax error (Requirement 4.8, Property 6).
//   - Null encodings: distinguishing $-1 (null bulk), *0 (empty array), and
//     *-1 (null array) (Requirement 1.6).
//   - Integer boundaries: INCR/DECR around int64 limits and float edges.
//
// Each sequence runs on fresh connections against both endpoints, and every
// reply is compared byte-for-byte. Sequences use unique key prefixes so runs
// are independent even against a persistent oracle.
func Matrix() []Sequence {
	return []Sequence{
		nullEncodingSequence(),
		errorTextSequence(),
		routeParamValidationSequence(),
		wrongTypeSequence(),
		integerBoundarySequence(),
		expireNullSequence(),
	}
}

// routeParamValidationSequence is the task 5.3 focused probe for the router and
// parameter-validation error text (Property 6, 需求 3.2–3.5). Where
// errorTextSequence spot-checks one case per category, this sequence sweeps the
// hot spots that most often diverge byte-for-byte from the Pika v3.2.2 oracle:
//
//   - Arity mismatch on both exact-arity and minimum-arity commands, with both
//     too-few and too-many arguments, and with mixed command casing to confirm
//     the reply always uses the lowercase command name (需求 3.2).
//   - Unknown command with the name echoed verbatim, including mixed case and a
//     name that only differs from a real command by case (需求 3.3).
//   - Non-integer / out-of-range integer arguments to counter commands
//     (需求 3.4).
//   - Illegal optional-argument combinations producing a syntax error
//     (需求 3.5).
func routeParamValidationSequence() Sequence {
	k := "difftest:route"
	return Sequence{
		Name: "route-param-validation",
		Commands: []Command{
			Cmd("DEL", k),

			// --- Arity mismatch (需求 3.2) ---
			// Exact-arity GET (needs exactly key): too few and too many.
			Cmd("GET"),
			Cmd("GET", k, "extra"),
			// Mixed casing must still report the lowercase name.
			Cmd("GeT"),
			// Minimum-arity commands below their floor.
			Cmd("SET", k),
			Cmd("MSET", k),
			Cmd("SETEX", k, "10"),
			Cmd("HSET", k, "field"),

			// --- Unknown command, name echoed verbatim (需求 3.3) ---
			Cmd("NOSUCHCOMMAND", "a", "b"),
			Cmd("DoesNotExist"),
			// A token that is not a real command even lowercased.
			Cmd("GETT", k),

			// --- Non-integer / out-of-range integer (需求 3.4) ---
			Cmd("SET", k, "notanumber"),
			Cmd("INCR", k),
			Cmd("INCRBY", k, "3.14"),
			Cmd("INCRBY", k, "notanint"),
			Cmd("INCRBY", k, "9223372036854775808"), // MaxInt64 + 1
			Cmd("DECRBY", k, "+5"),                  // leading '+' rejected
			Cmd("EXPIRE", k, "abc"),

			// --- Syntax error via bad optional-argument combination (需求 3.5) ---
			Cmd("SET", k, "v", "BOGUSOPT"),
			Cmd("SET", k, "v", "EX"),        // EX with no value
			Cmd("SET", k, "v", "EX", "xyz"), // EX with non-integer value
			Cmd("SET", k, "v", "NX", "XX"),  // mutually exclusive flags

			Cmd("DEL", k),
		},
	}
}

// nullEncodingSequence probes the $-1 vs *0 vs *-1 distinction that clients are
// notoriously sensitive to.
func nullEncodingSequence() Sequence {
	k := "difftest:null"
	return Sequence{
		Name: "null-encoding",
		Commands: []Command{
			Cmd("DEL", k, k+":list", k+":set"),
			// GET on a missing key -> null bulk string ($-1).
			Cmd("GET", k+":missing"),
			// LRANGE on a missing list -> empty array (*0).
			Cmd("LRANGE", k+":list", "0", "-1"),
			// SMEMBERS on a missing set -> empty array (*0).
			Cmd("SMEMBERS", k+":set"),
			// SET then GET -> non-null bulk, to contrast with the null case.
			Cmd("SET", k, "v"),
			Cmd("GET", k),
			Cmd("DEL", k),
		},
	}
}

// errorTextSequence probes byte-exact error strings for arity, unknown command,
// non-integer, and syntax errors.
func errorTextSequence() Sequence {
	k := "difftest:err"
	return Sequence{
		Name: "error-text",
		Commands: []Command{
			// Wrong number of arguments.
			Cmd("GET"),
			Cmd("SET", k),
			// Unknown command.
			Cmd("NOSUCHCOMMAND", "a", "b"),
			// Non-integer where an integer is expected.
			Cmd("SET", k, "notanumber"),
			Cmd("INCR", k),
			// Syntax error via a bad optional-argument combination.
			Cmd("SET", k, "v", "BOGUSOPT"),
			Cmd("DEL", k),
		},
	}
}

// wrongTypeSequence probes the -WRONGTYPE error text (Property 1 / Property 6).
func wrongTypeSequence() Sequence {
	k := "difftest:wrongtype"
	return Sequence{
		Name: "wrong-type",
		Commands: []Command{
			Cmd("DEL", k),
			Cmd("SET", k, "stringvalue"),
			// A list operation against a string key -> -WRONGTYPE.
			Cmd("LPUSH", k, "x"),
			// A hash operation against a string key -> -WRONGTYPE.
			Cmd("HSET", k, "f", "v"),
			Cmd("DEL", k),
		},
	}
}

// integerBoundarySequence probes counter behavior at int64 edges and float
// formatting, where DynamoDB Number vs Redis int64/double can diverge.
func integerBoundarySequence() Sequence {
	k := "difftest:int"
	maxInt64 := "9223372036854775807"
	return Sequence{
		Name: "integer-boundary",
		Commands: []Command{
			Cmd("DEL", k),
			// Set to int64 max, then INCR should overflow with an error.
			Cmd("SET", k, maxInt64),
			Cmd("INCR", k),
			// Reset and walk DECR down through zero into negatives.
			Cmd("SET", k, "1"),
			Cmd("DECR", k),
			Cmd("DECR", k),
			Cmd("INCRBY", k, "10"),
			Cmd("DECRBY", k, "-5"),
			// Float increment formatting.
			Cmd("SET", k+":f", "3.0"),
			Cmd("INCRBYFLOAT", k+":f", "1.5"),
			Cmd("INCRBYFLOAT", k+":f", "-0.5"),
			Cmd("DEL", k, k+":f"),
		},
	}
}

// expireNullSequence probes TTL/EXPIRE return values (:1/:0/-1/-2) which are
// integer replies that must match exactly.
func expireNullSequence() Sequence {
	k := "difftest:ttl"
	return Sequence{
		Name: "expire-null",
		Commands: []Command{
			Cmd("DEL", k),
			// TTL / PTTL on a missing key -> -2.
			Cmd("TTL", k),
			Cmd("PTTL", k),
			Cmd("SET", k, "v"),
			// TTL with no expire set -> -1.
			Cmd("TTL", k),
			// EXPIRE on an existing key -> 1; on a missing key -> 0.
			Cmd("EXPIRE", k, "100"),
			Cmd("EXPIRE", k+":missing", "100"),
			Cmd("PERSIST", k),
			Cmd("TTL", k),
			Cmd("DEL", k),
		},
	}
}

// MatrixNames returns the names of the matrix sequences, useful for logging and
// subtest naming.
func MatrixNames() []string {
	seqs := Matrix()
	names := make([]string, len(seqs))
	for i, s := range seqs {
		names[i] = s.Name
	}
	return names
}

// describeMatrix is a small helper for diagnostics.
func describeMatrix() string {
	return fmt.Sprintf("difftest matrix: %d sequences %v", len(Matrix()), MatrixNames())
}
