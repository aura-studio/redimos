package difftest

// resp2_codec.go supplies the RESP2-codec-focused differential sequences used by
// task 4.3. Where matrix.go targets the byte-level compatibility hot spots in a
// broad sweep, these sequences are organized strictly around the five RESP2
// types and the three null-value conventions so a failure points directly at
// which wire type diverged:
//
//   - Simple String (+)  : SET/OK, PING/PONG
//   - Error (-)          : unknown command, wrong arity, WRONGTYPE, not-integer
//   - Integer (:)        : EXISTS/INCR/LLEN/TTL boundary replies
//   - Bulk String ($)    : GET populated vs the $-1 null bulk
//   - Array (*)          : populated vs *0 (empty) vs *-1 (null)
//
// They are consumed by the env-guarded TestDiffRESP2Codec entry point, which
// skips cleanly when PIKA_ADDR / REDIMOS_ADDR are unset. The always-run,
// no-infrastructure guarantee for task 4.3 is provided by the byte-level
// encoder assertions and the round-trip property in resp2_codec_test.go.
//
// Requirements 1.1, 1.2, 1.3, 1.4, 1.6; Property 6 (error-text consistency).

// RESP2CodecSequences returns the differential sequences that isolate each
// RESP2 type and null convention. Each sequence uses a unique key prefix so it
// is independent even against a persistent oracle.
func RESP2CodecSequences() []Sequence {
	return []Sequence{
		simpleStringSequence(),
		integerTypeSequence(),
		bulkStringSequence(),
		arrayTypeSequence(),
		codecErrorTextSequence(),
	}
}

// simpleStringSequence exercises the Simple String type (+): +OK from SET and
// +PONG from PING. Requirement 1.1.
func simpleStringSequence() Sequence {
	k := "difftest:codec:ss"
	return Sequence{
		Name: "resp2-simple-string",
		Commands: []Command{
			Cmd("DEL", k),
			Cmd("SET", k, "v"), // +OK
			Cmd("PING"),        // +PONG
			Cmd("DEL", k),
		},
	}
}

// integerTypeSequence exercises the Integer type (:) across the values Pika
// returns for existence, counters, and TTL sentinels. Requirement 1.1.
func integerTypeSequence() Sequence {
	k := "difftest:codec:int"
	return Sequence{
		Name: "resp2-integer",
		Commands: []Command{
			Cmd("DEL", k),
			Cmd("EXISTS", k), // :0
			Cmd("SET", k, "10"),
			Cmd("EXISTS", k), // :1
			Cmd("INCR", k),   // :11
			Cmd("DEL", k),    // :1
			Cmd("TTL", k),    // :-2 (missing key)
			Cmd("DEL", k),
		},
	}
}

// bulkStringSequence exercises the Bulk String type ($): a populated value and
// the $-1 null bulk for a missing key. Requirements 1.1, 1.2.
func bulkStringSequence() Sequence {
	k := "difftest:codec:bulk"
	return Sequence{
		Name: "resp2-bulk-string",
		Commands: []Command{
			Cmd("DEL", k),
			Cmd("GET", k), // $-1 (null bulk, missing key)
			Cmd("SET", k, "hello"),
			Cmd("GET", k),     // $5\r\nhello
			Cmd("SET", k, ""), // empty value
			Cmd("GET", k),     // $0 (empty, non-null bulk)
			Cmd("DEL", k),
		},
	}
}

// arrayTypeSequence exercises the Array type (*): a populated array, the empty
// array *0, and the null array *-1. Requirements 1.1, 1.3, 1.4.
func arrayTypeSequence() Sequence {
	k := "difftest:codec:arr"
	return Sequence{
		Name: "resp2-array",
		Commands: []Command{
			Cmd("DEL", k, k+":l"),
			// Missing list -> empty array (*0).
			Cmd("LRANGE", k+":l", "0", "-1"),
			Cmd("RPUSH", k+":l", "a", "b", "c"),
			// Populated array (*3).
			Cmd("LRANGE", k+":l", "0", "-1"),
			// Out-of-range slice -> empty array (*0).
			Cmd("LRANGE", k+":l", "5", "10"),
			// BLPOP-style null array (*-1) is exercised via the guarded matrix
			// only where the oracle supports it; the null-array byte form is
			// pinned unconditionally by the encoder assertions.
			Cmd("DEL", k, k+":l"),
		},
	}
}

// codecErrorTextSequence pins the Error type (-) byte-for-byte for the codec
// path: unknown command, wrong arity, WRONGTYPE, and not-an-integer. This is
// Property 6 (error-text consistency). Requirement 1.1, 1.6.
func codecErrorTextSequence() Sequence {
	k := "difftest:codec:err"
	return Sequence{
		Name: "resp2-error-text",
		Commands: []Command{
			Cmd("DEL", k),
			Cmd("NOSUCHCMD"), // -ERR unknown command '...'
			Cmd("GET"),       // -ERR wrong number of arguments for 'get' command
			Cmd("SET", k, "abc"),
			Cmd("LPUSH", k, "x"), // -WRONGTYPE ...
			Cmd("SET", k, "notint"),
			Cmd("INCR", k), // -ERR value is not an integer or out of range
			Cmd("DEL", k),
		},
	}
}

// RESP2CodecSequenceNames returns the sequence names for logging and subtest
// naming.
func RESP2CodecSequenceNames() []string {
	seqs := RESP2CodecSequences()
	names := make([]string, len(seqs))
	for i, s := range seqs {
		names[i] = s.Name
	}
	return names
}
