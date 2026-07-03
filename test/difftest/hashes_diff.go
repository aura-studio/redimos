package difftest

import "fmt"

// hashes_diff.go supplies the Hash-command differential sequences used by task
// 13.3 (the full-command differential half that accompanies the Property 3 count
// consistency property test). They drive the same byte-level assertion engine as
// Matrix() (harness.go) but live in their own file and are exposed via
// HashDiffSequences() / HashDiffSequenceNames() so they can be wired into a
// dedicated live entry point (TestDiffHashes in hashes_diff_test.go) WITHOUT
// touching the shared Matrix() function — keeping this file conflict-free with
// concurrent work on matrix.go / difftest_test.go.
//
// Scope (Validates: 需求 6.1, 6.2, 6.4; Property 3 计数一致性 / Property 6 错误文案一致):
//
//   - HSET   new-vs-update field count reply, single and multi pair (需求 6.1).
//   - HGET   present value, missing field, and missing key → null bulk (需求 6.1).
//   - HMSET  legacy +OK reply (需求 6.1).
//   - HMGET  request-order array with present / missing / non-hash slots as null
//     bulks — request order makes the reply deterministic (需求 6.1).
//   - HGETALL / HKEYS / HVALS on a SINGLE-field hash and on an absent key.
//     Multi-field replies are deliberately avoided: the field iteration order is
//     unspecified and would diverge byte-for-byte from the oracle. Single-field
//     and empty replies are order-independent (需求 6.1).
//   - HDEL   removed-count reply, and last-field removal deleting the key (需求 6.1, 6.4).
//   - HEXISTS present / missing field / absent key (需求 6.1).
//   - HSETNX :1 (created) / :0 (already present) (需求 6.1).
//   - HINCRBY integer replies incl. negative deltas and new-field-from-zero;
//     HINCRBYFLOAT shortest-decimal formatting (trailing zeros trimmed) (需求 6.1).
//   - HSTRLEN value byte length, missing field / key → :0 (需求 6.1).
//   - HLEN    O(1) field count that tracks HSET/HDEL/HINCRBY/HSETNX churn — the
//     byte-level manifestation of Property 3 (需求 6.2, 6.4).
//   - HSCAN   a single-field hash scanned in one page: the reply is the
//     deterministic ["0", ["f","v"]] (cursor 0 = complete). Larger scans are
//     avoided because both the cursor token and multi-field order differ between
//     implementations (需求 6.3 surfaced only in its deterministic form).
//   - WRONGTYPE: every hash command against a String key replies the exact
//     -WRONGTYPE text (Property 6, 需求 6.1).
//   - Arity: wrong-number-of-arguments for every hash command, echoing the
//     lowercase command name, plus the odd HSET/HMSET field/value pairing; and
//     the non-integer HINCRBY argument (Property 6, 需求 3.2, 3.4 as surfaced
//     through the Hash family).
//
// Every sequence begins by DEL-ing the keys it uses (unique prefixes) so runs are
// independent even against a persistent oracle, and ends by cleaning up.

// HashDiffSequences returns the Hash-command differential sequences.
func HashDiffSequences() []Sequence {
	return []Sequence{
		hashSetGetSequence(),
		hashMGetSequence(),
		hashGetAllKeysValsSequence(),
		hashDelExistsSequence(),
		hashSetNXSequence(),
		hashIncrSequence(),
		hashStrlenLenSequence(),
		hashScanSingleSequence(),
		hashWrongTypeSequence(),
		hashErrorTextSequence(),
	}
}

// HashDiffSequenceNames returns the sequence names, for logging / subtest names.
func HashDiffSequenceNames() []string {
	seqs := HashDiffSequences()
	names := make([]string, len(seqs))
	for i, s := range seqs {
		names[i] = s.Name
	}
	return names
}

// hashSetGetSequence probes HSET's new-field count reply (existing fields updated
// in place are not counted), HGET on a present value / missing field / missing
// key, and HMSET's legacy +OK reply (需求 6.1).
func hashSetGetSequence() Sequence {
	k := "difftest:hash:setget"
	return Sequence{
		Name: "hash-set-get",
		Commands: []Command{
			Cmd("DEL", k),

			// HGET on a missing key -> null bulk.
			Cmd("HGET", k, "f1"),
			// Two new fields -> :2.
			Cmd("HSET", k, "f1", "v1", "f2", "v2"),
			Cmd("HGET", k, "f1"), // "v1"
			Cmd("HGET", k, "f2"), // "v2"
			// One update (f1) + one new (f3) -> :1.
			Cmd("HSET", k, "f1", "v1b", "f3", "v3"),
			Cmd("HGET", k, "f1"),   // "v1b" (updated)
			Cmd("HGET", k, "miss"), // null bulk (missing field, live key)
			// Single-pair HSET on a brand-new field -> :1.
			Cmd("HSET", k, "f4", "v4"),
			// HMSET replies the legacy +OK regardless of new/updated.
			Cmd("HMSET", k, "f4", "v4b", "f5", "v5"),
			Cmd("HGET", k, "f4"), // "v4b"

			Cmd("DEL", k),
		},
	}
}

// hashMGetSequence probes HMGET's request-order array, where missing fields and a
// non-hash key surface as null bulks in the exact request positions (需求 6.1).
func hashMGetSequence() Sequence {
	k := "difftest:hash:mget"
	return Sequence{
		Name: "hash-mget",
		Commands: []Command{
			Cmd("DEL", k),

			// HMGET on a missing key -> array of null bulks (one per field).
			Cmd("HMGET", k, "a", "b"),
			Cmd("HSET", k, "a", "1", "b", "2", "c", "3"),
			// Permuted request order with a missing field in the middle.
			Cmd("HMGET", k, "c", "absent", "a"),
			// A single present field.
			Cmd("HMGET", k, "b"),

			Cmd("DEL", k),
		},
	}
}

// hashGetAllKeysValsSequence probes HGETALL / HKEYS / HVALS in their
// order-independent forms only: a single-field hash (unambiguous order) and an
// absent key (the empty array *0). Multi-field replies are intentionally not
// asserted because field iteration order is unspecified across implementations
// (需求 6.1).
func hashGetAllKeysValsSequence() Sequence {
	k := "difftest:hash:getall"
	return Sequence{
		Name: "hash-getall-keys-vals-single",
		Commands: []Command{
			Cmd("DEL", k),

			// Absent key -> empty array for all three.
			Cmd("HGETALL", k),
			Cmd("HKEYS", k),
			Cmd("HVALS", k),

			// Single field -> deterministic order.
			Cmd("HSET", k, "only", "val"),
			Cmd("HGETALL", k), // ["only","val"]
			Cmd("HKEYS", k),   // ["only"]
			Cmd("HVALS", k),   // ["val"]

			Cmd("DEL", k),
		},
	}
}

// hashDelExistsSequence probes HDEL's removed-count reply (existing fields only),
// HEXISTS, and the last-field-removal that deletes the key so it reports absent
// via HLEN / HEXISTS (需求 6.1, 6.4).
func hashDelExistsSequence() Sequence {
	k := "difftest:hash:del"
	return Sequence{
		Name: "hash-del-exists",
		Commands: []Command{
			Cmd("DEL", k),

			// HDEL on a missing key -> :0.
			Cmd("HDEL", k, "f"),

			Cmd("HSET", k, "a", "1", "b", "2", "c", "3"),
			// HEXISTS present / missing / (absent key handled below).
			Cmd("HEXISTS", k, "a"),    // :1
			Cmd("HEXISTS", k, "nope"), // :0
			// Delete two existing + one absent -> :2, HLEN 1.
			Cmd("HDEL", k, "a", "b", "zzz"),
			Cmd("HLEN", k), // :1
			// Removing the last field deletes the key.
			Cmd("HDEL", k, "c"),
			Cmd("HLEN", k),         // :0 (key gone)
			Cmd("HEXISTS", k, "c"), // :0 (absent key)
			Cmd("EXISTS", k),       // :0 (empty hash does not exist)

			Cmd("DEL", k),
		},
	}
}

// hashSetNXSequence probes HSETNX: :1 when the field is created, :0 when it
// already exists (no overwrite) (需求 6.1).
func hashSetNXSequence() Sequence {
	k := "difftest:hash:setnx"
	return Sequence{
		Name: "hash-setnx",
		Commands: []Command{
			Cmd("DEL", k),

			Cmd("HSETNX", k, "f", "v1"), // :1 (created)
			Cmd("HGET", k, "f"),         // "v1"
			Cmd("HSETNX", k, "f", "v2"), // :0 (exists, no write)
			Cmd("HGET", k, "f"),         // still "v1"
			Cmd("HLEN", k),              // :1

			Cmd("DEL", k),
		},
	}
}

// hashIncrSequence probes HINCRBY integer replies (new field from zero, positive
// and negative deltas) and HINCRBYFLOAT shortest-decimal formatting (需求 6.1).
func hashIncrSequence() Sequence {
	k := "difftest:hash:incr"
	return Sequence{
		Name: "hash-incr",
		Commands: []Command{
			Cmd("DEL", k),

			// New field starts at 0.
			Cmd("HINCRBY", k, "n", "5"),  // :5
			Cmd("HINCRBY", k, "n", "-2"), // :3
			Cmd("HINCRBY", k, "n", "10"), // :13
			Cmd("HLEN", k),               // :1 (single field, count unchanged by in-place incr)

			// HINCRBYFLOAT formats the shortest decimal, trailing zeros trimmed.
			Cmd("HINCRBYFLOAT", k, "f", "3.14"), // "3.14"
			Cmd("HINCRBYFLOAT", k, "f", "1.86"), // "5" (3.14+1.86, integer result)
			Cmd("HINCRBYFLOAT", k, "f", "-0.5"), // "4.5"
			Cmd("HLEN", k),                      // :2

			Cmd("DEL", k),
		},
	}
}

// hashStrlenLenSequence probes HSTRLEN (value byte length, :0 for missing
// field/key) and HLEN's O(1) count tracking HSET/HDEL churn (需求 6.1, 6.2, 6.4).
func hashStrlenLenSequence() Sequence {
	k := "difftest:hash:strlen"
	return Sequence{
		Name: "hash-strlen-len",
		Commands: []Command{
			Cmd("DEL", k),

			// HLEN on an absent key -> :0.
			Cmd("HLEN", k),
			// HSTRLEN on an absent key -> :0.
			Cmd("HSTRLEN", k, "f"),

			Cmd("HSET", k, "f", "hello"),
			Cmd("HSTRLEN", k, "f"),    // :5
			Cmd("HSTRLEN", k, "nope"), // :0 (missing field)
			Cmd("HLEN", k),            // :1

			// Grow then shrink and confirm HLEN tracks each mutation.
			Cmd("HSET", k, "g", "x", "h", "y"),
			Cmd("HLEN", k), // :3
			Cmd("HDEL", k, "g"),
			Cmd("HLEN", k), // :2

			Cmd("DEL", k),
		},
	}
}

// hashScanSingleSequence probes HSCAN on a single-field hash, which completes in
// one page: the reply is the deterministic ["0", ["f","v"]]. Both a complete scan
// on the oracle and on redimos return the terminating cursor "0", so this is
// order- and cursor-token-independent (需求 6.3 in its deterministic form).
func hashScanSingleSequence() Sequence {
	k := "difftest:hash:scan"
	return Sequence{
		Name: "hash-scan-single",
		Commands: []Command{
			Cmd("DEL", k),

			// HSCAN on an absent key -> ["0", []].
			Cmd("HSCAN", k, "0"),

			Cmd("HSET", k, "only", "val"),
			// Complete scan of a one-field hash -> ["0", ["only","val"]].
			Cmd("HSCAN", k, "0"),
			// MATCH that excludes the only field -> ["0", []].
			Cmd("HSCAN", k, "0", "MATCH", "nomatch*"),
			// MATCH that includes it -> ["0", ["only","val"]].
			Cmd("HSCAN", k, "0", "MATCH", "on*"),

			Cmd("DEL", k),
		},
	}
}

// hashWrongTypeSequence probes the -WRONGTYPE error text for every hash command
// issued against a String key (Property 6, 需求 6.1). SET establishes the wrong
// type; each hash command must reply the exact WRONGTYPE line.
func hashWrongTypeSequence() Sequence {
	k := "difftest:hash:wrongtype"
	return Sequence{
		Name: "hash-wrong-type",
		Commands: []Command{
			Cmd("DEL", k),
			Cmd("SET", k, "stringvalue"),

			Cmd("HSET", k, "f", "v"),
			Cmd("HSETNX", k, "f", "v"),
			Cmd("HGET", k, "f"),
			Cmd("HMSET", k, "f", "v"),
			Cmd("HMGET", k, "f"),
			Cmd("HGETALL", k),
			Cmd("HDEL", k, "f"),
			Cmd("HEXISTS", k, "f"),
			Cmd("HKEYS", k),
			Cmd("HVALS", k),
			Cmd("HLEN", k),
			Cmd("HINCRBY", k, "f", "1"),
			Cmd("HINCRBYFLOAT", k, "f", "1.0"),
			Cmd("HSTRLEN", k, "f"),
			Cmd("HSCAN", k, "0"),

			Cmd("DEL", k),
		},
	}
}

// hashErrorTextSequence pins Property 6 for the Hash family: the
// wrong-number-of-arguments reply for every command (echoing the lowercase
// command name), the odd field/value pairing on HSET/HMSET, and the non-integer
// HINCRBY argument (需求 3.2, 3.4 as surfaced through Hash commands).
func hashErrorTextSequence() Sequence {
	k := "difftest:hash:err"
	return Sequence{
		Name: "hash-error-text",
		Commands: []Command{
			Cmd("DEL", k),

			// --- Wrong number of arguments (需求 3.2), lowercase name echoed ---
			Cmd("HSET", k, "f"),         // needs field/value pairs
			Cmd("HGET", k),              // exact arity 3
			Cmd("HSETNX", k, "f"),       // exact arity 4
			Cmd("HDEL", k),              // needs >=1 field
			Cmd("HMGET", k),             // needs >=1 field
			Cmd("HGETALL"),              // exact arity 2
			Cmd("HKEYS"),                // exact arity 2
			Cmd("HVALS"),                // exact arity 2
			Cmd("HLEN"),                 // exact arity 2
			Cmd("HEXISTS", k),           // exact arity 3
			Cmd("HINCRBY", k, "f"),      // exact arity 4
			Cmd("HINCRBYFLOAT", k, "f"), // exact arity 4
			Cmd("HMSET", k, "f"),        // needs field/value pairs
			Cmd("HSTRLEN", k),           // exact arity 3
			Cmd("HSCAN", k),             // needs >=3 args (key + cursor)

			// --- Odd field/value pairing (需求 3.2 via handler check) ---
			Cmd("HSET", k, "f1", "v1", "f2"),  // odd -> wrong number of arguments
			Cmd("HMSET", k, "f1", "v1", "f2"), // odd -> wrong number of arguments

			// --- Non-integer HINCRBY argument (需求 3.4) ---
			Cmd("HINCRBY", k, "n", "notanint"),
			Cmd("HINCRBY", k, "n", "3.14"),

			// --- Non-integer field value on HINCRBY (hash-specific error) ---
			Cmd("HSET", k, "s", "hello"),
			Cmd("HINCRBY", k, "s", "1"), // -ERR hash value is not an integer

			Cmd("DEL", k),
		},
	}
}

// describeHashDiff is a small helper for diagnostics, mirroring describeMatrix.
func describeHashDiff() string {
	return fmt.Sprintf("hash difftest: %d sequences %v",
		len(HashDiffSequences()), HashDiffSequenceNames())
}
