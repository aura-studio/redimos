package difftest

import "fmt"

// This file adds the Key-management + TTL differential sequences for task 10.4.
// They drive the same byte-level assertion engine as Matrix() (harness.go) but
// live in their own file and are exposed via KeysDiffSequences() /
// KeysDiffSequenceNames() so they can be wired into a dedicated live entry point
// (TestDiffKeys in keys_diff_test.go) WITHOUT touching the shared Matrix()
// function — this keeps the file conflict-free with concurrent work on
// matrix.go / difftest_test.go.
//
// Scope (Validates: 需求 10.1–10.7, 10.11; Property 6 错误文案一致):
//
//   - DEL      return-value counts: existing (:1), absent (:0), multiple (:N),
//     duplicate keys counted independently (需求 10.1).
//   - EXISTS   return-value counts: present (:1), absent (:0), repeated key
//     counted once per occurrence (需求 10.2).
//   - TYPE     +string / +none, plus every collection type +list/+hash/+set/
//     +zset (需求 10.3, 10.11).
//   - EXPIRE / EXPIREAT / PEXPIRE / PEXPIREAT: existing key -> :1, absent -> :0,
//     across all four family members (需求 10.4, 10.5).
//   - TTL / PTTL: sentinel replies only — -2 for an absent/expired key and -1
//     for a key with no expiry (需求 10.6). Remaining-seconds are deliberately
//     NOT asserted: they race a live oracle's clock and would diverge by ~1s
//     under a byte-for-byte comparison. The one "with expiry" case is made
//     deterministic by PERSIST-then-TTL, which returns to the -1 sentinel.
//   - PERSIST: :1 when a live key's expiry is removed, :0 when the key is
//     absent/expired or had no expiry to remove (需求 10.7).
//   - EXPIRE-family / TTL / PERSIST applied to collection keys (list/hash/set/
//     zset) to prove TTL acts on the whole key regardless of type (需求 10.11).
//   - Error text (Property 6): wrong-arity for every command in the family
//     (reply must use the lowercase command name) and non-integer / out-of-range
//     time arguments to the EXPIRE family (需求 3.2, 3.4 surfaced through the
//     Key family).
//
// Every sequence begins by DEL-ing the keys it uses (with unique prefixes) so
// runs are independent even against a persistent oracle, and ends by cleaning
// up so it leaves no residue for the next sequence.

// Fixed absolute expiry arguments used by the EXPIREAT / PEXPIREAT probes. These
// are far enough in the future (year 2286) that the resolved expiry is never in
// the past for any realistic test-run clock, so the reply is a deterministic :1
// rather than a delete-in-the-past :1-with-side-effect ambiguity. Being fixed
// constants (not now()+delta) also keeps the exact command bytes identical
// across both endpoints.
const (
	farFutureEpochSecs = "9999999999"    // ~2286-11-20 in epoch seconds
	farFutureEpochMs   = "9999999999000" // same instant in epoch milliseconds
)

// KeysDiffSequences returns the Key-management + TTL differential sequences.
func KeysDiffSequences() []Sequence {
	return []Sequence{
		delCountsSequence(),
		existsCountsSequence(),
		typeSequence(),
		expireFamilyReturnSequence(),
		ttlSentinelSequence(),
		persistSequence(),
		collectionTTLSequence(),
		keysErrorTextSequence(),
	}
}

// KeysDiffSequenceNames returns the sequence names, for logging / subtest names.
func KeysDiffSequenceNames() []string {
	seqs := KeysDiffSequences()
	names := make([]string, len(seqs))
	for i, s := range seqs {
		names[i] = s.Name
	}
	return names
}

// delCountsSequence probes DEL's integer reply: an existing key deletes and
// returns :1, a re-DEL of the now-absent key returns :0, a multi-key DEL returns
// the count of keys that existed, and duplicate keys are counted independently
// (需求 10.1).
func delCountsSequence() Sequence {
	k := "difftest:keys:del"
	a := k + ":a"
	b := k + ":b"
	return Sequence{
		Name: "keys-del-counts",
		Commands: []Command{
			Cmd("DEL", k, a, b),

			// Existing single key -> :1, then absent -> :0.
			Cmd("SET", k, "v"),
			Cmd("DEL", k),
			Cmd("DEL", k),

			// Multiple keys: two present + one absent -> :2.
			Cmd("SET", a, "1"),
			Cmd("SET", b, "2"),
			Cmd("DEL", a, b, k),

			// Duplicate key in one DEL is counted once (only the first sees it
			// live): SET then DEL a a -> :1.
			Cmd("SET", a, "1"),
			Cmd("DEL", a, a),

			Cmd("DEL", k, a, b),
		},
	}
}

// existsCountsSequence probes EXISTS's integer reply: present (:1), absent (:0),
// and a repeated key counted once per occurrence — EXISTS k k k -> :3 (需求 10.2).
func existsCountsSequence() Sequence {
	k := "difftest:keys:exists"
	miss := k + ":missing"
	return Sequence{
		Name: "keys-exists-counts",
		Commands: []Command{
			Cmd("DEL", k, miss),

			Cmd("EXISTS", miss),
			Cmd("SET", k, "v"),
			Cmd("EXISTS", k),
			// Repeated key counted per occurrence.
			Cmd("EXISTS", k, k, k),
			// Mix of present (twice) and absent -> :2.
			Cmd("EXISTS", k, miss, k),

			Cmd("DEL", k),
		},
	}
}

// typeSequence probes TYPE across the string type, the "none" reply for an
// absent key, and every collection type so the +list/+hash/+set/+zset simple
// strings are asserted byte-for-byte (需求 10.3, 10.11).
func typeSequence() Sequence {
	k := "difftest:keys:type"
	str := k + ":str"
	lst := k + ":list"
	hsh := k + ":hash"
	set := k + ":set"
	zst := k + ":zset"
	return Sequence{
		Name: "keys-type",
		Commands: []Command{
			Cmd("DEL", k, str, lst, hsh, set, zst),

			// Absent key -> +none.
			Cmd("TYPE", k),

			Cmd("SET", str, "v"),
			Cmd("TYPE", str),

			Cmd("RPUSH", lst, "a", "b"),
			Cmd("TYPE", lst),

			Cmd("HSET", hsh, "f", "v"),
			Cmd("TYPE", hsh),

			Cmd("SADD", set, "m"),
			Cmd("TYPE", set),

			Cmd("ZADD", zst, "1", "m"),
			Cmd("TYPE", zst),

			Cmd("DEL", str, lst, hsh, set, zst),
		},
	}
}

// expireFamilyReturnSequence probes the :1 (existing) / :0 (absent) integer
// reply of all four EXPIRE-family members. Absolute-timestamp variants use fixed
// far-future constants so the reply is a stable :1 and the command bytes are
// identical on both endpoints (需求 10.4, 10.5).
func expireFamilyReturnSequence() Sequence {
	k := "difftest:keys:expire"
	miss := k + ":missing"
	e := k + ":e"
	ea := k + ":ea"
	pe := k + ":pe"
	pea := k + ":pea"
	return Sequence{
		Name: "keys-expire-family-return",
		Commands: []Command{
			Cmd("DEL", k, e, ea, pe, pea),

			// EXPIRE: absent -> :0, existing -> :1.
			Cmd("EXPIRE", miss, "100"),
			Cmd("SET", e, "v"),
			Cmd("EXPIRE", e, "100"),

			// EXPIREAT: absent -> :0, existing (far future) -> :1.
			Cmd("EXPIREAT", miss, farFutureEpochSecs),
			Cmd("SET", ea, "v"),
			Cmd("EXPIREAT", ea, farFutureEpochSecs),

			// PEXPIRE: absent -> :0, existing -> :1.
			Cmd("PEXPIRE", miss, "100000"),
			Cmd("SET", pe, "v"),
			Cmd("PEXPIRE", pe, "100000"),

			// PEXPIREAT: absent -> :0, existing (far future) -> :1.
			Cmd("PEXPIREAT", miss, farFutureEpochMs),
			Cmd("SET", pea, "v"),
			Cmd("PEXPIREAT", pea, farFutureEpochMs),

			Cmd("DEL", e, ea, pe, pea),
		},
	}
}

// ttlSentinelSequence probes ONLY the deterministic TTL/PTTL sentinels: -2 for an
// absent key and -1 for a key with no expiry (需求 10.6). Remaining-seconds are
// not asserted here because they race the oracle's clock under byte-for-byte
// comparison; the "has an expiry" path is exercised deterministically by
// persistSequence (EXPIRE -> PERSIST -> TTL returns to -1).
func ttlSentinelSequence() Sequence {
	k := "difftest:keys:ttl"
	return Sequence{
		Name: "keys-ttl-sentinels",
		Commands: []Command{
			Cmd("DEL", k),

			// Absent -> -2 for both TTL and PTTL.
			Cmd("TTL", k),
			Cmd("PTTL", k),

			// Exists, no expiry -> -1 for both.
			Cmd("SET", k, "v"),
			Cmd("TTL", k),
			Cmd("PTTL", k),

			Cmd("DEL", k),
		},
	}
}

// persistSequence probes PERSIST's :1/:0 reply and closes the deterministic
// TTL loop: PERSIST on an absent key -> :0, on a live key with no expiry -> :0,
// on a live key whose expiry it removes -> :1, and a second PERSIST -> :0. A
// final TTL confirms the key is back to the no-expiry sentinel -1 (需求 10.6, 10.7).
func persistSequence() Sequence {
	k := "difftest:keys:persist"
	miss := k + ":missing"
	return Sequence{
		Name: "keys-persist",
		Commands: []Command{
			Cmd("DEL", k, miss),

			// Absent key -> :0.
			Cmd("PERSIST", miss),

			Cmd("SET", k, "v"),
			// Live key with no expiry to remove -> :0.
			Cmd("PERSIST", k),

			// Give it an expiry, then remove it -> :1, second attempt -> :0.
			Cmd("EXPIRE", k, "1000"),
			Cmd("PERSIST", k),
			Cmd("PERSIST", k),

			// Back to the no-expiry sentinel.
			Cmd("TTL", k),

			Cmd("DEL", k),
		},
	}
}

// collectionTTLSequence proves the EXPIRE family / TTL / PERSIST act on the whole
// key regardless of type by exercising them against list, hash, set and zset keys
// (需求 10.11). Only the deterministic sentinel / return-value replies are
// asserted (TTL -1, EXPIRE :1, PERSIST :1, DEL :1).
func collectionTTLSequence() Sequence {
	k := "difftest:keys:coll"
	lst := k + ":list"
	hsh := k + ":hash"
	set := k + ":set"
	zst := k + ":zset"
	return Sequence{
		Name: "keys-collection-ttl",
		Commands: []Command{
			Cmd("DEL", lst, hsh, set, zst),

			// List: no expiry -> -1, set expiry -> :1, persist -> :1, back to -1.
			Cmd("RPUSH", lst, "a", "b", "c"),
			Cmd("TTL", lst),
			Cmd("EXPIRE", lst, "1000"),
			Cmd("PERSIST", lst),
			Cmd("TTL", lst),
			Cmd("DEL", lst),

			// Hash.
			Cmd("HSET", hsh, "f1", "v1"),
			Cmd("TTL", hsh),
			Cmd("EXPIRE", hsh, "1000"),
			Cmd("PERSIST", hsh),
			Cmd("DEL", hsh),

			// Set: use the far-future absolute variant.
			Cmd("SADD", set, "m1", "m2"),
			Cmd("EXPIREAT", set, farFutureEpochSecs),
			Cmd("PERSIST", set),
			Cmd("DEL", set),

			// Sorted set: millisecond variant.
			Cmd("ZADD", zst, "1", "m"),
			Cmd("PEXPIRE", zst, "1000000"),
			Cmd("PERSIST", zst),
			Cmd("DEL", zst),
		},
	}
}

// keysErrorTextSequence probes Property 6 (错误文案一致) for the Key family: the
// wrong-number-of-arguments reply for every command (which must echo the
// lowercase command name), and the non-integer / out-of-range reply for the
// EXPIRE family's time argument (需求 3.2, 3.4 as surfaced through Key commands).
func keysErrorTextSequence() Sequence {
	k := "difftest:keys:err"
	return Sequence{
		Name: "keys-error-text",
		Commands: []Command{
			Cmd("DEL", k),

			// --- Wrong number of arguments (需求 3.2), lowercase name echoed ---
			Cmd("DEL"),              // needs >=1 key
			Cmd("EXISTS"),           // needs >=1 key
			Cmd("TYPE"),             // exact arity 2
			Cmd("TYPE", k, "extra"), // too many
			Cmd("EXPIRE", k),        // exact arity 3
			Cmd("EXPIRE", k, "1", "x"),
			Cmd("EXPIREAT", k),
			Cmd("PEXPIRE", k),
			Cmd("PEXPIREAT", k),
			Cmd("TTL"),
			Cmd("TTL", k, "extra"),
			Cmd("PTTL"),
			Cmd("PERSIST"),
			Cmd("PERSIST", k, "extra"),

			// --- Non-integer / out-of-range time argument (需求 3.4) ---
			Cmd("EXPIRE", k, "notanint"),
			Cmd("EXPIRE", k, "3.14"),
			Cmd("EXPIREAT", k, "notanint"),
			Cmd("PEXPIRE", k, "notanint"),
			Cmd("PEXPIREAT", k, "notanint"),
			Cmd("EXPIRE", k, "9223372036854775808"), // MaxInt64 + 1

			Cmd("DEL", k),
		},
	}
}

// describeKeysDiff is a small helper for diagnostics, mirroring describeMatrix.
func describeKeysDiff() string {
	return fmt.Sprintf("keys difftest: %d sequences %v",
		len(KeysDiffSequences()), KeysDiffSequenceNames())
}
