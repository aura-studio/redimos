package difftest

import "fmt"

// sets_diff.go supplies the Set-command differential sequences for task 14.3.
// They drive the same byte-level assertion engine as Matrix() (harness.go) but
// live in their own file and are exposed via SetDiffSequences() /
// SetDiffSequenceNames() so they can be wired into a dedicated live entry point
// (TestDiffSets in sets_diff_test.go) WITHOUT touching the shared Matrix()
// function — this keeps the file conflict-free with concurrent work on
// matrix.go / difftest_test.go.
//
// Scope (Validates: 需求 8.1–8.5; Property 6 错误文案一致):
//
//   - SADD / SREM return-value counts (new vs duplicate vs absent members),
//     SCARD, SISMEMBER present/absent, and last-member key deletion (需求 8.1,
//     8.2, 8.5).
//   - SPOP / SRANDMEMBER / SMEMBERS / SSCAN — these return members in an
//     UNSPECIFIED order, so a byte-for-byte oracle comparison of a multi-member
//     reply would be flaky. They are therefore exercised only in DETERMINISTIC
//     shapes: on a single-member set (a one-element reply has a unique
//     serialization), on an empty/absent key (the null bulk / empty array), and
//     — for SSCAN — a completed single-page scan (cursor "0", which every
//     Redis/Pika implementation returns when the scan is exhausted). The
//     order-sensitive MULTI-member behaviour of these commands is covered by the
//     in-process unit tests (internal/command/sets_test.go, sscan_test.go)
//     instead, not here.
//   - SUNION / SINTER / SDIFF — likewise order-unspecified. They are byte-compared
//     only where the result has 0 or 1 elements (a unique serialization); the
//     multi-element cases are covered deterministically via the *STORE variants
//     plus SCARD (cardinality) and SISMEMBER (membership) surrogates.
//   - SUNIONSTORE / SINTERSTORE / SDIFFSTORE — the stored cardinality reply is
//     deterministic and byte-compared; the resulting members are verified with
//     the deterministic SCARD + SISMEMBER surrogates rather than SMEMBERS.
//   - SMOVE — the :1/:0 reply is deterministic and byte-compared; the effect is
//     verified with SISMEMBER + SCARD surrogates on both source and destination.
//   - WRONGTYPE for every Set command against a string key, and the
//     wrong-number-of-arguments (arity) reply for every command (Property 6).
//
// Every sequence begins by DEL-ing the keys it uses (with unique prefixes) so
// runs are independent even against a persistent oracle, and cleans up at the
// end so it leaves no residue for the next sequence.

// SetDiffSequences returns the Set-command differential sequences.
func SetDiffSequences() []Sequence {
	return []Sequence{
		setAddRemCardSequence(),
		setSPopDeterministicSequence(),
		setSRandMemberDeterministicSequence(),
		setSMembersScanDeterministicSequence(),
		setAlgebraStoreSurrogateSequence(),
		setAlgebraReadSmallSequence(),
		setSMoveSequence(),
		setWrongTypeSequence(),
		setAritySequence(),
	}
}

// SetDiffSequenceNames returns the sequence names, for logging / subtest names.
func SetDiffSequenceNames() []string {
	seqs := SetDiffSequences()
	names := make([]string, len(seqs))
	for i, s := range seqs {
		names[i] = s.Name
	}
	return names
}

// setAddRemCardSequence probes the deterministic-reply Set core: SADD's added
// count (new members only), SCARD, SISMEMBER, SREM's removed count (existing
// members only), and the fact that removing the last member deletes the key —
// an empty set does not exist, so SCARD -> :0, EXISTS -> :0, TYPE -> +none
// (需求 8.1, 8.2, 8.5).
func setAddRemCardSequence() Sequence {
	k := "difftest:set:core"
	miss := k + ":missing"
	return Sequence{
		Name: "set-add-rem-card",
		Commands: []Command{
			Cmd("DEL", k, miss),

			// SADD: three new -> :3; one dup + one new -> :1; all dup -> :0.
			Cmd("SADD", k, "a", "b", "c"),
			Cmd("SADD", k, "a", "d"),
			Cmd("SADD", k, "a", "b"),
			Cmd("SCARD", k), // :4

			// SISMEMBER: present -> :1, absent member -> :0, absent key -> :0.
			Cmd("SISMEMBER", k, "a"),
			Cmd("SISMEMBER", k, "zzz"),
			Cmd("SISMEMBER", miss, "a"),

			// SREM: two present + one absent -> :2; remaining two -> :2 (empties).
			Cmd("SREM", k, "a", "b", "zzz"),
			Cmd("SCARD", k), // :2
			Cmd("SREM", k, "c", "d"),
			Cmd("SCARD", k),  // :0
			Cmd("EXISTS", k), // :0 (empty set must not exist)
			Cmd("TYPE", k),   // +none

			// SREM / SCARD on an absent key.
			Cmd("SREM", miss, "x"), // :0
			Cmd("SCARD", miss),     // :0

			Cmd("DEL", k, miss),
		},
	}
}

// setSPopDeterministicSequence exercises SPOP only in shapes whose reply is
// deterministic despite the unspecified pop order: a single-member set (the
// popped member is forced), an over-count pop of a single-member set (a
// one-element array), and the empty-key replies (null bulk for the scalar form,
// empty array for the count form). The post-conditions are confirmed with the
// deterministic SCARD / EXISTS surrogates (需求 8.1, 8.5). Multi-member SPOP order
// is covered by the in-process unit tests.
func setSPopDeterministicSequence() Sequence {
	k := "difftest:set:spop"
	return Sequence{
		Name: "set-spop-deterministic",
		Commands: []Command{
			Cmd("DEL", k),

			// Single-member set: scalar SPOP returns exactly that member.
			Cmd("SADD", k, "only"),
			Cmd("SPOP", k),   // $only
			Cmd("SCARD", k),  // :0
			Cmd("EXISTS", k), // :0 (emptied -> deleted)

			// Single-member set: SPOP with a count >= cardinality returns the
			// one member as a one-element array, deterministically.
			Cmd("SADD", k, "solo"),
			Cmd("SPOP", k, "5"), // [solo]
			Cmd("SCARD", k),     // :0

			// Empty / absent key: scalar -> null bulk, count -> empty array.
			Cmd("SPOP", k),      // $-1
			Cmd("SPOP", k, "3"), // *0

			Cmd("DEL", k),
		},
	}
}

// setSRandMemberDeterministicSequence exercises SRANDMEMBER on a single-member
// set, where every form has a deterministic reply: the scalar form returns the
// member, a positive count returns the single distinct member, and a negative
// count returns exactly |count| repeats of it. SRANDMEMBER must NOT remove
// anything, confirmed by SCARD (需求 8.1). The absent-key null bulk / empty array
// replies are also pinned. Multi-member ordering is covered by the unit tests.
func setSRandMemberDeterministicSequence() Sequence {
	k := "difftest:set:srand"
	miss := k + ":missing"
	return Sequence{
		Name: "set-srandmember-deterministic",
		Commands: []Command{
			Cmd("DEL", k, miss),

			Cmd("SADD", k, "only"),
			Cmd("SRANDMEMBER", k),       // $only
			Cmd("SRANDMEMBER", k, "5"),  // [only]  (positive count, distinct, capped)
			Cmd("SRANDMEMBER", k, "-3"), // [only, only, only]  (negative count, repeats)
			Cmd("SCARD", k),             // :1  (no removal)

			// Absent key: scalar -> null bulk, count -> empty array.
			Cmd("SRANDMEMBER", miss),      // $-1
			Cmd("SRANDMEMBER", miss, "3"), // *0

			Cmd("DEL", k),
		},
	}
}

// setSMembersScanDeterministicSequence exercises SMEMBERS and SSCAN in their only
// deterministic shape: on a single-member set (a one-element reply) and on an
// absent key (the empty array). For SSCAN, a set that fits in one page completes
// the scan, so the cursor is the universally-agreed terminating "0" and the
// member list is the single member — both endpoints must serialize this
// identically. MATCH filtering to exactly one / zero members is also pinned.
// Multi-member SMEMBERS/SSCAN ordering is covered by the in-process unit tests
// (需求 8.1, 8.3).
func setSMembersScanDeterministicSequence() Sequence {
	k := "difftest:set:members"
	miss := k + ":missing"
	return Sequence{
		Name: "set-smembers-sscan-deterministic",
		Commands: []Command{
			Cmd("DEL", k, miss),

			Cmd("SADD", k, "only"),
			Cmd("SMEMBERS", k),    // [only]
			Cmd("SMEMBERS", miss), // *0 (absent -> empty array)

			// A single-page SSCAN completes: terminating cursor "0" + the member.
			Cmd("SSCAN", k, "0"),                  // [0, [only]]
			Cmd("SSCAN", k, "0", "MATCH", "o*"),   // [0, [only]]  (matches)
			Cmd("SSCAN", k, "0", "MATCH", "zzz*"), // [0, []]      (no match)
			Cmd("SSCAN", miss, "0"),               // [0, []]      (absent set)

			Cmd("DEL", k),
		},
	}
}

// setAlgebraStoreSurrogateSequence covers the multi-member set-algebra results
// deterministically via the *STORE variants: the stored cardinality is a
// deterministic integer reply, and the resulting membership is verified with
// SCARD + SISMEMBER surrogates rather than the order-unspecified SMEMBERS. It
// also covers *STORE's overwrite-any-prior-value semantics (dest is reused) and
// the empty-result case that deletes dest (需求 8.1, 8.4, 8.5).
func setAlgebraStoreSurrogateSequence() Sequence {
	k := "difftest:set:algstore"
	s1, s2, s3, s4 := k+":s1", k+":s2", k+":s3", k+":s4"
	dest := k + ":dest"
	return Sequence{
		Name: "set-algebra-store-surrogate",
		Commands: []Command{
			Cmd("DEL", s1, s2, s3, s4, dest),

			Cmd("SADD", s1, "a", "b", "c"), // :3
			Cmd("SADD", s2, "b", "c", "d"), // :3
			Cmd("SADD", s3, "c", "e"),      // :2
			Cmd("SADD", s4, "x"),           // :1 (disjoint from s1)

			// Union = {a,b,c,d,e} -> :5. Verify via SCARD + SISMEMBER.
			Cmd("SUNIONSTORE", dest, s1, s2, s3),
			Cmd("SCARD", dest),           // :5
			Cmd("SISMEMBER", dest, "a"),  // :1
			Cmd("SISMEMBER", dest, "e"),  // :1
			Cmd("SISMEMBER", dest, "zz"), // :0

			// Intersection s1 ∩ s2 = {b,c} -> :2 (dest overwritten).
			Cmd("SINTERSTORE", dest, s1, s2),
			Cmd("SCARD", dest),          // :2
			Cmd("SISMEMBER", dest, "b"), // :1
			Cmd("SISMEMBER", dest, "c"), // :1
			Cmd("SISMEMBER", dest, "a"), // :0

			// Difference s1 - s2 = {a} -> :1.
			Cmd("SDIFFSTORE", dest, s1, s2),
			Cmd("SCARD", dest),          // :1
			Cmd("SISMEMBER", dest, "a"), // :1
			Cmd("SISMEMBER", dest, "b"), // :0

			// Empty result deletes dest: s1 ∩ s4 = {} -> :0, dest gone.
			Cmd("SINTERSTORE", dest, s1, s4),
			Cmd("SCARD", dest),  // :0
			Cmd("EXISTS", dest), // :0

			Cmd("DEL", s1, s2, s3, s4, dest),
		},
	}
}

// setAlgebraReadSmallSequence byte-compares the direct SUNION / SINTER / SDIFF
// reads only where the result is small enough to have a unique serialization:
// zero elements (the empty array) or exactly one element (a one-element array).
// This covers the read variants of the commands themselves; their multi-element
// ordering is left to the *STORE surrogates above and the unit tests (需求 8.1,
// 8.4).
func setAlgebraReadSmallSequence() Sequence {
	k := "difftest:set:algread"
	s1, s2 := k+":s1", k+":s2"
	miss := k + ":missing"
	return Sequence{
		Name: "set-algebra-read-small",
		Commands: []Command{
			Cmd("DEL", s1, s2, miss),

			Cmd("SADD", s1, "a"),      // {a}
			Cmd("SADD", s2, "a", "b"), // {a,b}

			// SINTER {a} ∩ {a,b} = {a} -> single element [a].
			Cmd("SINTER", s1, s2),
			// SDIFF {a,b} - {a} = {b} -> single element [b].
			Cmd("SDIFF", s2, s1),
			// SDIFF {a} - {a,b} = {} -> empty array *0.
			Cmd("SDIFF", s1, s2),
			// SUNION of a single operand {a} -> single element [a].
			Cmd("SUNION", s1),
			// SINTER with an absent operand -> {} -> empty array *0.
			Cmd("SINTER", s1, miss),

			Cmd("DEL", s1, s2, miss),
		},
	}
}

// setSMoveSequence byte-compares SMOVE's deterministic :1/:0 reply and verifies
// the move's effect with SISMEMBER + SCARD surrogates on both source and
// destination: a successful move (:1) removes from source and adds to
// destination; a member absent from source (:0) moves nothing; moving the last
// member deletes the (now empty) source; and SMOVE from an absent source is :0
// (需求 8.1, 8.4, 8.5).
func setSMoveSequence() Sequence {
	k := "difftest:set:smove"
	src, dst := k+":src", k+":dst"
	miss := k + ":missing"
	return Sequence{
		Name: "set-smove",
		Commands: []Command{
			Cmd("DEL", src, dst, miss),

			Cmd("SADD", src, "a", "b", "c"), // :3

			// Successful move of "a": :1, then a leaves src and appears in dst.
			Cmd("SMOVE", src, dst, "a"),
			Cmd("SISMEMBER", src, "a"), // :0
			Cmd("SISMEMBER", dst, "a"), // :1
			Cmd("SCARD", src),          // :2
			Cmd("SCARD", dst),          // :1

			// Member not in src -> :0, nothing changes.
			Cmd("SMOVE", src, dst, "zzz"),
			Cmd("SMOVE", src, dst, "a"), // a already moved -> :0
			Cmd("SCARD", src),           // :2
			Cmd("SCARD", dst),           // :1

			// Move the remaining members: src empties and is deleted.
			Cmd("SMOVE", src, dst, "b"),
			Cmd("SMOVE", src, dst, "c"),
			Cmd("SCARD", src),  // :0
			Cmd("EXISTS", src), // :0 (emptied -> deleted)
			Cmd("SCARD", dst),  // :3

			// Move from an absent source -> :0.
			Cmd("SMOVE", miss, dst, "a"),

			Cmd("DEL", src, dst, miss),
		},
	}
}

// setWrongTypeSequence pins the WRONGTYPE reply (Property 6) for every Set command
// issued against a key holding a String value. Every type-checked Set command —
// reads and writes alike — must reply the exact WRONGTYPE text byte-for-byte
// (需求 8.1). A wrong-type source or destination for SMOVE, and a wrong-type
// source for *STORE, are included too.
func setWrongTypeSequence() Sequence {
	k := "difftest:set:wrongtype"
	other := k + ":other"
	dest := k + ":dest"
	return Sequence{
		Name: "set-wrongtype",
		Commands: []Command{
			Cmd("DEL", k, other, dest),

			Cmd("SET", k, "v"), // k is a String now

			Cmd("SADD", k, "m"),
			Cmd("SREM", k, "m"),
			Cmd("SISMEMBER", k, "m"),
			Cmd("SMEMBERS", k),
			Cmd("SCARD", k),
			Cmd("SPOP", k),
			Cmd("SPOP", k, "2"),
			Cmd("SRANDMEMBER", k),
			Cmd("SRANDMEMBER", k, "2"),
			Cmd("SSCAN", k, "0"),

			// SMOVE with a wrong-type source, then a wrong-type destination.
			Cmd("SADD", other, "x"), // a real set for the counterpart operand
			Cmd("SMOVE", k, other, "m"),
			Cmd("SMOVE", other, k, "x"),

			// Set-algebra reads / stores with a wrong-type operand.
			Cmd("SUNION", k, other),
			Cmd("SINTER", other, k),
			Cmd("SDIFF", k),
			Cmd("SUNIONSTORE", dest, k),

			Cmd("DEL", k, other, dest),
		},
	}
}

// setAritySequence pins the wrong-number-of-arguments reply (Property 6) for every
// Set command, exercising both the negative (minimum) and exact arities. The
// reply must echo the lowercase command name byte-for-byte (需求 3.2 surfaced
// through the Set family).
func setAritySequence() Sequence {
	k := "difftest:set:arity"
	return Sequence{
		Name: "set-arity",
		Commands: []Command{
			Cmd("DEL", k),

			Cmd("SADD", k),                // arity -3
			Cmd("SREM", k),                // arity -3
			Cmd("SISMEMBER", k),           // arity 3 (too few)
			Cmd("SISMEMBER", k, "a", "b"), // arity 3 (too many)
			Cmd("SMEMBERS"),               // arity 2 (too few)
			Cmd("SMEMBERS", k, "x"),       // arity 2 (too many)
			Cmd("SCARD"),                  // arity 2
			Cmd("SCARD", k, "x"),          // arity 2 (too many)
			Cmd("SPOP"),                   // arity -2
			Cmd("SRANDMEMBER"),            // arity -2
			Cmd("SSCAN", k),               // arity -3 (needs a cursor)
			Cmd("SUNION"),                 // arity -2
			Cmd("SINTER"),                 // arity -2
			Cmd("SDIFF"),                  // arity -2
			Cmd("SUNIONSTORE", k),         // arity -3
			Cmd("SINTERSTORE", k),         // arity -3
			Cmd("SDIFFSTORE", k),          // arity -3
			Cmd("SMOVE", k, k),            // arity 4 (too few)

			Cmd("DEL", k),
		},
	}
}

// describeSetsDiff is a small diagnostics helper, mirroring describeKeysDiff.
func describeSetsDiff() string {
	return fmt.Sprintf("sets difftest: %d sequences %v",
		len(SetDiffSequences()), SetDiffSequenceNames())
}
