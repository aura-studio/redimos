package difftest

import "fmt"

// lists_diff.go supplies the List-command differential sequences that are the
// P0 gate for the List family (task 16.3). The redimo fork's list implementation
// was still being fixed, so — per the design's "list 重点验证" note — List cannot
// enter P0 until its whole command surface matches the Pika v3.2.2 oracle
// byte-for-byte. These sequences are that gate.
//
// List replies are deterministic and ordered (LRANGE returns elements in a fixed
// head-to-tail order, LPOP/RPOP take the defined end, LINDEX a defined position),
// so unlike Sets/Hashes there is no ordering ambiguity: every reply can be
// compared byte-for-byte against the oracle.
//
// They drive the same byte-level assertion engine as Matrix() (harness.go) but
// live in their own file and are exposed via ListDiffSequences() /
// ListDiffSequenceNames() so they can be wired into a dedicated live entry point
// (TestDiffLists in lists_diff_test.go) WITHOUT touching the shared Matrix()
// function — keeping the file conflict-free with concurrent work on matrix.go /
// difftest_test.go.
//
// Scope:
//
//   - LPUSH / RPUSH element ordering, and the LLEN O(1) length (需求 7.1, 7.2).
//   - LPUSHX / RPUSHX gated on the key already existing (需求 7.1).
//   - LPOP / RPOP from the defined ends, the null bulk on an empty/absent key,
//     and the key ceasing to exist once emptied (需求 7.1, 7.3, 7.7).
//   - LRANGE positive / negative / clamped / empty ranges, and LINDEX positions
//     including the out-of-range null bulk (需求 7.3).
//   - LSET at positive/negative indices, plus the index-out-of-range and
//     no-such-key errors (需求 7.4).
//   - LTRIM middle / negative / empty-and-delete ranges (需求 7.4).
//   - LREM head->tail / tail->head / all-occurrences / no-match / absent, all
//     reporting the removed count (需求 7.4).
//   - LINSERT BEFORE / AFTER, pivot-not-found (:-1), absent key (:0), and the
//     bad-where syntax error (需求 7.4).
//   - RPOPLPUSH two-key move, destination creation, source emptying, single-key
//     rotation, absent-source null bulk, and wrong-type destination rejection
//     without losing the source element (需求 7.5).
//   - Error text (Property 6 错误文案一致): WRONGTYPE against a non-list key for
//     every list command, and the wrong-number-of-arguments reply (lowercase
//     command name) for each command (需求 3.2, surfaced through List).
//
// LLEN / LTRIM / LREM / LINSERT / LSET length reconciliation is what makes these
// sequences double as the Property 3 (计数一致性) gate at the wire level: every
// mutation is followed by an LLEN (and often an LRANGE) whose reply the oracle
// defines, so a drifting counter diverges immediately.
//
// Every sequence begins by DEL-ing the keys it uses (unique prefixes) so runs are
// independent even against a persistent oracle, and ends by cleaning up.
//
// **Property 3: 计数一致性**
// Validates: 需求 7.2, 7.7 (plus the full List surface 需求 7.1, 7.3, 7.4, 7.5).

// ListDiffSequences returns the differential sequences that cover the full List
// command set and its boundaries.
func ListDiffSequences() []Sequence {
	return []Sequence{
		listPushPopOrderSequence(),
		listPushXSequence(),
		listRangeIndexSequence(),
		listLSetLTrimSequence(),
		listLRemLInsertSequence(),
		listRPopLPushSequence(),
		listErrorTextSequence(),
	}
}

// ListDiffSequenceNames returns the sequence names, for logging / subtest names.
func ListDiffSequenceNames() []string {
	seqs := ListDiffSequences()
	names := make([]string, len(seqs))
	for i, s := range seqs {
		names[i] = s.Name
	}
	return names
}

// listPushPopOrderSequence covers LPUSH/RPUSH element ordering, the LLEN counter,
// LPOP/RPOP from the defined ends, the null bulk on an empty pop, and the key
// ceasing to exist once its last element is popped (需求 7.1, 7.2, 7.3, 7.7).
func listPushPopOrderSequence() Sequence {
	k := "difftest:list:pushpop"
	return Sequence{
		Name: "list-push-pop-order",
		Commands: []Command{
			Cmd("DEL", k),

			// RPUSH appends in argument order: head-to-tail a, b, c.
			Cmd("RPUSH", k, "a", "b", "c"), // :3
			Cmd("LLEN", k),                 // :3
			Cmd("LRANGE", k, "0", "-1"),    // ["a","b","c"]

			// LPUSH prepends in argument order: LPUSH x y -> y, x in front.
			Cmd("LPUSH", k, "x", "y"),   // :5
			Cmd("LRANGE", k, "0", "-1"), // ["y","x","a","b","c"]
			Cmd("LLEN", k),              // :5

			// LPOP takes the head, RPOP the tail; LLEN tracks the length.
			Cmd("LPOP", k),              // "y"
			Cmd("RPOP", k),              // "c"
			Cmd("LLEN", k),              // :3
			Cmd("LRANGE", k, "0", "-1"), // ["x","a","b"]

			// Drain the rest; the final pop makes the key cease to exist.
			Cmd("LPOP", k),   // "x"
			Cmd("LPOP", k),   // "a"
			Cmd("LPOP", k),   // "b"
			Cmd("LPOP", k),   // $-1 (empty)
			Cmd("EXISTS", k), // :0 (empty list does not exist)
			Cmd("LLEN", k),   // :0
			Cmd("TYPE", k),   // +none

			// RPOP on an absent key is the null bulk too.
			Cmd("RPOP", k),

			Cmd("DEL", k),
		},
	}
}

// listPushXSequence covers LPUSHX/RPUSHX: they push only when the key already
// exists as a list (reporting the new length), and are a no-op reporting :0 on an
// absent key — which they must NOT create (需求 7.1).
func listPushXSequence() Sequence {
	k := "difftest:list:pushx"
	return Sequence{
		Name: "list-pushx",
		Commands: []Command{
			Cmd("DEL", k),

			// Absent key: LPUSHX/RPUSHX push nothing and reply :0.
			Cmd("LPUSHX", k, "a"), // :0
			Cmd("RPUSHX", k, "a"), // :0
			Cmd("EXISTS", k),      // :0 (must not have created it)

			// Seed the key, then LPUSHX/RPUSHX succeed and report the new length.
			Cmd("RPUSH", k, "m"),        // :1
			Cmd("LPUSHX", k, "head"),    // :2
			Cmd("RPUSHX", k, "tail"),    // :3
			Cmd("LRANGE", k, "0", "-1"), // ["head","m","tail"]
			Cmd("LLEN", k),              // :3

			Cmd("DEL", k),
		},
	}
}

// listRangeIndexSequence covers LRANGE positive/negative/clamped/empty ranges and
// LINDEX positions including the out-of-range null bulk and the absent-key
// replies (需求 7.3).
func listRangeIndexSequence() Sequence {
	k := "difftest:list:range"
	miss := k + ":missing"
	return Sequence{
		Name: "list-range-index",
		Commands: []Command{
			Cmd("DEL", k, miss),

			Cmd("RPUSH", k, "a", "b", "c", "d", "e"), // 0..4

			// LRANGE variants.
			Cmd("LRANGE", k, "0", "-1"),     // whole list
			Cmd("LRANGE", k, "0", "2"),      // ["a","b","c"]
			Cmd("LRANGE", k, "1", "3"),      // ["b","c","d"]
			Cmd("LRANGE", k, "-3", "-1"),    // ["c","d","e"]
			Cmd("LRANGE", k, "-100", "100"), // clamped -> whole list
			Cmd("LRANGE", k, "2", "1"),      // empty (start > stop)
			Cmd("LRANGE", k, "5", "10"),     // empty (past end)
			Cmd("LRANGE", miss, "0", "-1"),  // empty array (absent key)

			// LINDEX variants.
			Cmd("LINDEX", k, "0"),    // "a"
			Cmd("LINDEX", k, "2"),    // "c"
			Cmd("LINDEX", k, "-1"),   // "e" (last)
			Cmd("LINDEX", k, "-5"),   // "a" (first)
			Cmd("LINDEX", k, "5"),    // $-1 (out of range)
			Cmd("LINDEX", k, "-6"),   // $-1 (out of range)
			Cmd("LINDEX", miss, "0"), // $-1 (absent key)

			Cmd("DEL", k),
		},
	}
}

// listLSetLTrimSequence covers LSET (positive/negative index, index-out-of-range,
// no-such-key) and LTRIM (middle, negative, empty-and-delete) (需求 7.4).
func listLSetLTrimSequence() Sequence {
	k := "difftest:list:lsettrim"
	empt := k + ":empty"
	miss := k + ":missing"
	return Sequence{
		Name: "list-lset-ltrim",
		Commands: []Command{
			Cmd("DEL", k, empt, miss),

			// LSET at a positive and a negative index; length unchanged.
			Cmd("RPUSH", k, "a", "b", "c"), // 0:a 1:b 2:c
			Cmd("LSET", k, "1", "B"),       // +OK
			Cmd("LSET", k, "-1", "C"),      // +OK
			Cmd("LRANGE", k, "0", "-1"),    // ["a","B","C"]
			Cmd("LLEN", k),                 // :3
			// LSET out of range (positive and negative overshoot) -> error.
			Cmd("LSET", k, "3", "x"),  // -ERR index out of range
			Cmd("LSET", k, "-4", "x"), // -ERR index out of range
			// LSET on an absent key -> no such key error.
			Cmd("LSET", miss, "0", "x"), // -ERR no such key

			// LTRIM to an inner range and reconcile the length.
			Cmd("DEL", empt),
			Cmd("RPUSH", empt, "a", "b", "c", "d", "e"), // 0..4
			Cmd("LTRIM", empt, "1", "3"),                // +OK
			Cmd("LRANGE", empt, "0", "-1"),              // ["b","c","d"]
			Cmd("LLEN", empt),                           // :3
			// Negative-bound LTRIM (keep the last two).
			Cmd("LTRIM", empt, "-2", "-1"), // +OK
			Cmd("LRANGE", empt, "0", "-1"), // ["c","d"]
			// A range selecting nothing empties and deletes the key.
			Cmd("LTRIM", empt, "5", "10"), // +OK
			Cmd("EXISTS", empt),           // :0
			Cmd("LLEN", empt),             // :0
			// LTRIM on an absent key is a no-op that still replies +OK.
			Cmd("LTRIM", miss, "0", "-1"), // +OK

			Cmd("DEL", k, empt, miss),
		},
	}
}

// listLRemLInsertSequence covers LREM (head->tail, tail->head, all, no-match,
// absent — reporting the removed count) and LINSERT (before/after, pivot not
// found, absent key, and the bad-where syntax error) (需求 7.4).
func listLRemLInsertSequence() Sequence {
	k := "difftest:list:reminsert"
	miss := k + ":missing"
	return Sequence{
		Name: "list-lrem-linsert",
		Commands: []Command{
			Cmd("DEL", k, miss),

			// LREM count>0 removes head->tail, reporting the removed count.
			Cmd("RPUSH", k, "a", "b", "a", "c", "a"), // three a's
			Cmd("LREM", k, "2", "a"),                 // :2 (first two a's)
			Cmd("LRANGE", k, "0", "-1"),              // ["b","c","a"]
			Cmd("LLEN", k),                           // :3
			Cmd("DEL", k),

			// LREM count<0 removes tail->head.
			Cmd("RPUSH", k, "a", "b", "a", "c", "a"),
			Cmd("LREM", k, "-2", "a"),   // :2 (last two a's)
			Cmd("LRANGE", k, "0", "-1"), // ["a","b","c"]
			Cmd("DEL", k),

			// LREM count==0 removes every occurrence; removing all deletes key.
			Cmd("RPUSH", k, "a", "b", "a", "c", "a"),
			Cmd("LREM", k, "0", "a"),    // :3
			Cmd("LRANGE", k, "0", "-1"), // ["b","c"]
			// No-match LREM reports :0 and leaves the list unchanged.
			Cmd("LREM", k, "0", "zzz"), // :0
			Cmd("LLEN", k),             // :2
			// LREM on an absent key reports :0.
			Cmd("LREM", miss, "0", "a"), // :0
			Cmd("DEL", k),

			// LINSERT BEFORE / AFTER the first pivot, reporting the new length.
			Cmd("RPUSH", k, "a", "b", "c"),
			Cmd("LINSERT", k, "BEFORE", "b", "X"), // :4
			Cmd("LRANGE", k, "0", "-1"),           // ["a","X","b","c"]
			Cmd("LINSERT", k, "AFTER", "b", "Y"),  // :5
			Cmd("LRANGE", k, "0", "-1"),           // ["a","X","b","Y","c"]
			Cmd("LLEN", k),                        // :5
			// Pivot not found -> :-1, list unchanged.
			Cmd("LINSERT", k, "BEFORE", "zzz", "Z"), // :-1
			Cmd("LLEN", k),                          // :5
			// LINSERT on an absent key -> :0.
			Cmd("LINSERT", miss, "BEFORE", "p", "v"), // :0
			// Bad where token -> syntax error.
			Cmd("LINSERT", k, "SIDEWAYS", "a", "v"), // -ERR syntax error

			Cmd("DEL", k, miss),
		},
	}
}

// listRPopLPushSequence covers RPOPLPUSH: the two-key move, destination creation,
// source emptying (delete), single-key rotation, the absent-source null bulk, and
// a wrong-type destination that is rejected WITHOUT losing the source element
// (需求 7.5).
func listRPopLPushSequence() Sequence {
	k := "difftest:list:rpoplpush"
	src := k + ":src"
	dst := k + ":dst"
	str := k + ":str"
	return Sequence{
		Name: "list-rpoplpush",
		Commands: []Command{
			Cmd("DEL", k, src, dst, str),

			// Two-key move: source tail -> destination head; both lengths tracked.
			Cmd("RPUSH", src, "a", "b", "c"), // head-to-tail a, b, c
			Cmd("RPUSH", dst, "x", "y"),      // head-to-tail x, y
			Cmd("RPOPLPUSH", src, dst),       // "c" (moved tail)
			Cmd("LRANGE", src, "0", "-1"),    // ["a","b"]
			Cmd("LRANGE", dst, "0", "-1"),    // ["c","x","y"]
			Cmd("LLEN", src),                 // :2
			Cmd("LLEN", dst),                 // :3
			Cmd("DEL", src, dst),

			// Destination is created when absent.
			Cmd("RPUSH", src, "a", "b"),
			Cmd("RPOPLPUSH", src, dst),    // "b"
			Cmd("LLEN", dst),              // :1
			Cmd("LRANGE", dst, "0", "-1"), // ["b"]
			Cmd("DEL", src, dst),

			// Moving the last element deletes the (now empty) source.
			Cmd("RPUSH", src, "only"),
			Cmd("RPOPLPUSH", src, dst), // "only"
			Cmd("EXISTS", src),         // :0
			Cmd("LLEN", dst),           // :1
			Cmd("DEL", src, dst),

			// Single-key rotation: tail moves to head, length unchanged.
			Cmd("RPUSH", k, "a", "b", "c"), // a, b, c
			Cmd("RPOPLPUSH", k, k),         // "c"
			Cmd("LRANGE", k, "0", "-1"),    // ["c","a","b"]
			Cmd("LLEN", k),                 // :3 (unchanged)
			Cmd("DEL", k),

			// Absent source -> null bulk; destination must not be created.
			Cmd("RPOPLPUSH", src, dst), // $-1
			Cmd("EXISTS", dst),         // :0

			// Wrong-type destination is rejected WITHOUT popping the source.
			Cmd("RPUSH", src, "a", "b"),
			Cmd("SET", str, "iam-a-string"),
			Cmd("RPOPLPUSH", src, str), // -WRONGTYPE ...
			Cmd("LLEN", src),           // :2 (source untouched)

			Cmd("DEL", k, src, dst, str),
		},
	}
}

// listErrorTextSequence pins the List-command error text byte-for-byte
// (Property 6 错误文案一致): WRONGTYPE against a non-list key for every list
// command, and the wrong-number-of-arguments reply (which must echo the lowercase
// command name) for each command (需求 3.2, surfaced through List).
func listErrorTextSequence() Sequence {
	k := "difftest:list:err"
	return Sequence{
		Name: "list-error-text",
		Commands: []Command{
			Cmd("DEL", k),

			// --- WRONGTYPE: every list command against a String key ---
			Cmd("SET", k, "iam-a-string"),
			Cmd("LPUSH", k, "v"),                  // -WRONGTYPE ...
			Cmd("RPUSH", k, "v"),                  // -WRONGTYPE ...
			Cmd("LPUSHX", k, "v"),                 // -WRONGTYPE ...
			Cmd("RPUSHX", k, "v"),                 // -WRONGTYPE ...
			Cmd("LPOP", k),                        // -WRONGTYPE ...
			Cmd("RPOP", k),                        // -WRONGTYPE ...
			Cmd("LRANGE", k, "0", "-1"),           // -WRONGTYPE ...
			Cmd("LINDEX", k, "0"),                 // -WRONGTYPE ...
			Cmd("LLEN", k),                        // -WRONGTYPE ...
			Cmd("LSET", k, "0", "v"),              // -WRONGTYPE ...
			Cmd("LTRIM", k, "0", "-1"),            // -WRONGTYPE ...
			Cmd("LREM", k, "0", "v"),              // -WRONGTYPE ...
			Cmd("LINSERT", k, "BEFORE", "p", "v"), // -WRONGTYPE ...
			Cmd("RPOPLPUSH", k, k+":dst"),         // -WRONGTYPE ...
			Cmd("DEL", k),

			// --- Wrong number of arguments (需求 3.2), lowercase name echoed ---
			Cmd("LPUSH", k),                  // needs >=3
			Cmd("RPUSH", k),                  // needs >=3
			Cmd("LPUSHX", k),                 // needs >=3
			Cmd("RPUSHX", k),                 // needs >=3
			Cmd("LPOP"),                      // exact arity 2
			Cmd("LPOP", k, "extra"),          // too many
			Cmd("RPOP"),                      // exact arity 2
			Cmd("LRANGE", k, "0"),            // exact arity 4
			Cmd("LINDEX", k),                 // exact arity 3
			Cmd("LLEN"),                      // exact arity 2
			Cmd("LLEN", k, "extra"),          // too many
			Cmd("LSET", k, "0"),              // exact arity 4
			Cmd("LTRIM", k, "0"),             // exact arity 4
			Cmd("LREM", k, "0"),              // exact arity 4
			Cmd("LINSERT", k, "BEFORE", "p"), // exact arity 5
			Cmd("RPOPLPUSH", k),              // exact arity 3

			Cmd("DEL", k),
		},
	}
}

// describeListDiff is a small helper for diagnostics, mirroring describeKeysDiff.
func describeListDiff() string {
	return fmt.Sprintf("list difftest: %d sequences %v",
		len(ListDiffSequences()), ListDiffSequenceNames())
}
