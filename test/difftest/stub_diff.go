package difftest

import "fmt"

// stub_diff.go adds the "client-probe stub" half of task 18.3 (Property 6:
// 错误文案一致; Validates 需求 19.1–19.5). It drives the same byte-level
// assertion engine as Matrix() (harness.go) but lives in its own file and is
// exposed via StubDiffSequences() / StubDiffSequenceNames() so it can be wired
// into a dedicated live entry point (TestDiffStubs in
// unsupported_stub_diff_test.go) WITHOUT touching the shared Matrix() /
// difftest_test.go — keeping the file conflict-free with concurrent work on
// matrix.go.
//
// # Which stubs are byte-for-byte comparable against the live oracle
//
// task 18.2 added benign fallbacks for the introspection/probe commands clients
// run during connection setup (see internal/command/stub.go). Only the ones with
// a DETERMINISTIC reply that also MATCHES Pika v3.2.2 belong in the live
// differential sequences:
//
//   - COMMAND COUNT  -> ":0"                      (需求 19.1) — deterministic.
//   - CLIENT SETNAME -> "+OK"                      (需求 19.2) — deterministic.
//   - CLIENT GETNAME -> "$-1" (no name set)        (需求 19.2) — deterministic.
//   - CONFIG GET maxmemory -> ["maxmemory", "0"]   (需求 19.3) — deterministic.
//
// These are the only entries in StubDiffSequences(). The remaining two stubs are
// DELIBERATELY EXCLUDED from the byte-for-byte live sequences:
//
//   - TIME (需求 19.5): the reply carries the wall clock ([unix_seconds,
//     microseconds]), which is NON-DETERMINISTIC and would diverge from the
//     oracle by whatever time elapses between the two DoCmd calls. Byte-for-byte
//     comparison is therefore meaningless. TIME is instead covered by an
//     always-run in-process SHAPE assertion (a 2-element array of numeric bulk
//     strings) in TestStubTimeShape.
//
//   - DBSIZE (需求 19.4): redimos replies the documented APPROXIMATION ":0"
//     (an exact count would require a full meta scan per call — see
//     handleDBSize), whereas a live Pika reports its real key count. The two
//     legitimately differ, so DBSIZE is excluded from byte-for-byte and covered
//     by an always-run in-process assertion that the reply is a well-formed
//     integer (TestStubDBSizeInteger).
//
// COMMAND (bare) is likewise excluded: redimos replies the empty array "*0"
// while a live Pika enumerates its full command table, so the two differ by
// design; only COMMAND COUNT is oracle-comparable.

// StubDiffSequences returns the live byte-for-byte differential sequences for
// the client-probe stubs whose replies are deterministic AND expected to match
// Pika v3.2.2. TIME and DBSIZE are intentionally excluded (see the file
// comment) and covered by in-process assertions instead.
//
// Each sequence begins with a cleanup DEL so runs are independent even against a
// persistent oracle (the probe commands themselves create no keys, but the
// leading DEL keeps the "sequences start with DEL" invariant that the well-formed
// guard enforces).
func StubDiffSequences() []Sequence {
	return []Sequence{
		commandProbeSequence(),
		clientProbeSequence(),
		configProbeSequence(),
	}
}

// commandProbeSequence probes COMMAND COUNT, whose ":0" integer reply is
// deterministic and matches Pika v3.2.2 (需求 19.1). Bare COMMAND is excluded
// because redimos returns "*0" while a live Pika enumerates its command table.
func commandProbeSequence() Sequence {
	return Sequence{
		Name: "stub-command-count",
		Commands: []Command{
			Cmd("DEL", "stub:command"),
			Cmd("COMMAND", "COUNT"),
		},
	}
}

// clientProbeSequence probes CLIENT SETNAME (-> "+OK") and CLIENT GETNAME
// (-> "$-1" when no name is set). Both replies are deterministic and match Pika
// v3.2.2 (需求 19.2). GETNAME is issued on a fresh connection where no name was
// assigned, so the null-bulk reply is stable. (SETNAME's discarded name does not
// affect GETNAME because redimos keeps no per-connection name — see handleClient.)
func clientProbeSequence() Sequence {
	return Sequence{
		Name: "stub-client-name",
		Commands: []Command{
			Cmd("DEL", "stub:client"),
			Cmd("CLIENT", "GETNAME"),
			Cmd("CLIENT", "SETNAME", "myconn"),
			Cmd("CLIENT", "GETNAME"),
		},
	}
}

// configProbeSequence probes CONFIG GET maxmemory, whose 2-element array reply
// ["maxmemory", "0"] is deterministic and matches Pika v3.2.2's default (需求
// 19.3). Only maxmemory is asserted against the oracle: it is the parameter
// clients most commonly read at startup and the one with a stable, matching
// default. Other CONFIG parameters are covered by the in-process stub unit tests
// (internal/command/stub_test.go) rather than the live differential, since their
// values may legitimately differ from a specific Pika build's configuration.
func configProbeSequence() Sequence {
	return Sequence{
		Name: "stub-config-get-maxmemory",
		Commands: []Command{
			Cmd("DEL", "stub:config"),
			Cmd("CONFIG", "GET", "maxmemory"),
		},
	}
}

// StubDiffSequenceNames returns the sequence names, for logging / subtest naming.
func StubDiffSequenceNames() []string {
	seqs := StubDiffSequences()
	names := make([]string, len(seqs))
	for i, s := range seqs {
		names[i] = s.Name
	}
	return names
}

// describeStubDiff is a small helper for diagnostics, mirroring describeMatrix.
func describeStubDiff() string {
	return fmt.Sprintf("stub difftest (deterministic, byte-for-byte): %d sequences %v; "+
		"TIME (nondeterministic) and DBSIZE (approximation) covered in-process only",
		len(StubDiffSequences()), StubDiffSequenceNames())
}
