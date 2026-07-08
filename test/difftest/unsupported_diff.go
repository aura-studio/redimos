package difftest

import (
	"fmt"

	"github.com/aura-studio/redimos/internal/command"
)

// unsupported_diff.go adds the "explicit rejection" half of task 18.3
// (Property 6: 错误文案一致; Validates 需求 4.1–4.8). It drives the same
// byte-level assertion engine as Matrix() (harness.go) but lives in its own file
// and is exposed via UnsupportedDiffSequences() / UnsupportedDiffSequenceNames()
// so it can be wired into a dedicated live entry point (TestDiffUnsupported in
// unsupported_stub_diff_test.go) WITHOUT touching the shared Matrix() /
// difftest_test.go — keeping the file conflict-free with concurrent work on
// matrix.go.
//
// # The parity split (the load-bearing decision of this task)
//
// task 18.1 left every unsupported command family UNREGISTERED, so the router's
// default path replies "-ERR unknown command '<name>'" (name echoed verbatim)
// for all of them. Whether that reply is byte-for-byte identical to the live
// Pika v3.2.2 oracle depends entirely on whether Pika itself implements the
// command:
//
//   - Families Pika v3.2.2 GENUINELY LACKS — Lua (EVAL/EVALSHA/SCRIPT),
//     Streams (X*), and blocking pops (BLPOP/BRPOP/BRPOPLPUSH) — Pika ALSO
//     replies "-ERR unknown command '<name>'". redimos reproduces that reply
//     verbatim, so a byte-for-byte differential against the oracle PASSES.
//     ==> These families, and ONLY these, go into the live differential
//         sequences returned by UnsupportedDiffSequences().
//
//   - Families Pika v3.2.2 IMPLEMENTS — Pub/Sub, transactions, bit operations,
//     PF* (HyperLogLog), GEO*, and FLUSHALL/FLUSHDB — Pika would EXECUTE the
//     command (e.g. FLUSHALL -> "+OK", GETBIT -> ":0"), while redimos returns
//     the unknown-command error by design (requirements 4.1–4.7 mandate an
//     explicit error, never a silent downgrade). A byte-for-byte differential
//     against the oracle would therefore SPURIOUSLY FAIL — the mismatch is
//     intended, not a bug.
//     ==> These families are DELIBERATELY EXCLUDED from the live differential
//         sequences. They are instead covered by an always-run in-process
//         assertion (TestUnsupportedPikaImplementedRejected in
//         unsupported_stub_diff_test.go) that checks redimos replies
//         unknown-command — the local "Property 6 for rejection" check that
//         proves the proxy rejects rather than silently downgrades, without
//         requiring an (unattainable) byte match against the oracle.
//
// This mirrors, and is kept in lockstep with, the design decision documented in
// internal/command/unsupported.go. The two halves share the single source of
// truth command.UnsupportedCommands so the family lists never drift.

// pikaLacksNames is the set of unsupported commands that genuinely do NOT exist in
// the oracle either, so both the oracle and redimos reject them with the identical
// "-ERR unknown command" reply — the ONLY commands eligible for a byte-for-byte
// differential. After the reject families (Pub/Sub, Lua, transactions, blocking
// pops) were converted to first-class proxy rejections (rejected.go), the sole
// remaining unknown-command family is Streams (absent from Redis 3.2 / Pika 3.2.2).
var pikaLacksNames = map[string]bool{
	// Streams (Redis 5.0+; absent from the 3.2 oracle).
	"XADD": true, "XLEN": true, "XRANGE": true, "XREVRANGE": true,
	"XREAD": true, "XDEL": true, "XTRIM": true, "XINFO": true,
}

// unsupportedArgs returns a representative argument line (WITHOUT the command
// name) for an unsupported command, so each command is sent in a realistic
// shape. The reply must be identical regardless of arguments — dispatch fails
// the table lookup before it ever inspects arity — but sending real arguments
// proves the proxy does not quietly accept a well-formed EVAL/SETBIT/PUBLISH.
// A nil result means "send the bare command name".
//
// This mirrors representativeArgs in internal/command/unsupported_test.go; it is
// duplicated here (rather than exported from that _test.go file, which is not
// importable) so the difftest sequences send equally realistic commands.
func unsupportedArgs(name string) []string {
	switch name {
	// Pub/Sub.
	case "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE":
		return []string{"chan"}
	case "PUBLISH":
		return []string{"chan", "msg"}
	case "PUBSUB":
		return []string{"CHANNELS"}
	// Lua.
	case "EVAL":
		return []string{"return 1", "0"}
	case "EVALSHA":
		return []string{"abc", "0"}
	case "SCRIPT":
		return []string{"LOAD", "return 1"}
	// Transactions.
	case "WATCH":
		return []string{"k"}
	// Blocking pops.
	case "BLPOP", "BRPOP":
		return []string{"k", "0"}
	case "BRPOPLPUSH":
		return []string{"src", "dst", "0"}
	// Bit ops.
	case "SETBIT":
		return []string{"k", "7", "1"}
	case "GETBIT":
		return []string{"k", "7"}
	case "BITCOUNT":
		return []string{"k"}
	case "BITOP":
		return []string{"AND", "dest", "k"}
	case "BITPOS":
		return []string{"k", "1"}
	// HyperLogLog.
	case "PFADD":
		return []string{"hll", "a"}
	case "PFCOUNT":
		return []string{"hll"}
	case "PFMERGE":
		return []string{"dst", "src"}
	// GEO.
	case "GEOADD":
		return []string{"geo", "13.36", "38.11", "palermo"}
	case "GEODIST":
		return []string{"geo", "a", "b"}
	case "GEOPOS", "GEOHASH":
		return []string{"geo", "a"}
	case "GEORADIUS":
		return []string{"geo", "15", "37", "200", "km"}
	case "GEORADIUSBYMEMBER":
		return []string{"geo", "a", "200", "km"}
	// Streams.
	case "XADD":
		return []string{"s", "*", "f", "v"}
	case "XLEN":
		return []string{"s"}
	case "XRANGE", "XREVRANGE":
		return []string{"s", "-", "+"}
	case "XREAD":
		return []string{"COUNT", "2", "STREAMS", "s", "0"}
	case "XDEL":
		return []string{"s", "1-1"}
	case "XTRIM":
		return []string{"s", "MAXLEN", "10"}
	case "XINFO":
		return []string{"STREAM", "s"}
	// FLUSHALL / FLUSHDB and anything else take no args.
	default:
		return nil
	}
}

// cmdFor builds a Command for an unsupported command name with its
// representative arguments.
func cmdFor(name string) Command {
	args := append([]string{name}, unsupportedArgs(name)...)
	return Cmd(args...)
}

// PikaLacksUnsupportedCommands returns the unsupported commands (with
// representative arguments) that Pika v3.2.2 genuinely lacks and therefore
// rejects identically to redimos. These are the commands used in the live
// byte-for-byte differential sequences. Order follows command.UnsupportedCommands.
func PikaLacksUnsupportedCommands() []Command {
	var cmds []Command
	for _, name := range command.UnsupportedCommands {
		if pikaLacksNames[name] {
			cmds = append(cmds, cmdFor(name))
		}
	}
	return cmds
}

// PikaImplementsUnsupportedCommands returns the unsupported commands (with
// representative arguments) that Pika v3.2.2 DOES implement — Pub/Sub,
// transactions, bit ops, PF*, GEO*, FLUSHALL/FLUSHDB. redimos still rejects them
// with unknown-command (requirements 4.1–4.7), but a byte-for-byte match with
// the oracle is unattainable, so they are excluded from the live sequences and
// covered by an in-process rejection assertion instead. Order follows
// command.UnsupportedCommands.
func PikaImplementsUnsupportedCommands() []Command {
	var cmds []Command
	for _, name := range command.UnsupportedCommands {
		if !pikaLacksNames[name] {
			cmds = append(cmds, cmdFor(name))
		}
	}
	return cmds
}

// UnsupportedDiffSequences returns the live byte-for-byte differential sequences
// for the explicit-rejection families. It includes ONLY the families Pika
// v3.2.2 genuinely lacks (Lua, Streams, blocking pops), grouped per family for
// readable subtest names. Each sequence begins with a cleanup DEL so runs are
// independent even against a persistent oracle.
//
// The Pika-implemented families are intentionally absent here; see the file
// comment and PikaImplementsUnsupportedCommands.
func UnsupportedDiffSequences() []Sequence {
	return []Sequence{
		// Streams is the only family both the oracle and redimos answer with the
		// identical unknown-command reply, so it is the only byte-comparable one.
		// (Lua and blocking pops are now first-class proxy rejections on redimos —
		// see rejected.go — and would not match an oracle that either executes them
		// (real Redis 3.2) or unknown-commands them (Pika 3.2.2), so they are covered
		// by the in-process TestRejectedFamiliesReturnDedicatedError instead.)
		unsupportedFamilySequence("unsupported-streams",
			"XADD", "XLEN", "XRANGE", "XREVRANGE", "XREAD", "XDEL", "XTRIM", "XINFO"),
	}
}

// unsupportedFamilySequence builds a Sequence that sends each named command with
// representative arguments, prefixed by a cleanup DEL of the scratch keys the
// representative args touch. Every listed name must be a Pika-lacks command so
// the sequence is safe for a byte-for-byte oracle comparison.
func unsupportedFamilySequence(name string, cmds ...string) Sequence {
	seq := Sequence{
		Name: name,
		// Clean up every scratch key the representative args may create on the
		// oracle side, so a persistent Pika stays pristine between runs. (On
		// redimos these commands never execute, so nothing is created there.)
		Commands: []Command{Cmd("DEL", "k", "src", "dst", "s", "hll", "geo", "dest", "chan")},
	}
	for _, c := range cmds {
		seq.Commands = append(seq.Commands, cmdFor(c))
	}
	return seq
}

// UnsupportedDiffSequenceNames returns the sequence names, for logging / subtest
// naming.
func UnsupportedDiffSequenceNames() []string {
	seqs := UnsupportedDiffSequences()
	names := make([]string, len(seqs))
	for i, s := range seqs {
		names[i] = s.Name
	}
	return names
}

// describeUnsupportedDiff is a small helper for diagnostics, mirroring
// describeMatrix.
func describeUnsupportedDiff() string {
	return fmt.Sprintf("unsupported difftest (Pika-lacks, byte-for-byte): %d sequences %v; "+
		"Pika-implemented families covered in-process only",
		len(UnsupportedDiffSequences()), UnsupportedDiffSequenceNames())
}
