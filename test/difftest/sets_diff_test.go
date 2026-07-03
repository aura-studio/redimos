package difftest

import "testing"

// sets_diff_test.go is the task 14.3 Set-command differential test.
//
// TestDiffSets is the live differential entry point: it replays every
// SetDiffSequences() sequence against a real Pika v3.2.2 oracle and redimos on
// fresh connections and fails on any byte-level divergence. It is guarded on
// PIKA_ADDR / REDIMOS_ADDR via endpointsFromEnv and skips cleanly when either is
// unset, so `go test ./...` passes without any live infrastructure. Point it at
// real endpoints with:
//
//	PIKA_ADDR=localhost:6379 REDIMOS_ADDR=localhost:6380 go test ./test/difftest -run DiffSets
//
// The sequences keep the byte-for-byte comparison to deterministic-reply Set
// commands (SADD/SREM counts, SCARD, SISMEMBER, SMOVE :1/:0, *STORE cardinality)
// and exercise the order-unspecified commands (SMEMBERS/SPOP/SRANDMEMBER/SSCAN/
// SUNION/SINTER/SDIFF) only in deterministic shapes (single-member sets, empty
// keys, completed single-page scans, ≤1-element algebra results) or via SCARD /
// SISMEMBER surrogates. The multi-member ordering of those commands is covered by
// the in-process unit tests in internal/command.
//
// Validates: 需求 8.1–8.5; Property 6 (错误文案一致, WRONGTYPE / arity).
func TestDiffSets(t *testing.T) {
	ep := endpointsFromEnv(t)
	t.Logf("%s", describeSetsDiff())

	for _, seq := range SetDiffSequences() {
		seq := seq
		t.Run(seq.Name, func(t *testing.T) {
			diffs, err := CompareSequence(ep, seq)
			if err != nil {
				t.Fatalf("sequence %q could not run: %v", seq.Name, err)
			}
			for _, d := range diffs {
				t.Errorf("%s", d)
			}
		})
	}
}

// TestSetDiffSequencesWellFormed is a pure, always-run guard that keeps the Set
// differential sequences trustworthy independent of any live endpoint. It
// asserts each sequence is named, that SetDiffSequenceNames agrees with
// SetDiffSequences, that each sequence starts with a cleanup DEL so runs are
// independent against a persistent oracle, and that every command is well-formed
// (non-empty, non-nil arguments).
func TestSetDiffSequencesWellFormed(t *testing.T) {
	seqs := SetDiffSequences()
	if len(seqs) == 0 {
		t.Fatal("expected at least one Set differential sequence")
	}

	names := SetDiffSequenceNames()
	if len(names) != len(seqs) {
		t.Fatalf("SetDiffSequenceNames returned %d names for %d sequences", len(names), len(seqs))
	}

	seen := make(map[string]bool, len(seqs))
	for i, seq := range seqs {
		if seq.Name == "" {
			t.Errorf("sequence %d has no name", i)
		}
		if seen[seq.Name] {
			t.Errorf("duplicate sequence name %q", seq.Name)
		}
		seen[seq.Name] = true
		if names[i] != seq.Name {
			t.Errorf("name[%d] = %q, want %q", i, names[i], seq.Name)
		}

		if len(seq.Commands) < 2 {
			t.Errorf("sequence %q too short: %d commands", seq.Name, len(seq.Commands))
			continue
		}
		first := seq.Commands[0]
		if len(first.Args) == 0 || string(first.Args[0]) != "DEL" {
			t.Errorf("sequence %q must start with a cleanup DEL, got %v", seq.Name, first)
		}
		for j, cmd := range seq.Commands {
			if len(cmd.Args) == 0 {
				t.Errorf("sequence %q command %d has no name", seq.Name, j)
				continue
			}
			for a, arg := range cmd.Args {
				if arg == nil {
					t.Errorf("sequence %q command %d arg %d is nil", seq.Name, j, a)
				}
			}
		}
	}
}
