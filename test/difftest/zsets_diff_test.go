package difftest

import (
	"testing"
	"time"
)

// zsets_diff_test.go is the Sorted Set score-precision differential test half of
// task 15.3 (需求 9.6).
//
// TestDiffZSets replays ZSetDiffSequences against a real Pika v3.2.2 oracle and
// redimos, comparing raw RESP replies byte-for-byte. It skips cleanly when
// PIKA_ADDR / REDIMOS_ADDR are unset, so `go test ./...` passes with no live
// infrastructure. The sequences drive extreme / high-precision / infinite /
// signed-zero scores through ZADD/ZSCORE/ZINCRBY (plus ZRANGE WITHSCORES and the
// deterministic ZCARD/ZRANK/ZCOUNT replies) to pin that redimos reproduces
// Pika's IEEE754-double score semantics rather than the DynamoDB 38-digit
// decimal's wider precision.
//
// Validates: 需求 9.6 (score precision differential); exercises 需求 9.1/9.2/9.7
// reply shapes.
func TestDiffZSets(t *testing.T) {
	ep := endpointsFromEnv(t)
	if ep.Timeout == 0 {
		ep.Timeout = 5 * time.Second
	}
	t.Logf("%s", describeZSetDiff())

	for _, seq := range ZSetDiffSequences() {
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

// TestZSetDiffSequencesWellFormed is an always-run, no-infrastructure guard on
// the Sorted Set differential sequences themselves: every sequence must be
// named, unique, bracket its work with a leading DEL cleanup, and contain only
// non-empty commands with non-nil arguments. This keeps the sequences
// trustworthy independent of any live endpoint (mirrors the guards in
// strings_diff_test.go / difftest_test.go). Validates: 需求 9.6.
func TestZSetDiffSequencesWellFormed(t *testing.T) {
	seqs := ZSetDiffSequences()
	if len(seqs) == 0 {
		t.Fatal("expected at least one Sorted Set differential sequence")
	}

	names := ZSetDiffSequenceNames()
	if len(names) != len(seqs) {
		t.Fatalf("name count %d != sequence count %d", len(names), len(seqs))
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
