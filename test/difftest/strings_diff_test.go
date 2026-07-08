package difftest

import (
	"testing"
	"time"
)

// strings_diff_test.go is the task 9.5 String-command differential test.
//
// It provides the env-guarded live differential entry point that replays the
// StringDiffSequences against a real Pika v3.2.2 oracle and redimos, comparing
// raw RESP replies byte-for-byte. It skips cleanly when PIKA_ADDR / REDIMOS_ADDR
// are unset, so `go test ./...` passes without any live infrastructure.
//
// The sequences cover the full String command surface and its boundaries:
// null values ($-1 for GET miss, SET NX/XX rejection), the EX/PX/NX/XX option
// combinations, integer boundaries (INCR overflow at int64 max, DECR into
// negatives), INCRBYFLOAT formatting, APPEND/STRLEN/SETRANGE/GETRANGE ranges,
// and error text (Property 6: WRONGTYPE, not-an-integer, syntax error, invalid
// expire time).
//
// Property 6: error-text consistency.
// Validates: Requirements 5.1–5.11.
func TestDiffStrings(t *testing.T) {
	ep := endpointsFromEnv(t)
	if ep.Timeout == 0 {
		ep.Timeout = 5 * time.Second
	}
	t.Logf("string differential: %d sequences %v",
		len(StringDiffSequences()), StringDiffSequenceNames())

	for _, seq := range StringDiffSequences() {
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

// TestStringDiffSequencesWellFormed is an always-run, no-infrastructure guard on
// the String differential sequences themselves: every sequence must be named,
// bracket its work with a leading DEL cleanup, and contain only non-empty
// commands with non-nil arguments. This keeps the sequences trustworthy
// independent of any live endpoint (mirrors the generator check in
// difftest_test.go). Validates: Requirements 5.1–5.11.
func TestStringDiffSequencesWellFormed(t *testing.T) {
	seqs := StringDiffSequences()
	if len(seqs) == 0 {
		t.Fatal("expected at least one String differential sequence")
	}

	names := StringDiffSequenceNames()
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
