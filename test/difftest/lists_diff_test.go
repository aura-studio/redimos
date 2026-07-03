package difftest

import "testing"

// TestDiffLists is the live differential entry point for the List family and the
// P0 gate for List (task 16.3). It runs every ListDiffSequences() sequence
// against both the Pika v3.2.2 oracle and redimos on fresh connections and fails
// on any byte-level divergence, validating the full List command surface, its
// boundaries and its error text (Property 6) for LPUSH/RPUSH/LPUSHX/RPUSHX/
// LPOP/RPOP/LRANGE/LINDEX/LLEN/LSET/LTRIM/LREM/LINSERT/RPOPLPUSH (需求 7.1, 7.2,
// 7.3, 7.4, 7.5, 7.7). Because List replies are deterministic and ordered, the
// comparison is byte-for-byte.
//
// Like TestDiffMatrix / TestDiffKeys, it is guarded on PIKA_ADDR / REDIMOS_ADDR
// via endpointsFromEnv and skips cleanly when either is unset, so `go test ./...`
// passes without any live infrastructure. Point it at real endpoints with:
//
//	PIKA_ADDR=localhost:6379 REDIMOS_ADDR=localhost:6380 go test ./test/difftest -run DiffLists
//
// **Property 3: 计数一致性**
// Validates: 需求 7.2, 7.7 (plus the full List surface 需求 7.1, 7.3, 7.4, 7.5).
func TestDiffLists(t *testing.T) {
	ep := endpointsFromEnv(t)
	t.Logf("%s", describeListDiff())

	for _, seq := range ListDiffSequences() {
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

// TestListDiffSequencesWellFormed is a pure, always-run guard that keeps the List
// differential sequences trustworthy independent of any live endpoint. It asserts
// each sequence is named, starts with a cleanup DEL so runs are independent
// against a persistent oracle, and contains only well-formed (non-empty,
// non-nil-arg) commands. ListDiffSequenceNames must agree with ListDiffSequences.
func TestListDiffSequencesWellFormed(t *testing.T) {
	seqs := ListDiffSequences()
	if len(seqs) == 0 {
		t.Fatal("expected at least one list diff sequence")
	}

	seen := make(map[string]bool, len(seqs))
	for _, seq := range seqs {
		if seq.Name == "" {
			t.Error("sequence has empty name")
		}
		if seen[seq.Name] {
			t.Errorf("duplicate sequence name %q", seq.Name)
		}
		seen[seq.Name] = true

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

	names := ListDiffSequenceNames()
	if len(names) != len(seqs) {
		t.Fatalf("ListDiffSequenceNames returned %d names for %d sequences", len(names), len(seqs))
	}
	for i, s := range seqs {
		if names[i] != s.Name {
			t.Errorf("name[%d] = %q, want %q", i, names[i], s.Name)
		}
	}
}
