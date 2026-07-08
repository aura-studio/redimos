package difftest

import "testing"

// TestDiffHashes is the live differential entry point for the Hash family (task
// 13.3). It runs every HashDiffSequences() sequence against both the Pika v3.2.2
// oracle and redimos on fresh connections and fails on any byte-level divergence,
// validating the return values, boundaries, count consistency (Property 3) and
// error text (Property 6) for the full Hash command surface — HSET / HGET /
// HMSET / HMGET / HGETALL / HDEL / HEXISTS / HKEYS / HVALS / HSETNX / HINCRBY /
// HINCRBYFLOAT / HSTRLEN / HLEN / HSCAN (需求 6.1, 6.2, 6.4).
//
// Like TestDiffMatrix / TestDiffKeys it is guarded on PIKA_ADDR / REDIMOS_ADDR
// via endpointsFromEnv and skips cleanly when either is unset, so `go test ./...`
// passes without any live infrastructure. Point it at real endpoints with:
//
//	PIKA_ADDR=localhost:6379 REDIMOS_ADDR=localhost:6380 go test ./test/difftest -run DiffHashes
func TestDiffHashes(t *testing.T) {
	ep := endpointsFromEnv(t)
	t.Logf("%s", describeHashDiff())

	for _, seq := range HashDiffSequences() {
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

// TestHashDiffSequencesWellFormed is a pure, always-run guard that keeps the Hash
// sequences trustworthy independent of any live endpoint. It asserts each
// sequence is named uniquely, starts with a cleanup DEL so runs are independent
// against a persistent oracle, and contains only well-formed (non-empty,
// non-nil-arg) commands. It also checks HashDiffSequenceNames agrees with
// HashDiffSequences.
func TestHashDiffSequencesWellFormed(t *testing.T) {
	seqs := HashDiffSequences()
	if len(seqs) == 0 {
		t.Fatal("expected at least one hash diff sequence")
	}

	seen := make(map[string]bool)
	for _, seq := range seqs {
		if seq.Name == "" {
			t.Error("sequence has empty name")
		}
		if seen[seq.Name] {
			t.Errorf("duplicate sequence name %q", seq.Name)
		}
		seen[seq.Name] = true

		if len(seq.Commands) == 0 {
			t.Fatalf("sequence %q has no commands", seq.Name)
		}
		first := seq.Commands[0]
		if len(first.Args) == 0 || string(first.Args[0]) != "DEL" {
			t.Errorf("sequence %q must start with a cleanup DEL, got %v", seq.Name, first)
		}
		for j, cmd := range seq.Commands {
			if len(cmd.Args) == 0 {
				t.Fatalf("sequence %q command %d has no name", seq.Name, j)
			}
			for a, arg := range cmd.Args {
				if arg == nil {
					t.Fatalf("sequence %q command %d arg %d is nil", seq.Name, j, a)
				}
			}
		}
	}

	names := HashDiffSequenceNames()
	if len(names) != len(seqs) {
		t.Fatalf("HashDiffSequenceNames returned %d names for %d sequences", len(names), len(seqs))
	}
}
