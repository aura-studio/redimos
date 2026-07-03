package difftest

import (
	"math/rand"
	"os"
	"testing"
	"testing/quick"
	"time"
)

// endpointsFromEnv reads the oracle and proxy addresses from the environment.
// When either is unset the differential tests skip cleanly, so `go test ./...`
// passes without any live infrastructure. Point the harness at real endpoints
// with, for example:
//
//	PIKA_ADDR=localhost:6379 REDIMOS_ADDR=localhost:6380 go test ./test/difftest -run Diff
func endpointsFromEnv(t *testing.T) Endpoints {
	t.Helper()
	oracle := os.Getenv("PIKA_ADDR")
	proxy := os.Getenv("REDIMOS_ADDR")
	if oracle == "" || proxy == "" {
		t.Skip("difftest: set PIKA_ADDR and REDIMOS_ADDR to run differential tests against live endpoints")
	}
	return Endpoints{
		OracleAddr:  oracle,
		RedimosAddr: proxy,
		Timeout:     5 * time.Second,
	}
}

// TestDiffMatrix is the command-matrix driven entry point. It runs each curated
// sequence against both endpoints and fails on any byte-level divergence.
func TestDiffMatrix(t *testing.T) {
	ep := endpointsFromEnv(t)
	t.Logf("%s", describeMatrix())

	for _, seq := range Matrix() {
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

// TestDiffFuzz is the random-sequence fuzz entry point built on testing/quick.
// It generates well-formed random command sequences and asserts the two
// endpoints reply identically byte-for-byte.
func TestDiffFuzz(t *testing.T) {
	ep := endpointsFromEnv(t)

	property := func(rs RandomSequence) bool {
		diffs, err := CompareSequence(ep, rs.Sequence)
		if err != nil {
			t.Logf("fuzz sequence %q could not run: %v", rs.Sequence.Name, err)
			return false
		}
		for _, d := range diffs {
			t.Logf("%s", d)
		}
		return len(diffs) == 0
	}

	cfg := &quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	if err := quick.Check(property, cfg); err != nil {
		t.Fatalf("fuzz differential check failed: %v", err)
	}
}

// --- Pure generator tests (always run, no infrastructure) ------------------

// TestGenerateSequenceWellFormed verifies the fuzz generator emits only
// non-empty, well-formed commands and always begins with a cleanup DEL, so the
// generator itself is trustworthy independent of any live endpoint.
func TestGenerateSequenceWellFormed(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 200; i++ {
		seq := GenerateSequence(r, r.Intn(12)+1)
		if len(seq.Commands) < 2 {
			t.Fatalf("sequence too short: %d commands", len(seq.Commands))
		}
		first := seq.Commands[0]
		if len(first.Args) == 0 || string(first.Args[0]) != "DEL" {
			t.Fatalf("sequence must start with cleanup DEL, got %v", first)
		}
		for j, cmd := range seq.Commands {
			if len(cmd.Args) == 0 {
				t.Fatalf("command %d has no name", j)
			}
			for k, a := range cmd.Args {
				if a == nil {
					t.Fatalf("command %d arg %d is nil", j, k)
				}
			}
		}
	}
}

// TestRandomSequenceGeneratorLength verifies the quick.Generator produces
// sequences bounded by the size parameter.
func TestRandomSequenceGeneratorLength(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for size := 0; size < 40; size++ {
		v := RandomSequence{}.Generate(r, size)
		rs := v.Interface().(RandomSequence)
		// length = size%16 + 1 payload commands, plus one leading cleanup DEL.
		maxCmds := 16 + 1
		if len(rs.Sequence.Commands) < 2 || len(rs.Sequence.Commands) > maxCmds {
			t.Fatalf("size %d: unexpected command count %d", size, len(rs.Sequence.Commands))
		}
	}
}
