package command

import (
	"strings"
	"testing"
)

// serverstub_test.go covers the server persistence/replication no-op stubs added in
// v1.15.0 (SAVE/BGSAVE/BGREWRITEAOF/LASTSAVE/ROLE/WAIT/PFSELFTEST) and the PFDEBUG
// implementation: each must return its fixed benign reply / register dump, never the
// unknown-command error.

func TestServerStubsFixedReplies(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1700000000))

	cases := []struct {
		line string
		want string
	}{
		{"SAVE", "+OK"},
		{"BGSAVE", "+Background saving started"},
		{"BGSAVE SCHEDULE", "+Background saving started"},
		{"BGREWRITEAOF", "+Background append only file rewriting started"},
		{"WAIT 0 100", ":0"},
		{"WAIT 3 1000", ":0"},
		{"PFSELFTEST", "+OK"},
		{"LASTSAVE", ":1700000000"}, // fixedNow seconds
	}
	for _, tc := range cases {
		if got := sendRead(t, conn, r, tc.line); got != tc.want {
			t.Errorf("%q = %q, want %q", tc.line, got, tc.want)
		}
	}
}

// TestRoleStub asserts ROLE returns the standalone master form ["master", 0, []].
func TestRoleStub(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	got := sendReadValue(t, r, conn, "ROLE")
	if want := "[$master :0 []]"; got != want {
		t.Errorf("ROLE = %q, want %q", got, want)
	}
}

// TestPFDebugGetreg asserts PFDEBUG GETREG on a freshly-built HLL returns an array
// of exactly HLL_REGISTERS (16384) integer registers.
func TestPFDebugGetreg(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	if got := sendRead(t, conn, r, "PFADD hll a b c d e"); got != ":1" {
		t.Fatalf("PFADD = %q, want :1", got)
	}
	got := sendReadValue(t, r, conn, "PFDEBUG GETREG hll")
	if !strings.HasPrefix(got, "[") || !strings.HasSuffix(got, "]") {
		t.Fatalf("PFDEBUG GETREG not an array: %.40q", got)
	}
	// Each of the 16384 elements is a space-separated ":N" token.
	if c := strings.Count(got, " ") + 1; c != hllRegisters {
		t.Errorf("PFDEBUG GETREG element count = %d, want %d", c, hllRegisters)
	}
}

// TestPFDebugEncodingAndErrors covers ENCODING (always "dense"), TODENSE (:0),
// DECODE (dense error), the missing-key error, and an unknown subcommand.
func TestPFDebugEncodingAndErrors(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))
	sendRead(t, conn, r, "PFADD hll x")

	cases := []struct{ line, want string }{
		{"PFDEBUG ENCODING hll", "+dense"},
		{"PFDEBUG TODENSE hll", ":0"},
		{"PFDEBUG DECODE hll", "-ERR HLL encoding is not sparse"},
		{"PFDEBUG GETREG missing", "-ERR The specified key does not exist"},
		// Redis 3.2 splits the two failures: an UNKNOWN subcommand echoes the name,
		// a KNOWN subcommand with the wrong arity gets a distinct arity message.
		{"PFDEBUG BOGUS hll", "-ERR Unknown PFDEBUG subcommand 'BOGUS'"},
		{"PFDEBUG GETREG hll extra", "-ERR Wrong number of arguments for the 'GETREG' subcommand"},
	}
	for _, tc := range cases {
		if got := sendRead(t, conn, r, tc.line); got != tc.want {
			t.Errorf("%q = %q, want %q", tc.line, got, tc.want)
		}
	}
}
