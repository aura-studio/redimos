package command

import "testing"

// Wire tests for the HLL family (PFADD/PFCOUNT/PFMERGE), which had no dedicated coverage.
// HLL is string-backed, so it runs against the shared fake string store harness. Counts of
// a handful of distinct elements are exact under the sparse HLL representation, so the
// assertions are deterministic.

func TestPFAddPFCount(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	send(t, conn, "PFCOUNT absent")
	if got := readReply(t, r); got != ":0" {
		t.Fatalf("PFCOUNT absent = %q, want :0", got)
	}

	// First add of new elements reports a change (:1).
	send(t, conn, "PFADD hll a b c")
	if got := readReply(t, r); got != ":1" {
		t.Fatalf("PFADD new = %q, want :1", got)
	}
	send(t, conn, "PFCOUNT hll")
	if got := readReply(t, r); got != ":3" {
		t.Fatalf("PFCOUNT = %q, want :3", got)
	}

	// Re-adding existing elements changes nothing (:0).
	send(t, conn, "PFADD hll a b")
	if got := readReply(t, r); got != ":0" {
		t.Fatalf("PFADD existing = %q, want :0", got)
	}
	send(t, conn, "PFCOUNT hll")
	if got := readReply(t, r); got != ":3" {
		t.Fatalf("PFCOUNT after no-op add = %q, want :3", got)
	}

	// A fourth distinct element bumps the estimate to 4.
	send(t, conn, "PFADD hll d")
	if got := readReply(t, r); got != ":1" {
		t.Fatalf("PFADD 4th = %q, want :1", got)
	}
	send(t, conn, "PFCOUNT hll")
	if got := readReply(t, r); got != ":4" {
		t.Fatalf("PFCOUNT = %q, want :4", got)
	}
}

func TestPFMerge(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	send(t, conn, "PFADD s1 a b c")
	_ = readReply(t, r)
	send(t, conn, "PFADD s2 c d e")
	_ = readReply(t, r)

	// Union of {a,b,c} and {c,d,e} = {a,b,c,d,e} -> 5.
	send(t, conn, "PFMERGE dst s1 s2")
	if got := readReply(t, r); got != "+OK" {
		t.Fatalf("PFMERGE = %q, want +OK", got)
	}
	send(t, conn, "PFCOUNT dst")
	if got := readReply(t, r); got != ":5" {
		t.Fatalf("PFCOUNT merged = %q, want :5", got)
	}
}

func TestPFCommandArity(t *testing.T) {
	conn, r := startStringServer(t, newFakeStringStore(), fixedNow(1000))

	// PFADD requires at least a key.
	send(t, conn, "PFADD")
	if got := readReply(t, r); got == "" || got[0] != '-' {
		t.Fatalf("PFADD no args = %q, want an error", got)
	}
	// PFCOUNT requires at least one key.
	send(t, conn, "PFCOUNT")
	if got := readReply(t, r); got == "" || got[0] != '-' {
		t.Fatalf("PFCOUNT no args = %q, want an error", got)
	}
}
