package difftest

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/aura-studio/redimos/v2/internal/command"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// unsupported_stub_diff_test.go is the task 18.3 entry point for Property 6
// (错误文案一致) applied to explicit rejection (需求 4.1–4.8) and client-probe
// stubs (需求 19.1–19.5). It has two halves:
//
//  1. Env-guarded LIVE differential tests (TestDiffUnsupported / TestDiffStubs)
//     that compare raw RESP replies against a live Pika v3.2.2 oracle
//     byte-for-byte. They skip cleanly when PIKA_ADDR / REDIMOS_ADDR are unset,
//     so `go test ./...` passes without any infrastructure. They cover ONLY the
//     cases where a byte-for-byte match with the oracle is actually achievable
//     (see unsupported_diff.go / stub_diff.go for the parity split rationale).
//
//  2. Always-run IN-PROCESS assertions that boot a real redimos server
//     (command.NewRouter + server.New, the same seam clientmatrix uses) and
//     check, over the raw RESP wire, the cases that CANNOT be byte-compared
//     against the oracle:
//       - Pika-implemented unsupported families still reply unknown-command
//         (the local "Property 6 for rejection" check: reject, never silently
//         downgrade).
//       - TIME has the correct nondeterministic-value shape.
//       - DBSIZE is a well-formed integer.
//     Plus well-formed guards on the differential sequences so the harness stays
//     trustworthy independent of any live endpoint.

// --- Live differential entry points (env-guarded) ---------------------------

// TestDiffUnsupported runs the Pika-lacks unsupported-command sequences (Lua,
// Streams, blocking pops) against both endpoints and fails on any byte-level
// divergence. redimos and Pika v3.2.2 both reply "-ERR unknown command '<name>'"
// for these families, so the replies must match verbatim (需求 4.2, 4.4, 4.6,
// 4.8; Property 6).
func TestDiffUnsupported(t *testing.T) {
	ep := endpointsFromEnv(t)
	t.Logf("%s", describeUnsupportedDiff())

	for _, seq := range UnsupportedDiffSequences() {
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

// TestDiffStubs runs the deterministic client-probe stub sequences (COMMAND
// COUNT, CLIENT SETNAME/GETNAME, CONFIG GET maxmemory) against both endpoints and
// fails on any byte-level divergence (需求 19.1–19.3; Property 6). TIME and
// DBSIZE are excluded here and covered by the in-process assertions below.
func TestDiffStubs(t *testing.T) {
	ep := endpointsFromEnv(t)
	t.Logf("%s", describeStubDiff())

	for _, seq := range StubDiffSequences() {
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

// --- In-process server helper -----------------------------------------------

// startInProcRedimos boots an in-process redimos server on an ephemeral port
// using a connection-level command.Router (NewRouter registers the handshake
// commands AND the client-probe stubs via registerStubs). Unsupported commands
// are, by design, unregistered and therefore fall through to the router's
// unknown-command reply. It returns the "host:port" address and is torn down via
// t.Cleanup. This mirrors clientmatrix/server_test.go's startServer.
func startInProcRedimos(t *testing.T) string {
	t.Helper()

	r := command.NewRouter(command.Config{})
	s := server.New(server.Options{Addr: "127.0.0.1:0"}, r)

	signal := make(chan error, 1)
	go func() { _ = s.ListenServeAndSignal(signal) }()
	if err := <-signal; err != nil {
		t.Fatalf("failed to start in-process redimos server: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	return s.Addr().String()
}

// dialInProc opens a raw RESP client against the in-process server.
func dialInProc(t *testing.T, addr string) *Client {
	t.Helper()
	c, err := Dial(addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial in-process redimos %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// --- In-process: Pika-implemented families reply unknown-command ------------

// TestUnsupportedPikaImplementedRejected is the always-run "Property 6 for
// rejection" check for the families Pika v3.2.2 IMPLEMENTS (Pub/Sub,
// transactions, bit ops, PF*, GEO*, FLUSHALL/FLUSHDB). These CANNOT be
// byte-compared against the live oracle (Pika would execute them), so instead we
// assert directly against redimos that each replies the exact unknown-command
// error — proving redimos rejects rather than silently downgrading (需求
// 4.1, 4.3, 4.5, 4.6, 4.7).
func TestUnsupportedPikaImplementedRejected(t *testing.T) {
	addr := startInProcRedimos(t)
	c := dialInProc(t, addr)

	cmds := PikaImplementsUnsupportedCommands()
	if len(cmds) == 0 {
		t.Fatal("expected at least one Pika-implemented unsupported command")
	}
	for _, cmd := range cmds {
		name := string(cmd.Args[0])
		want := resp.AppendError(nil, resp.ErrUnknownCommand(name))
		got, err := c.DoCmd(cmd)
		if err != nil {
			t.Fatalf("%s: transport error: %v", cmd, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s = %q, want %q (must reject, never silently downgrade)",
				cmd, string(got), string(want))
		}
	}
}

// TestUnsupportedPikaLacksRejectedInProcess also asserts, always-run, that the
// Pika-lacks families (Lua, Streams, blocking) reply the exact unknown-command
// error from redimos. The live TestDiffUnsupported proves these ALSO match the
// oracle byte-for-byte; this in-process half pins redimos' own reply so the
// invariant holds even without infrastructure.
func TestUnsupportedPikaLacksRejectedInProcess(t *testing.T) {
	addr := startInProcRedimos(t)
	c := dialInProc(t, addr)

	cmds := PikaLacksUnsupportedCommands()
	if len(cmds) == 0 {
		t.Fatal("expected at least one Pika-lacks unsupported command")
	}
	for _, cmd := range cmds {
		name := string(cmd.Args[0])
		want := resp.AppendError(nil, resp.ErrUnknownCommand(name))
		got, err := c.DoCmd(cmd)
		if err != nil {
			t.Fatalf("%s: transport error: %v", cmd, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s = %q, want %q", cmd, string(got), string(want))
		}
	}
}

// --- In-process: TIME shape (nondeterministic value) ------------------------

// TestStubTimeShape asserts TIME's wire SHAPE without byte-comparing its
// nondeterministic value: a 2-element RESP2 array of bulk strings, each a
// non-negative decimal integer (需求 19.5). This is the in-process substitute
// for a byte-for-byte oracle comparison, which is impossible because the reply
// carries the wall clock.
func TestStubTimeShape(t *testing.T) {
	addr := startInProcRedimos(t)
	c := dialInProc(t, addr)

	raw, err := c.DoCmd(Cmd("TIME"))
	if err != nil {
		t.Fatalf("TIME: transport error: %v", err)
	}
	elems, err := parseBulkArray(raw)
	if err != nil {
		t.Fatalf("TIME reply %q not a bulk array: %v", string(raw), err)
	}
	if len(elems) != 2 {
		t.Fatalf("TIME = %q, want a 2-element array, got %d elements", string(raw), len(elems))
	}
	for i, e := range elems {
		n, perr := strconv.ParseInt(string(e), 10, 64)
		if perr != nil {
			t.Errorf("TIME element %d = %q, want a decimal integer: %v", i, string(e), perr)
			continue
		}
		if n < 0 {
			t.Errorf("TIME element %d = %d, want non-negative", i, n)
		}
	}
}

// --- In-process: DBSIZE integer ---------------------------------------------

// TestStubDBSizeInteger asserts DBSIZE replies a well-formed RESP2 integer
// (需求 19.4). DBSIZE is excluded from the live byte-for-byte sequences because
// redimos returns the documented approximation while a live Pika reports its real
// key count; here we only pin that redimos' reply is a valid integer frame.
func TestStubDBSizeInteger(t *testing.T) {
	addr := startInProcRedimos(t)
	c := dialInProc(t, addr)

	raw, err := c.DoCmd(Cmd("DBSIZE"))
	if err != nil {
		t.Fatalf("DBSIZE: transport error: %v", err)
	}
	if len(raw) < 4 || raw[0] != ':' || !bytes.HasSuffix(raw, []byte("\r\n")) {
		t.Fatalf("DBSIZE = %q, want a RESP2 integer frame :<n>\\r\\n", string(raw))
	}
	body := raw[1 : len(raw)-2]
	if _, perr := strconv.ParseInt(string(body), 10, 64); perr != nil {
		t.Errorf("DBSIZE integer payload %q not numeric: %v", string(body), perr)
	}
}

// --- In-process: sanity of the deterministic stub replies -------------------

// TestStubDeterministicRepliesInProcess pins the exact wire bytes redimos emits
// for the stubs that DO go into the live byte-for-byte sequences, so the
// StubDiffSequences() expectations are anchored to redimos' actual behaviour
// independent of any oracle (需求 19.1–19.3).
func TestStubDeterministicRepliesInProcess(t *testing.T) {
	addr := startInProcRedimos(t)
	c := dialInProc(t, addr)

	cases := []struct {
		cmd  Command
		want []byte
	}{
		{Cmd("COMMAND", "COUNT"), resp.AppendInt(nil, 0)},                                                          // :0
		{Cmd("CLIENT", "SETNAME", "myconn"), resp.AppendSimpleString(nil, "OK")},                                   // +OK
		{Cmd("CLIENT", "GETNAME"), resp.AppendNullBulk(nil)},                                                       // $-1
		{Cmd("CONFIG", "GET", "maxmemory"), resp.AppendBulkArray(nil, [][]byte{[]byte("maxmemory"), []byte("0")})}, // ["maxmemory","0"]
	}
	for _, tc := range cases {
		got, err := c.DoCmd(tc.cmd)
		if err != nil {
			t.Fatalf("%s: transport error: %v", tc.cmd, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Errorf("%s = %q, want %q", tc.cmd, string(got), string(tc.want))
		}
	}
}

// --- Always-run well-formed guards on the sequences -------------------------

// TestUnsupportedStubDiffSequencesWellFormed keeps both new sequence families
// trustworthy independent of any live endpoint: each sequence is named, unique,
// starts with a cleanup DEL (so runs are independent against a persistent
// oracle), and contains only well-formed (non-empty, non-nil-arg) commands. It
// also checks the *Names() helpers agree with the *Sequences() functions.
func TestUnsupportedStubDiffSequencesWellFormed(t *testing.T) {
	groups := map[string]struct {
		seqs  []Sequence
		names []string
	}{
		"unsupported": {UnsupportedDiffSequences(), UnsupportedDiffSequenceNames()},
		"stub":        {StubDiffSequences(), StubDiffSequenceNames()},
	}

	for group, g := range groups {
		if len(g.seqs) == 0 {
			t.Errorf("%s: expected at least one diff sequence", group)
		}
		if len(g.names) != len(g.seqs) {
			t.Errorf("%s: names(%d) disagree with sequences(%d)", group, len(g.names), len(g.seqs))
		}
		seen := make(map[string]bool)
		for _, seq := range g.seqs {
			if seq.Name == "" {
				t.Errorf("%s: sequence has empty name", group)
			}
			if seen[seq.Name] {
				t.Errorf("%s: duplicate sequence name %q", group, seq.Name)
			}
			seen[seq.Name] = true

			if len(seq.Commands) == 0 {
				t.Fatalf("%s: sequence %q has no commands", group, seq.Name)
			}
			first := seq.Commands[0]
			if len(first.Args) == 0 || string(first.Args[0]) != "DEL" {
				t.Errorf("%s: sequence %q must start with a cleanup DEL, got %v", group, seq.Name, first)
			}
			for j, cmd := range seq.Commands {
				if len(cmd.Args) == 0 {
					t.Fatalf("%s: sequence %q command %d has no name", group, seq.Name, j)
				}
				for a, arg := range cmd.Args {
					if arg == nil {
						t.Fatalf("%s: sequence %q command %d arg %d is nil", group, seq.Name, j, a)
					}
				}
			}
		}
	}
}

// TestUnsupportedParitySplitDisjointAndComplete guards the load-bearing parity
// split: the Pika-lacks and Pika-implemented command groups must be disjoint and
// together cover exactly command.UnsupportedCommands, so neither list can drift
// from the single source of truth without failing here.
func TestUnsupportedParitySplitDisjointAndComplete(t *testing.T) {
	lacks := PikaLacksUnsupportedCommands()
	impl := PikaImplementsUnsupportedCommands()

	if got, want := len(lacks)+len(impl), len(command.UnsupportedCommands); got != want {
		t.Fatalf("parity split covers %d commands, want %d (command.UnsupportedCommands)", got, want)
	}

	inLacks := make(map[string]bool)
	for _, c := range lacks {
		inLacks[string(c.Args[0])] = true
	}
	for _, c := range impl {
		name := string(c.Args[0])
		if inLacks[name] {
			t.Errorf("%q appears in BOTH parity groups; they must be disjoint", name)
		}
	}
	// Every UnsupportedCommands entry must be in exactly one group.
	covered := make(map[string]bool)
	for _, c := range append(append([]Command{}, lacks...), impl...) {
		covered[string(c.Args[0])] = true
	}
	for _, name := range command.UnsupportedCommands {
		if !covered[name] {
			t.Errorf("unsupported command %q is in neither parity group", name)
		}
	}
}

// parseBulkArray parses a raw RESP2 reply expected to be an array of bulk
// strings and returns the element payloads. It is a minimal parser used only by
// the TIME shape assertion; ReadReply already validated the frame is well-formed
// on the wire, so this only needs to walk the known-good bytes.
func parseBulkArray(raw []byte) ([][]byte, error) {
	if len(raw) == 0 || raw[0] != '*' {
		return nil, errUnexpected("array", raw)
	}
	line, rest, ok := splitCRLF(raw[1:])
	if !ok {
		return nil, errUnexpected("array header", raw)
	}
	n, err := strconv.Atoi(string(line))
	if err != nil {
		return nil, err
	}
	elems := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		if len(rest) == 0 || rest[0] != '$' {
			return nil, errUnexpected("bulk string", rest)
		}
		hdr, after, ok := splitCRLF(rest[1:])
		if !ok {
			return nil, errUnexpected("bulk header", rest)
		}
		blen, err := strconv.Atoi(string(hdr))
		if err != nil {
			return nil, err
		}
		if blen < 0 || len(after) < blen+2 {
			return nil, errUnexpected("bulk body", after)
		}
		elems = append(elems, after[:blen])
		rest = after[blen+2:] // skip body + CRLF
	}
	return elems, nil
}

// splitCRLF splits b at the first CRLF, returning the line (without CRLF), the
// remainder (after CRLF), and whether a CRLF was found.
func splitCRLF(b []byte) (line, rest []byte, ok bool) {
	idx := bytes.Index(b, []byte("\r\n"))
	if idx < 0 {
		return nil, nil, false
	}
	return b[:idx], b[idx+2:], true
}

func errUnexpected(what string, b []byte) error {
	return &parseError{what: what, got: string(b)}
}

type parseError struct {
	what string
	got  string
}

func (e *parseError) Error() string {
	return "difftest: expected " + e.what + ", got " + strconv.Quote(e.got)
}
