package command

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"testing"

	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// property6_test.go is the always-run half of task 5.3 (Property 6: 错误文案一致).
//
// It drives the real command router over an in-process server + TCP connection
// (the same seam router_test.go uses) and asserts the four generic
// routing/parameter-validation error paths emit their exact RESP2 wire bytes,
// byte-for-byte matching the Pika v3.2.2 text:
//
//   - arity mismatch  -> "-ERR wrong number of arguments for '{cmd}' command\r\n" (需求 3.2)
//   - unknown command -> "-ERR unknown command '{name}'\r\n"                       (需求 3.3)
//   - non-integer     -> "-ERR value is not an integer or out of range\r\n"        (需求 3.4)
//   - syntax error    -> "-ERR syntax error\r\n"                                   (需求 3.5)
//
// The assertions are expressed as properties over generated inputs (random
// commands, argument counts, command names, and non-integer values) so the
// invariant "the wire text depends only on the error category, never on the
// specific input" is checked across the input space, not just fixed examples.
// The pure resp-encoder layer is asserted independently so a divergence can be
// localized to either the encoder or the router path.
//
// The env-guarded differential matrix half (comparing these same paths against
// a live Pika oracle) lives in test/difftest/matrix.go.

// property6Table registers commands with a range of arity shapes plus generic
// handlers that exercise the parameter-validation replies.
func property6Table() Table {
	tbl := NewTable()
	// Exact-arity command: handler echoes so a success is distinguishable.
	tbl.Register("GET", 2, false, func(_ context.Context, c *server.Conn, args [][]byte) {
		c.Redcon().WriteBulk(args[1])
	})
	// Negative (minimum) arity commands.
	tbl.Register("MSET", -3, true, func(_ context.Context, c *server.Conn, _ [][]byte) {
		c.Redcon().WriteString("OK")
	})
	tbl.Register("DEL", -2, true, func(_ context.Context, c *server.Conn, _ [][]byte) {
		c.Redcon().WriteInt(1)
	})
	// Exact-arity 1 command (name only).
	tbl.Register("PING", 1, false, func(_ context.Context, c *server.Conn, _ [][]byte) {
		c.Redcon().WriteString("PONG")
	})
	// Integer-argument command: parses args[2], replying not-an-integer on
	// failure (needs 需求 3.4 exactness) and the value on success.
	tbl.Register("INCRBY", 3, true, func(_ context.Context, c *server.Conn, args [][]byte) {
		n, ok := ParseIntReply(c, args[2])
		if !ok {
			return
		}
		c.Redcon().WriteInt64(n)
	})
	// A command that always reports a syntax error (需求 3.5).
	tbl.Register("BADOPT", 1, false, func(_ context.Context, c *server.Conn, _ [][]byte) {
		WriteSyntaxError(c)
	})
	return tbl
}

// sendCmdLine encodes args as a RESP2 array of bulk strings (binary-safe, so
// values may contain spaces or other bytes the inline protocol would split)
// and returns the single-line reply (error / simple string / integer) with the
// trailing CRLF stripped. All four Property 6 paths reply on a single line.
func sendCmdLine(t *testing.T, conn net.Conn, r *bufio.Reader, args ...string) string {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := conn.Write([]byte(b.String())); err != nil {
		t.Fatalf("write %v: %v", args, err)
	}
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read reply for %v: %v", args, err)
	}
	return strings.TrimRight(line, "\r\n")
}

// arityCommand pairs a registered command with its arity so the property can
// derive argument counts that violate it.
type arityCommand struct {
	name  string
	arity int
}

// violatingArgc returns an argument count (including the command name, i.e.
// >= 1) that violates arity, or ok=false if arity cannot be violated with a
// non-empty command (e.g. arity -1 accepts any count >= 1).
func violatingArgc(r *rand.Rand, arity int) (int, bool) {
	if arity > 0 {
		// Exact arity: any count in [1, arity+4] except arity itself violates.
		for tries := 0; tries < 8; tries++ {
			argc := 1 + r.Intn(arity+4)
			if argc != arity {
				return argc, true
			}
		}
		// Deterministic fallback.
		if arity == 1 {
			return 2, true
		}
		return 1, true
	}
	// Negative arity -min: violated only by argc in [1, min-1].
	min := -arity
	if min <= 1 {
		return 0, false // "at least 1" can never be violated by a real command
	}
	return 1 + r.Intn(min-1), true
}

// looksLikeInt reports whether s would be accepted by ParseInt, so the
// non-integer property only asserts the error path on genuinely invalid input.
func looksLikeInt(s string) bool {
	_, err := ParseInt([]byte(s))
	return err == nil
}

// randToken builds a random ASCII letter token, used for unknown command names.
func randToken(r *rand.Rand) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	n := 1 + r.Intn(12)
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[r.Intn(len(alphabet))]
	}
	return string(b)
}

// randMaybeIntString biases toward non-integer strings but occasionally emits a
// valid integer, so the property exercises both branches of ParseIntReply.
func randMaybeIntString(r *rand.Rand) string {
	const junk = "abcdefg .-+_/xyz0123456789"
	switch r.Intn(4) {
	case 0:
		// A genuine integer.
		return fmt.Sprintf("%d", r.Int63()-r.Int63())
	default:
		n := r.Intn(10)
		b := make([]byte, n)
		for i := range b {
			b[i] = junk[r.Intn(len(junk))]
		}
		return string(b)
	}
}

// --- Property: arity mismatch wire text (需求 3.2) --------------------------

func TestProperty6ArityErrorText(t *testing.T) {
	tbl := property6Table()
	conn, br := startRouterServer(t, tbl)

	cmds := []arityCommand{
		{"GET", 2}, {"MSET", -3}, {"DEL", -2}, {"PING", 1}, {"INCRBY", 3}, {"BADOPT", 1},
	}
	// Case variants ensure the reply always uses the lowercase registered name
	// regardless of how the client cased the command (需求 3.2).
	casings := []func(string) string{
		strings.ToLower, strings.ToUpper, func(s string) string {
			if len(s) < 2 {
				return s
			}
			return strings.ToUpper(s[:1]) + strings.ToLower(s[1:])
		},
	}

	r := rand.New(rand.NewSource(0x5a3d))
	checks := 0
	for i := 0; i < 500; i++ {
		cmd := cmds[r.Intn(len(cmds))]
		argc, ok := violatingArgc(r, cmd.arity)
		if !ok {
			continue
		}
		args := make([]string, argc)
		args[0] = casings[r.Intn(len(casings))](cmd.name)
		for j := 1; j < argc; j++ {
			args[j] = "x"
		}

		got := sendCmdLine(t, conn, br, args...)
		want := "-" + resp.ErrWrongNumberOfArgs(cmd.name) // lowercase name
		if got != want {
			t.Fatalf("arity(%s argc=%d) = %q, want %q", args[0], argc, got, want)
		}
		checks++
	}
	if checks == 0 {
		t.Fatal("no arity-violating cases were generated")
	}
	t.Logf("arity error-text property held over %d generated cases", checks)
}

// --- Property: unknown command wire text (需求 3.3) -------------------------

func TestProperty6UnknownCommandErrorText(t *testing.T) {
	tbl := property6Table()
	conn, br := startRouterServer(t, tbl)

	known := map[string]bool{}
	for name := range tbl {
		known[name] = true
	}

	r := rand.New(rand.NewSource(0x1234))
	checks := 0
	for i := 0; i < 500; i++ {
		name := randToken(r)
		if known[toLower(name)] {
			continue // it is actually a registered command (case-insensitive)
		}
		// Random trailing args must not change the reply.
		args := []string{name}
		for extra := r.Intn(3); extra > 0; extra-- {
			args = append(args, "arg")
		}

		got := sendCmdLine(t, conn, br, args...)
		// Name is echoed verbatim, case preserved (需求 3.3).
		want := "-" + resp.ErrUnknownCommand(name)
		if got != want {
			t.Fatalf("unknown(%q) = %q, want %q", name, got, want)
		}
		checks++
	}
	if checks == 0 {
		t.Fatal("no unknown-command cases were generated")
	}
	t.Logf("unknown-command error-text property held over %d generated cases", checks)
}

// --- Property: non-integer wire text (需求 3.4) -----------------------------

func TestProperty6NonIntegerErrorText(t *testing.T) {
	tbl := property6Table()
	conn, br := startRouterServer(t, tbl)

	r := rand.New(rand.NewSource(0x9e77))
	nonInt, valid := 0, 0
	for i := 0; i < 500; i++ {
		val := randMaybeIntString(r)
		got := sendCmdLine(t, conn, br, "INCRBY", "k", val)

		if looksLikeInt(val) {
			// Valid integers must NOT produce the error (reply is the integer).
			want := ":" + val
			if got != want {
				t.Fatalf("INCRBY k %q (valid int) = %q, want %q", val, got, want)
			}
			valid++
			continue
		}
		// Any non-integer yields exactly the non-integer error (需求 3.4).
		want := "-" + resp.ErrNotInteger
		if got != want {
			t.Fatalf("INCRBY k %q = %q, want %q", val, got, want)
		}
		nonInt++
	}
	if nonInt == 0 {
		t.Fatal("no non-integer cases were generated")
	}
	t.Logf("non-integer error-text property held over %d invalid / %d valid cases", nonInt, valid)
}

// --- Property: syntax error wire text (需求 3.5) ----------------------------

func TestProperty6SyntaxErrorText(t *testing.T) {
	tbl := property6Table()
	conn, br := startRouterServer(t, tbl)

	// The syntax-error reply is category-determined: it must be identical on
	// every invocation regardless of the (valid-arity) arguments supplied.
	want := "-" + resp.ErrSyntax
	for i := 0; i < 50; i++ {
		got := sendCmdLine(t, conn, br, "BADOPT")
		if got != want {
			t.Fatalf("BADOPT #%d = %q, want %q", i, got, want)
		}
	}
	t.Logf("syntax error-text held constant over 50 invocations: %q", want)
}

// --- Pure encoder-level assertions (localize divergence to the encoder) -----

// TestProperty6EncoderWireBytes asserts the resp layer produces the exact RESP2
// error frames (leading '-' and trailing CRLF) for all four categories, so a
// mismatch in the router tests can be attributed to the router rather than the
// encoder. The arity/unknown builders are checked over random command names.
func TestProperty6EncoderWireBytes(t *testing.T) {
	// Static-text categories.
	if got := string(resp.AppendError(nil, resp.ErrNotInteger)); got != "-ERR value is not an integer or out of range\r\n" {
		t.Errorf("not-integer frame = %q", got)
	}
	if got := string(resp.AppendError(nil, resp.ErrSyntax)); got != "-ERR syntax error\r\n" {
		t.Errorf("syntax frame = %q", got)
	}

	r := rand.New(rand.NewSource(0xabcd))
	for i := 0; i < 300; i++ {
		name := randToken(r)

		// Unknown command: name echoed verbatim.
		gotUnknown := string(resp.AppendError(nil, resp.ErrUnknownCommand(name)))
		wantUnknown := "-ERR unknown command '" + name + "'\r\n"
		if gotUnknown != wantUnknown {
			t.Fatalf("unknown frame(%q) = %q, want %q", name, gotUnknown, wantUnknown)
		}

		// Arity: command name lowercased in the text.
		gotArity := string(resp.AppendError(nil, resp.ErrWrongNumberOfArgs(name)))
		wantArity := "-ERR wrong number of arguments for '" + strings.ToLower(name) + "' command\r\n"
		if gotArity != wantArity {
			t.Fatalf("arity frame(%q) = %q, want %q", name, gotArity, wantArity)
		}
	}
}
