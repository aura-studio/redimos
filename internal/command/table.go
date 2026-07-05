package command

import (
	"context"
	"errors"
	"fmt"

	"github.com/aura-studio/redimos/v2/internal/server"
)

// Handler processes one parsed command, writing a RESP2 reply to the connection
// via c.Redcon() (optionally through a resp.Writer). It mirrors the server
// dispatch seam and therefore returns no error: every handler is responsible
// for emitting its own reply, including error replies, so the router never has
// to translate a returned error into wire bytes.
//
// The signature follows the design's command-routing contract
// (design.md "核心类型与函数签名"):
//
//	type Handler func(ctx context.Context, c *server.Conn, args [][]byte)
//
// args[0] is the command name as sent by the client (case preserved); the
// remaining elements are the command arguments.
type Handler func(ctx context.Context, c *server.Conn, args [][]byte)

// CmdSpec describes a command's metadata, used for arity validation and routing
// (requirement 3.1, 3.2). It corresponds to an entry in Pika's command table.
type CmdSpec struct {
	// Name is the lowercase command name used for table lookup and for the
	// byte-for-byte error text (e.g. "get" in
	// "wrong number of arguments for 'get' command"). Requirement 3.2.
	Name string

	// Arity is the argument-count constraint, counting the command name itself:
	//   - Arity > 0 requires exactly Arity arguments (command name + args).
	//   - Arity < 0 requires at least |Arity| arguments.
	// This matches the Redis command-table convention. Requirement 3.2.
	Arity int

	// Handler runs the command once lookup and arity validation pass.
	Handler Handler

	// Write reports whether the command mutates state. It drives dual-write and
	// consistency policy in later tasks; the router only records it.
	Write bool
}

// Table is the command registry mapping a lowercase command name to its
// CmdSpec. Requirement 3.1 dispatches by lowercased command name, so all keys
// are stored lowercase and lookups lowercase the incoming name.
//
// Table implements server.Dispatcher (see router.go) so it can be wired
// directly into the server shell without the server package importing this
// package, avoiding an import cycle.
type Table map[string]CmdSpec

// NewTable returns an empty command table ready for Register.
func NewTable() Table {
	return make(Table)
}

// Register adds a command to the table. The command name is normalized to
// lowercase so lookups by lowercased name (requirement 3.1) always match, and
// spec.Name is set to that lowercase form so error text uses the lowercase name
// (requirement 3.2) regardless of how the caller cased it.
//
// Register returns an error on a programming mistake (empty name, nil handler, or a
// duplicate registration) instead of panicking, so it is unit-testable and so the
// initialization path can AGGREGATE every bad registration and report them together
// (see Router.reg / finishRegistration) rather than aborting on the first.
func (t Table) Register(name string, arity int, write bool, h Handler) error {
	if name == "" {
		return errors.New("command: Register called with empty name")
	}
	if h == nil {
		return fmt.Errorf("command: Register called with nil handler for %q", name)
	}
	lower := toLower(name)
	if _, dup := t[lower]; dup {
		return fmt.Errorf("command: duplicate registration for %q", lower)
	}
	t[lower] = CmdSpec{
		Name:    lower,
		Arity:   arity,
		Handler: h,
		Write:   write,
	}
	return nil
}

// Lookup returns the CmdSpec registered under name (matched case-insensitively)
// and whether it exists. Requirement 3.1.
func (t Table) Lookup(name string) (CmdSpec, bool) {
	spec, ok := t[toLower(name)]
	return spec, ok
}

// arityOK reports whether an argument count (including the command name)
// satisfies the spec's arity constraint. Requirement 3.2.
func (s CmdSpec) arityOK(argc int) bool {
	if s.Arity >= 0 {
		return argc == s.Arity
	}
	return argc >= -s.Arity
}

// toLower lowercases an ASCII command name without allocating when the input is
// already lowercase. It mirrors resp.toLower so the command package does not
// depend on the strings package for this hot-path helper.
func toLower(s string) string {
	hasUpper := false
	for i := 0; i < len(s); i++ {
		if c := s[i]; c >= 'A' && c <= 'Z' {
			hasUpper = true
			break
		}
	}
	if !hasUpper {
		return s
	}
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

// Ensure Table satisfies the server dispatch seam at compile time.
var _ server.Dispatcher = (Table)(nil)
