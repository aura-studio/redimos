package command

import (
	"context"

	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// Dispatch routes one parsed command to its handler, implementing
// server.Dispatcher so a Table can be plugged straight into the server shell.
//
// The flow follows requirement 3.1–3.3:
//
//  1. Look the command up by its lowercased name (requirement 3.1). An unknown
//     command replies "-ERR unknown command '<name>'" with the name echoed
//     exactly as the client sent it (requirement 3.3).
//  2. Validate arity against the spec (requirement 3.2). A mismatch replies
//     "-ERR wrong number of arguments for '<cmd>' command" with the lowercase
//     command name.
//  3. Invoke the handler, which owns writing the successful (or command
//     specific error) reply.
//
// The server shell guarantees args is non-empty and calls Dispatch strictly one
// command at a time per connection, so no locking is needed here.
func (t Table) Dispatch(ctx context.Context, c *server.Conn, args [][]byte) {
	if len(args) == 0 {
		// Defensive: the shell filters empty commands, but never index into an
		// empty slice.
		return
	}

	name := string(args[0])

	spec, ok := t.Lookup(name)
	if !ok {
		// Requirement 3.3: unknown command, name echoed verbatim.
		writeError(c, resp.ErrUnknownCommand(name))
		return
	}

	if !spec.arityOK(len(args)) {
		// Requirement 3.2: arity mismatch, lowercase command name.
		writeError(c, resp.ErrWrongNumberOfArgs(spec.Name))
		return
	}

	// Requirement 3.1: dispatch to the resolved handler. Handlers write their
	// own reply (success or a command-specific error such as WRONGTYPE or the
	// value-not-an-integer / syntax errors handled in task 5.2).
	spec.Handler(ctx, c, args)
}

// writeError emits a RESP2 error reply through a resp.Writer over the
// connection's redcon transport, keeping byte-for-byte control over the wire
// format.
func writeError(c *server.Conn, msg string) {
	resp.NewWriter(c.Redcon()).Error(msg)
}
