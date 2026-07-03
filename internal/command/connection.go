package command

import (
	"context"

	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// This file implements the handshake and connection-management commands
// (HELLO/PING/ECHO/AUTH/SELECT/QUIT) plus the pre-dispatch NOAUTH gate, per
// requirement 2.1–2.9 and design.md "握手与连接管理命令" / algorithm 4.
//
// The command layer owns the connection-layer configuration (requirepass and
// multi-DB) via Config, keeping the server package free of business logic. A
// Router wraps a command Table with that Config and implements
// server.Dispatcher, adding the auth gate ahead of table dispatch.

// Config carries the connection-layer configuration owned by the command layer.
// It is passed to the command Router at assembly time (task 23.1 wires it from
// the process flags).
type Config struct {
	// RequirePass is the single AUTH password. When empty, authentication is
	// disabled and no NOAUTH gate is applied. Requirement 2.5, 2.6.
	RequirePass string

	// MultiDB reports whether SELECT n (n != 0) is permitted. When false, any
	// non-zero SELECT is rejected with "-ERR invalid DB index"; when true, the
	// selected index is stored on the connection so later commands map to the
	// pk prefix "d{n}:". Requirement 2.8, 2.9.
	MultiDB bool
}

// Router wraps a command Table with connection-layer Config and implements
// server.Dispatcher. It performs the pre-dispatch NOAUTH gate (requirement 2.6,
// design algorithm 4) before delegating to the Table's routing (lookup, arity
// validation, handler invocation).
//
// Storage holds the storage-layer components (meta store, data store, read path)
// used by the data-command handlers. It is empty for a connection-only router
// built with NewRouter; NewRouterWithStorage (router_storage.go) populates it and
// registers the data-command families.
type Router struct {
	Table   Table
	Config  Config
	Storage Storage
}

// NewRouter builds a Router with a fresh command Table, registers the handshake
// and connection-management commands, and returns it ready to be wired into the
// server shell as a server.Dispatcher. Later tasks register additional command
// families on r.Table.
func NewRouter(cfg Config) *Router {
	r := &Router{Table: NewTable(), Config: cfg}
	r.registerConnection()
	return r
}

// Dispatch applies the NOAUTH gate, then forwards to the command Table.
//
// Per design algorithm 4 the gate runs before command lookup: when a
// requirepass is configured and the connection has not authenticated, every
// command except AUTH and QUIT is rejected with "-NOAUTH Authentication
// required." (requirement 2.6). When requirepass is empty the gate is skipped
// entirely and all connections are treated as authenticated.
func (r *Router) Dispatch(ctx context.Context, c *server.Conn, args [][]byte) {
	if len(args) == 0 {
		return
	}

	if r.Config.RequirePass != "" && !c.Authed() {
		name := toLower(string(args[0]))
		if name != "auth" && name != "quit" {
			writeError(c, resp.ErrNoAuth)
			return
		}
	}

	r.Table.Dispatch(ctx, c, args)
}

// Ensure *Router satisfies the server dispatch seam at compile time.
var _ server.Dispatcher = (*Router)(nil)

// registerConnection installs the handshake and connection-management commands
// on the router's table. AUTH and SELECT are bound methods so they can read the
// router's Config; the rest are config-free package functions.
func (r *Router) registerConnection() {
	t := r.Table
	// HELLO takes optional args (e.g. "HELLO 3 AUTH user pass"); accept any
	// form (arity -1) so the handler can always emit the exact unknown-command
	// reply regardless of arguments. Requirement 2.1.
	t.Register("HELLO", -1, false, handleHello)
	// PING accepts zero or one argument; arity -1 lets the handler distinguish
	// the two forms and reject 2+ args itself. Requirement 2.2, 2.3.
	t.Register("PING", -1, false, handlePing)
	t.Register("ECHO", 2, false, handleEcho)
	t.Register("AUTH", 2, false, r.handleAuth)
	t.Register("SELECT", 2, false, r.handleSelect)
	t.Register("QUIT", 1, false, handleQuit)

	// Client-probe fallback stubs (COMMAND/CLIENT/CONFIG/DBSIZE/TIME) live on the
	// same connection-level path as PING/ECHO so they work before (and without) a
	// storage backend, matching how real Redis answers these probes during client
	// init. See stub.go. Requirement 19.1–19.5.
	r.registerStubs()
}

// handleHello replies "-ERR unknown command '<name>'" so that go-redis v9 and
// redis-py 5+ fall back to RESP2 (requirement 2.1). The name is echoed exactly as
// the client sent it (case preserved), matching the generic unknown-command path
// and real Redis, which reflects the client's casing.
func handleHello(_ context.Context, c *server.Conn, args [][]byte) {
	writeError(c, resp.ErrUnknownCommand(string(args[0])))
}

// handlePing implements PING: no argument replies "+PONG"; a single argument is
// echoed back as a bulk string. Two or more arguments are an arity error whose
// text matches Redis/Pika. Requirement 2.2, 2.3.
func handlePing(_ context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	switch len(args) {
	case 1:
		w.SimpleString("PONG")
	case 2:
		w.BulkString(args[1])
	default:
		w.Error(resp.ErrWrongNumberOfArgs("ping"))
	}
}

// handleEcho echoes its single argument as a bulk string. Requirement 2.4.
func handleEcho(_ context.Context, c *server.Conn, args [][]byte) {
	resp.NewWriter(c.Redcon()).BulkString(args[1])
}

// handleAuth implements AUTH against the configured requirepass. With a
// requirepass set, a matching password replies "+OK" and marks the connection
// authenticated (requirement 2.5); a mismatch replies "-ERR invalid password".
// When no requirepass is configured, AUTH replies with the Redis/Pika
// "no password is set" error.
func (r *Router) handleAuth(_ context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	if r.Config.RequirePass == "" {
		w.Error(resp.ErrNoPasswordSet)
		return
	}
	if string(args[1]) != r.Config.RequirePass {
		w.Error(resp.ErrInvalidPassword)
		return
	}
	c.SetAuthed(true)
	w.SimpleString("OK")
}

// handleSelect implements SELECT. "SELECT 0" always replies "+OK". A non-zero
// index is rejected with "-ERR invalid DB index" unless multi-DB is enabled, in
// which case the index is stored on the connection so later commands map to the
// pk prefix "d{n}:" (requirement 2.7, 2.8, 2.9). A non-integer index yields the
// standard integer-parse error.
func (r *Router) handleSelect(_ context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	idx, err := ParseInt(args[1])
	if err != nil {
		// Redis 3.2 reports a non-numeric DB index as "invalid DB index", not the
		// generic not-an-integer error.
		w.Error(resp.ErrInvalidDBIndex)
		return
	}
	if idx == 0 {
		c.SetDB(0)
		w.SimpleString("OK")
		return
	}
	if !r.Config.MultiDB || idx < 0 {
		w.Error(resp.ErrInvalidDBIndex)
		return
	}
	c.SetDB(int(idx))
	w.SimpleString("OK")
}

// handleQuit replies "+OK" and closes the connection. redcon flushes the staged
// reply before tearing down the socket, so the client reliably receives the OK.
func handleQuit(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).SimpleString("OK")
	_ = c.Redcon().Close()
}
