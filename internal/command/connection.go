package command

import (
	"context"
	"crypto/subtle"
	"fmt"
	"strings"
	"time"

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

	// Databases is the number of logical DBs SELECT accepts when MultiDB is enabled:
	// a valid index is [0, Databases). A value <= 0 defaults to Redis 3.2's 16.
	// Matching this bound is what makes SELECT reject an out-of-range index the same
	// way Redis does.
	Databases int

	// MaxCollectionResult caps the number of members a whole-collection reply
	// (HGETALL/HKEYS/HVALS/SMEMBERS/LRANGE/ZRANGE...) or *STORE operand may
	// materialize in proxy memory before the command is rejected, bounding the heap
	// an authenticated client can force with one command. 0 disables the cap.
	MaxCollectionResult int

	// ScanTimeout bounds how long a single SCAN page (the backend Scan) may run before
	// the command is aborted, so a SCAN against a slow/large backend cannot hold a
	// connection goroutine indefinitely. 0 disables the timeout.
	ScanTimeout time.Duration
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

	// regErrs accumulates registration errors during construction so all bad
	// registrations are reported together by finishRegistration.
	regErrs []error
}

// reg registers a command, collecting any error instead of aborting so the whole
// registration pass can complete and finishRegistration can summarize every problem.
func (r *Router) reg(name string, arity int, write bool, h Handler) {
	if err := r.Table.Register(name, arity, write, h); err != nil {
		r.regErrs = append(r.regErrs, err)
	}
}

// finishRegistration fails fast with an aggregated summary if any command failed to
// register. Registration happens once at startup and a bad table is a programming error,
// so a panic is appropriate — but it now lists ALL offending registrations at once.
func (r *Router) finishRegistration() {
	if len(r.regErrs) == 0 {
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "command: %d invalid command registration(s):", len(r.regErrs))
	for _, e := range r.regErrs {
		fmt.Fprintf(&b, "\n  - %v", e)
	}
	panic(b.String())
}

// NewRouter builds a Router with a fresh command Table, registers the handshake
// and connection-management commands, and returns it ready to be wired into the
// server shell as a server.Dispatcher. Later tasks register additional command
// families on r.Table.
func NewRouter(cfg Config) *Router {
	r := &Router{Table: NewTable(), Config: cfg}
	r.registerConnection()
	r.finishRegistration()
	return r
}

// Dispatch mirrors Redis 3.2 processCommand's ordering: QUIT is special-cased at the
// very top, then the command is looked up and arity-validated, and ONLY THEN is the
// NOAUTH gate applied, before the handler runs.
//
//  1. QUIT (case-insensitive) replies "+OK" and closes REGARDLESS of argument count and
//     even on an unauthenticated connection — Redis handles it before lookup/arity/auth.
//  2. An unknown command ("-ERR unknown command '<name>'") or a wrong arity
//     ("-ERR wrong number of arguments ...") is reported even on an unauthenticated
//     connection — the lookup and arity checks precede the auth gate.
//  3. NOAUTH gate: with a requirepass configured and the connection not yet
//     authenticated, every command except AUTH (QUIT already handled above) replies
//     "-NOAUTH Authentication required." When requirepass is empty the gate is skipped.
func (r *Router) Dispatch(ctx context.Context, c *server.Conn, args [][]byte) {
	if len(args) == 0 {
		return
	}

	// (1) QUIT special-case — before lookup, arity and the auth gate.
	if toLower(string(args[0])) == "quit" {
		handleQuit(ctx, c, args)
		return
	}

	// (2) Command lookup + arity, before the auth gate.
	spec, ok := r.Table.Lookup(string(args[0]))
	if !ok {
		writeError(c, resp.ErrUnknownCommand(string(args[0])))
		return
	}
	if !spec.arityOK(len(args)) {
		writeError(c, resp.ErrWrongNumberOfArgs(spec.Name))
		return
	}

	// (3) NOAUTH gate — AUTH is the only command permitted while unauthenticated.
	if r.Config.RequirePass != "" && !c.Authed() && spec.Name != "auth" {
		writeError(c, resp.ErrNoAuth)
		return
	}

	spec.Handler(ctx, c, args)
}

// Ensure *Router satisfies the server dispatch seam at compile time.
var _ server.Dispatcher = (*Router)(nil)

// registerConnection installs the handshake and connection-management commands
// on the router's table. AUTH and SELECT are bound methods so they can read the
// router's Config; the rest are config-free package functions.
func (r *Router) registerConnection() {
	// HELLO takes optional args (e.g. "HELLO 3 AUTH user pass"); accept any
	// form (arity -1) so the handler can always emit the exact unknown-command
	// reply regardless of arguments. Requirement 2.1.
	r.reg("HELLO", -1, false, handleHello)
	// PING accepts zero or one argument; arity -1 lets the handler distinguish
	// the two forms and reject 2+ args itself. Requirement 2.2, 2.3.
	r.reg("PING", -1, false, handlePing)
	r.reg("ECHO", 2, false, handleEcho)
	r.reg("AUTH", 2, false, r.handleAuth)
	r.reg("SELECT", 2, false, r.handleSelect)
	r.reg("QUIT", 1, false, handleQuit)

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
	// Constant-time compare so a network attacker cannot recover the password
	// byte-by-byte from reply-timing differences. ConstantTimeCompare returns 0
	// on a length mismatch (which itself leaks only the length), so the running
	// time does not depend on how many leading bytes matched.
	if subtle.ConstantTimeCompare(args[1], []byte(r.Config.RequirePass)) != 1 {
		// Redis authCommand sets authenticated=0 on a password mismatch, so a wrong
		// AUTH REVOKES an already-authenticated session (a later command then gets
		// NOAUTH), not merely rejects this one command.
		c.SetAuthed(false)
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
	// Non-zero index with multi-DB disabled: only DB 0 exists, so reject as an invalid
	// index (redimos single-DB mode).
	if !r.Config.MultiDB {
		w.Error(resp.ErrInvalidDBIndex)
		return
	}
	// Multi-DB: bound the index to [0, databases) like Redis 3.2 (default 16).
	// Previously any positive index was accepted. Redis 3.2.12 replies the SAME
	// "invalid DB index" text for a numeric-but-out-of-range index as for a
	// non-numeric one (verified against the live oracle), so reuse ErrInvalidDBIndex.
	if idx < 0 || idx >= int64(r.databases()) {
		w.Error(resp.ErrInvalidDBIndex)
		return
	}
	c.SetDB(int(idx))
	w.SimpleString("OK")
}

// databases returns the configured logical DB count, defaulting to Redis' 16 when
// unset so a bare Config still bounds SELECT the same way Redis does.
func (r *Router) databases() int {
	if r.Config.Databases > 0 {
		return r.Config.Databases
	}
	return 16
}

// handleQuit replies "+OK" and closes the connection. redcon flushes the staged
// reply before tearing down the socket, so the client reliably receives the OK.
func handleQuit(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).SimpleString("OK")
	_ = c.Redcon().Close()
}
