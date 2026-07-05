package command

import (
	"context"
	"strconv"
	"time"

	"github.com/aura-studio/redimos/v2/internal/metrics"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// stub.go implements the client-probe fallback stubs (requirement 19.1–19.5 and
// design.md "客户端探测兜底 stub"). Client libraries (go-redis, redis-py,
// jedis, ...) run a handful of introspection/probe commands during connection
// setup — COMMAND / COMMAND COUNT, CLIENT SETNAME/GETNAME, CONFIG GET, DBSIZE,
// TIME. redimos does not model those features, but a missing command would fail
// the client's init flow, so each returns a benign, spec-mandated fallback.
//
// # Registration site
//
// These stubs are registered in registerConnection (connection.go), i.e. on the
// SAME path as PING/ECHO, so they are available even on a connection-only Router
// built with NewRouter (no storage wired). This mirrors real Redis, where these
// probes work before any data command and must not depend on a backend being
// reachable. Because registerConnection runs from both NewRouter and
// NewRouterWithStorage, the stubs are present on both the connection-only and the
// storage-backed routers.
//
// The one stub that could in principle consult storage — DBSIZE — is written to
// degrade gracefully when no store is wired (see handleDBSize).

// registerStubs installs the client-probe fallback commands on the router's
// table. It is called from registerConnection so the stubs live alongside the
// other connection-level commands and require no storage backend.
//
// Arity notes (Redis command-table convention, name counted):
//   - COMMAND  -1: name only ("*0") or with a subcommand such as COMMAND COUNT.
//   - CLIENT   -2: always a subcommand (CLIENT SETNAME <name> / CLIENT GETNAME).
//   - CONFIG   -2: at least a subcommand (CONFIG GET <param> / CONFIG SET ...).
//   - DBSIZE    1: no arguments.
//   - TIME      1: no arguments.
func (r *Router) registerStubs() {
	// Ensure the observability commands (INFO / SLOWLOG) always have a live
	// slowlog buffer, even on a connection-only router where no storage — and
	// hence no injected SlowLog — was wired. See ensureObservability.
	r.ensureObservability()

	r.reg("COMMAND", -1, false, handleCommand)
	r.reg("CLIENT", -2, false, handleClient)
	r.reg("CONFIG", -2, false, handleConfig)
	r.reg("DBSIZE", 1, false, r.handleDBSize)
	r.reg("TIME", 1, false, r.handleTime)

	// INFO / SLOWLOG are read-only observability commands. They live on the
	// connection path alongside the other probe stubs so they answer before (and
	// without) a storage backend, matching how real Redis serves them during
	// client init and monitoring. Requirement 18.6, 18.7. See info.go / slowlog.go.
	//
	// Arity notes (Redis command-table convention, name counted):
	//   - INFO    -1: bare INFO or INFO <section>.
	//   - SLOWLOG -2: always a subcommand (GET [count] / LEN / RESET).
	r.reg("INFO", -1, false, r.handleInfo)
	r.reg("SLOWLOG", -2, false, r.handleSlowlog)

	// Server persistence / replication no-op stubs. redimos keeps no RDB/AOF
	// (DynamoDB is the durable store, every write already persisted) and has no
	// Redis replicas, so these reply the benign fixed value a standalone Redis
	// would, keeping ops scripts and client frameworks (e.g. write-then-WAIT,
	// connection self-checks) from failing on an unknown command.
	//
	// Arity notes (Redis 3.2 command-table convention, name counted):
	//   - SAVE 1 · BGSAVE -1 (optional SCHEDULE) · BGREWRITEAOF 1 · LASTSAVE 1
	//   - ROLE 1 · WAIT 3 (numreplicas timeout) · PFSELFTEST 1
	r.reg("SAVE", 1, false, handleSave)
	r.reg("BGSAVE", -1, false, handleBgSave)
	r.reg("BGREWRITEAOF", 1, false, handleBgRewriteAOF)
	r.reg("LASTSAVE", 1, false, r.handleLastSave)
	r.reg("ROLE", 1, false, handleRole)
	r.reg("WAIT", 3, false, handleWait)
	r.reg("PFSELFTEST", 1, false, handlePFSelfTest)
}

// handleSave stubs SAVE. Real Redis synchronously snapshots to an RDB file; redimos
// has no RDB and DynamoDB already persists every write, so "already saved" -> +OK.
func handleSave(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).SimpleString("OK")
}

// handleBgSave stubs BGSAVE (with optional SCHEDULE). No fork/RDB to do; reply the
// conventional status string real Redis returns when a background save begins.
func handleBgSave(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).SimpleString("Background saving started")
}

// handleBgRewriteAOF stubs BGREWRITEAOF. redimos runs no AOF; reply Redis' status.
func handleBgRewriteAOF(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).SimpleString("Background append only file rewriting started")
}

// handleLastSave stubs LASTSAVE, which returns the unix time of the last successful
// RDB save. With no RDB but continuous DynamoDB persistence, the honest degenerate
// answer is the current epoch second from the router clock.
func (r *Router) handleLastSave(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).Int(r.now())
}

// handleRole stubs ROLE. A standalone instance honestly answers the master form:
// ["master", <replication offset 0>, <empty replica list>].
func handleRole(_ context.Context, c *server.Conn, _ [][]byte) {
	buf := resp.AppendArrayHeader(nil, 3)
	buf = resp.AppendBulkString(buf, []byte("master"))
	buf = resp.AppendInt(buf, 0)
	buf = resp.AppendEmptyArray(buf)
	c.Redcon().WriteRaw(buf)
}

// handleWait stubs WAIT numreplicas timeout. A DynamoDB-backed proxy has zero Redis
// replicas (and data is already durable), so the count of replicas that acknowledged
// prior writes is genuinely 0 — the reply is a real value, not a placeholder.
func handleWait(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).Int(0)
}

// handlePFSelfTest stubs PFSELFTEST. Redis' internal HyperLogLog self-test replies
// +OK on success; the proxy has no native HLL internals to exercise, but its whole
// observable contract is that +OK health signal.
func handlePFSelfTest(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).SimpleString("OK")
}

// ensureObservability guarantees r.Storage.Slowlog is non-nil so the INFO and
// SLOWLOG handlers always have a live ring buffer to serve. A caller may inject
// its own SlowLog (via Storage.Slowlog, typically the one wired into
// metrics/main); when absent — e.g. a connection-only router built with
// NewRouter — a fresh default SlowLog is installed here. It runs once at
// construction time (from registerConnection, which is single-threaded), so no
// locking is needed.
func (r *Router) ensureObservability() {
	if r.Storage.Slowlog == nil {
		r.Storage.Slowlog = metrics.NewSlowLog(metrics.SlowlogConfig{})
	}
}

// handleCommand implements the COMMAND probe (requirement 19.1). Bare COMMAND
// (and any subcommand redimos does not special-case, e.g. COMMAND DOCS/INFO)
// replies with the empty array "*0" rather than enumerating a command table the
// proxy does not expose. COMMAND COUNT replies the integer 0. Both are benign
// fallbacks that keep client init flows from failing.
func handleCommand(_ context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	if len(args) >= 2 && toLower(string(args[1])) == "count" {
		w.Int(0)
		return
	}
	w.EmptyArray()
}

// handleClient implements the CLIENT probe (requirement 19.2). CLIENT SETNAME
// replies "+OK" (the name is accepted and discarded — redimos keeps no per-conn
// name), and CLIENT GETNAME replies the null bulk "$-1" (no name set), matching
// Redis when no name was assigned. Any other CLIENT subcommand (ID, SETINFO,
// NO-EVICT, ...) gets a minimal "+OK" so client setup never breaks on an
// unmodeled subcommand; redimos intentionally does not implement real
// client-management semantics.
func handleClient(_ context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	switch toLower(string(args[1])) {
	case "getname":
		w.NullBulk()
	default: // setname and every other subcommand
		w.SimpleString("OK")
	}
}

// configDefaults holds the handful of CONFIG parameters redimos answers with a
// fixed default value (requirement 19.3). The proxy has no runtime config, so
// these are constant, Redis-compatible defaults that satisfy clients probing for
// them (most notably maxmemory, which several clients read at startup). Any
// parameter not listed replies with an empty array "*0", exactly as Redis does
// for an unknown CONFIG GET parameter.
var configDefaults = map[string]string{
	"maxmemory":        "0",
	"maxmemory-policy": "noeviction",
	"save":             "",
	"appendonly":       "no",
	"timeout":          "0",
}

// handleConfig implements the CONFIG probe (requirement 19.3). CONFIG GET
// <param> returns a 2-element array [param, value] for a known default (e.g.
// CONFIG GET maxmemory -> ["maxmemory", "0"]) and the empty array "*0" for an
// unknown parameter or a missing parameter argument. CONFIG SET replies "+OK"
// (the write is accepted and discarded — redimos has no mutable runtime config).
// Any other subcommand also replies "+OK" as a minimal fallback.
func handleConfig(_ context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	switch toLower(string(args[1])) {
	case "get":
		if len(args) < 3 {
			w.EmptyArray()
			return
		}
		param := toLower(string(args[2]))
		val, ok := configDefaults[param]
		if !ok {
			w.EmptyArray()
			return
		}
		w.BulkArray([][]byte{[]byte(param), []byte(val)})
	default: // set and every other subcommand
		w.SimpleString("OK")
	}
}

// handleDBSize implements DBSIZE (requirement 19.4) as a best-effort
// approximation. An exact key count would require a full scan of the meta space
// on every call, which is explicitly rejected here as too expensive for a probe
// command. Since redimos keeps no cheap running key counter, DBSIZE replies the
// documented approximation of 0. This also lets DBSIZE work on a connection-only
// router (no storage wired) without special-casing. If a cheap counter is added
// later, this is the single place to source it from r.Storage.Meta.
func (r *Router) handleDBSize(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).Int(0)
}

// handleTime implements TIME (requirement 19.5), replying the real current time
// as a 2-element array of bulk strings [unix_seconds, microseconds]. The seconds
// come from the router clock r.now() (injectable for tests/expiry consistency);
// the sub-second microseconds come from time.Now(), matching Redis' TIME shape.
func (r *Router) handleTime(_ context.Context, c *server.Conn, _ [][]byte) {
	sec := r.now()
	micros := int64(time.Now().Nanosecond()) / 1000
	elems := [][]byte{
		[]byte(strconv.FormatInt(sec, 10)),
		[]byte(strconv.FormatInt(micros, 10)),
	}
	resp.NewWriter(c.Redcon()).BulkArray(elems)
}
