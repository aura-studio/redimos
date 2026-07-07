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
// replies with the empty array "*0" rather than enumerating a command table the
// proxy does not expose; COMMAND COUNT replies the integer 0; COMMAND INFO /
// GETKEYS keep the benign empty-array fallback (Redis 3.2 recognises them, so
// they must not error). An UNKNOWN subcommand — or COUNT with surplus args, or
// GETKEYS with no command — replies Redis 3.2 commandCommand's exact final-else
// error, since real 3.2 errors there rather than replying a benign value.
func handleCommand(_ context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	if len(args) == 1 {
		w.EmptyArray()
		return
	}
	sub := toLower(string(args[1]))
	switch {
	case sub == "count" && len(args) == 2:
		w.Int(0)
	case sub == "info": // any arg count: 3.2 accepts INFO with 0+ names
		w.EmptyArray()
	case sub == "getkeys" && len(args) >= 3:
		w.EmptyArray()
	default:
		w.Error("ERR Unknown subcommand or wrong number of arguments.")
	}
}

// handleClient implements the CLIENT probe (requirement 19.2). CLIENT SETNAME
// replies "+OK" (the name is accepted and discarded — redimos keeps no per-conn
// name) after validating the name the way Redis 3.2 does (every byte must be a
// printable ASCII in '!'..'~': no spaces/newlines/special characters), and
// CLIENT GETNAME replies the null bulk "$-1" (no name set). The real 3.2
// subcommands whose semantics redimos intentionally does not model
// (LIST/KILL/PAUSE/REPLY) keep the benign "+OK" stub for their VALID forms, but
// their wrong-arity / bad-argument forms error exactly as Redis 3.2 clientCommand
// does (each branch gates on argc before falling to the final-else). An UNKNOWN
// subcommand (e.g. ID — a 5.0+ addition — or a typo) also replies that syntax error.
func handleClient(_ context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	sub := toLower(string(args[1]))
	switch {
	case sub == "getname" && len(args) == 2:
		w.NullBulk()
	case sub == "setname" && len(args) == 3:
		for _, b := range args[2] {
			if b < '!' || b > '~' {
				w.Error("ERR Client names cannot contain spaces, newlines or special characters.")
				return
			}
		}
		w.SimpleString("OK")
	case sub == "list" && len(args) == 2:
		w.SimpleString("OK") // stub: real Redis returns a client-list bulk (§4.5)
	case sub == "kill" && len(args) == 2:
		// CLIENT KILL with no addr/filter is a plain "syntax error" in 3.2 (the KILL
		// branch requires argc==3 old-form or argc>3 filter-form; argc==2 falls
		// through to addReplyError "syntax error"), not the CLIENT-usage help text.
		w.Error(resp.ErrSyntax)
	case sub == "kill" && len(args) >= 3:
		// KILL addr / KILL <filters>: stub-accepts; addr/filter validation and the
		// "No such client" reply are not modeled (documented §4.5 residual).
		w.SimpleString("OK")
	case sub == "pause" && len(args) == 3:
		v, perr := ParseInt(args[2])
		if perr != nil {
			w.Error("ERR timeout is not an integer or out of range")
			return
		}
		if v < 0 {
			// getTimeoutFromObjectOrReply rejects a negative millisecond timeout.
			w.Error("ERR timeout is negative")
			return
		}
		w.SimpleString("OK")
	case sub == "reply" && len(args) == 3:
		switch toLower(string(args[2])) {
		case "on", "off", "skip":
			w.SimpleString("OK")
		default:
			w.Error(resp.ErrSyntax)
		}
	default:
		w.Error("ERR Syntax error, try CLIENT (LIST | KILL | GETNAME | SETNAME | PAUSE | REPLY)")
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
// unknown parameter. CONFIG SET / RESETSTAT / REWRITE reply "+OK" (accepted and
// discarded — redimos has no mutable runtime config). Arity mirrors Redis 3.2
// configCommand exactly: GET takes 1 param, SET takes 2, RESETSTAT/REWRITE take
// none — a wrong count replies "Wrong number of arguments for CONFIG <sub>"
// echoing the subcommand as the client sent it (badarity label); an UNKNOWN
// subcommand replies the exact Redis subcommand error.
func handleConfig(_ context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	badArity := func() {
		w.Error("ERR Wrong number of arguments for CONFIG " + string(args[1]))
	}
	switch toLower(string(args[1])) {
	case "get":
		if len(args) != 3 {
			badArity()
			return
		}
		param := toLower(string(args[2]))
		val, ok := configDefaults[param]
		if !ok {
			w.EmptyArray()
			return
		}
		w.BulkArray([][]byte{[]byte(param), []byte(val)})
	case "set":
		if len(args) != 4 {
			badArity()
			return
		}
		w.SimpleString("OK")
	case "resetstat", "rewrite":
		if len(args) != 2 {
			badArity()
			return
		}
		w.SimpleString("OK")
	default:
		w.Error("ERR CONFIG subcommand must be one of GET, SET, RESETSTAT, REWRITE")
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
