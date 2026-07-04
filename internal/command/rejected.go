package command

// rejected.go registers the command families that redimos deliberately declines
// with a FIRST-CLASS proxy rejection — a dedicated, descriptive error — rather than
// the generic "unknown command" reply. These commands all EXIST in Redis 3.2 (a
// real server would execute them), so answering "unknown command" is misleading;
// a clear "not supported on this proxy, because …" message tells the client the
// command is recognised but intentionally unavailable, and why.
//
// Each command is registered with its real Redis 3.2 arity so the arity check runs
// first (e.g. `EXEC x` still returns "wrong number of arguments for 'exec'",
// matching Redis); only a correctly-shaped call reaches the rejection message.
//
// This mirrors keys.go's treatment of KEYS / RENAME / FLUSHALL / FLUSHDB. The
// families that do NOT exist in Redis 3.2 (or are server/replication/cluster
// admin) are handled elsewhere: genuinely-absent commands stay on the
// unknown-command path (see unsupported.go).

import (
	"context"

	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

const (
	// errPubSubUnsupported rejects the Pub/Sub family. A stateless proxy cannot hold
	// per-connection subscriptions or fan messages out across connections.
	errPubSubUnsupported = "ERR Pub/Sub is not supported on this proxy (requires stateful, connection-level subscriptions)"

	// errScriptUnsupported rejects the Lua scripting family. The proxy embeds no Lua
	// interpreter and cannot run server-side scripts.
	errScriptUnsupported = "ERR Lua scripting is not supported on this proxy (no embedded interpreter)"

	// errTxnUnsupported rejects the transaction family. MULTI/EXEC require queuing
	// and atomically applying multiple commands, which the stateless proxy cannot do.
	errTxnUnsupported = "ERR transactions (MULTI/EXEC/WATCH) are not supported on this proxy"

	// errBlockingUnsupported rejects the blocking list pops. The proxy cannot hold a
	// connection blocked waiting for a push; clients should use the non-blocking
	// variant (LPOP/RPOP/RPOPLPUSH).
	errBlockingUnsupported = "ERR blocking commands are not supported on this proxy (use the non-blocking LPOP/RPOP/RPOPLPUSH)"

	// errShutdownUnsupported rejects SHUTDOWN. The proxy process is shared by all
	// tenants, so honouring it would terminate everyone's service; and there is no
	// RDB to persist first (DynamoDB is already the durable store).
	errShutdownUnsupported = "ERR SHUTDOWN is not supported on this proxy (it would terminate a process shared by all tenants)"

	// errAskingUnsupported rejects ASKING. It is the one-shot flag a client sets
	// after a Redis Cluster ASK redirect during slot migration — a mode this
	// non-cluster, single-keyspace proxy never enters, so there is nothing to toggle.
	errAskingUnsupported = "ERR ASKING is not supported on this proxy (Redis Cluster slot migration does not apply)"

	// errReadOnlyUnsupported rejects READONLY. It is the per-connection flag that
	// lets a Redis Cluster replica serve (possibly stale) reads for the slots it
	// replicates instead of redirecting to its master — inapplicable on a
	// non-cluster proxy that has no replicas, slots, or replica read role.
	errReadOnlyUnsupported = "ERR READONLY is not supported on this proxy (Redis Cluster replica reads do not apply)"
)

// registerRejected registers the deliberately-declined-but-real Redis 3.2 families
// as first-class proxy rejections. Arities are the Redis 3.2 command-table values.
func (r *Router) registerRejected() {
	t := r.Table

	// Pub/Sub (requirement 4.1).
	t.Register("SUBSCRIBE", -2, false, r.handlePubSubRejected)
	t.Register("UNSUBSCRIBE", -1, false, r.handlePubSubRejected)
	t.Register("PSUBSCRIBE", -2, false, r.handlePubSubRejected)
	t.Register("PUNSUBSCRIBE", -1, false, r.handlePubSubRejected)
	t.Register("PUBLISH", 3, false, r.handlePubSubRejected)
	t.Register("PUBSUB", -2, false, r.handlePubSubRejected)

	// Lua scripting (requirement 4.2).
	t.Register("EVAL", -3, false, r.handleScriptRejected)
	t.Register("EVALSHA", -3, false, r.handleScriptRejected)
	t.Register("SCRIPT", -2, false, r.handleScriptRejected)

	// Transactions (requirement 4.3).
	t.Register("MULTI", 1, false, r.handleTxnRejected)
	t.Register("EXEC", 1, false, r.handleTxnRejected)
	t.Register("DISCARD", 1, false, r.handleTxnRejected)
	t.Register("WATCH", -2, false, r.handleTxnRejected)
	t.Register("UNWATCH", 1, false, r.handleTxnRejected)

	// Blocking list pops (requirement 4.4).
	t.Register("BLPOP", -3, false, r.handleBlockingRejected)
	t.Register("BRPOP", -3, false, r.handleBlockingRejected)
	t.Register("BRPOPLPUSH", 4, false, r.handleBlockingRejected)

	// Individual real-Redis-3.2 commands the proxy declines, each with its own
	// message. Arities are the Redis 3.2 command-table values.
	r.registerReject("SHUTDOWN", -1, errShutdownUnsupported)
	r.registerReject("ASKING", 1, errAskingUnsupported)
	r.registerReject("READONLY", 1, errReadOnlyUnsupported)
}

// registerReject registers a single command as a first-class proxy rejection: the
// command is recognised (so arity is checked and it does NOT fall through to the
// unknown-command path) and any correctly-shaped call replies msg verbatim.
func (r *Router) registerReject(name string, arity int, msg string) {
	r.Table.Register(name, arity, false, func(_ context.Context, c *server.Conn, _ [][]byte) {
		resp.NewWriter(c.Redcon()).Error(msg)
	})
}

func (r *Router) handlePubSubRejected(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).Error(errPubSubUnsupported)
}

func (r *Router) handleScriptRejected(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).Error(errScriptUnsupported)
}

func (r *Router) handleTxnRejected(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).Error(errTxnUnsupported)
}

func (r *Router) handleBlockingRejected(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).Error(errBlockingUnsupported)
}
