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

	// errReadWriteUnsupported rejects READWRITE, the inverse of READONLY: it clears
	// the Redis Cluster replica read-only flag. With no cluster/replica state to
	// reset, it is equally inapplicable on this proxy.
	errReadWriteUnsupported = "ERR READWRITE is not supported on this proxy (Redis Cluster replica reads do not apply)"

	// errReplconfUnsupported rejects REPLCONF, the internal master<->replica
	// replication sub-protocol (listening-port/capa negotiation and ACK <offset>
	// heartbeats). It is meaningless without an active replication link and a
	// maintained replication offset, neither of which a stateless proxy has.
	errReplconfUnsupported = "ERR REPLCONF is not supported on this proxy (replication sub-protocol; no master/replica link exists)"

	// errRandomKeyUnsupported rejects RANDOMKEY. Picking a random key from the whole
	// keyspace has no bounded path on a partitioned DynamoDB table — it forces the
	// same unbounded full-table scan already declined for KEYS.
	errRandomKeyUnsupported = "ERR RANDOMKEY is not supported on this proxy (it requires an unbounded full-table scan; use SCAN)"

	// errMoveUnsupported rejects MOVE. DBs are just the pk prefix in one table, so
	// moving a key means rewriting every member item under a new prefix plus moving
	// the meta — a costly, non-atomic whole-collection migrate, like RENAME.
	errMoveUnsupported = "ERR MOVE is not supported on this proxy (a cross-DB move is a non-atomic whole-collection rewrite)"

	// errSortUnsupported rejects SORT. BY/GET resolve one external lookup per element
	// per pattern (unbounded fan-out) and STORE is a non-atomic whole-collection
	// replace, so it is declined rather than served at unbounded cost.
	errSortUnsupported = "ERR SORT is not supported on this proxy (BY/GET fan-out is unbounded and STORE is not atomic)"

	// errObjectUnsupported rejects OBJECT. REFCOUNT/ENCODING/IDLETIME expose
	// Redis-internal representation metadata (encoding names, refcounts, the LRU
	// idle clock) that DynamoDB items do not have; any answer would be fabricated.
	errObjectUnsupported = "ERR OBJECT is not supported on this proxy (DynamoDB items have no Redis-internal encoding/refcount/idletime)"

	// errMonitorUnsupported rejects MONITOR. Streaming every command the server
	// processes needs a global, cross-connection command bus and a long-lived stream,
	// which a stateless per-connection proxy has no way to provide (like Pub/Sub).
	errMonitorUnsupported = "ERR MONITOR is not supported on this proxy (it requires a global, cross-connection command stream)"

	// errClusterUnsupported rejects the CLUSTER family. redimos is a single logical
	// keyspace over one DynamoDB table, with no hash slots or cluster membership, so
	// the whole family is semantically inapplicable.
	errClusterUnsupported = "ERR CLUSTER is not supported on this proxy (a single logical keyspace over one DynamoDB table has no cluster/slots)"

	// errLatencyUnsupported rejects the LATENCY family. It reports a stateful,
	// in-process latency-spike monitor that a stateless proxy never accumulates and
	// could not keep coherent across instances behind a load balancer.
	errLatencyUnsupported = "ERR LATENCY is not supported on this proxy (no in-process latency-spike monitor is kept)"

	// errDebugUnsupported rejects the DEBUG family. Its subcommands poke
	// server-internal state (OBJECT encoding/refcount, RELOAD of a nonexistent RDB,
	// SET-ACTIVE-EXPIRE, SEGFAULT) — no single reply is correct and several are
	// meaningless or actively dangerous on a stateless proxy.
	errDebugUnsupported = "ERR DEBUG is not supported on this proxy (server-internal debugging knobs are not available)"

	// The commands below cannot be IMPLEMENTED on a stateless DynamoDB proxy — they
	// require Redis' internal RDB serialization, another live Redis instance, or a
	// replication backlog. But they exist in Redis 3.2, so they are still declined
	// with a dedicated rejection (disposition is independent of implementability)
	// rather than left on the unknown-command path.
	errDumpUnsupported          = "ERR DUMP is not supported on this proxy (it requires Redis' internal RDB serialization)"
	errRestoreUnsupported       = "ERR RESTORE is not supported on this proxy (it requires deserializing a Redis RDB payload)"
	errRestoreAskingUnsupported = "ERR RESTORE-ASKING is not supported on this proxy (Redis Cluster slot migration plus RDB deserialization)"
	errMigrateUnsupported       = "ERR MIGRATE is not supported on this proxy (it requires DUMP/RESTORE against another Redis instance)"
	errSyncUnsupported          = "ERR SYNC is not supported on this proxy (there is no in-memory dataset to stream as RDB)"
	errPsyncUnsupported         = "ERR PSYNC is not supported on this proxy (there is no replication backlog or offset)"
	errSlaveofUnsupported       = "ERR SLAVEOF is not supported on this proxy (it owns no local dataset and cannot replicate)"
)

// registerRejected registers the deliberately-declined-but-real Redis 3.2 families
// as first-class proxy rejections. Arities are the Redis 3.2 command-table values.
func (r *Router) registerRejected() {

	// Pub/Sub (requirement 4.1).
	r.reg("SUBSCRIBE", -2, false, r.handlePubSubRejected)
	r.reg("UNSUBSCRIBE", -1, false, r.handlePubSubRejected)
	r.reg("PSUBSCRIBE", -2, false, r.handlePubSubRejected)
	r.reg("PUNSUBSCRIBE", -1, false, r.handlePubSubRejected)
	r.reg("PUBLISH", 3, false, r.handlePubSubRejected)
	r.reg("PUBSUB", -2, false, r.handlePubSubRejected)

	// Lua scripting (requirement 4.2).
	r.reg("EVAL", -3, false, r.handleScriptRejected)
	r.reg("EVALSHA", -3, false, r.handleScriptRejected)
	r.reg("SCRIPT", -2, false, r.handleScriptRejected)

	// Transactions (requirement 4.3).
	r.reg("MULTI", 1, false, r.handleTxnRejected)
	r.reg("EXEC", 1, false, r.handleTxnRejected)
	r.reg("DISCARD", 1, false, r.handleTxnRejected)
	r.reg("WATCH", -2, false, r.handleTxnRejected)
	r.reg("UNWATCH", 1, false, r.handleTxnRejected)

	// Blocking list pops (requirement 4.4).
	r.reg("BLPOP", -3, false, r.handleBlockingRejected)
	r.reg("BRPOP", -3, false, r.handleBlockingRejected)
	r.reg("BRPOPLPUSH", 4, false, r.handleBlockingRejected)

	// Individual real-Redis-3.2 commands the proxy declines, each with its own
	// message. Arities are the Redis 3.2 command-table values.
	r.registerReject("SHUTDOWN", -1, errShutdownUnsupported)
	r.registerReject("ASKING", 1, errAskingUnsupported)
	r.registerReject("READONLY", 1, errReadOnlyUnsupported)
	r.registerReject("READWRITE", 1, errReadWriteUnsupported)
	r.registerReject("REPLCONF", -1, errReplconfUnsupported)
	r.registerReject("RANDOMKEY", 1, errRandomKeyUnsupported)
	r.registerReject("MOVE", 3, errMoveUnsupported)
	r.registerReject("SORT", -2, errSortUnsupported)
	r.registerReject("OBJECT", 3, errObjectUnsupported)
	r.registerReject("MONITOR", 1, errMonitorUnsupported)
	r.registerReject("CLUSTER", -2, errClusterUnsupported)
	r.registerReject("LATENCY", -2, errLatencyUnsupported)
	r.registerReject("DEBUG", -1, errDebugUnsupported)

	// Real Redis 3.2 commands that cannot be implemented here (RDB serialization /
	// another Redis / replication) but are still declined explicitly.
	r.registerReject("DUMP", 2, errDumpUnsupported)
	r.registerReject("RESTORE", -4, errRestoreUnsupported)
	r.registerReject("RESTORE-ASKING", -4, errRestoreAskingUnsupported)
	r.registerReject("MIGRATE", -6, errMigrateUnsupported)
	r.registerReject("SYNC", 1, errSyncUnsupported)
	r.registerReject("PSYNC", 3, errPsyncUnsupported)
	r.registerReject("SLAVEOF", 3, errSlaveofUnsupported)
}

// registerReject registers a single command as a first-class proxy rejection: the
// command is recognised (so arity is checked and it does NOT fall through to the
// unknown-command path) and any correctly-shaped call replies msg verbatim.
func (r *Router) registerReject(name string, arity int, msg string) {
	r.reg(name, arity, false, func(_ context.Context, c *server.Conn, _ [][]byte) {
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
