package command

// unsupported.go documents how redimos disposes of the Redis commands it does not
// serve from the DynamoDB store, and pins down the one family that is left on the
// generic "unknown command" path.
//
// # Two dispositions for "not served"
//
//  1. PROXY-REJECT (registered, dedicated error). Every command that EXISTS in
//     Redis 3.2 but the proxy declines is registered with its real arity and a
//     descriptive rejection, so the client learns the command is recognised but
//     intentionally unavailable (and why) instead of being told it does not exist.
//     This covers KEYS / RENAME / RENAMENX / FLUSHALL / FLUSHDB (keys.go) and the
//     Pub/Sub, Lua, transaction and blocking-pop families (rejected.go). The
//     server / replication / cluster / key-management admin commands currently
//     still fall through to (2) pending an implement-vs-reject decision.
//
//  2. UNKNOWN COMMAND (unregistered → default reply). The router's Dispatch
//     (router.go) answers any unregistered command with the byte-for-byte
//     "-ERR unknown command '<name>'\r\n". This is the CORRECT reply only for
//     commands that genuinely do not exist in Redis 3.2 — namely the Streams
//     family (X*, introduced in Redis 5.0). A real Redis 3.2 server replies with
//     the same unknown-command error for those, so the proxy stays byte-for-byte
//     identical to the oracle.
//
// TestUnsupported* (unsupported_test.go) guards this: the Streams family must stay
// unregistered and reach the unknown-command reply, while the reject families must
// return their dedicated errors and never silently succeed.

// UnsupportedCommands enumerates the commands that must reach the generic
// unknown-command reply: the Streams family, which does not exist in Redis 3.2.
// Names are the canonical uppercase spellings.
//
// It deliberately does NOT include the proxy-reject families (Pub/Sub, Lua,
// transactions, blocking pops — rejected.go; KEYS/RENAME/FLUSH — keys.go): those
// are registered with dedicated rejections, so listing them here would wrongly
// assert they fall through to the unknown-command path.
var UnsupportedCommands = []string{
	// Streams (Redis 5.0+; absent from Redis 3.2, so "unknown command" is the
	// oracle-correct reply).
	"XADD", "XLEN", "XRANGE", "XREVRANGE", "XREAD", "XDEL", "XTRIM", "XINFO",
}
