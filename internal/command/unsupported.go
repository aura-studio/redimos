package command

// unsupported.go documents and pins down redimos' handling of the command
// families the proxy deliberately does NOT support (requirement 4.1–4.8 and
// design.md "明确不支持"): Pub/Sub, Lua (EVAL/EVALSHA/SCRIPT), transactions
// (MULTI/EXEC/WATCH/UNWATCH/DISCARD), blocking pops (B*), bit operations,
// HyperLogLog (PF*), GEO (GEO*), Streams (X*), and FLUSHALL/FLUSHDB.
//
// # Design decision: rely on the default unknown-command reply
//
// These commands are intentionally NOT registered in the command table. The
// router's Dispatch (router.go) already replies to any unregistered command with
// the byte-for-byte "-ERR unknown command '<name>'\r\n", echoing the command name
// exactly as the client sent it (requirement 3.3 / 4.8). Leaving these families
// unregistered therefore routes them straight through that single, well-tested
// rejection path — no per-command handler is required.
//
// Why this is the correct choice for byte-for-byte oracle parity:
//
//   - For the families Pika v3.2.2 genuinely lacks — Lua (EVAL/EVALSHA/SCRIPT),
//     Streams (X*), and the blocking pops (BLPOP/BRPOP/BRPOPLPUSH) — Pika itself
//     replies "-ERR unknown command '<name>'". The default path reproduces that
//     reply verbatim, so the proxy is byte-for-byte identical to the oracle
//     (requirement 4.8).
//   - For the families Pika v3.2.2 DOES implement — Pub/Sub, transactions, bit
//     operations, PF* (HyperLogLog), GEO*, and FLUSHALL/FLUSHDB — byte-for-byte
//     parity is impossible without actually executing the command, which is an
//     explicit non-goal (design "非目标"). Requirements 4.1–4.7 only require that
//     these commands return an ERROR and never silently downgrade; the
//     unknown-command reply satisfies that exactly. Reproducing Pika's real reply
//     (e.g. FLUSHALL's "+OK") would BE the silent downgrade the requirement
//     forbids, so a hard error is the intended behaviour, and 4.8's parity clause
//     scopes byte-for-byte matching to "任意不在命令矩阵内的命令" (the generic
//     catch-all), not to these explicitly-declined families.
//
// The alternative — registering each command with a dedicated rejection handler
// (as keys.go does for KEYS/RENAME, which have proxy-specific semantics) — was
// rejected here because it would (a) add handler surface with no behavioural
// benefit over the existing default and (b) DIVERGE from Pika's own
// unknown-command text for the families Pika lacks, breaking 4.8 parity for
// EVAL/X*/B* without gaining anything for the rest.
//
// This decision is load-bearing: it must not be silently undone. TestUnsupported*
// (unsupported_test.go) asserts (1) each family is rejected with the exact
// unknown-command reply and is never silently accepted, and (2) none of these
// names is registered on a fully wired storage-backed router — so if a future
// task accidentally registers one (e.g. implementing SETBIT), the guard fails
// loudly rather than letting an unsupported command slip through.

// UnsupportedCommands enumerates the commands redimos explicitly does not support
// (requirement 4.1–4.7), grouped by family. Names are the canonical uppercase
// spellings. The list is the single source of truth shared by the guard tests
// here and by the differential-parity tests (task 18.3), so the two never drift.
//
// It deliberately does NOT include commands that merely have proxy-specific
// rejections with their own handlers (KEYS, RENAME/RENAMENX — see keys.go) or the
// Redis admin/replication commands Pika 3.2.2 also lacks (they too fall through to
// the default unknown-command path but are outside requirement 4's families).
var UnsupportedCommands = []string{
	// Pub/Sub (requirement 4.1).
	"SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PUBLISH", "PUBSUB",

	// Lua scripting (requirement 4.2).
	"EVAL", "EVALSHA", "SCRIPT",

	// Transactions (requirement 4.3).
	"MULTI", "EXEC", "WATCH", "UNWATCH", "DISCARD",

	// Blocking list pops (requirement 4.4).
	"BLPOP", "BRPOP", "BRPOPLPUSH",

	// Bit operations (requirement 4.5).
	"SETBIT", "GETBIT", "BITCOUNT", "BITOP", "BITPOS",

	// HyperLogLog (requirement 4.6).
	"PFADD", "PFCOUNT", "PFMERGE",

	// GEO (requirement 4.6).
	"GEOADD", "GEODIST", "GEOPOS", "GEOHASH", "GEORADIUS", "GEORADIUSBYMEMBER",

	// Streams (requirement 4.6).
	"XADD", "XLEN", "XRANGE", "XREVRANGE", "XREAD", "XDEL", "XTRIM", "XINFO",

	// Whole-DB flush, declined in P0 (requirement 4.7).
	"FLUSHALL", "FLUSHDB",
}
