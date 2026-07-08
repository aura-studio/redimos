package command

import (
	"context"
	"math"

	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// This file implements the Key-management commands DEL, EXISTS and TYPE
// (requirements 10.1, 10.2, 10.3) plus the EXPIRE family (EXPIRE/EXPIREAT/
// PEXPIRE/PEXPIREAT) and TTL/PTTL/PERSIST (requirements 10.4–10.7, 10.11). It is
// the first Key-family family and, like
// strings.go, reads existence/expiry through the meta item — the source of truth
// for a key's logical presence — so a key whose meta.exp has passed is treated as
// absent regardless of when DynamoDB's native TTL physically removes it.
//
// All three commands operate purely on the meta item (no data-value read), which
// is what makes EXISTS and TYPE O(1) per key. DEL additionally removes the meta
// item and hands the pk to the lazy-delete enqueuer so the key's members are
// reclaimed off the request path (design's惰性删除 path; requirement 10.1). The
// enqueuer is wired via the Storage.Enqueuer seam (router_storage.go); when none
// is configured DeleteMeta still removes meta correctly and the weekly sweeper
// (task 11.2) is the backstop for the orphaned members.

// registerKeys installs the Key-management command family on the router's table.
// It is invoked from registerDataCommands (router_storage.go). Arity counts
// include the command name; DEL is marked Write (it mutates state), while EXISTS
// and TYPE are read-only.
func (r *Router) registerKeys() {
	r.reg("DEL", -2, true, r.handleDel)
	r.reg("EXISTS", -2, false, r.handleExists)
	// TOUCH counts how many of the given keys exist. redimos has no LRU/idle clock
	// to update, so it is byte-for-byte identical to EXISTS (existence count with
	// multiplicity), matching Redis 3.2's reply.
	r.reg("TOUCH", -2, false, r.handleExists)
	r.reg("TYPE", 2, false, r.handleType)
	r.reg("EXPIRE", 3, true, r.handleExpire)
	r.reg("EXPIREAT", 3, true, r.handleExpireAt)
	r.reg("PEXPIRE", 3, true, r.handlePExpire)
	r.reg("PEXPIREAT", 3, true, r.handlePExpireAt)
	r.reg("TTL", 2, false, r.handleTTL)
	r.reg("PTTL", 2, false, r.handlePTTL)
	r.reg("PERSIST", 2, true, r.handlePersist)
	// SCAN is the cursor-based keyspace iterator (task 17.2); its handler lives in
	// scan.go. Arity -2 so a bare SCAN still gets the wrong-number-of-arguments
	// reply while SCAN cursor [MATCH p] [COUNT n] reaches the handler. Read-only.
	r.reg("SCAN", -2, false, r.handleScan)
	// KEYS and RENAME/RENAMENX are registered only to give them a first-class,
	// byte-for-byte rejection (requirements 10.9, 10.10) rather than the generic
	// "unknown command" reply. See handleKeys / handleRename below.
	r.reg("KEYS", 2, false, r.handleKeys)
	r.reg("RENAME", 3, true, r.handleRename)
	r.reg("RENAMENX", 3, true, r.handleRename)
	// FLUSHALL / FLUSHDB are registered only to give them a first-class proxy
	// rejection rather than the generic "unknown command" reply: flushing the
	// keyspace would mean a full wipe of the shared DynamoDB table. Arity 1 matches
	// Redis 3.2 (the command takes no arguments).
	r.reg("FLUSHALL", 1, true, r.handleFlush)
	r.reg("FLUSHDB", 1, true, r.handleFlush)
}

// Rejection error texts for the guarded / unsupported Key commands. These are
// proxy-specific (they have no Pika v3.2.2 oracle counterpart, since this proxy
// deliberately declines these commands), so they live here rather than in the
// resp oracle-parity constant block. The bodies omit the leading '-' and
// trailing CRLF, matching the resp.ErrXxx convention.
const (
	// errKeysOpsOnly rejects KEYS. Per design ("KEYS: 全表 Scan，默认拒绝无
	// MATCH，仅运维用途") and requirement 10.9, KEYS would require an unbounded
	// full-table Scan and is therefore not exposed as a general client command;
	// it is reserved for operational use. Clients that need to iterate the
	// keyspace must use SCAN (cursor registry, task 17), which pages bounded
	// batches and supports MATCH-side filtering.
	errKeysOpsOnly = "ERR KEYS is disabled on this proxy (operations-only full scan); use SCAN to iterate the keyspace"

	// errRenameUnsupported rejects RENAME/RENAMENX. Per design (RENAME/RENAMENX
	// marked ❌ "P0：整集合复制代价高，M0 摸底后再评估") and requirement 10.10,
	// renaming a key would require copying an entire collection's members under
	// a new pk, which is not supported in P0.
	errRenameUnsupported = "ERR RENAME/RENAMENX is not supported"

	// errFlushDisabled rejects FLUSHALL / FLUSHDB. Real Redis flushes the keyspace
	// and replies "+OK"; on this proxy that would mean wiping the whole shared
	// DynamoDB table, so the command is declined with a descriptive error rather
	// than silently performing (or silently downgrading) a destructive full flush.
	errFlushDisabled = "ERR FLUSHALL/FLUSHDB is disabled on this proxy (would wipe the whole DynamoDB table)"
)

// handleKeys implements KEYS pattern (requirement 10.9). This proxy treats KEYS
// as an operations-only, guarded command: serving it for an arbitrary client
// would mean an unbounded full-table Scan of the backend (design "KEYS: 全表
// Scan，默认拒绝无 MATCH，仅运维用途"). Rather than silently perform that
// expensive scan, KEYS is rejected with a descriptive error that points clients
// at SCAN (the bounded, cursor-based iterator, task 17). Registered with arity 2
// so that a bare KEYS still gets the standard wrong-number-of-arguments reply
// (requirement 3.2) while KEYS <pattern> (including the classic full-scan
// KEYS *) is declined here.
func (r *Router) handleKeys(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).Error(errKeysOpsOnly)
}

// handleRename implements the P0 rejection of RENAME key newkey and
// RENAMENX key newkey (requirement 10.10). Both commands would require copying
// every member of a (potentially large) collection to a new pk and atomically
// swapping meta, which is out of scope for P0 (design marks them ❌). Registered
// with arity 3 so a malformed RENAME still gets the standard
// wrong-number-of-arguments reply (requirement 3.2) before this rejection.
func (r *Router) handleRename(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).Error(errRenameUnsupported)
}

// handleFlush rejects FLUSHALL / FLUSHDB. Registered (arity 1) so these
// destructive full-flush commands get a first-class proxy rejection instead of
// the generic unknown-command reply; a bare-vs-argument mismatch still yields the
// standard wrong-number-of-arguments error first.
func (r *Router) handleFlush(_ context.Context, c *server.Conn, _ [][]byte) {
	resp.NewWriter(c.Redcon()).Error(errFlushDisabled)
}

// handleDel implements DEL key [key ...] (requirement 10.1). For each key it
// removes the meta item — making the key immediately logically absent — and
// enqueues the pk for asynchronous member deletion (via MetaStore.DeleteMeta).
// The reply is the integer count of keys that existed (were live, i.e. present
// and not expired) before deletion; an already-expired or absent key does not
// count, matching Redis/Pika. Expired-but-present keys are still cleaned up: their
// meta is removed and members enqueued, they simply do not contribute to the
// count. Duplicate keys in the argument list are processed (and counted)
// independently, as Redis does.
func (r *Router) handleDel(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	now := r.now()

	var deleted int64
	for _, key := range args[1:] {
		pk := r.encodePK(c.DB(), key)

		m, found, err := r.Storage.Meta.Load(ctx, pk)
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		live := found && !meta.IsExpired(m, now)

		// Remove the meta item and enqueue member cleanup regardless of expiry, so
		// an expired-but-present key's data is still reclaimed even though it does
		// not count toward the reply.
		if _, err := r.Storage.Meta.DeleteMeta(ctx, pk); err != nil {
			r.writeStoreError(c, err)
			return
		}

		if live {
			deleted++
		}
	}

	w.Int(deleted)
}

// handleExists implements EXISTS key [key ...] (requirement 10.2). It replies the
// integer count of keys that currently exist (are live: present and not expired),
// determined by an O(1) meta load + expiry check per key. A repeated key is
// counted once per occurrence, matching Redis/Pika (EXISTS k k returns 2 when k
// exists).
func (r *Router) handleExists(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())

	var count int64
	for _, key := range args[1:] {
		pk := r.encodePK(c.DB(), key)

		live, err := r.keyLive(ctx, pk)
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if live {
			count++
		}
	}

	w.Int(count)
}

// handleType implements TYPE key (requirement 10.3). It reads the key's meta.t and
// replies the Redis type name as a Simple String: "+string"/"+hash"/"+list"/
// "+set"/"+zset". A missing or expired key replies "+none", matching Redis/Pika.
func (r *Router) handleType(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := r.encodePK(c.DB(), args[1])

	m, found, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !found || meta.IsExpired(m, r.now()) {
		w.SimpleString("none")
		return
	}

	w.SimpleString(redisTypeName(m.Type))
}

// redisTypeName maps a meta KeyType to the Redis type name TYPE replies with. The
// meta layer records the String type as "str"; TYPE surfaces it as "string" to
// match Redis/Pika. The collection types share their meta spelling with the wire
// name. An unrecognized type falls back to "none" (defensive; the meta layer only
// ever writes the known set).
func redisTypeName(t meta.KeyType) string {
	switch t {
	case meta.TypeString:
		return "string"
	case meta.TypeHash:
		return "hash"
	case meta.TypeList:
		return "list"
	case meta.TypeSet:
		return "set"
	case meta.TypeZSet:
		return "zset"
	default:
		return "none"
	}
}

// --- EXPIRE family & TTL/PTTL/PERSIST (requirements 10.4, 10.5, 10.6, 10.7,
// 10.11) --------------------------------------------------------------------
//
// All of these commands touch only the key's meta item (meta.exp), never its
// data members, which is what makes them O(1) and — per requirement 10.11 — apply
// uniformly to a key of ANY type (String or collection): the meta item is the
// single source of a key's logical presence and expiry, so setting/reading/
// clearing exp expires or persists the whole key regardless of what its members
// look like.
//
// Existence and expiry are always evaluated from the meta item via keyLive /
// meta.IsExpired against the router's injected clock, independent of when
// DynamoDB's native TTL physically removes the item (design "正确性完全由读路径过滤保证").
//
// Past-expiry semantics (documented choice): EXPIRE/EXPIREAT/PEXPIRE/PEXPIREAT
// with a resolved absolute expiry that is <= now (a time in the past, e.g.
// EXPIRE with a negative TTL or EXPIREAT with an already-elapsed timestamp) delete
// a live key immediately and reply :1, matching Redis/Pika (where an expire in the
// past deletes the key). We DELETE rather than write the past exp for two reasons:
// (1) it is the exact Redis behaviour — the key must be gone, and DeleteMeta makes
// it immediately logically absent while enqueuing member cleanup; and (2) the meta
// model treats exp == 0 as "never expires" (meta.IsExpired requires exp > 0), so a
// resolved expiry of 0 or a negative epoch could not be represented as an
// "already expired" exp — deleting avoids that ambiguity entirely. An absent (or
// already-expired) key is never created/modified and replies :0.

// applyExpire is the shared implementation of EXPIRE/EXPIREAT/PEXPIRE/PEXPIREAT.
// It resolves the command's time argument to an absolute expiry in epoch seconds
// (via resolveExp, which already applies the command's unit and second-truncation)
// and then:
//
//   - replies :0 when the key is absent or already expired (nothing is written);
//   - deletes the key and replies :1 when the resolved expiry is in the past
//     (expEpoch <= now), per the past-expiry semantics documented above;
//   - otherwise writes meta.exp (O(1)) and replies :1.
//
// A non-integer / out-of-range time argument replies the byte-for-byte
// "-ERR value is not an integer or out of range" (requirement 3.4).
func (r *Router) applyExpire(ctx context.Context, c *server.Conn, args [][]byte, resolveExp func(now, arg int64) int64) {
	w := resp.NewWriter(c.Redcon())

	arg, err := ParseInt(args[2])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}

	pk := r.encodePK(c.DB(), args[1])
	now := r.now()

	live, err := r.keyLive(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !live {
		w.Int(0)
		return
	}

	expEpoch := resolveExp(now, arg)

	// Resolved expiry in the past (or exactly now): delete the live key now and
	// reply :1 (Redis/Pika past-expiry semantics).
	if expEpoch <= now {
		if _, err := r.Storage.Meta.DeleteMeta(ctx, pk); err != nil {
			r.writeStoreError(c, err)
			return
		}
		w.Int(1)
		return
	}

	found, err := r.Storage.Meta.SetExpire(ctx, pk, expEpoch)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !found {
		// Defensive: keyLive saw the key but it vanished before the write. Serial
		// per-connection execution makes this unreachable today.
		w.Int(0)
		return
	}
	w.Int(1)
}

// handleExpire implements EXPIRE key seconds (requirement 10.4): set the key's
// expiry to now + seconds. A negative/zero seconds resolves to a past expiry and
// deletes a live key (see applyExpire).
func (r *Router) handleExpire(ctx context.Context, c *server.Conn, args [][]byte) {
	r.applyExpire(ctx, c, args, secExpiryEpoch)
}

// msDeadlineToSec converts an absolute millisecond expiry deadline to whole epoch
// seconds. The proxy stores expiry at second precision (Pika v3.2.2 has no sub-second
// TTL), so the deadline is truncated to seconds — BUT with one guard: a deadline that is
// in the FUTURE yet less than a second away truncates back to the current second and
// would be read as already-expired, so a positive sub-second TTL (PEXPIRE k 200, SET k v
// PX 200, PSETEX k 200 v) would DELETE the key immediately. Redis instead keeps the key
// alive for the requested ~200ms. To avoid that harmful instant/early expiry, a future
// deadline that truncates to <= now is clamped up to now+1, so the key lives ~1s rather
// than vanishing. A deadline already in the past is left truncated (<= now) so PEXPIREAT
// with a past timestamp still deletes the key. This does not add millisecond precision;
// it only removes the surprising sub-second instant-death within the second-precision model.
func msDeadlineToSec(now, deadlineMs int64) int64 {
	sec := deadlineMs / 1000
	if deadlineMs > now*1000 && sec <= now {
		return now + 1
	}
	return sec
}

// secExpiryEpoch resolves a relative expiry in SECONDS (EXPIRE / SET EX / SETEX) to an
// absolute epoch in seconds. Redis carries expiry in the millisecond domain, so a value so
// large that now*1000 + seconds*1000 overflows int64 is UB in Redis (it wraps to an
// arbitrary deadline; empirically Redis DELETES such a key — EXPIRE k 9223372036854775807
// leaves TTL -2). redimos does not replicate that C undefined behaviour (cf. the
// SELECT/DECRBY int64-min cases in §4.6): an overflowing expiry deterministically resolves
// to `now`, so the key is immediately expired (deleted / TTL -2, GET nil) instead of
// overflowing now+n into a bogus permanent or negative-TTL key. A non-overflowing value —
// including a negative or zero one — resolves to now+n, which applyExpire deletes when it is
// <= now (EXPIRE 0 / a negative TTL delete the key, matching Redis).
func secExpiryEpoch(now, n int64) int64 {
	if n > math.MaxInt64/1000-now { // now*1000 + n*1000 would overflow the ms domain
		return now
	}
	return now + n
}

// msExpiryEpoch resolves a relative expiry in MILLISECONDS (PEXPIRE / SET PX / PSETEX) to an
// absolute epoch in seconds. Like secExpiryEpoch it resolves an ms-domain overflow to an
// immediately-expired `now` rather than replicating Redis' UB wrap; otherwise it truncates
// to seconds via msDeadlineToSec (which also lifts a positive sub-second TTL to now+1 so it
// does not instant-delete the key). Negative ms resolve to a past second and delete the key.
func msExpiryEpoch(now, n int64) int64 {
	if n > math.MaxInt64-now*1000 { // now*1000 + n would overflow
		return now
	}
	return msDeadlineToSec(now, now*1000+n)
}

// handleExpireAt implements EXPIREAT key timestamp (requirement 10.4): set the key's expiry
// to the absolute epoch-seconds timestamp. A timestamp <= now deletes a live key (see
// applyExpire). A timestamp so large that ts*1000 overflows the millisecond domain is
// deleted immediately, matching Redis (EXPIREAT k 9223372036854775807 leaves TTL -2) rather
// than storing a bogus far-future expiry.
func (r *Router) handleExpireAt(ctx context.Context, c *server.Conn, args [][]byte) {
	r.applyExpire(ctx, c, args, func(now, ts int64) int64 {
		if ts > math.MaxInt64/1000 {
			return now
		}
		return ts
	})
}

// handlePExpire implements PEXPIRE key milliseconds (requirements 10.4, 10.5): set the
// key's expiry to now + milliseconds, truncated to whole seconds (Pika v3.2.2 has no
// millisecond precision) — sharing the SET PX / PSETEX computation (msExpiryEpoch), so a
// positive sub-second PEXPIRE keeps the key alive and an overflowing one deletes it.
func (r *Router) handlePExpire(ctx context.Context, c *server.Conn, args [][]byte) {
	r.applyExpire(ctx, c, args, msExpiryEpoch)
}

// handlePExpireAt implements PEXPIREAT key ms-timestamp (requirements 10.4, 10.5):
// set the key's expiry to the absolute millisecond timestamp, truncated to whole
// seconds. A resolved second <= now deletes a live key (see applyExpire).
func (r *Router) handlePExpireAt(ctx context.Context, c *server.Conn, args [][]byte) {
	r.applyExpire(ctx, c, args, func(now, msTS int64) int64 { return msDeadlineToSec(now, msTS) })
}

// handleTTL implements TTL key (requirements 10.6, 10.11): reply the key's
// remaining time-to-live in whole seconds. It replies -2 when the key is absent or
// already expired, -1 when the key exists but has no expiry, else the remaining
// seconds (meta.exp - now, always > 0 here since the key is live with exp > now).
func (r *Router) handleTTL(ctx context.Context, c *server.Conn, args [][]byte) {
	r.replyTTL(ctx, c, args, false)
}

// handlePTTL implements PTTL key (requirements 10.6, 10.11): as TTL but in
// milliseconds. Because the proxy stores expiry at second precision, the remaining
// milliseconds are the remaining whole seconds * 1000.
func (r *Router) handlePTTL(ctx context.Context, c *server.Conn, args [][]byte) {
	r.replyTTL(ctx, c, args, true)
}

// replyTTL is the shared implementation of TTL/PTTL. millis selects the unit of
// the remaining-time reply.
func (r *Router) replyTTL(ctx context.Context, c *server.Conn, args [][]byte, millis bool) {
	w := resp.NewWriter(c.Redcon())
	pk := r.encodePK(c.DB(), args[1])
	now := r.now()

	m, found, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !found || meta.IsExpired(m, now) {
		w.Int(-2)
		return
	}
	if m.Exp == 0 {
		w.Int(-1)
		return
	}

	remaining := m.Exp - now
	if millis {
		remaining *= 1000
	}
	w.Int(remaining)
}

// handlePersist implements PERSIST key (requirement 10.7): remove the key's expiry
// (meta.exp). It replies :1 when the key existed, was live, and had an expiry that
// was removed; it replies :0 when the key is absent/expired or had no expiry to
// remove. Applies to a key of any type (requirement 10.11).
func (r *Router) handlePersist(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := r.encodePK(c.DB(), args[1])
	now := r.now()

	m, found, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	// Absent, already expired, or never had an expiry: nothing to persist.
	if !found || meta.IsExpired(m, now) || m.Exp == 0 {
		w.Int(0)
		return
	}

	if _, err := r.Storage.Meta.Persist(ctx, pk); err != nil {
		r.writeStoreError(c, err)
		return
	}
	w.Int(1)
}
