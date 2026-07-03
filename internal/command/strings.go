package command

import (
	"context"
	"math"
	"strconv"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

// This file implements the String read/write commands GET, SET (with EX/PX/NX/XX),
// SETNX, SETEX, PSETEX and GETSET (requirements 5.1–5.5). It is the first
// data-command family and establishes the write-path pattern every later family
// follows:
//
//	guard.CheckWrite(...)                          // size limits, no partial write
//	  -> r.Storage.Meta.EnsureType(TypeString, 0)  // create key + type check
//	  -> r.Storage.Store.Set/GetSetString(...)     // data value write
//	  -> r.Storage.Meta.SetExpire / Persist(...)   // TTL adjustment
//
// Reads go through the meta read path (meta.ReadPath) so existence and expiry are
// enforced from the meta item, independent of DynamoDB native-TTL timing.
//
// NX/XX/SETNX condition on *logical* key existence (the meta item), which is why
// the existence gate runs before EnsureType: SET NX / SETNX on an existing key of
// any type is a plain rejection (never WRONGTYPE), matching Redis. The gate is a
// meta read followed by the conditional write; true cross-item atomicity would
// need a transaction and is deferred (P0 serves each connection serially).

// registerStrings installs the String command family on the router's table. It is
// invoked from registerDataCommands (router_storage.go). Arity counts include the
// command name; Write marks the mutating commands for the dual-write/consistency
// policy in later tasks.
func (r *Router) registerStrings() {
	t := r.Table
	t.Register("GET", 2, false, r.handleGet)
	t.Register("SET", -3, true, r.handleSet)
	t.Register("SETNX", 3, true, r.handleSetNX)
	t.Register("SETEX", 4, true, r.handleSetEX)
	t.Register("PSETEX", 4, true, r.handlePSetEX)
	t.Register("GETSET", 3, true, r.handleGetSet)
	t.Register("MGET", -2, false, r.handleMGet)
	t.Register("MSET", -3, true, r.handleMSet)
	t.Register("INCR", 2, true, r.handleIncr)
	t.Register("DECR", 2, true, r.handleDecr)
	t.Register("INCRBY", 3, true, r.handleIncrBy)
	t.Register("DECRBY", 3, true, r.handleDecrBy)
	t.Register("INCRBYFLOAT", 3, true, r.handleIncrByFloat)
	t.Register("APPEND", 3, true, r.handleAppend)
	t.Register("STRLEN", 2, false, r.handleStrlen)
	t.Register("SETRANGE", 4, true, r.handleSetRange)
	t.Register("GETRANGE", 4, false, r.handleGetRange)
}

// msetBatchKeys bounds how many keys MSET writes per TransactWriteItems batch
// (design: MSET writes ≤25/batch). A command with more keys is split into several
// batches; overall atomicity across batches is NOT guaranteed (requirement 5.7).
const msetBatchKeys = 25

// strData carries the String read-path data result so the handler can distinguish
// a present-but-empty value ("$0") from a missing value item ("$-1") even though
// the meta item reports the key as live.
type strData struct {
	val   []byte
	found bool
}

// handleGet implements GET (requirement 5.1). It runs the parallel meta+data read
// path: when the key's meta is absent or expired the key is treated as
// non-existent and the reply is the null bulk string "$-1"; otherwise the stored
// value is returned as a bulk string. A live meta whose value item is missing
// (e.g. a non-String key, whose value lives under other sort keys) also replies
// "$-1".
func (r *Router) handleGet(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	// A live key of a non-String type must reply WRONGTYPE, not the null bulk
	// string — GET is type-checked from the key's meta exactly like its String
	// siblings STRLEN and GETRANGE. An absent or expired key, or a live String
	// whose value item is missing (e.g. a partial write that never landed), replies
	// "$-1"; an empty String value replies the empty bulk string.
	m, ok, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !ok || meta.IsExpired(m, r.now()) {
		w.NullBulk()
		return
	}
	if m.Type != meta.TypeString {
		w.Error(resp.ErrWrongType)
		return
	}

	v, found, err := r.Storage.Store.GetString(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !found {
		w.NullBulk()
		return
	}

	w.BulkString(v)
}

// setOptions holds the parsed SET optional arguments.
type setOptions struct {
	nx bool
	xx bool

	// expSet is true when EX or PX was supplied; expEpoch is then the absolute
	// expiry in epoch seconds to write to meta.exp. When expSet is false the SET
	// clears any existing TTL (Redis/Pika SET semantics).
	expSet   bool
	expEpoch int64
}

// parseSetOptions parses the optional SET arguments following "SET key value".
// now is the current epoch seconds, used to turn a relative EX/PX interval into
// the absolute meta.exp. It returns the parsed options and an empty errMsg on
// success; on failure errMsg is the RESP2 error body to reply (syntax error,
// not-an-integer, or invalid-expire-time) and the options are unusable.
//
// Recognized tokens (case-insensitive): EX <seconds>, PX <milliseconds>, NX, XX.
// EX and PX are mutually exclusive, as are NX and XX; a repeated or conflicting
// option, an unknown token, or a missing EX/PX value is a syntax error. A
// non-integer EX/PX value is the not-an-integer error; a non-positive value is
// the invalid-expire-time error.
func parseSetOptions(opts [][]byte, now int64) (setOptions, string) {
	var o setOptions

	for i := 0; i < len(opts); i++ {
		switch toLower(string(opts[i])) {
		case "nx":
			if o.xx || o.nx {
				return setOptions{}, resp.ErrSyntax
			}
			o.nx = true
		case "xx":
			if o.nx || o.xx {
				return setOptions{}, resp.ErrSyntax
			}
			o.xx = true
		case "ex", "px":
			isMillis := toLower(string(opts[i])) == "px"
			if o.expSet || i+1 >= len(opts) {
				return setOptions{}, resp.ErrSyntax
			}
			n, err := ParseInt(opts[i+1])
			if err != nil {
				return setOptions{}, resp.ErrNotInteger
			}
			if n <= 0 {
				return setOptions{}, resp.ErrInvalidExpireTime("set")
			}
			o.expSet = true
			if isMillis {
				// Absolute expiry in epoch seconds, truncating sub-second
				// precision (Pika v3.2.2 has no millisecond precision).
				o.expEpoch = (now*1000 + n) / 1000
			} else {
				o.expEpoch = now + n
			}
			i++ // consume the value argument.
		default:
			return setOptions{}, resp.ErrSyntax
		}
	}

	return o, ""
}

// overwriteAnyType prepares a destructive String write (plain SET / SETEX /
// PSETEX): in Redis these commands replace a key of ANY type, so a key currently
// holding a live non-String value must be removed first — its meta is dropped and
// its members are enqueued for reclaim, exactly like DEL — so the subsequent
// EnsureType(String) creates a fresh string instead of failing WRONGTYPE. A
// String, absent, or already-expired key needs no action. (This mirrors Redis'
// type-agnostic SET; it is not used by SETNX, which never overwrites, nor by
// GETSET, which reads the old value as a string and so keeps WRONGTYPE.)
func (r *Router) overwriteAnyType(ctx context.Context, pk string) error {
	m, ok, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		return err
	}
	if !ok || meta.IsExpired(m, r.now()) || m.Type == meta.TypeString {
		return nil
	}
	_, err = r.Storage.Meta.DeleteMeta(ctx, pk)
	return err
}

// handleSet implements SET with the EX/PX/NX/XX options (requirements 5.2, 5.3,
// 5.4). On success it replies "+OK"; an NX rejection (key exists) or XX rejection
// (key absent) replies the null bulk string "$-1". EX/PX write meta.exp; a SET
// without EX/PX clears any existing TTL to match Redis/Pika semantics. A plain SET
// over a key of another type overwrites it (Redis SET is type-agnostic).
func (r *Router) handleSet(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key, val := args[1], args[2]

	o, errMsg := parseSetOptions(args[3:], r.now())
	if errMsg != "" {
		w.Error(errMsg)
		return
	}

	if err := guard.CheckWrite(key, nil, [][]byte{val}); err != nil {
		r.writeStoreError(c, err)
		return
	}

	pk := encodePK(c.DB(), key)

	// NX/XX gate on logical existence before any write (see file header).
	if o.nx || o.xx {
		live, err := r.keyLive(ctx, pk)
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if (o.nx && live) || (o.xx && !live) {
			w.NullBulk()
			return
		}
	}

	if err := r.overwriteAnyType(ctx, pk); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.Storage.Store.SetString(ctx, pk, val); err != nil {
		r.writeStoreError(c, err)
		return
	}

	if o.expSet {
		if _, err := r.Storage.Meta.SetExpire(ctx, pk, o.expEpoch); err != nil {
			r.writeStoreError(c, err)
			return
		}
	} else if _, err := r.Storage.Meta.Persist(ctx, pk); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.SimpleString("OK")
}

// handleSetNX implements SETNX (requirement 5.5): set the key only if it does not
// already exist, replying ":1" when set and ":0" when the key already exists (of
// any type — a rejection, never WRONGTYPE). A new key carries no TTL.
func (r *Router) handleSetNX(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key, val := args[1], args[2]

	if err := guard.CheckWrite(key, nil, [][]byte{val}); err != nil {
		r.writeStoreError(c, err)
		return
	}

	pk := encodePK(c.DB(), key)

	live, err := r.keyLive(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if live {
		w.Int(0)
		return
	}

	if err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.Storage.Store.SetString(ctx, pk, val); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(1)
}

// handleSetEX implements SETEX key seconds value (requirement 5.5): an atomic set
// with a second-precision expiry. A non-integer seconds argument replies the
// not-an-integer error; a non-positive seconds replies the invalid-expire-time
// error. On success it replies "+OK".
func (r *Router) handleSetEX(ctx context.Context, c *server.Conn, args [][]byte) {
	r.setWithExpiry(ctx, c, args[1], args[3], args[2], false, "setex")
}

// handlePSetEX implements PSETEX key milliseconds value (requirement 5.5): as
// SETEX but the expiry is given in milliseconds, truncated to whole seconds
// (Pika v3.2.2 has no millisecond precision).
func (r *Router) handlePSetEX(ctx context.Context, c *server.Conn, args [][]byte) {
	r.setWithExpiry(ctx, c, args[1], args[3], args[2], true, "psetex")
}

// setWithExpiry is the shared implementation of SETEX/PSETEX. cmd is the lowercase
// command name used in the invalid-expire-time error text; isMillis selects
// millisecond vs second interpretation of the expiry argument.
func (r *Router) setWithExpiry(ctx context.Context, c *server.Conn, key, val, expiryArg []byte, isMillis bool, cmd string) {
	w := resp.NewWriter(c.Redcon())

	n, err := ParseInt(expiryArg)
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}
	if n <= 0 {
		w.Error(resp.ErrInvalidExpireTime(cmd))
		return
	}

	if gerr := guard.CheckWrite(key, nil, [][]byte{val}); gerr != nil {
		r.writeStoreError(c, gerr)
		return
	}

	now := r.now()
	var expEpoch int64
	if isMillis {
		expEpoch = (now*1000 + n) / 1000
	} else {
		expEpoch = now + n
	}

	pk := encodePK(c.DB(), key)

	if err := r.overwriteAnyType(ctx, pk); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.Storage.Store.SetString(ctx, pk, val); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if _, err := r.Storage.Meta.SetExpire(ctx, pk, expEpoch); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.SimpleString("OK")
}

// handleGetSet implements GETSET key value (requirement 5.5): atomically set the
// key to value and return its previous value as a bulk string, or the null bulk
// string "$-1" when the key had no previous value. GETSET is type-checked like a
// String write (a wrong-type key replies WRONGTYPE) and, like SET, clears any
// existing TTL.
func (r *Router) handleGetSet(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key, val := args[1], args[2]

	if err := guard.CheckWrite(key, nil, [][]byte{val}); err != nil {
		r.writeStoreError(c, err)
		return
	}

	pk := encodePK(c.DB(), key)

	if err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); err != nil {
		r.writeStoreError(c, err)
		return
	}

	old, existed, err := r.Storage.Store.GetSetString(ctx, pk, val)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	if _, err := r.Storage.Meta.Persist(ctx, pk); err != nil {
		r.writeStoreError(c, err)
		return
	}

	if !existed {
		w.NullBulk()
		return
	}

	w.BulkString(old)
}

// handleMGet implements MGET key [key ...] (requirement 5.6). It replies an array
// with one element per requested key, in request order: the key's value as a bulk
// string when the key is a live String, or the null bulk string "$-1" when the key
// is missing, expired, or holds a non-String type (matching Redis, where MGET
// yields nil for any key that is absent or not a string).
//
// Per-key existence/expiry is enforced from the meta item (the source of logical
// truth), independent of DynamoDB native-TTL timing: each key's meta is loaded and
// evaluated with meta.IsExpired against the router's clock. Only the pks that are
// live Strings are handed to the batched value read (Store.MGetStrings), which
// fetches them in BatchGetItem-sized chunks of ≤100; the reply is then assembled
// back in the original request order.
func (r *Router) handleMGet(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	keys := args[1:]
	now := r.now()

	// First pass: resolve each key's pk and decide whether it is a live String,
	// collecting the live pks for a single batched value read.
	pks := make([]string, len(keys))
	liveStr := make([]bool, len(keys))
	toFetch := make([]string, 0, len(keys))
	for i, key := range keys {
		pk := encodePK(c.DB(), key)
		pks[i] = pk

		m, found, err := r.Storage.Meta.Load(ctx, pk)
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if found && !meta.IsExpired(m, now) && m.Type == meta.TypeString {
			liveStr[i] = true
			toFetch = append(toFetch, pk)
		}
	}

	vals, err := r.Storage.Store.MGetStrings(ctx, toFetch)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	// Second pass: assemble the reply in request order. A live String whose value
	// item is somehow absent still surfaces as a null bulk string.
	elems := make([][]byte, len(keys))
	present := make([]bool, len(keys))
	for i := range keys {
		if !liveStr[i] {
			continue
		}
		if v, ok := vals[pks[i]]; ok {
			elems[i] = v
			present[i] = true
		}
	}

	w.OptBulkArray(elems, present)
}

// handleMSet implements MSET key value [key value ...] (requirement 5.7). An odd
// number of key/value arguments replies the wrong-number-of-arguments error. Each
// pair is written like a plain SET: size guard, meta EnsureType(TypeString), value
// write, then TTL cleared. On success it replies "+OK".
//
// Writes are applied in batches of at most msetBatchKeys (25) keys, mirroring the
// design's TransactWriteItems ≤25/batch; a command with more keys is split into
// several batches. Overall atomicity across batches is NOT guaranteed
// (requirement 5.7): a failure partway through leaves earlier batches written.
// All value sizes are validated up front so a size-guard violation rejects the
// whole command without any partial write (requirement 14.3).
func (r *Router) handleMSet(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pairs := args[1:]

	if len(pairs)%2 != 0 {
		w.Error(resp.ErrWrongNumberOfArgs("mset"))
		return
	}

	// Validate every value's size before writing anything: a violation rejects the
	// entire command with no partial write.
	for i := 0; i < len(pairs); i += 2 {
		if err := guard.CheckWrite(pairs[i], nil, [][]byte{pairs[i+1]}); err != nil {
			r.writeStoreError(c, err)
			return
		}
	}

	// Apply the writes in ≤25-key batches (design: TransactWriteItems ≤25/batch).
	// pairs holds interleaved key/value entries, so one batch spans
	// msetBatchKeys*2 slice elements.
	for start := 0; start < len(pairs); start += msetBatchKeys * 2 {
		end := start + msetBatchKeys*2
		if end > len(pairs) {
			end = len(pairs)
		}
		for i := start; i < end; i += 2 {
			key, val := pairs[i], pairs[i+1]
			pk := encodePK(c.DB(), key)

			if err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); err != nil {
				r.writeStoreError(c, err)
				return
			}
			if err := r.Storage.Store.SetString(ctx, pk, val); err != nil {
				r.writeStoreError(c, err)
				return
			}
			if _, err := r.Storage.Meta.Persist(ctx, pk); err != nil {
				r.writeStoreError(c, err)
				return
			}
		}
	}

	w.SimpleString("OK")
}

// handleIncr implements INCR key (requirements 5.8, 5.9): increment the integer
// at key by 1. A missing key is treated as 0 before the increment. It replies the
// new value as an integer (":N").
func (r *Router) handleIncr(ctx context.Context, c *server.Conn, args [][]byte) {
	r.incrBy(ctx, c, args[1], 1)
}

// handleDecr implements DECR key (requirements 5.8, 5.9): decrement the integer at
// key by 1, replying the new value as an integer.
func (r *Router) handleDecr(ctx context.Context, c *server.Conn, args [][]byte) {
	r.incrBy(ctx, c, args[1], -1)
}

// handleIncrBy implements INCRBY key increment (requirements 5.8, 5.9). A
// non-integer increment argument replies "-ERR value is not an integer or out of
// range"; otherwise the key's integer value is increased by the amount and the new
// value replied as an integer.
func (r *Router) handleIncrBy(ctx context.Context, c *server.Conn, args [][]byte) {
	n, err := ParseInt(args[2])
	if err != nil {
		WriteNotInteger(c)
		return
	}
	r.incrBy(ctx, c, args[1], n)
}

// handleDecrBy implements DECRBY key decrement (requirements 5.8, 5.9). A
// non-integer decrement argument replies the not-an-integer error. The decrement
// is applied as a negative delta; a decrement of exactly the most negative int64
// cannot be negated without overflow and replies "-ERR decrement would overflow"
// (matching Redis), before any value is touched.
func (r *Router) handleDecrBy(ctx context.Context, c *server.Conn, args [][]byte) {
	n, err := ParseInt(args[2])
	if err != nil {
		WriteNotInteger(c)
		return
	}
	if n == math.MinInt64 {
		resp.NewWriter(c.Redcon()).Error(resp.ErrDecrOverflow)
		return
	}
	r.incrBy(ctx, c, args[1], -n)
}

// incrBy is the shared integer INCR/DECR write path (requirement 5.8). It applies
// the write-path pattern (size guard -> EnsureType(TypeString) -> atomic counter
// at the storage seam): the key is created as an empty String when absent, then
// Store.IncrBy adds delta to the integer value and returns the new total. A
// non-integer target value or an out-of-range result surfaces through
// writeStoreError as the byte-for-byte not-an-integer / overflow reply; a
// wrong-type key surfaces as WRONGTYPE from EnsureType. On success it replies the
// new value as a RESP2 integer (":N").
func (r *Router) incrBy(ctx context.Context, c *server.Conn, key []byte, delta int64) {
	w := resp.NewWriter(c.Redcon())

	if err := guard.CheckWrite(key, nil, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}

	pk := encodePK(c.DB(), key)

	if err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); err != nil {
		r.writeStoreError(c, err)
		return
	}

	next, err := r.Storage.Store.IncrBy(ctx, pk, delta)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(next)
}

// handleIncrByFloat implements INCRBYFLOAT key increment (requirements 5.8, 5.9).
// A non-float increment argument replies "-ERR value is not a valid float". The
// key's floating-point value is increased by the amount (a missing key starts at
// 0) and the new value is replied as a bulk string formatted the way Redis formats
// INCRBYFLOAT (shortest decimal, no exponent, trailing zeros trimmed). A
// non-float target value replies the not-a-valid-float error; a result that would
// be NaN or infinite replies "-ERR increment would produce NaN or Infinity".
func (r *Router) handleIncrByFloat(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())

	delta, ok := parseFloatArg(args[2])
	if !ok {
		w.Error(resp.ErrNotValidFloat)
		return
	}

	key := args[1]
	if err := guard.CheckWrite(key, nil, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}

	pk := encodePK(c.DB(), key)

	if err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); err != nil {
		r.writeStoreError(c, err)
		return
	}

	val, err := r.Storage.Store.IncrByFloat(ctx, pk, delta)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.BulkString(val)
}

// parseFloatArg parses an INCRBYFLOAT increment argument with Redis' semantics:
// finite or infinite decimals/exponents are accepted, NaN is rejected, and
// surrounding whitespace or trailing garbage is rejected (ParseFloat requires the
// whole string to be consumed). ok is false when the argument is not a valid
// float, mapping to "-ERR value is not a valid float".
func parseFloatArg(arg []byte) (float64, bool) {
	f, err := strconv.ParseFloat(string(arg), 64)
	if err != nil || math.IsNaN(f) {
		return 0, false
	}
	return f, true
}

// --- APPEND / STRLEN / SETRANGE / GETRANGE (requirements 5.10, 5.11, 16.4) ----
//
// APPEND and SETRANGE are read-modify-write String mutations: they read the
// current live value, compute the new value, then write it back. Per requirements
// 5.10 / 15.2 / 16.4 the write is a conditional compare-and-set on the value the
// read observed, retried on conflict, so concurrent read-modify-writes cannot
// silently lose an update: two connections appending to the same key each land in
// turn, the loser's conditional write fails, and it re-reads and recomputes on top
// of the winner's value. The loop is bounded by storage.MaxRMWRetries; a run that
// exhausts it (pathological hot-key contention) surfaces the generic retryable
// "-ERR" reply from storage.ErrRMWMaxRetries. This does NOT rely on read
// consistency — the condition is evaluated at write time against the current item
// (requirement 15.2) — so it is correct even on an eventually-consistent read.
//
// The base value is read through the meta layer (Load + IsExpired), so an absent
// or expired key contributes an empty base (APPEND creates the key, SETRANGE
// zero-pads from empty) exactly as Redis/Pika treat a missing key. The
// compare-and-set precondition, by contrast, is asserted on the PHYSICAL value
// item (its bytes, or its absence) the read observed, so an expired-but-not-yet-
// swept stale value item is still overwritten deterministically. The resulting
// value size is validated through the guard before each write attempt, so an
// oversized result is rejected with no partial write (requirement 14.3).
//
// Scope note: this closes lost updates for the single-item String value. True
// cross-connection atomicity for the multi-item collection commands (e.g.
// LSET/LTRIM/LREM/LINSERT rebuilding a list) still needs a DynamoDB transaction
// and remains best-effort in P0; those commands are not part of this
// compare-and-set path.

// readCurrentString reads the current live String value at pk for a read-path or
// RMW command. It loads the meta item and evaluates expiry against the router's
// clock: an absent or expired key yields found=false with an empty value.
// wrongType is true when the key is live but not a String, which read commands
// (STRLEN/GETRANGE) surface as WRONGTYPE; RMW commands ignore it and let the
// subsequent EnsureType(TypeString) reject the write with the same error. When
// the key is a live String the stored value bytes are returned.
func (r *Router) readCurrentString(ctx context.Context, pk string) (val []byte, found, wrongType bool, err error) {
	m, ok, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		return nil, false, false, err
	}
	if !ok || meta.IsExpired(m, r.now()) {
		return nil, false, false, nil
	}
	if m.Type != meta.TypeString {
		return nil, false, true, nil
	}

	v, has, err := r.Storage.Store.GetString(ctx, pk)
	if err != nil {
		return nil, false, false, err
	}
	if !has {
		// Live String meta with no value item: treat as an empty string.
		return nil, true, false, nil
	}

	return v, true, false, nil
}

// readStringForRMW reads one attempt's worth of state for a read-modify-write
// String command (APPEND/SETRANGE). It returns two views of the value:
//
//   - base is the LOGICAL current value the command builds on — the stored bytes
//     when the key is a live (present, unexpired) String, or empty when the key is
//     absent, expired, or a live String with no value item yet. This matches how
//     Redis/Pika treat a missing key (APPEND creates it, SETRANGE zero-pads from
//     empty).
//   - physVal / physExists describe the PHYSICAL value item exactly as stored, for
//     the compare-and-set precondition: physExists reports whether a value item is
//     present and physVal its bytes. These differ from base only for an
//     expired-but-not-yet-swept key (base empty, but physExists true with the stale
//     bytes), so the conditional write still targets the concrete item the read saw.
//
// A live non-String key yields base empty with wrongType=true; the caller lets the
// subsequent EnsureType(TypeString) reject the write with WRONGTYPE rather than
// treating it as an empty base.
func (r *Router) readStringForRMW(ctx context.Context, pk string) (base, physVal []byte, physExists, wrongType bool, err error) {
	m, ok, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		return nil, nil, false, false, err
	}
	liveString := ok && !meta.IsExpired(m, r.now()) && m.Type == meta.TypeString
	wrongType = ok && !meta.IsExpired(m, r.now()) && m.Type != meta.TypeString

	v, has, err := r.Storage.Store.GetString(ctx, pk)
	if err != nil {
		return nil, nil, false, false, err
	}
	physVal, physExists = v, has

	if liveString && has {
		base = v
	}

	return base, physVal, physExists, wrongType, nil
}

// rmwString runs the bounded compare-and-set read-modify-write loop shared by
// APPEND and SETRANGE (requirements 15.2, 16.4). Each attempt reads the current
// value, hands the logical base to compute to derive the new value (compute also
// runs the size guard and returns its error, so an oversized result is rejected
// with no write), verifies/creates the String type via EnsureType, then writes the
// result back conditionally on the value item being unchanged since the read. A
// conflicting concurrent write makes the conditional fail; the loop re-reads and
// retries up to storage.MaxRMWRetries, then returns storage.ErrRMWMaxRetries. A
// live non-String key is rejected by EnsureType and surfaces as WRONGTYPE. On
// success it returns the new value's byte length (the integer both commands reply).
func (r *Router) rmwString(ctx context.Context, pk string, compute func(base []byte) ([]byte, error)) (newLen int, err error) {
	for attempt := 0; attempt < storage.MaxRMWRetries; attempt++ {
		base, physVal, physExists, _, rerr := r.readStringForRMW(ctx, pk)
		if rerr != nil {
			return 0, rerr
		}

		next, cerr := compute(base)
		if cerr != nil {
			return 0, cerr
		}

		// EnsureType creates/verifies the String type (rejecting a live non-String
		// key with WRONGTYPE) before the value write, atomically re-checked each
		// attempt so a concurrent type change is still caught.
		if eerr := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); eerr != nil {
			return 0, eerr
		}

		ok, werr := r.Storage.Store.SetStringIfEquals(ctx, pk, next, physVal, physExists)
		if werr != nil {
			return 0, werr
		}
		if ok {
			return len(next), nil
		}
		// Lost the compare-and-set race: another writer changed the value between
		// the read and the write. Re-read and recompute.
	}

	return 0, storage.ErrRMWMaxRetries
}

// handleAppend implements APPEND key value (requirements 5.10, 16.4): append value
// to the string at key, creating the key with value when it is absent or expired.
// It replies the new string length as an integer (":N"). The existing TTL is
// preserved (APPEND does not clear expiry). A live non-String key replies
// WRONGTYPE (from EnsureType). The read-modify-write is not yet atomic across
// connections; see the file section header.
func (r *Router) handleAppend(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key, val := args[1], args[2]
	pk := encodePK(c.DB(), key)

	newLen, err := r.rmwString(ctx, pk, func(base []byte) ([]byte, error) {
		// Compute the appended result. A fresh slice keeps base's backing array
		// intact so a retry recomputes cleanly from the freshly-read base.
		next := make([]byte, 0, len(base)+len(val))
		next = append(next, base...)
		next = append(next, val...)

		if err := guard.CheckWrite(key, nil, [][]byte{next}); err != nil {
			return nil, err
		}

		return next, nil
	})
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(newLen))
}

// handleStrlen implements STRLEN key (requirement 5.11): reply the length of the
// String value at key as an integer, or ":0" when the key is absent or expired. A
// live key holding a non-String type replies WRONGTYPE.
func (r *Router) handleStrlen(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())

	cur, found, wrongType, err := r.readCurrentString(ctx, encodePK(c.DB(), args[1]))
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !found {
		w.Int(0)
		return
	}

	w.Int(int64(len(cur)))
}

// handleSetRange implements SETRANGE key offset value (requirements 5.10, 16.4):
// overwrite the string at key starting at offset, zero-padding with NUL bytes when
// offset is beyond the current length, and reply the new string length. A negative
// offset replies "-ERR offset is out of range"; a non-integer offset replies the
// not-an-integer error. An empty value performs no write and replies the current
// length (0 when the key is absent), matching Redis. The existing TTL is preserved.
// A live non-String key replies WRONGTYPE (from EnsureType). The read-modify-write
// is not yet atomic across connections; see the file section header.
func (r *Router) handleSetRange(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key, val := args[1], args[3]

	offset, err := ParseInt(args[2])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}
	if offset < 0 {
		w.Error(resp.ErrOffsetOutOfRange)
		return
	}

	pk := encodePK(c.DB(), key)

	// An empty value never creates or grows the key; it just reports the current
	// length (Redis SETRANGE semantics). No write, so no read-modify-write is
	// needed — a single read of the current value suffices.
	if len(val) == 0 {
		cur, _, _, rerr := r.readCurrentString(ctx, pk)
		if rerr != nil {
			r.writeStoreError(c, rerr)
			return
		}
		w.Int(int64(len(cur)))
		return
	}

	newLen, err := r.rmwString(ctx, pk, func(base []byte) ([]byte, error) {
		// Resulting length = max(current length, offset+len(value)). Compute it in
		// int64 first and reject an oversized result through the guard before
		// allocating, so a huge offset cannot trigger a huge allocation.
		end := offset + int64(len(val))
		nl := int64(len(base))
		if end > nl {
			nl = end
		}
		if gerr := guard.CheckWrite(key, nil, nil); gerr != nil {
			return nil, gerr
		}
		if gerr := guard.CheckValueSize(nl); gerr != nil {
			return nil, gerr
		}

		// Build the new value: copy the current bytes (make zero-fills the gap up
		// to offset when offset > len(base)), then overwrite from offset with value.
		buf := make([]byte, nl)
		copy(buf, base)
		copy(buf[offset:], val)

		return buf, nil
	})
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(newLen))
}

// handleGetRange implements GETRANGE key start end (requirement 5.11): reply the
// substring of the String value in the inclusive [start, end] range, using Redis'
// negative-index semantics (an index counts from the end of the string). A missing
// or expired key, an empty string, or a range that resolves to start > end replies
// the empty string. A non-integer start or end replies the not-an-integer error; a
// live non-String key replies WRONGTYPE.
func (r *Router) handleGetRange(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())

	start, err := ParseInt(args[2])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}
	end, err := ParseInt(args[3])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}

	cur, found, wrongType, rerr := r.readCurrentString(ctx, encodePK(c.DB(), args[1]))
	if rerr != nil {
		r.writeStoreError(c, rerr)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !found || len(cur) == 0 {
		w.BulkString(nil) // empty bulk string "$0\r\n\r\n"
		return
	}

	lo, hi := rangeBounds(int64(len(cur)), start, end)
	w.BulkString(cur[lo:hi])
}

// rangeBounds resolves Redis' GETRANGE [start, end] indices against a string of
// length strlen into a Go half-open slice range [lo, hi). It mirrors Redis'
// getrangeCommand: a negative index counts from the end (strlen+idx); both indices
// are then clamped to 0, end is clamped to strlen-1, and when start > end the
// range is empty (lo == hi == 0). strlen must be > 0 (the caller handles the
// empty string separately).
func rangeBounds(strlen, start, end int64) (lo, hi int) {
	if start < 0 {
		start = strlen + start
	}
	if end < 0 {
		end = strlen + end
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}
	if end >= strlen {
		end = strlen - 1
	}
	if start > end {
		return 0, 0
	}

	return int(start), int(end + 1)
}
