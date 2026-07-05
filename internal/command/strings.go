package command

import (
	"context"
	"math"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
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
	r.reg("GET", 2, false, r.handleGet)
	r.reg("SET", -3, true, r.handleSet)
	r.reg("SETNX", 3, true, r.handleSetNX)
	r.reg("SETEX", 4, true, r.handleSetEX)
	r.reg("PSETEX", 4, true, r.handlePSetEX)
	r.reg("GETSET", 3, true, r.handleGetSet)
	r.reg("MGET", -2, false, r.handleMGet)
	r.reg("MSET", -3, true, r.handleMSet)
	r.reg("MSETNX", -3, true, r.handleMSetNX)
	r.reg("INCR", 2, true, r.handleIncr)
	r.reg("DECR", 2, true, r.handleDecr)
	r.reg("INCRBY", 3, true, r.handleIncrBy)
	r.reg("DECRBY", 3, true, r.handleDecrBy)
	r.reg("INCRBYFLOAT", 3, true, r.handleIncrByFloat)
	r.reg("APPEND", 3, true, r.handleAppend)
	r.reg("STRLEN", 2, false, r.handleStrlen)
	r.reg("SETRANGE", 4, true, r.handleSetRange)
	r.reg("GETRANGE", 4, false, r.handleGetRange)
	// SUBSTR is the deprecated Redis alias of GETRANGE with identical semantics.
	r.reg("SUBSTR", 4, false, r.handleGetRange)
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

	// SET NX: atomically claim a logically-absent (or expired) key. The single
	// conditional meta write is the concurrency-safe gate — racing SET NX callers
	// can never both win. The claim resets the meta and clears any stale expiry, so
	// only the value and (optional) new expiry remain to be written.
	if o.nx {
		created, err := r.Storage.Meta.CreateTypeIfAbsent(ctx, pk, meta.TypeString, 0, r.now())
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if !created {
			w.NullBulk()
			return
		}
		// created==true means the key was logically absent — either truly absent or
		// an EXPIRED key of any type (CreateTypeIfAbsent's condition claims both). If
		// it was an expired non-String collection, its member items still sit under pk
		// and are now hidden by the fresh String meta (see overwriteAnyType): the async
		// deleter's IsLive guard and SweepOrphans would both consider them live, so they
		// would leak forever. We now own the meta (no other writer can claim it), and the
		// value item is not written yet, so DeleteMembers here reclaims exactly those
		// stale members. The common case (a brand-new key) finds none in one Query.
		if _, err := r.Storage.Store.DeleteMembers(ctx, pk); err != nil {
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
		}
		w.SimpleString("OK")
		return
	}

	// XX gate on logical existence before any write (see file header). Plain SET
	// (no flag) falls straight through and overwrites any existing key/type.
	if o.xx {
		live, err := r.keyLive(ctx, pk)
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if !live {
			w.NullBulk()
			return
		}
	}

	if err := r.overwriteAnyType(ctx, pk); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if _, err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); err != nil {
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

	// Atomically claim the key only if it is logically absent (or expired). This
	// single conditional meta write replaces a read-then-write existence check, so
	// two SETNX racing on the same fresh key can no longer both report success:
	// exactly one observes created=true.
	created, err := r.Storage.Meta.CreateTypeIfAbsent(ctx, pk, meta.TypeString, 0, r.now())
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !created {
		w.Int(0)
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
	if _, err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); err != nil {
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

	if _, err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); err != nil {
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
		w.Error(resp.ErrMSetOddArgs)
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

			if err := r.overwriteAnyType(ctx, pk); err != nil {
				r.writeStoreError(c, err)
				return
			}
			if _, err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); err != nil {
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

// handleMSetNX implements MSETNX key value [key value ...] (Redis 3.2): set all
// the given keys only if NONE of them already exists, replying ":1" when applied
// and ":0" when any key existed (no key is written in that case). Like MSET it is
// type-agnostic and carries no TTL. The all-or-nothing existence check and the
// writes are separate steps (not atomic across keys), matching redimos' other
// multi-key writes; a concurrent writer can race the gap.
func (r *Router) handleMSetNX(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pairs := args[1:]

	if len(pairs)%2 != 0 {
		// Redis 3.2's shared msetGenericCommand reports "MSET" here even for MSETNX.
		w.Error(resp.ErrMSetOddArgs)
		return
	}

	// Validate every value's size up front: a violation rejects the whole command.
	for i := 0; i < len(pairs); i += 2 {
		if err := guard.CheckWrite(pairs[i], nil, [][]byte{pairs[i+1]}); err != nil {
			r.writeStoreError(c, err)
			return
		}
	}

	// Reject if any target key is already live (of any type).
	for i := 0; i < len(pairs); i += 2 {
		live, err := r.keyLive(ctx, encodePK(c.DB(), pairs[i]))
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if live {
			w.Int(0)
			return
		}
	}

	for i := 0; i < len(pairs); i += 2 {
		pk := encodePK(c.DB(), pairs[i])
		if _, err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); err != nil {
			r.writeStoreError(c, err)
			return
		}
		if err := r.Storage.Store.SetString(ctx, pk, pairs[i+1]); err != nil {
			r.writeStoreError(c, err)
			return
		}
		if _, err := r.Storage.Meta.Persist(ctx, pk); err != nil {
			r.writeStoreError(c, err)
			return
		}
	}

	w.Int(1)
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
	n, err := Args(args).Int(2)
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
	n, err := Args(args).Int(2)
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

	if _, err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); err != nil {
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

	if _, err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeString, 0); err != nil {
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
