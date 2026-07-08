package command

import (
	"context"
	"strconv"
	"strings"

	"github.com/aura-studio/redimos/internal/guard"
	"github.com/aura-studio/redimos/internal/meta"
	"github.com/aura-studio/redimos/internal/resp"
	"github.com/aura-studio/redimos/internal/server"
	"github.com/aura-studio/redimos/internal/storage"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This file implements the Hash command family: HSET/HGET/HMSET/HMGET/HGETALL/
// HDEL/HEXISTS/HKEYS/HVALS/HSETNX/HINCRBY/HINCRBYFLOAT/HSTRLEN (requirements 6.1,
// 6.2, 6.4). It is the FIRST collection family and establishes the pattern the
// Set/SortedSet/List families (tasks 14/15/16) follow.
//
// Data model: each hash field is an independent item under the key's partition
// key with the field name as the sort key (sk = field), so per-field reads and
// writes are concurrency-safe (requirement 6.1). The key's field COUNT lives in
// the meta item's cnt attribute, maintained by the meta conditional write's
// atomic ADD, so HLEN is O(1) (requirement 6.2) and always equals the current
// field count (requirement 6.4).
//
// Collection write-path pattern (the seam tasks 14/15/16 reuse):
//
//	guard.CheckWrite(key, fieldNames, values)     // size limits, no partial write
//	  -> Meta.EnsureType(TypeHash, 0)             // create key + type check (WRONGTYPE)
//	  -> Store.H<op>(...)                          // per-field data mutation; returns net member delta
//	  -> r.adjustCount(pk, TypeHash, delta)        // atomic cnt maintenance, deletes key when empty
//
// EnsureType runs with a zero cnt delta FIRST, purely for the type check and key
// creation, so a wrong-type key is rejected with WRONGTYPE before any field item
// is written. The count is then adjusted by the NET number of fields the data
// mutation actually created or removed (learned from the conditional write's own
// report — e.g. HSET reports which fields were newly created), which keeps
// meta.cnt exactly equal to the field count regardless of how many of the
// supplied fields already existed. adjustCount deletes the key when a removal
// empties it, matching Redis (an empty hash does not exist).
//
// Reads (HGET/HMGET/HGETALL/HKEYS/HVALS/HEXISTS/HSTRLEN/HLEN) go through the meta
// item for existence/expiry and type: an absent or expired key behaves as an
// empty hash (or a null field), and a live key of a different type replies
// WRONGTYPE. Expiry is judged from meta.exp against the router's clock,
// independent of DynamoDB native-TTL timing (like the String/Key families).

// registerHashes installs the Hash command family on the router's table. It is
// invoked from registerDataCommands (router_storage.go). Arity counts include the
// command name; the mutating commands are marked Write.
func (r *Router) registerHashes() {
	r.reg("HSET", 4, true, r.handleHSet) // Redis 3.2 HSET is exactly arity 4 (single field/value); multi-field HSET is 4.0+
	r.reg("HSETNX", 4, true, r.handleHSetNX)
	r.reg("HGET", 3, false, r.handleHGet)
	r.reg("HMSET", -4, true, r.handleHMSet)
	r.reg("HMGET", -3, false, r.handleHMGet)
	r.reg("HGETALL", 2, false, r.handleHGetAll)
	r.reg("HDEL", -3, true, r.handleHDel)
	r.reg("HEXISTS", 3, false, r.handleHExists)
	r.reg("HKEYS", 2, false, r.handleHKeys)
	r.reg("HVALS", 2, false, r.handleHVals)
	r.reg("HLEN", 2, false, r.handleHLen)
	r.reg("HINCRBY", 4, true, r.handleHIncrBy)
	r.reg("HINCRBYFLOAT", 4, true, r.handleHIncrByFloat)
	// v1 line (redimo v1.7.2): HSCAN is registered — it reads the whole hash via rv1's
	// HGETALL and returns it as a single terminal page (cursor 0), so a Redis GUI can
	// open a hash key. HSTRLEN stays GATED → "ERR unknown command 'hstrlen'" (no
	// dedicated rv1 field-length primitive); handleHStrlen stays compiled but unreachable.
	r.reg("HSCAN", -3, false, r.handleHScan)
}

// hashState is the outcome of loading a key's meta for a Hash command: whether it
// is a live Hash, whether it is live but a different type (WRONGTYPE), and the
// loaded meta (valid only when the key is a live Hash). An absent or expired key
// reports live=false, wrongType=false — a Hash read then behaves as if the key
// were an empty hash.
func (r *Router) hashState(ctx context.Context, pk string) (m meta.Meta, live, wrongType bool, err error) {
	return r.loadMetaState(ctx, pk, meta.TypeHash)
}

// ensureHashWritable runs the collection write-path preamble shared by every Hash
// mutation: it validates sizes through the guard (key + field names + values),
// then performs the meta type check / key creation with a zero count delta. It
// reports ok=false and has already written the RESP error (WRONGTYPE, backend
// limit, or a backend error) when the write must not proceed. On ok=true the key
// exists as a Hash and the caller may perform the field mutation and then adjust
// the count.
func (r *Router) ensureHashWritable(ctx context.Context, c *server.Conn, pk string, key []byte, fields, values [][]byte) bool {
	if err := guard.CheckWrite(key, fields, values); err != nil {
		r.writeStoreError(c, err)
		return false
	}
	if err := r.ensureTypeExpiring(ctx, pk, meta.TypeHash); err != nil {
		r.writeStoreError(c, err)
		return false
	}

	return true
}

// handleHSet implements HSET key field value [field value ...] (requirements 6.1,
// 6.4). It writes each field/value pair (each an independent item) and replies the
// integer number of fields that were newly created (existing fields updated in
// place do not count), matching Redis. cnt is bumped by that same number so HLEN
// stays equal to the field count. An odd number of field/value arguments replies
// the wrong-number-of-arguments error; a live non-Hash key replies WRONGTYPE.
func (r *Router) handleHSet(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key := args[1]
	rest := args[2:]

	if len(rest)%2 != 0 {
		w.Error(resp.ErrWrongNumberOfArgs("hset"))
		return
	}

	fields, values, hfields := splitHashPairs(rest)

	if !r.ensureHashWritable(ctx, c, r.encodePK(c.DB(), key), key, fields, values) {
		return
	}

	pk := r.encodePK(c.DB(), key)
	added, err := r.Storage.Store.HSet(ctx, pk, hfields)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeHash, int64(added)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(added))
}

// handleHMSet implements HMSET key field value [field value ...] (requirement
// 6.1). It behaves like HSET but replies "+OK" (the legacy HMSET reply) instead of
// the new-field count. Field-count maintenance is identical. An odd number of
// field/value arguments replies the wrong-number-of-arguments error; a live
// non-Hash key replies WRONGTYPE.
func (r *Router) handleHMSet(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key := args[1]
	rest := args[2:]

	if len(rest)%2 != 0 {
		// Redis 3.2 hmsetCommand special-cases odd argc with a literal that (a) uses
		// uppercase HMSET and (b) omits the "'...' command" wrapper of the generic
		// arity error — match it byte-for-byte.
		w.Error("ERR wrong number of arguments for HMSET")
		return
	}

	fields, values, hfields := splitHashPairs(rest)

	if !r.ensureHashWritable(ctx, c, r.encodePK(c.DB(), key), key, fields, values) {
		return
	}

	pk := r.encodePK(c.DB(), key)
	added, err := r.Storage.Store.HSet(ctx, pk, hfields)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeHash, int64(added)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.SimpleString("OK")
}

// splitHashPairs splits an interleaved [field, value, field, value, ...] slice
// into a slice of field-name bytes and a slice of value bytes (for the size
// guard) plus the storage.HField pairs the store write consumes. rest must have
// even length (the caller validated arity).
func splitHashPairs(rest [][]byte) (fields, values [][]byte, hfields []storage.HField) {
	n := len(rest) / 2
	fields = make([][]byte, 0, n)
	values = make([][]byte, 0, n)
	hfields = make([]storage.HField, 0, n)
	for i := 0; i+1 < len(rest); i += 2 {
		fields = append(fields, rest[i])
		values = append(values, rest[i+1])
		hfields = append(hfields, storage.HField{Field: string(rest[i]), Value: rest[i+1]})
	}

	return fields, values, hfields
}

// handleHSetNX implements HSETNX key field value (requirement 6.1): set field only
// if it does not already exist, replying ":1" when the field was created and ":0"
// when it already existed (no write). A new field bumps cnt by 1. A live non-Hash
// key replies WRONGTYPE.
func (r *Router) handleHSetNX(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key, field, val := args[1], args[2], args[3]

	pk := r.encodePK(c.DB(), key)
	if !r.ensureHashWritable(ctx, c, pk, key, [][]byte{field}, [][]byte{val}) {
		return
	}

	set, err := r.Storage.Store.HSetNX(ctx, pk, string(field), val)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if set {
		if err := r.adjustCount(ctx, pk, meta.TypeHash, 1); err != nil {
			r.writeStoreError(c, err)
			return
		}
		w.Int(1)
		return
	}

	w.Int(0)
}

// handleHGet implements HGET key field (requirement 6.1): reply the field's value
// as a bulk string, or the null bulk string "$-1" when the key is absent/expired
// or the field does not exist. A live non-Hash key replies WRONGTYPE.
func (r *Router) handleHGet(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := r.encodePK(c.DB(), args[1])

	_, live, wrongType, err := r.hashState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.NullBulk()
		return
	}
	// A field too large to be stored (its sort key would exceed DynamoDB's limit) can
	// never exist, so it is simply absent — reply nil rather than hitting the backend.
	if !memberStorable(args[2]) {
		w.NullBulk()
		return
	}

	val, found, err := r.Storage.Store.HGet(ctx, pk, string(args[2]))
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !found {
		w.NullBulk()
		return
	}

	w.BulkString(val)
}

// handleHMGet implements HMGET key field [field ...] (requirement 6.1): reply an
// array with one element per requested field, in request order — the field's value
// as a bulk string when present, or the null bulk string when the field is missing
// (or the key is absent/expired). A live non-Hash key replies WRONGTYPE.
func (r *Router) handleHMGet(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := r.encodePK(c.DB(), args[1])
	reqFields := args[2:]

	_, live, wrongType, err := r.hashState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}

	elems := make([][]byte, len(reqFields))
	present := make([]bool, len(reqFields))

	if live {
		names := make([]string, len(reqFields))
		// Only query fields that CAN exist; an oversized field (sort key past the
		// DynamoDB limit) is never present and must not reach the backend.
		queryNames := make([]string, 0, len(reqFields))
		for i, f := range reqFields {
			names[i] = string(f)
			if memberStorable(f) {
				queryNames = append(queryNames, names[i])
			}
		}
		vals, err := r.Storage.Store.HMGet(ctx, pk, queryNames)
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		for i, name := range names {
			if v, ok := vals[name]; ok {
				elems[i] = v
				present[i] = true
			}
		}
	}

	// Absent/expired key: every element is a null bulk string, matching Redis.
	w.OptBulkArray(elems, present)
}

// handleHGetAll implements HGETALL key (requirement 6.1): reply a flat array of
// alternating field and value bulk strings. An absent/expired key replies the
// empty array "*0"; a live non-Hash key replies WRONGTYPE.
func (r *Router) handleHGetAll(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := r.encodePK(c.DB(), args[1])

	m, live, wrongType, err := r.hashState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.EmptyArray()
		return
	}
	if r.resultCapExceeded(w, m.Count) {
		return
	}

	fields, err := r.Storage.Store.HGetAll(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	elems := make([][]byte, 0, len(fields)*2)
	for _, f := range fields {
		elems = append(elems, []byte(f.Field), f.Value)
	}

	w.BulkArray(elems)
}

// handleHKeys implements HKEYS key (requirement 6.1): reply an array of the hash's
// field names. An absent/expired key replies the empty array; a live non-Hash key
// replies WRONGTYPE.
func (r *Router) handleHKeys(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := r.encodePK(c.DB(), args[1])

	m, live, wrongType, err := r.hashState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.EmptyArray()
		return
	}
	if r.resultCapExceeded(w, m.Count) {
		return
	}

	keys, err := r.Storage.Store.HKeys(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	elems := make([][]byte, 0, len(keys))
	for _, k := range keys {
		elems = append(elems, []byte(k))
	}

	w.BulkArray(elems)
}

// handleHVals implements HVALS key (requirement 6.1): reply an array of the hash's
// values. An absent/expired key replies the empty array; a live non-Hash key
// replies WRONGTYPE.
func (r *Router) handleHVals(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := r.encodePK(c.DB(), args[1])

	m, live, wrongType, err := r.hashState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.EmptyArray()
		return
	}
	if r.resultCapExceeded(w, m.Count) {
		return
	}

	vals, err := r.Storage.Store.HVals(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.BulkArray(vals)
}

// handleHDel implements HDEL key field [field ...] (requirements 6.1, 6.4): remove
// the given fields and reply the integer count of fields that actually existed and
// were removed. cnt is decremented by that count, and the key is deleted when its
// last field is removed (an empty hash does not exist). An absent/expired key
// replies ":0"; a live non-Hash key replies WRONGTYPE.
func (r *Router) handleHDel(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := r.encodePK(c.DB(), args[1])
	reqFields := args[2:]

	_, live, wrongType, err := r.hashState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.Int(0)
		return
	}

	// An oversized field can never exist, so it removes nothing; drop it before the
	// store call so its sort key never reaches the backend.
	names := make([]string, 0, len(reqFields))
	for _, f := range reqFields {
		if memberStorable(f) {
			names = append(names, string(f))
		}
	}

	removed, err := r.Storage.Store.HDel(ctx, pk, names)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeHash, -int64(removed)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(removed))
}

// handleHExists implements HEXISTS key field (requirement 6.1): reply ":1" when
// the field exists in a live hash, else ":0". An absent/expired key replies ":0";
// a live non-Hash key replies WRONGTYPE.
func (r *Router) handleHExists(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := r.encodePK(c.DB(), args[1])

	_, live, wrongType, err := r.hashState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.Int(0)
		return
	}
	if !memberStorable(args[2]) { // oversized field can never exist
		w.Int(0)
		return
	}

	exists, err := r.Storage.Store.HExists(ctx, pk, string(args[2]))
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if exists {
		w.Int(1)
		return
	}

	w.Int(0)
}

// handleHLen implements HLEN key (requirements 6.2, 6.4): reply the hash's field
// count in O(1) from meta.cnt. An absent/expired key replies ":0"; a live non-Hash
// key replies WRONGTYPE.
func (r *Router) handleHLen(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := r.encodePK(c.DB(), args[1])

	m, live, wrongType, err := r.hashState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.Int(0)
		return
	}

	w.Int(m.Count)
}

// handleHStrlen implements HSTRLEN key field (requirement 6.1): reply the byte
// length of the field's value, or ":0" when the field or key is absent. A live
// non-Hash key replies WRONGTYPE.
func (r *Router) handleHStrlen(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := r.encodePK(c.DB(), args[1])

	_, live, wrongType, err := r.hashState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		w.Int(0)
		return
	}
	if !memberStorable(args[2]) { // oversized field can never exist
		w.Int(0)
		return
	}

	n, err := r.Storage.Store.HStrlen(ctx, pk, string(args[2]))
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(n))
}

// handleHIncrBy implements HINCRBY key field increment (requirement 6.1): add the
// integer increment to the field's integer value (a missing field starts at 0) and
// reply the new value as an integer. A non-integer increment replies the
// not-an-integer error; a non-integer field value replies "-ERR hash value is not
// an integer"; an overflowing result replies the overflow error. A new field bumps
// cnt by 1. A live non-Hash key replies WRONGTYPE.
func (r *Router) handleHIncrBy(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key, field := args[1], args[2]

	delta, err := ParseInt(args[3])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}

	pk := r.encodePK(c.DB(), key)
	if !r.ensureHashWritable(ctx, c, pk, key, [][]byte{field}, nil) {
		return
	}

	next, isNew, err := r.Storage.Store.HIncrBy(ctx, pk, string(field), delta)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if isNew {
		if err := r.adjustCount(ctx, pk, meta.TypeHash, 1); err != nil {
			r.writeStoreError(c, err)
			return
		}
	}

	w.Int(next)
}

// handleHIncrByFloat implements HINCRBYFLOAT key field increment (requirement
// 6.1): add the float increment to the field's float value (a missing field starts
// at 0) and reply the new value as a bulk string (shortest decimal, trailing zeros
// trimmed). A non-float increment replies the not-a-valid-float error; a non-float
// field value replies "-ERR hash value is not a valid float". Unlike INCRBYFLOAT,
// Redis 3.2's hincrbyfloatCommand has NO isnan/isinf result guard, so an inf/-inf
// increment (or an inf+(-inf) NaN result) is accepted and stored as "inf"/"-inf"/
// "-nan" (a subsequent read of a "-nan" field then fails, mirroring string2ld). A new
// field bumps cnt by 1. A live non-Hash key replies WRONGTYPE.
func (r *Router) handleHIncrByFloat(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key, field := args[1], args[2]

	delta, ok := parseFloatArg(args[3])
	if !ok {
		w.Error(resp.ErrNotValidFloat)
		return
	}

	pk := r.encodePK(c.DB(), key)
	if !r.ensureHashWritable(ctx, c, pk, key, [][]byte{field}, nil) {
		return
	}

	val, isNew, err := r.Storage.Store.HIncrByFloat(ctx, pk, string(field), delta)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if isNew {
		if err := r.adjustCount(ctx, pk, meta.TypeHash, 1); err != nil {
			r.writeStoreError(c, err)
			return
		}
	}

	w.BulkString(val)
}

// handleHScan implements HSCAN key cursor [MATCH pattern] [COUNT n] (requirement
// 6.3). It is the Hash-scoped analogue of SCAN (see scan.go): where SCAN pages the
// WHOLE keyspace, HSCAN pages WITHIN a single pk — the fields of one hash — via
// Store.HScan (a partition Query), REUSING the exact same cursor machinery. The
// uint64 cursor bridges to the backend's opaque pagination token through the
// per-instance SCAN registry (r.Storage.Scan), MATCH is applied proxy-side to the
// field name (glob.go), and COUNT maps to the Query page limit.
//
// Cursor lifecycle mirrors SCAN: "HSCAN key 0" starts a fresh page; a non-zero
// cursor must be an own-instance registry entry, and a miss (LRU eviction,
// instance restart, or a cursor minted elsewhere) replies "-ERR invalid cursor,
// restart scan"; the terminating page carries cursor "0", otherwise the next
// page's token is registered under a fresh cursor.
//
// The reply is the two-element array [cursor, [field1, value1, field2, value2,
// ...]] — the inner array interleaves each field name with its value, matching
// Redis/Pika. A wrong-type key replies WRONGTYPE; an absent or expired key replies
// the terminating ["0", []] (an empty, non-null inner array), exactly as HGETALL
// treats an absent key as an empty hash.
func (r *Router) handleHScan(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := r.encodePK(c.DB(), args[1])

	// The cursor is a Redis uint64. A value that does not parse is treated as an
	// invalid cursor (the "restart scan" contract), not a syntax error, matching
	// SCAN.
	cursor, ok := parseScanCursor(args[2])
	if !ok {
		w.Error(resp.ErrInvalidCursor)
		return
	}

	// Type / existence check BEFORE the option parse: Redis' hscanCommand does the
	// lookup + checkType before scanGenericCommand parses MATCH/COUNT, so a live
	// non-Hash key is WRONGTYPE and an absent/expired key replies the terminating
	// ["0", []] — both regardless of a malformed MATCH/COUNT option.
	_, live, wrongType, err := r.hashState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		writeHScanReply(c, "0", nil)
		return
	}

	// Optional [MATCH pattern] [COUNT n] pairs, in any order.
	var (
		pattern  []byte
		hasMatch bool
		limit    int32
	)
	opts := args[3:]
	if len(opts)%2 != 0 {
		w.Error(resp.ErrSyntax)
		return
	}
	for i := 0; i+1 < len(opts); i += 2 {
		switch strings.ToUpper(string(opts[i])) {
		case "MATCH":
			pattern = opts[i+1]
			hasMatch = true
		case "COUNT":
			// string2ll semantics (reject leading '+'/zeros), not strconv.Atoi.
			n, err := ParseInt(opts[i+1])
			if err != nil {
				w.Error(resp.ErrNotInteger)
				return
			}
			if n < 1 {
				w.Error(resp.ErrSyntax)
				return
			}
			limit = int32(n)
		default:
			w.Error(resp.ErrSyntax)
			return
		}
	}

	// Resolve the pagination token. Cursor 0 starts a fresh page (nil token); any
	// other cursor must be a live, own-instance entry in the registry.
	var lek map[string]types.AttributeValue
	if cursor != 0 {
		l, ok := r.Storage.Scan.LoadOwned(cursor, c.InstID())
		if !ok {
			w.Error(resp.ErrInvalidCursor)
			return
		}
		lek = l
	}

	fields, nextLEK, err := r.Storage.Store.HScan(ctx, pk, lek, limit)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	// Flatten the page into interleaved field/value pairs, applying the MATCH
	// filter proxy-side on the field name (COUNT-limited pages, MATCH-filtered
	// content — exactly SCAN's contract). out is a non-nil slice, so an empty
	// filtered page still encodes as an empty array, never the null array.
	out := make([][]byte, 0, len(fields)*2)
	for _, f := range fields {
		if hasMatch && !globMatch(pattern, []byte(f.Field)) {
			continue
		}
		out = append(out, []byte(f.Field), f.Value)
	}

	// A nil next token means the partition has been fully paged → terminating
	// cursor "0". Otherwise register the token under a fresh cursor.
	cursorOut := "0"
	if nextLEK != nil {
		cursorOut = strconv.FormatUint(r.Storage.Scan.Save(nextLEK), 10)
	}

	writeHScanReply(c, cursorOut, out)
}

// writeHScanReply writes the two-element HSCAN array [cursor, [pairs...]]. A nil
// pairs slice is normalized to a non-nil empty slice so the inner array always
// encodes as "*0" (empty array), never the null array "*-1", matching Redis/Pika.
func writeHScanReply(c *server.Conn, cursor string, pairs [][]byte) {
	writeScanReply(c, cursor, pairs)
}
