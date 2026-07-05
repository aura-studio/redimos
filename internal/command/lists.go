package command

import (
	"context"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// This file implements the List command family: LPUSH/RPUSH/LPUSHX/RPUSHX,
// LPOP/RPOP, LRANGE/LINDEX and the O(1) LLEN counter (requirements 7.1, 7.2,
// 7.3, 7.7). It follows the collection pattern the Hash/Set/Sorted-Set families
// (tasks 13.1/14.1/15.1) established.
//
// Data model: each list element is an independent item under the key's partition
// key, ordered by an integer index the storage layer assigns (head pushes get a
// decreasing index, tail pushes an increasing one) so the score index returns
// elements in head-to-tail order (requirement 7.1). The key's length lives in the
// meta item's cnt attribute, maintained by the meta conditional write's atomic
// ADD, so LLEN is O(1) (requirement 7.2) and always equals the current length
// (requirement 7.7).
//
// Collection write-path pattern (shared with the other families via datacmd.go):
//
//	guard.CheckWrite(key, elements, nil)          // size limits, no partial write
//	  -> Meta.EnsureType(TypeList, 0)             // create key + type check (WRONGTYPE)
//	  -> Store.L<op>(...)                          // element mutation; returns net member delta
//	  -> r.adjustCount(pk, TypeList, delta)        // atomic cnt maintenance, deletes key when empty
//
// EnsureType runs with a zero cnt delta FIRST, purely for the type check and key
// creation, so a wrong-type key is rejected with WRONGTYPE before any element
// item is written. The count is then adjusted by the NET number of elements the
// mutation added or removed, keeping meta.cnt exactly equal to the length.
// adjustCount deletes the key when a pop empties it, matching Redis (an empty
// list does not exist).
//
// Reads (LRANGE/LINDEX/LLEN) go through the meta item for existence/expiry and
// type: an absent or expired key behaves as an empty list, and a live key of a
// different type replies WRONGTYPE. Expiry is judged from meta.exp against the
// router's clock, independent of DynamoDB native-TTL timing.
//
// The high-cost combined commands LSET/LTRIM/LREM/LINSERT and the two-key
// RPOPLPUSH are task 16.2, implemented in lists2.go.

// registerLists installs the List command family on the router's table. It is
// invoked from registerDataCommands (router_storage.go). Arity counts include the
// command name; the mutating commands are marked Write.
func (r *Router) registerLists() {
	r.reg("LPUSH", -3, true, r.handleLPush)
	r.reg("RPUSH", -3, true, r.handleRPush)
	r.reg("LPUSHX", -3, true, r.handleLPushX)
	r.reg("RPUSHX", -3, true, r.handleRPushX)
	r.reg("LPOP", 2, true, r.handleLPop)
	r.reg("RPOP", 2, true, r.handleRPop)
	r.reg("LRANGE", 4, false, r.handleLRange)
	r.reg("LINDEX", 3, false, r.handleLIndex)
	r.reg("LLEN", 2, false, r.handleLLen)

	// Task 16.2: high-cost combined mutators + two-key rotation (lists2.go).
	r.reg("LSET", 4, true, r.handleLSet)
	r.reg("LTRIM", 4, true, r.handleLTrim)
	r.reg("LREM", 4, true, r.handleLRem)
	r.reg("LINSERT", 5, true, r.handleLInsert)
	r.reg("RPOPLPUSH", 3, true, r.handleRPopLPush)
}

// listState is the outcome of loading a key's meta for a List command: whether it
// is a live List, whether it is live but a different type (WRONGTYPE), and the
// loaded meta (valid only when the key is a live List). An absent or expired key
// reports live=false, wrongType=false — a List read then behaves as if the key
// were an empty list.
func (r *Router) listState(ctx context.Context, pk string) (m meta.Meta, live, wrongType bool, err error) {
	m, found, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		return meta.Meta{}, false, false, err
	}
	if !found || meta.IsExpired(m, r.now()) {
		return meta.Meta{}, false, false, nil
	}
	if m.Type != meta.TypeList {
		return m, false, true, nil
	}

	return m, true, false, nil
}

// pushCommon is the shared tail of LPUSH/RPUSH/LPUSHX/RPUSHX once the key is known
// to exist as a List (or was just created): it performs the element mutation via
// push (LPush or RPush), applies the net count delta and replies the new length
// read O(1) from meta.cnt. It writes any RESP error itself.
func (r *Router) pushCommon(
	ctx context.Context, c *server.Conn, pk string, elements [][]byte,
	push func(context.Context, string, [][]byte) (int, error),
) {
	w := resp.NewWriter(c.Redcon())

	pushed, err := push(ctx, pk, elements)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeList, int64(pushed)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	m, _, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(m.Count)
}

// handleLPush implements LPUSH key value [value ...] (requirements 7.1, 7.7):
// create the key if needed, prepend the values to the head and reply the new
// length. cnt is bumped by the number pushed so LLEN stays equal to the length. A
// live non-List key replies WRONGTYPE.
func (r *Router) handleLPush(ctx context.Context, c *server.Conn, args [][]byte) {
	key := args[1]
	elements := args[2:]

	pk := encodePK(c.DB(), key)
	if err := guard.CheckWrite(key, elements, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if _, err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeList, 0); err != nil {
		r.writeStoreError(c, err)
		return
	}

	r.pushCommon(ctx, c, pk, elements, r.Storage.Store.LPush)
}

// handleRPush implements RPUSH key value [value ...] (requirements 7.1, 7.7):
// as LPUSH but appends the values to the tail.
func (r *Router) handleRPush(ctx context.Context, c *server.Conn, args [][]byte) {
	key := args[1]
	elements := args[2:]

	pk := encodePK(c.DB(), key)
	if err := guard.CheckWrite(key, elements, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if _, err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeList, 0); err != nil {
		r.writeStoreError(c, err)
		return
	}

	r.pushCommon(ctx, c, pk, elements, r.Storage.Store.RPush)
}

// handleLPushX implements LPUSHX key value [value ...] (requirements 7.1, 7.7):
// prepend the values ONLY if the key already exists as a list, replying the new
// length; when the key is absent/expired it replies ":0" and creates nothing. A
// live non-List key replies WRONGTYPE.
func (r *Router) handleLPushX(ctx context.Context, c *server.Conn, args [][]byte) {
	r.pushXCommon(ctx, c, args, r.Storage.Store.LPush)
}

// handleRPushX implements RPUSHX key value [value ...]: as LPUSHX but appends.
func (r *Router) handleRPushX(ctx context.Context, c *server.Conn, args [][]byte) {
	r.pushXCommon(ctx, c, args, r.Storage.Store.RPush)
}

// pushXCommon is the shared body of LPUSHX/RPUSHX: it gates the push on the key
// already existing as a live list (never creating it), replying ":0" when absent
// and WRONGTYPE for a live non-list.
func (r *Router) pushXCommon(
	ctx context.Context, c *server.Conn, args [][]byte,
	push func(context.Context, string, [][]byte) (int, error),
) {
	w := resp.NewWriter(c.Redcon())
	key := args[1]
	elements := args[2:]
	pk := encodePK(c.DB(), key)

	_, live, wrongType, err := r.listState(ctx, pk)
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

	if err := guard.CheckWrite(key, elements, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}

	r.pushCommon(ctx, c, pk, elements, push)
}

// handleLPop implements LPOP key (requirements 7.3, 7.7): remove and return the
// head element as a bulk string, or the null bulk string when the key is
// absent/expired or empty. cnt is decremented by one and the key is deleted when
// its last element is popped. A live non-List key replies WRONGTYPE.
func (r *Router) handleLPop(ctx context.Context, c *server.Conn, args [][]byte) {
	r.popCommon(ctx, c, args, r.Storage.Store.LPop)
}

// handleRPop implements RPOP key: as LPOP but from the tail.
func (r *Router) handleRPop(ctx context.Context, c *server.Conn, args [][]byte) {
	r.popCommon(ctx, c, args, r.Storage.Store.RPop)
}

// popCommon is the shared body of LPOP/RPOP.
func (r *Router) popCommon(
	ctx context.Context, c *server.Conn, args [][]byte,
	pop func(context.Context, string) ([]byte, bool, error),
) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	_, live, wrongType, err := r.listState(ctx, pk)
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

	val, found, err := pop(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !found {
		w.NullBulk()
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeList, -1); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.BulkString(val)
}

// handleLRange implements LRANGE key start stop (requirement 7.1): reply an array
// of the elements in [start, stop] with Redis negative-index semantics. An
// absent/expired key replies the empty array; a live non-List key replies
// WRONGTYPE. A non-integer start/stop replies the not-an-integer error.
func (r *Router) handleLRange(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	start, err := ParseInt(args[2])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}
	stop, err := ParseInt(args[3])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}

	_, live, wrongType, err := r.listState(ctx, pk)
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

	elements, err := r.Storage.Store.LRange(ctx, pk, int(start), int(stop))
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.BulkArray(elements)
}

// handleLIndex implements LINDEX key index (requirement 7.1): reply the element at
// index (Redis negative-index semantics) as a bulk string, or the null bulk
// string when the index is out of range or the key is absent/expired. A live
// non-List key replies WRONGTYPE. A non-integer index replies the not-an-integer
// error.
func (r *Router) handleLIndex(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	// Redis checks key existence and type BEFORE parsing the index, so a missing
	// key replies "$-1" and a wrong-type key replies WRONGTYPE even when the index
	// argument is malformed.
	_, live, wrongType, err := r.listState(ctx, pk)
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

	index, err := ParseInt(args[2])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}

	val, found, err := r.Storage.Store.LIndex(ctx, pk, int(index))
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

// handleLLen implements LLEN key (requirements 7.2, 7.7): reply the list length in
// O(1) from meta.cnt. An absent/expired key replies ":0"; a live non-List key
// replies WRONGTYPE.
func (r *Router) handleLLen(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	m, live, wrongType, err := r.listState(ctx, pk)
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
