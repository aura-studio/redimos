package command

import (
	"bytes"
	"context"
	"strings"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

// This file implements task 16.2: the high-cost combined List mutators
// (LSET/LTRIM/LREM/LINSERT) and the two-key rotation RPOPLPUSH (requirements 7.4,
// 7.5). It is registered from the existing registerLists() in lists.go.
//
// Combined (read-modify-write) implementation — LSET/LTRIM/LREM/LINSERT:
// the redimo fork's in-place list mutators are unstable/incomplete (it has no
// LINSERT, and its LREM returns (remainingLength, success) rather than Redis'
// removed-count), so rather than depend on them these commands are implemented at
// the proxy/storage layer as a read-modify-write over the whole list: read every
// element in head-to-tail order (Store.LRangeAll), compute the new element
// sequence in process with Redis' semantics, and rewrite it (Store.LReplaceAll).
// The list's length counter (meta.cnt) is then reconciled to the new length via
// adjustCount(newLen - oldCnt), keeping LLEN O(1) and exact (requirement 7.7), and
// adjustCount deletes the key when a trim/removal empties it (an empty list does
// not exist in Redis). Elements are persisted as redimo.StringValue by the storage
// layer, consistent with LPUSH/RPUSH, avoiding the fork's lPush type-assertion
// panic.
//
// The read-modify-write is NOT atomic across concurrent connections: another
// connection could mutate the list between the LRangeAll read and the LReplaceAll
// write. P0 serves each connection serially, so a single connection's own
// LSET/LTRIM/LREM/LINSERT are consistent.
//
// Task 20.1 landed the conditional-version + retry mechanism for the String value
// RMW (APPEND/SETRANGE and the INCR-family), where a single value item has a
// natural compare-and-set anchor: storage.SetStringIfEquals issues a DynamoDB
// conditional write and the bounded retry loop (storage.casRetry / the command
// layer's rmwString, storage.MaxRMWRetries) re-reads and recomputes on a lost
// race. The List RMW commands cannot reuse that helper as-is: a list is a
// MULTI-ITEM structure and LReplaceAll rewrites every element item, so a correct
// conditional version + retry needs either a version/etag bumped and asserted
// across the meta + all element items in one transaction, or a transactional
// LReplaceAll — both of which are fork-list-specific transactional work on the
// redimo fork's still-unstable list implementation (which also lacks LINSERT and
// returns non-Redis LREM semantics). To avoid destabilizing that code, the list
// RMW conditional-version + retry is deliberately deferred; cross-connection
// atomicity for these multi-item commands remains best-effort in P0. See the
// commit for task 20.1.
//
// RPOPLPUSH: the design calls for a TransactWriteItems two-key atomic move. The
// redimo fork v1.7 exposes no reusable transactional list primitive (its
// transaction support is private to specific commands like SMOVE/MSET, and its
// list items carry fork-assigned skN index boundaries that a hand-rolled
// cross-key transaction over the element + meta items cannot safely reproduce on
// the fork's still-unstable list code). RPOPLPUSH is therefore composed at the
// command layer from the existing RPop (tail of source) + LPush (head of
// destination) primitives, with both keys' meta counters maintained here. This is
// best-effort and NOT truly atomic across the two keys: a failure or a concurrent
// writer between the pop and the push is possible. Task 20.1, which landed the
// String value conditional-version + retry, left the two-key transactional move
// deferred for the same reason the list RMW retry is deferred (see the combined-
// mutator note above): it is fork-list-specific transactional work on the fork's
// unstable list code. The limitation is documented here as the design's
// "需验证 redimo" note anticipated.

// handleLSet implements LSET key index value (requirement 7.4): set the element at
// index (Redis negative-index semantics) to value and reply +OK. An out-of-range
// index replies "-ERR index out of range"; an absent key replies "-ERR no such
// key"; a live non-List key replies WRONGTYPE; a non-integer index replies the
// not-an-integer error. The list length is unchanged, so meta.cnt is untouched.
func (r *Router) handleLSet(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key := args[1]
	pk := encodePK(c.DB(), key)
	value := args[3]

	// Redis checks key existence and type BEFORE parsing the index, so a missing
	// key replies "no such key" and a wrong-type key replies WRONGTYPE even when
	// the index argument is malformed.
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
		w.Error(resp.ErrNoSuchKey)
		return
	}

	index, err := ParseInt(args[2])
	if err != nil {
		w.Error(resp.ErrNotInteger)
		return
	}
	if err := guard.CheckWrite(key, [][]byte{value}, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}

	all, err := r.Storage.Store.LRangeAll(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	n := len(all)
	idx := int(index)
	if idx < 0 {
		idx += n
	}
	if idx < 0 || idx >= n {
		w.Error(resp.ErrIndexOutOfRange)
		return
	}

	all[idx] = append([]byte(nil), value...)
	if _, err := r.Storage.Store.LReplaceAll(ctx, pk, all); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.SimpleString("OK")
}

// handleLTrim implements LTRIM key start stop (requirement 7.4): trim the list to
// the inclusive [start, stop] range (Redis negative-index semantics) and reply
// +OK. A range that selects nothing empties and deletes the key. An absent key is
// a no-op that still replies +OK; a live non-List key replies WRONGTYPE; a
// non-integer bound replies the not-an-integer error. meta.cnt is reconciled to
// the new length.
func (r *Router) handleLTrim(ctx context.Context, c *server.Conn, args [][]byte) {
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
		w.SimpleString("OK")
		return
	}

	all, err := r.Storage.Store.LRangeAll(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	newList := [][]byte{}
	if lo, hi, ok := storage.ZNormalizeRankRange(len(all), int(start), int(stop)); ok {
		newList = append([][]byte(nil), all[lo:hi+1]...)
	}

	if _, err := r.Storage.Store.LReplaceAll(ctx, pk, newList); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeList, int64(len(newList))-m.Count); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.SimpleString("OK")
}

// handleLRem implements LREM key count value (requirement 7.4): remove elements
// equal to value and reply the integer number REMOVED (Redis' contract; note the
// fork's LREM returns a different value). count>0 removes from head to tail,
// count<0 from tail to head, count==0 removes every occurrence. An absent key
// replies ":0"; a live non-List key replies WRONGTYPE; a non-integer count replies
// the not-an-integer error. meta.cnt is reconciled to the new length.
func (r *Router) handleLRem(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])
	value := args[3]

	count, err := ParseInt(args[2])
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
		w.Int(0)
		return
	}

	all, err := r.Storage.Store.LRangeAll(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	newList, removed := lremCompute(all, count, value)
	if removed == 0 {
		w.Int(0)
		return
	}

	if _, err := r.Storage.Store.LReplaceAll(ctx, pk, newList); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeList, -int64(removed)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(removed))
}

// lremCompute returns the element list with elements equal to value removed per
// Redis' LREM count semantics, and how many were removed. count>0 removes up to
// count occurrences scanning head->tail; count<0 removes up to |count| scanning
// tail->head; count==0 removes every occurrence. It never mutates all.
//
// count is an int64 (not int) and the magnitude is capped at n BEFORE any negation
// so that count == math.MinInt64 does not overflow: `-math.MinInt64` wraps back to a
// negative value, which previously made `left > 0` false immediately and removed
// nothing. Redis treats such an over-large magnitude as "remove every match in the
// scan direction" (its `removed == toremove` guard never fires), which capping at n
// reproduces exactly — and removing all matches from the tail yields the same final
// list as from the head, so direction is immaterial once the whole list is covered.
func lremCompute(all [][]byte, count int64, value []byte) (newList [][]byte, removed int) {
	n := len(all)
	drop := make([]bool, n)

	switch {
	case count > 0:
		left := count
		if left > int64(n) {
			left = int64(n)
		}
		for i := 0; i < n && left > 0; i++ {
			if bytes.Equal(all[i], value) {
				drop[i] = true
				removed++
				left--
			}
		}
	case count < 0:
		// Cap the magnitude at n before negating: for count <= -n (which includes the
		// overflow-prone math.MinInt64) that is the whole list, so left = n; otherwise
		// -count is a safe small magnitude.
		var left int64
		if count < -int64(n) {
			left = int64(n)
		} else {
			left = -count
		}
		for i := n - 1; i >= 0 && left > 0; i-- {
			if bytes.Equal(all[i], value) {
				drop[i] = true
				removed++
				left--
			}
		}
	default: // count == 0: remove all occurrences
		for i := 0; i < n; i++ {
			if bytes.Equal(all[i], value) {
				drop[i] = true
				removed++
			}
		}
	}

	newList = make([][]byte, 0, n-removed)
	for i := 0; i < n; i++ {
		if !drop[i] {
			newList = append(newList, all[i])
		}
	}

	return newList, removed
}

// handleLInsert implements LINSERT key BEFORE|AFTER pivot value (requirement 7.4):
// insert value before/after the first element equal to pivot and reply the new
// length; reply :-1 when pivot is not present, :0 when the key is absent. An
// illegal where token replies "-ERR syntax error"; a live non-List key replies
// WRONGTYPE. meta.cnt is bumped by one on a successful insert.
func (r *Router) handleLInsert(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key := args[1]
	pk := encodePK(c.DB(), key)
	pivot := args[3]
	value := args[4]

	var before bool
	switch strings.ToUpper(string(args[2])) {
	case "BEFORE":
		before = true
	case "AFTER":
		before = false
	default:
		w.Error(resp.ErrSyntax)
		return
	}

	// Type-check BEFORE the value-size guard: Redis 3.2 linsertCommand replies
	// WRONGTYPE for a non-List key with no notion of value size, so a WRONGTYPE key
	// must win over an oversized value (the size guard would otherwise mask it).
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

	if err := guard.CheckWrite(key, [][]byte{value}, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}

	all, err := r.Storage.Store.LRangeAll(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	pivotIdx := -1
	for i := range all {
		if bytes.Equal(all[i], pivot) {
			pivotIdx = i
			break
		}
	}
	if pivotIdx < 0 {
		w.Int(-1)
		return
	}

	insertAt := pivotIdx
	if !before {
		insertAt = pivotIdx + 1
	}

	newList := make([][]byte, 0, len(all)+1)
	newList = append(newList, all[:insertAt]...)
	newList = append(newList, append([]byte(nil), value...))
	newList = append(newList, all[insertAt:]...)

	if _, err := r.Storage.Store.LReplaceAll(ctx, pk, newList); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeList, 1); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(len(newList)))
}

// handleRPopLPush implements RPOPLPUSH source destination (requirement 7.5):
// remove the tail element of source and push it to the head of destination,
// replying the moved element as a bulk string, or the null bulk string when
// source is absent/empty. When source == destination it is a single-key rotation
// (tail moves to head, length unchanged). A live non-List source or destination
// replies WRONGTYPE.
//
// Atomicity: composed from RPop + LPush and best-effort only — see the file
// header. Both keys' meta counters are maintained here (source is deleted when it
// empties; destination is created if absent); true two-key atomicity is deferred
// to task 20.1.
func (r *Router) handleRPopLPush(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	srcPK := encodePK(c.DB(), args[1])
	dstPK := encodePK(c.DB(), args[2])

	_, live, wrongType, err := r.listState(ctx, srcPK)
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

	// Single-key rotation: pop the tail, push it back to the head. The length is
	// unchanged, so meta.cnt is left untouched.
	if srcPK == dstPK {
		val, found, err := r.Storage.Store.RPop(ctx, srcPK)
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if !found {
			w.NullBulk()
			return
		}
		if _, err := r.Storage.Store.LPush(ctx, srcPK, [][]byte{val}); err != nil {
			r.writeStoreError(c, err)
			return
		}
		w.BulkString(val)
		return
	}

	// Two-key move: verify the destination type BEFORE popping the source so a
	// WRONGTYPE destination does not lose the source element (best-effort; not
	// atomic across connections).
	_, _, dstWrong, err := r.listState(ctx, dstPK)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if dstWrong {
		w.Error(resp.ErrWrongType)
		return
	}

	val, found, err := r.Storage.Store.RPop(ctx, srcPK)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !found {
		w.NullBulk()
		return
	}

	// Source lost its tail element; delete the source key when it becomes empty.
	if err := r.adjustCount(ctx, srcPK, meta.TypeList, -1); err != nil {
		r.writeStoreError(c, err)
		return
	}

	// Push onto the destination head, creating/type-checking it, then bump its
	// length. EnsureType(delta 0) creates the meta + rejects a wrong type before
	// the element write, matching the push write-path ordering.
	if _, err := r.Storage.Meta.EnsureType(ctx, dstPK, meta.TypeList, 0); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if _, err := r.Storage.Store.LPush(ctx, dstPK, [][]byte{val}); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, dstPK, meta.TypeList, 1); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.BulkString(val)
}
