package command

import (
	"context"
	"strconv"
	"strings"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This file implements the Set command family: SADD/SREM/SISMEMBER/SMEMBERS/
// SPOP/SRANDMEMBER and the O(1) SCARD counter (requirements 8.1, 8.2, 8.5). It
// follows the collection pattern the Hash family (task 13.1) established.
//
// Data model: each set member is an independent item under the key's partition
// key with the member value as the sort key (sk = member), so per-member reads
// and writes are concurrency-safe (requirement 8.1). The key's cardinality lives
// in the meta item's cnt attribute, maintained by the meta conditional write's
// atomic ADD, so SCARD is O(1) (requirement 8.2) and always equals the current
// cardinality (requirement 8.5).
//
// Collection write-path pattern (shared with the Hash family via datacmd.go):
//
//	guard.CheckWrite(key, members, nil)          // size limits, no partial write
//	  -> Meta.EnsureType(TypeSet, 0)             // create key + type check (WRONGTYPE)
//	  -> Store.S<op>(...)                         // per-member data mutation; returns net member delta
//	  -> r.adjustCount(pk, TypeSet, delta)        // atomic cnt maintenance, deletes key when empty
//
// EnsureType runs with a zero cnt delta FIRST, purely for the type check and key
// creation, so a wrong-type key is rejected with WRONGTYPE before any member item
// is written. The count is then adjusted by the NET number of members the data
// mutation actually created or removed, keeping meta.cnt exactly equal to the
// cardinality regardless of how many supplied members already existed.
// adjustCount deletes the key when a removal empties it, matching Redis (an empty
// set does not exist).
//
// Reads (SISMEMBER/SMEMBERS/SCARD) go through the meta item for existence/expiry
// and type: an absent or expired key behaves as an empty set, and a live key of a
// different type replies WRONGTYPE. Expiry is judged from meta.exp against the
// router's clock, independent of DynamoDB native-TTL timing.
//
// SSCAN and the set-algebra commands (SUNION/SINTER/SDIFF/*STORE/SMOVE) are task
// 14.2. SSCAN pages a single set's members within one pk (reusing the SCAN cursor
// machinery, exactly as HSCAN does for a hash's fields). The set-algebra commands
// read the operand sets into proxy memory and compute the union/intersection/
// difference there: this is a NON-ATOMIC SNAPSHOT (design 需求 8.4) — each operand
// set is read at a slightly different instant, so the result reflects a
// point-in-time snapshot per key rather than one atomic view across keys, and a
// concurrent mutation to any operand may or may not be reflected. Large sets carry
// the memory and RCU cost of reading every member.

// registerSets installs the Set command family on the router's table. It is
// invoked from registerDataCommands (router_storage.go). Arity counts include the
// command name; the mutating commands are marked Write.
func (r *Router) registerSets() {
	r.reg("SADD", -3, true, r.handleSAdd)
	r.reg("SREM", -3, true, r.handleSRem)
	r.reg("SISMEMBER", 3, false, r.handleSIsMember)
	r.reg("SMEMBERS", 2, false, r.handleSMembers)
	r.reg("SCARD", 2, false, r.handleSCard)
	r.reg("SPOP", -2, true, r.handleSPop)
	r.reg("SRANDMEMBER", -2, false, r.handleSRandMember)
	r.reg("SSCAN", -3, false, r.handleSScan)
	r.reg("SUNION", -2, false, r.handleSUnion)
	r.reg("SINTER", -2, false, r.handleSInter)
	r.reg("SDIFF", -2, false, r.handleSDiff)
	r.reg("SUNIONSTORE", -3, true, r.handleSUnionStore)
	r.reg("SINTERSTORE", -3, true, r.handleSInterStore)
	r.reg("SDIFFSTORE", -3, true, r.handleSDiffStore)
	r.reg("SMOVE", 4, true, r.handleSMove)
}

// setState is the outcome of loading a key's meta for a Set command: whether it is
// a live Set, whether it is live but a different type (WRONGTYPE), and the loaded
// meta (valid only when the key is a live Set). An absent or expired key reports
// live=false, wrongType=false — a Set read then behaves as if the key were an
// empty set.
func (r *Router) setState(ctx context.Context, pk string) (m meta.Meta, live, wrongType bool, err error) {
	return r.loadMetaState(ctx, pk, meta.TypeSet)
}

// ensureSetWritable runs the collection write-path preamble shared by the Set
// mutations that add members: it validates member sizes through the guard, then
// performs the meta type check / key creation with a zero count delta. It reports
// ok=false and has already written the RESP error (WRONGTYPE, backend limit, or a
// backend error) when the write must not proceed. On ok=true the key exists as a
// Set and the caller may perform the member mutation and then adjust the count.
func (r *Router) ensureSetWritable(ctx context.Context, c *server.Conn, pk string, key []byte, members [][]byte) bool {
	if err := guard.CheckWrite(key, members, nil); err != nil {
		r.writeStoreError(c, err)
		return false
	}
	if err := r.ensureTypeExpiring(ctx, pk, meta.TypeSet); err != nil {
		r.writeStoreError(c, err)
		return false
	}

	return true
}

// bytesToStrings converts member argument bytes to the string form the storage
// seam consumes (a DynamoDB sort key is always a string).
func bytesToStrings(bs [][]byte) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = string(b)
	}

	return out
}

// stringsToBulk converts member strings to the bulk-string element form the RESP
// writer emits.
func stringsToBulk(ss []string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}

	return out
}

// handleSAdd implements SADD key member [member ...] (requirements 8.1, 8.5): add
// the members and reply the integer number that were newly added (members already
// present do not count). cnt is bumped by that same number so SCARD stays equal to
// the cardinality. A live non-Set key replies WRONGTYPE.
func (r *Router) handleSAdd(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key := args[1]
	members := args[2:]

	pk := encodePK(c.DB(), key)
	if !r.ensureSetWritable(ctx, c, pk, key, members) {
		return
	}

	added, err := r.Storage.Store.SAdd(ctx, pk, bytesToStrings(members))
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeSet, int64(added)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(added))
}

// handleSRem implements SREM key member [member ...] (requirements 8.1, 8.5):
// remove the given members and reply the integer count that actually existed and
// were removed. cnt is decremented by that count, and the key is deleted when its
// last member is removed (an empty set does not exist). An absent/expired key
// replies ":0"; a live non-Set key replies WRONGTYPE.
func (r *Router) handleSRem(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])
	members := args[2:]

	_, live, wrongType, err := r.setState(ctx, pk)
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

	removed, err := r.Storage.Store.SRem(ctx, pk, bytesToStrings(members))
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeSet, -int64(removed)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(removed))
}

// handleSIsMember implements SISMEMBER key member (requirement 8.1): reply ":1"
// when the member exists in a live set, else ":0". An absent/expired key replies
// ":0"; a live non-Set key replies WRONGTYPE.
func (r *Router) handleSIsMember(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	_, live, wrongType, err := r.setState(ctx, pk)
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

	isMember, err := r.Storage.Store.SIsMember(ctx, pk, string(args[2]))
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if isMember {
		w.Int(1)
		return
	}

	w.Int(0)
}

// handleSMembers implements SMEMBERS key (requirement 8.1): reply an array of the
// set's members. An absent/expired key replies the empty array; a live non-Set key
// replies WRONGTYPE.
func (r *Router) handleSMembers(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	m, live, wrongType, err := r.setState(ctx, pk)
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

	members, err := r.Storage.Store.SMembers(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.BulkArray(stringsToBulk(members))
}

// handleSCard implements SCARD key (requirements 8.2, 8.5): reply the set's
// cardinality in O(1) from meta.cnt. An absent/expired key replies ":0"; a live
// non-Set key replies WRONGTYPE.
func (r *Router) handleSCard(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	m, live, wrongType, err := r.setState(ctx, pk)
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

// handleSPop implements SPOP key [count] (requirements 8.1, 8.5): remove and return
// random member(s), decrementing cnt by the number removed and deleting the key
// when its last member is popped.
//
//   - Without count: reply the removed member as a bulk string, or the null bulk
//     string when the key is absent/expired or empty.
//   - With count: reply an array of the removed members (the empty array when the
//     key is absent/expired). A non-integer count replies the not-an-integer error;
//     a negative count replies the out-of-range error; more than one optional
//     argument replies the syntax error.
//
// A live non-Set key replies WRONGTYPE.
func (r *Router) handleSPop(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	// Parse the optional count and decide the reply shape (scalar vs array).
	withCount := false
	count := 1
	if len(args) == 3 {
		withCount = true
		n, err := ParseInt(args[2])
		if err != nil {
			w.Error(resp.ErrNotInteger)
			return
		}
		if n < 0 {
			// Redis 3.2's spopWithCountCommand replies "index out of range" for a negative
			// count (the "value is out of range, must be positive" wording is Redis 5.0+).
			w.Error("ERR index out of range")
			return
		}
		count = int(n)
	} else if len(args) > 3 {
		w.Error(resp.ErrSyntax)
		return
	}

	_, live, wrongType, err := r.setState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		if withCount {
			w.EmptyArray()
		} else {
			w.NullBulk()
		}
		return
	}

	removed, err := r.Storage.Store.SPop(ctx, pk, count)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeSet, -int64(len(removed))); err != nil {
		r.writeStoreError(c, err)
		return
	}

	if withCount {
		w.BulkArray(stringsToBulk(removed))
		return
	}

	if len(removed) == 0 {
		w.NullBulk()
		return
	}

	w.BulkString([]byte(removed[0]))
}

// handleSRandMember implements SRANDMEMBER key [count] (requirement 8.1): return
// random member(s) WITHOUT removing any (cnt is unchanged).
//
//   - Without count: reply a random member as a bulk string, or the null bulk
//     string when the key is absent/expired or empty.
//   - With count: reply an array. A non-negative count returns up to that many
//     distinct members; a negative count returns exactly -count members with
//     possible repeats (Redis semantics). An absent/expired key replies the empty
//     array. A non-integer count replies the not-an-integer error; more than one
//     optional argument replies the syntax error.
//
// A live non-Set key replies WRONGTYPE.
func (r *Router) handleSRandMember(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	withCount := false
	count := 1
	if len(args) == 3 {
		withCount = true
		n, err := ParseInt(args[2])
		if err != nil {
			w.Error(resp.ErrNotInteger)
			return
		}
		count = int(n)
	} else if len(args) > 3 {
		w.Error(resp.ErrSyntax)
		return
	}

	_, live, wrongType, err := r.setState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		if withCount {
			w.EmptyArray()
		} else {
			w.NullBulk()
		}
		return
	}

	members, err := r.Storage.Store.SRandMember(ctx, pk, count)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	if withCount {
		w.BulkArray(stringsToBulk(members))
		return
	}

	if len(members) == 0 {
		w.NullBulk()
		return
	}

	w.BulkString([]byte(members[0]))
}

// handleSScan implements SSCAN key cursor [MATCH pattern] [COUNT n] (requirement
// 8.3). It is the Set-scoped analogue of SCAN (see scan.go) and mirrors HSCAN
// (hashes.go): where SCAN pages the WHOLE keyspace, SSCAN pages WITHIN a single pk
// — the members of one set — via Store.SScan (a partition Query), REUSING the
// exact same cursor machinery. The uint64 cursor bridges to the backend's opaque
// pagination token through the per-instance SCAN registry (r.Storage.Scan), MATCH
// is applied proxy-side to the member name (glob.go), and COUNT maps to the Query
// page limit.
//
// Cursor lifecycle mirrors SCAN/HSCAN: "SSCAN key 0" starts a fresh page; a
// non-zero cursor must be an own-instance registry entry, and a miss (LRU
// eviction, instance restart, or a cursor minted elsewhere) replies "-ERR invalid
// cursor, restart scan"; the terminating page carries cursor "0", otherwise the
// next page's token is registered under a fresh cursor.
//
// The reply is the two-element array [cursor, [member1, member2, ...]]. A
// wrong-type key replies WRONGTYPE; an absent or expired key replies the
// terminating ["0", []] (an empty, non-null inner array), exactly as SMEMBERS
// treats an absent key as an empty set.
func (r *Router) handleSScan(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])

	// The cursor is a Redis uint64. A value that does not parse is treated as an
	// invalid cursor (the "restart scan" contract), not a syntax error, matching
	// SCAN/HSCAN.
	cursor, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil {
		w.Error(resp.ErrInvalidCursor)
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
			n, err := strconv.Atoi(string(opts[i+1]))
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

	// Type / existence check via meta: a live non-Set key is WRONGTYPE; an
	// absent/expired key behaves as an empty set and replies the terminating
	// ["0", []].
	_, live, wrongType, err := r.setState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !live {
		writeSScanReply(c, "0", nil)
		return
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

	members, nextLEK, err := r.Storage.Store.SScan(ctx, pk, lek, limit)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	// Apply the MATCH filter proxy-side on the member name (COUNT-limited pages,
	// MATCH-filtered content — exactly SCAN's contract). out is a non-nil slice, so
	// an empty filtered page still encodes as an empty array, never the null array.
	out := make([][]byte, 0, len(members))
	for _, m := range members {
		if hasMatch && !globMatch(pattern, []byte(m)) {
			continue
		}
		out = append(out, []byte(m))
	}

	// A nil next token means the partition has been fully paged → terminating
	// cursor "0". Otherwise register the token under a fresh cursor.
	cursorOut := "0"
	if nextLEK != nil {
		cursorOut = strconv.FormatUint(r.Storage.Scan.Save(nextLEK), 10)
	}

	writeSScanReply(c, cursorOut, out)
}

// writeSScanReply writes the two-element SSCAN array [cursor, [members...]]. A nil
// members slice is normalized to a non-nil empty slice so the inner array always
// encodes as "*0" (empty array), never the null array "*-1", matching Redis/Pika.
func writeSScanReply(c *server.Conn, cursor string, members [][]byte) {
	writeScanReply(c, cursor, members)
}

// setOp selects which set-algebra operation computeSetAlgebra performs.
type setOp int

const (
	opUnion setOp = iota // members present in ANY operand set
	opInter              // members present in EVERY operand set
	opDiff               // members of the first set absent from all the rest
)

// loadSetMembers reads every member of the set at pk for the in-memory
// set-algebra computation. wrongType is true (and the caller replies WRONGTYPE)
// when the key is live but not a Set; an absent or expired key yields no members
// (treated as the empty set), matching SMEMBERS. This is the per-key read the
// NON-ATOMIC set-algebra snapshot (需求 8.4) is built from.
func (r *Router) loadSetMembers(ctx context.Context, pk string) (members []string, wrongType bool, err error) {
	_, live, wt, err := r.setState(ctx, pk)
	if err != nil {
		return nil, false, err
	}
	if wt {
		return nil, true, nil
	}
	if !live {
		return nil, false, nil
	}

	members, err = r.Storage.Store.SMembers(ctx, pk)

	return members, false, err
}

// computeSetAlgebra reads every operand set (pks) into proxy memory and computes
// the union/intersection/difference in process. It is a NON-ATOMIC SNAPSHOT (需求
// 8.4): the operand sets are read one after another, so the result reflects a
// point-in-time snapshot per key rather than one atomic view across keys. It
// returns wrongType=true (and no result) as soon as any operand is a live key of a
// non-Set type; an absent/expired operand is treated as the empty set.
func (r *Router) computeSetAlgebra(ctx context.Context, op setOp, pks []string) (result []string, wrongType bool, err error) {
	sets := make([]map[string]struct{}, len(pks))
	for i, pk := range pks {
		members, wt, err := r.loadSetMembers(ctx, pk)
		if err != nil {
			return nil, false, err
		}
		if wt {
			return nil, true, nil
		}
		// Redis SINTER/SINTERSTORE short-circuits in key order: the FIRST empty operand
		// (absent, or a non-existent = empty set) makes the intersection empty and it
		// returns WITHOUT type-checking later operands. Ending the loop here means a
		// later wrong-type key is never loaded — e.g. `SINTER absent wrongstring` is *0,
		// not WRONGTYPE. (Union/Diff have no such short-circuit; they type-check every
		// operand.)
		if op == opInter && len(members) == 0 {
			return []string{}, false, nil
		}
		m := make(map[string]struct{}, len(members))
		for _, member := range members {
			m[member] = struct{}{}
		}
		sets[i] = m
	}

	if len(sets) == 0 {
		return nil, false, nil
	}

	acc := make(map[string]struct{})
	switch op {
	case opUnion:
		for _, s := range sets {
			for m := range s {
				acc[m] = struct{}{}
			}
		}
	case opInter:
		// Seed with the first set, then drop any member missing from a later set.
		for m := range sets[0] {
			acc[m] = struct{}{}
		}
		for _, s := range sets[1:] {
			for m := range acc {
				if _, ok := s[m]; !ok {
					delete(acc, m)
				}
			}
		}
	case opDiff:
		// Seed with the first set, then subtract every member of the later sets.
		for m := range sets[0] {
			acc[m] = struct{}{}
		}
		for _, s := range sets[1:] {
			for m := range s {
				delete(acc, m)
			}
		}
	}

	result = make([]string, 0, len(acc))
	for m := range acc {
		result = append(result, m)
	}

	return result, false, nil
}

// handleSUnion implements SUNION key [key ...] (requirements 8.1, 8.4): reply the
// union of all the given sets as an array. It reads every operand set into memory
// and computes the union proxy-side — a NON-ATOMIC snapshot (需求 8.4). A non-Set
// operand replies WRONGTYPE; an absent operand contributes no members.
func (r *Router) handleSUnion(ctx context.Context, c *server.Conn, args [][]byte) {
	r.handleSetAlgebraRead(ctx, c, opUnion, args[1:])
}

// handleSInter implements SINTER key [key ...] (requirements 8.1, 8.4): reply the
// intersection of all the given sets. NON-ATOMIC snapshot. A non-Set operand
// replies WRONGTYPE; an absent operand (the empty set) makes the intersection
// empty.
func (r *Router) handleSInter(ctx context.Context, c *server.Conn, args [][]byte) {
	r.handleSetAlgebraRead(ctx, c, opInter, args[1:])
}

// handleSDiff implements SDIFF key [key ...] (requirements 8.1, 8.4): reply the
// members of the first set that are not present in any of the later sets.
// NON-ATOMIC snapshot. A non-Set operand replies WRONGTYPE.
func (r *Router) handleSDiff(ctx context.Context, c *server.Conn, args [][]byte) {
	r.handleSetAlgebraRead(ctx, c, opDiff, args[1:])
}

// handleSetAlgebraRead is the shared body of SUNION/SINTER/SDIFF: it encodes the
// key arguments to pks, computes the algebra as a non-atomic in-memory snapshot,
// and replies the resulting members as a (possibly empty) array. Member order is
// unspecified, matching Redis.
func (r *Router) handleSetAlgebraRead(ctx context.Context, c *server.Conn, op setOp, keys [][]byte) {
	w := resp.NewWriter(c.Redcon())

	pks := make([]string, len(keys))
	for i, k := range keys {
		pks[i] = encodePK(c.DB(), k)
	}

	result, wrongType, err := r.computeSetAlgebra(ctx, op, pks)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}

	w.BulkArray(stringsToBulk(result))
}

// handleSUnionStore implements SUNIONSTORE dest key [key ...] (requirements 8.1,
// 8.4). handleSInterStore / handleSDiffStore implement the intersection /
// difference variants. See handleSetAlgebraStore for the shared semantics.
func (r *Router) handleSUnionStore(ctx context.Context, c *server.Conn, args [][]byte) {
	r.handleSetAlgebraStore(ctx, c, opUnion, args)
}

// handleSInterStore implements SINTERSTORE dest key [key ...].
func (r *Router) handleSInterStore(ctx context.Context, c *server.Conn, args [][]byte) {
	r.handleSetAlgebraStore(ctx, c, opInter, args)
}

// handleSDiffStore implements SDIFFSTORE dest key [key ...].
func (r *Router) handleSDiffStore(ctx context.Context, c *server.Conn, args [][]byte) {
	r.handleSetAlgebraStore(ctx, c, opDiff, args)
}

// handleSetAlgebraStore is the shared body of SUNIONSTORE/SINTERSTORE/SDIFFSTORE.
// It computes the set algebra over the source keys (a NON-ATOMIC snapshot, 需求
// 8.4) and STORES the result into dest as a set, replying the resulting
// cardinality (requirements 8.1, 8.4, 8.5).
//
// Store/overwrite semantics match Redis: dest is REPLACED entirely. The result is
// computed from the sources FIRST (before dest is touched) so a dest that is also
// a source is read pre-overwrite. dest's meta and any existing members are then
// removed — regardless of dest's previous type — so a fresh Set can be written; an
// empty result leaves dest deleted and replies 0 (an empty set does not exist).
// dest's meta.cnt is maintained to the result cardinality via adjustCount so SCARD
// stays O(1) and exact (requirement 8.5). A non-Set SOURCE key replies WRONGTYPE
// and leaves dest untouched.
func (r *Router) handleSetAlgebraStore(ctx context.Context, c *server.Conn, op setOp, args [][]byte) {
	w := resp.NewWriter(c.Redcon())

	destKey := args[1]
	srcKeys := args[2:]
	destPK := encodePK(c.DB(), destKey)

	srcPKs := make([]string, len(srcKeys))
	for i, k := range srcKeys {
		srcPKs[i] = encodePK(c.DB(), k)
	}

	// Compute the result from the source sets FIRST (non-atomic snapshot). Reading
	// before touching dest means a dest that is also a source is read pre-overwrite,
	// matching Redis.
	result, wrongType, err := r.computeSetAlgebra(ctx, op, srcPKs)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}

	// Guard the members that will be stored (dest key name + each result member).
	if err := guard.CheckWrite(destKey, stringsToBulk(result), nil); err != nil {
		r.writeStoreError(c, err)
		return
	}

	// Replace dest entirely: drop its meta (clears any prior type) and reclaim any
	// existing members, so the STORE overwrites whatever was there — matching
	// Redis' *STORE overwrite-regardless-of-type semantics.
	if _, err := r.Storage.Meta.DeleteMeta(ctx, destPK); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if _, err := r.Storage.Store.DeleteMembers(ctx, destPK); err != nil {
		r.writeStoreError(c, err)
		return
	}

	// An empty result leaves dest deleted (an empty set does not exist) and
	// replies 0.
	if len(result) == 0 {
		w.Int(0)
		return
	}

	// Create dest as a fresh Set and add the result members, maintaining cnt.
	if err := r.ensureTypeExpiring(ctx, destPK, meta.TypeSet); err != nil {
		r.writeStoreError(c, err)
		return
	}
	added, err := r.Storage.Store.SAdd(ctx, destPK, result)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, destPK, meta.TypeSet, int64(added)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(added))
}

// handleSMove implements SMOVE source destination member (requirements 8.1, 8.4):
// atomically-ish move member from the source set to the destination set. When
// member is in source, it is removed from source and added to destination and the
// reply is ":1"; otherwise nothing is moved and the reply is ":0".
//
// The move is BEST EFFORT and NON-ATOMIC (需求 8.4): the remove-from-source and
// add-to-destination are separate writes on P0's serial per-connection path, not a
// single atomic transaction, so a mid-move failure could leave the member removed
// from source without being added to destination. Both keys' meta.cnt are
// maintained (source is deleted when its last member leaves; destination is
// created on demand). A wrong-type source or destination replies WRONGTYPE and
// moves nothing.
func (r *Router) handleSMove(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())

	srcKey, dstKey, member := args[1], args[2], args[3]
	srcPK := encodePK(c.DB(), srcKey)
	dstPK := encodePK(c.DB(), dstKey)

	// Redis 3.2's smoveCommand order is load-tested here and must be preserved exactly:
	//   1. source ABSENT  -> reply :0 immediately, WITHOUT type-checking the destination;
	//   2. source WRONG type -> WRONGTYPE;
	//   3. destination WRONG type (only reached once the source is a live set) -> WRONGTYPE;
	//   4. member membership decides :1 (moved) vs :0.
	// Checking the destination type before the absent-source short-circuit was a real
	// divergence (`SMOVE absent-src string-dst m` wrongly replied WRONGTYPE instead of :0).
	_, srcLive, srcWrong, err := r.setState(ctx, srcPK)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if srcWrong {
		w.Error(resp.ErrWrongType)
		return
	}
	// An absent source cannot contain the member → nothing to move. Return BEFORE the
	// destination type-check to match Redis (step 1 above).
	if !srcLive {
		w.Int(0)
		return
	}
	// Redis: when source and destination are the SAME key, SMOVE is a pure no-op that
	// only reports whether the member is present (:1) or not (:0) — it never removes and
	// recreates the key. Short-circuit here (after the source type check) so we don't run
	// the SREM+SADD path below, which for a single-member set would transiently empty the
	// key (delete-on-empty) and rebuild it, dropping its TTL. (t_set.c: `if (srcset ==
	// dstset) { addReply(ismember ? cone : czero); return; }`.)
	if srcPK == dstPK {
		isMember, err := r.Storage.Store.SIsMember(ctx, srcPK, string(member))
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if isMember {
			w.Int(1)
		} else {
			w.Int(0)
		}
		return
	}
	if _, _, dstWrong, err := r.setState(ctx, dstPK); err != nil {
		r.writeStoreError(c, err)
		return
	} else if dstWrong {
		w.Error(resp.ErrWrongType)
		return
	}

	isMember, err := r.Storage.Store.SIsMember(ctx, srcPK, string(member))
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !isMember {
		w.Int(0)
		return
	}

	// Guard the destination member write (the member size, under the dest key).
	if err := guard.CheckWrite(dstKey, [][]byte{member}, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}

	// Remove from source, then add to destination (non-atomic; see the doc above).
	removed, err := r.Storage.Store.SRem(ctx, srcPK, []string{string(member)})
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, srcPK, meta.TypeSet, -int64(removed)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	if err := r.ensureTypeExpiring(ctx, dstPK, meta.TypeSet); err != nil {
		r.writeStoreError(c, err)
		return
	}
	added, err := r.Storage.Store.SAdd(ctx, dstPK, []string{string(member)})
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, dstPK, meta.TypeSet, int64(added)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(1)
}
