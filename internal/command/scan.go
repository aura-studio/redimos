package command

import (
	"context"
	"strconv"
	"strings"

	"github.com/aura-studio/redimos/internal/resp"
	"github.com/aura-studio/redimos/internal/server"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This file implements SCAN cursor [MATCH pattern] [COUNT n] (requirements 3.8,
// 10.8, 13.3, 13.4, 13.5, 13.7) — the cursor-based keyspace iterator that clients
// use instead of the disabled KEYS (see keys.go). It ties together three seams:
//
//   - the storage scan primitive (Store.ScanKeys) that pages the table returning
//     the pks of live (present, non-expired) meta items, with the expiry predicate
//     pushed into the backend FilterExpression (`sk = "#meta" AND (未过期)`);
//   - the per-instance cursor registry (internal/scan) that bridges Redis' uint64
//     cursor to the backend's opaque pagination token (design algorithm 3); and
//   - the proxy-side MATCH glob filter (glob.go) applied to each decoded key.
//
// A single SCAN call does NOT stop at one backend page: MATCH is a proxy-side
// filter, so a sparse pattern can filter a whole page down to zero keys while
// matches live on later pages (the backend Limit counts ITEMS, and every hash
// field / set member is an item). handleScan therefore keeps pulling pages —
// the "fill-page loop" — until it has collected ~COUNT matching keys, the
// table is exhausted, or a safety valve (scanMaxItemsPerCall) trips. The
// per-fetch page size is scanFetchChunkItems, decoupled from COUNT. A MATCH
// pattern without glob metacharacters is normalized to *pattern* (substring
// match) — see normalizeMatchPattern in glob.go.
//
// Cursor lifecycle (design "SCAN 游标设计"):
//   - `SCAN 0` starts a fresh scan from the beginning of the table;
//   - a non-zero cursor is looked up in the registry — a miss (LRU eviction,
//     instance restart, or a cursor minted by a different instance) replies
//     "-ERR invalid cursor, restart scan" (requirement 13.5);
//   - when the backend reports no further pages the reply carries the terminating
//     cursor "0"; otherwise the next page's token is registered under a fresh
//     cursor returned to the client.
//
// The reply is the standard two-element SCAN array: a bulk-string cursor followed
// by an array of matching key names. The keys array is always a (possibly empty)
// array, never a null array, matching Redis/Pika.

// Fill-page loop tuning (bugfix v1-scan-substring-match). These are
// package-level vars (not consts) so tests can shrink them to exercise
// multi-page and valve paths without seeding thousands of keys.
var (
	// scanFetchChunkItems is the per-backend-call page size the fill loop pulls.
	// It is deliberately decoupled from COUNT: COUNT is the target number of
	// MATCHING keys to collect per SCAN call, not the backend page size (the
	// backend Limit counts items, and sparse MATCH pages would come back empty).
	scanFetchChunkItems int32 = 1024

	// scanMaxItemsPerCall caps the total number of items one SCAN call may pull
	// from the backend before settling on the last successful page's token. It
	// bounds the latency of a pathological sparse MATCH (e.g. a pattern matching
	// nothing over a huge table).
	scanMaxItemsPerCall = 65536
)

// scanDefaultTarget is the number of keys a SCAN call aims to collect when the
// client omits COUNT (Redis' default COUNT is 10).
const scanDefaultTarget = 10

// handleScan implements SCAN. See the file comment for the cursor lifecycle.
func (r *Router) handleScan(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())

	// The cursor is a Redis uint64. A value that does not parse (e.g. a mangled or
	// non-numeric token) is treated as an invalid cursor rather than a syntax error,
	// matching the "restart scan" contract (requirement 13.5).
	cursor, ok := parseScanCursor(args[1])
	if !ok {
		w.Error(resp.ErrInvalidCursor)
		return
	}

	// Optional [MATCH pattern] [COUNT n] pairs, in any order, each appearing at
	// most once in practice (a repeated option simply overrides the earlier one).
	var (
		pattern  []byte
		hasMatch bool
		limit    int32
	)
	opts := args[2:]
	if len(opts)%2 != 0 {
		// A dangling option keyword without its value is a syntax error.
		w.Error(resp.ErrSyntax)
		return
	}
	for i := 0; i+1 < len(opts); i += 2 {
		switch strings.ToUpper(string(opts[i])) {
		case "MATCH":
			// Plain text (no glob metacharacters) becomes a substring query;
			// see normalizeMatchPattern.
			pattern = normalizeMatchPattern(opts[i+1])
			hasMatch = true
		case "COUNT":
			// Redis parses COUNT via getLongFromObjectOrReply -> string2ll, which
			// rejects a leading '+' and leading zeros; ParseInt mirrors that exactly.
			// strconv.Atoi would wrongly accept "+5"/"007".
			n, err := ParseInt(opts[i+1])
			if err != nil {
				w.Error(resp.ErrNotInteger)
				return
			}
			if n < 1 {
				// COUNT must be a positive integer (Redis/Pika reject 0 and
				// negatives with a syntax error).
				w.Error(resp.ErrSyntax)
				return
			}
			limit = int32(n)
		default:
			w.Error(resp.ErrSyntax)
			return
		}
	}

	// COUNT is the TARGET number of matching keys to collect per call, not the
	// backend page size; a COUNT-less SCAN aims for Redis' default of 10.
	target := int(limit)
	if target <= 0 {
		target = scanDefaultTarget
	}

	// Resolve the pagination token. Cursor 0 starts fresh (nil token); any other
	// cursor must be a live, own-instance entry in the registry.
	var lek map[string]types.AttributeValue
	if cursor != 0 {
		l, ok := r.Storage.Scan.LoadOwned(cursor, c.InstID())
		if !ok {
			w.Error(resp.ErrInvalidCursor)
			return
		}
		lek = l
	}

	scanCtx := ctx
	if r.Config.ScanTimeout > 0 {
		var cancel context.CancelFunc
		scanCtx, cancel = context.WithTimeout(ctx, r.Config.ScanTimeout)
		defer cancel()
	}

	// Fill-page loop. A single backend page filtered by a sparse MATCH can yield
	// ZERO matching keys while the table still holds matches on later pages —
	// the pre-fix single-page behavior that made GUIs show "0 keys" for live
	// keys. Keep pulling pages until we have ~target matching keys, hit the end
	// of the table, or trip the safety valve.
	db := c.DB()
	out := make([][]byte, 0, target)
	seen := make(map[string]struct{}, target) // de-dup within this call
	fetched := 0                              // items pulled this call (valve accounting)
	pagesOK := 0                              // backend pages successfully read
	var nextLEK map[string]types.AttributeValue
	for {
		// Safety valve: never pull more than scanMaxItemsPerCall items in one
		// call. Settle on the last successful page's token so the next call
		// resumes exactly where this one stopped. (The pagesOK guard guarantees
		// at least one fetch per call, so a fresh scan always makes progress.)
		if pagesOK > 0 && fetched >= scanMaxItemsPerCall {
			nextLEK = lek
			break
		}
		// A timeout/deadline mid-scan settles like a backend error below: with
		// at least one successful page, return what we have (partial results
		// beat failing the whole call); with none, surface the error.
		if err := scanCtx.Err(); err != nil {
			if pagesOK == 0 {
				r.writeStoreError(c, err)
				return
			}
			nextLEK = lek
			break
		}
		keys, nl, err := r.Storage.Store.ScanKeys(scanCtx, lek, scanFetchChunkItems, r.now())
		if err != nil {
			if pagesOK == 0 {
				r.writeStoreError(c, err)
				return
			}
			nextLEK = lek
			break
		}
		pagesOK++
		fetched += len(keys)

		// Decode each pk back to its logical key, keeping only those in the
		// connection's selected database, and apply the MATCH filter proxy-side.
		for _, pk := range keys {
			key, ok := r.decodePK(db, pk)
			if !ok {
				continue
			}
			if hasMatch && !globMatch(pattern, []byte(key)) {
				continue
			}
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, []byte(key))
		}

		if nl == nil {
			// Table exhausted → terminating cursor "0" (nextLEK stays nil).
			break
		}
		lek = nl
		if len(out) >= target {
			nextLEK = lek
			break
		}
	}

	// A nil next token means the scan reached the end of the table → terminating
	// cursor "0". Otherwise register the token under a fresh cursor for the next call.
	cursorOut := "0"
	if nextLEK != nil {
		cursorOut = strconv.FormatUint(r.Storage.Scan.Save(nextLEK), 10)
	}

	// Reply the two-element SCAN array [cursor, [keys...]]. out is a non-nil slice,
	// so an empty page encodes as "*0" (empty array), never the null array.
	buf := resp.AppendArrayHeader(nil, 2)
	buf = resp.AppendBulkString(buf, []byte(cursorOut))
	buf = resp.AppendBulkArray(buf, out)
	c.Redcon().WriteRaw(buf)
}
