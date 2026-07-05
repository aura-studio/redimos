package command

import (
	"context"
	"strconv"
	"strings"

	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
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

// handleScan implements SCAN. See the file comment for the cursor lifecycle.
func (r *Router) handleScan(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())

	// The cursor is a Redis uint64. A value that does not parse (e.g. a mangled or
	// non-numeric token) is treated as an invalid cursor rather than a syntax error,
	// matching the "restart scan" contract (requirement 13.5).
	cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
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
			pattern = opts[i+1]
			hasMatch = true
		case "COUNT":
			n, err := strconv.Atoi(string(opts[i+1]))
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

	keys, nextLEK, err := r.Storage.Store.ScanKeys(scanCtx, lek, limit, r.now())
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	// Decode each pk back to its logical key, keeping only those in the
	// connection's selected database, and apply the MATCH filter proxy-side.
	db := c.DB()
	out := make([][]byte, 0, len(keys))
	for _, pk := range keys {
		key, ok := decodePK(db, pk)
		if !ok {
			continue
		}
		if hasMatch && !globMatch(pattern, []byte(key)) {
			continue
		}
		out = append(out, []byte(key))
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
