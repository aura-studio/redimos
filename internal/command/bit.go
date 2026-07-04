package command

// bit.go implements the Bit command family (SETBIT / GETBIT / BITCOUNT / BITPOS /
// BITOP / BITFIELD). Every bit operation works on the bytes of a String value,
// which redimos already stores and reads back as binary, so these handlers live
// purely in the command layer and reuse the String read + compare-and-set write
// machinery (readCurrentString / rmwString) — no redimo change is needed.
//
// Constraint vs Redis: a value is capped at guard.MaxValueSize (390KB, from the
// DynamoDB 400KB item limit), so the maximum reachable bit offset is ~3.19M
// (Redis allows up to 2^32). Beyond that a growing SETBIT / BITFIELD / BITOP write
// is rejected with the value-size error. BITOP is a multi-key write and, like
// MSET / *STORE, is not atomic across the source reads and the dest write.

import (
	"context"
	"math/bits"
	"strconv"
	"strings"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// maxBitOffset mirrors Redis' SETBIT/GETBIT limit: the bit offset must be < 2^32
// (the byte offset must fit in the 512MB proto-max-bulk range).
const maxBitOffset = int64(4) * 1024 * 1024 * 1024

const (
	errBitOffset   = "ERR bit offset is not an integer or out of range"
	errBitValue    = "ERR bit is not an integer or out of range"
	errBitPosBit   = "ERR The bit argument must be 1 or 0."
	errBitOpNotOne = "ERR BITOP NOT must be called with a single source key."
)

func (r *Router) registerBit() {
	t := r.Table
	t.Register("SETBIT", 4, true, r.handleSetBit)
	t.Register("GETBIT", 3, false, r.handleGetBit)
	t.Register("BITCOUNT", -2, false, r.handleBitCount)
	t.Register("BITPOS", -3, false, r.handleBitPos)
	t.Register("BITOP", -4, true, r.handleBitOp)
	t.Register("BITFIELD", -2, true, r.handleBitField)
}

// handleGetBit implements GETBIT key offset. Replies the bit (0/1) at offset; 0
// when the key is absent or the offset is past the value.
func (r *Router) handleGetBit(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	offset, ok := parseBitOffset(args[2])
	if !ok {
		w.Error(errBitOffset)
		return
	}

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

	w.Int(int64(getBit(cur, offset)))
}

// handleSetBit implements SETBIT key offset value. Sets the bit, growing the
// string with zero bytes as needed, and replies the previous bit value.
func (r *Router) handleSetBit(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key := args[1]
	offset, ok := parseBitOffset(args[2])
	if !ok {
		w.Error(errBitOffset)
		return
	}
	bitVal, ok := parseBitValue(args[3])
	if !ok {
		w.Error(errBitValue)
		return
	}

	pk := encodePK(c.DB(), key)
	var oldBit int
	_, err := r.rmwString(ctx, pk, func(base []byte) ([]byte, error) {
		byteIdx := int(offset / 8)
		next := make([]byte, len(base), maxInt(len(base), byteIdx+1))
		copy(next, base)
		for len(next) <= byteIdx {
			next = append(next, 0)
		}
		oldBit = getBit(next, offset)
		mask := byte(1 << (7 - uint(offset%8)))
		if bitVal == 1 {
			next[byteIdx] |= mask
		} else {
			next[byteIdx] &^= mask
		}
		if gerr := guard.CheckWrite(key, nil, [][]byte{next}); gerr != nil {
			return nil, gerr
		}
		return next, nil
	})
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(oldBit))
}

// handleBitCount implements BITCOUNT key [start end] (byte range, negative from
// the end). Replies the number of set bits.
func (r *Router) handleBitCount(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	// Only the bare form or the full "start end" form are valid.
	if len(args) != 2 && len(args) != 4 {
		w.Error(resp.ErrSyntax)
		return
	}

	cur, found, wrongType, err := r.readCurrentString(ctx, encodePK(c.DB(), args[1]))
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !found || len(cur) == 0 {
		w.Int(0)
		return
	}

	lo, hi := 0, len(cur)-1
	if len(args) == 4 {
		s, ok1 := parseIntArg(args[2])
		e, ok2 := parseIntArg(args[3])
		if !ok1 || !ok2 {
			w.Error(resp.ErrNotInteger)
			return
		}
		lo, hi = byteRange(len(cur), s, e)
		if lo > hi {
			w.Int(0)
			return
		}
	}

	var count int64
	for _, b := range cur[lo : hi+1] {
		count += int64(bits.OnesCount8(b))
	}
	w.Int(count)
}

// handleBitPos implements BITPOS key bit [start [end]]. Replies the position of
// the first bit set to `bit`, following Redis' clear-bit-past-the-end semantics.
func (r *Router) handleBitPos(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	if len(args) > 5 {
		w.Error(resp.ErrSyntax)
		return
	}
	bit, ok := parseBitValue(args[2])
	if !ok {
		w.Error(errBitPosBit)
		return
	}

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
		cur = nil
	}

	endGiven := len(args) == 5
	lo, hi := 0, len(cur)-1
	if len(args) >= 4 {
		s, ok1 := parseIntArg(args[3])
		if !ok1 {
			w.Error(resp.ErrNotInteger)
			return
		}
		e := int64(hi)
		if endGiven {
			e2, ok2 := parseIntArg(args[4])
			if !ok2 {
				w.Error(resp.ErrNotInteger)
				return
			}
			e = e2
		}
		lo, hi = byteRange(len(cur), s, e)
	}

	if len(cur) == 0 || lo > hi {
		// Empty range: a clear-bit search with no explicit end reports position 0
		// on a truly empty string, otherwise -1.
		if bit == 0 && !endGiven && len(cur) == 0 {
			w.Int(0)
			return
		}
		w.Int(-1)
		return
	}

	pos := bitPos(cur, bit, lo, hi)
	if pos < 0 && bit == 0 && !endGiven {
		// Redis: when searching for a clear bit with no explicit end and the range
		// is all ones, return the first bit right after the range.
		pos = (hi + 1) * 8
	}
	w.Int(int64(pos))
}

// handleBitOp implements BITOP op destkey srckey [srckey ...]. Replies the length
// of the resulting string; an all-empty result deletes destkey and replies 0.
func (r *Router) handleBitOp(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	op := strings.ToUpper(string(args[1]))
	destKey := args[2]
	srcArgs := args[3:]

	if op == "NOT" && len(srcArgs) != 1 {
		w.Error(errBitOpNotOne)
		return
	}
	if op != "AND" && op != "OR" && op != "XOR" && op != "NOT" {
		w.Error(resp.ErrSyntax)
		return
	}

	srcs := make([][]byte, len(srcArgs))
	maxLen := 0
	for i, sk := range srcArgs {
		v, found, wrongType, err := r.readCurrentString(ctx, encodePK(c.DB(), sk))
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if wrongType {
			w.Error(resp.ErrWrongType)
			return
		}
		if !found {
			v = nil
		}
		srcs[i] = v
		if len(v) > maxLen {
			maxLen = len(v)
		}
	}

	result := bitopCompute(op, srcs, maxLen)

	destPK := encodePK(c.DB(), destKey)
	if len(result) == 0 {
		// Redis deletes the destination when the result is empty.
		if _, err := r.Storage.Meta.DeleteMeta(ctx, destPK); err != nil {
			r.writeStoreError(c, err)
			return
		}
		w.Int(0)
		return
	}
	if err := guard.CheckWrite(destKey, nil, [][]byte{result}); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.overwriteAnyType(ctx, destPK); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.Storage.Meta.EnsureType(ctx, destPK, meta.TypeString, 0); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.Storage.Store.SetString(ctx, destPK, result); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if _, err := r.Storage.Meta.Persist(ctx, destPK); err != nil {
		r.writeStoreError(c, err)
		return
	}
	w.Int(int64(len(result)))
}

// --- helpers ---------------------------------------------------------------

func parseBitOffset(b []byte) (int64, bool) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil || n < 0 || n >= maxBitOffset {
		return 0, false
	}
	return n, true
}

func parseBitValue(b []byte) (int, bool) {
	switch string(b) {
	case "0":
		return 0, true
	case "1":
		return 1, true
	default:
		return 0, false
	}
}

func parseIntArg(b []byte) (int64, bool) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	return n, err == nil
}

func getBit(val []byte, offset int64) int {
	byteIdx := int(offset / 8)
	if byteIdx >= len(val) {
		return 0
	}
	return int((val[byteIdx] >> (7 - uint(offset%8))) & 1)
}

// byteRange resolves Redis' [start, end] byte indices (negative counts from the
// end) against a value of length n into a clamped inclusive [lo, hi]. When the
// range is empty lo > hi.
func byteRange(n int, start, end int64) (lo, hi int) {
	if start < 0 {
		start += int64(n)
	}
	if end < 0 {
		end += int64(n)
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}
	if end >= int64(n) {
		end = int64(n) - 1
	}
	if start > end || n == 0 {
		return 1, 0 // empty
	}
	return int(start), int(end)
}

// bitPos returns the bit index (from the start of the whole value) of the first
// bit equal to `bit` within bytes [lo, hi], or -1 if none.
func bitPos(val []byte, bit, lo, hi int) int {
	var skip byte
	if bit == 0 {
		skip = 0xff
	}
	for i := lo; i <= hi; i++ {
		if val[i] == skip {
			continue
		}
		for j := 0; j < 8; j++ {
			if int((val[i]>>(7-uint(j)))&1) == bit {
				return i*8 + j
			}
		}
	}
	return -1
}

func bitopCompute(op string, srcs [][]byte, maxLen int) []byte {
	if op == "NOT" {
		out := make([]byte, len(srcs[0]))
		for i, b := range srcs[0] {
			out[i] = ^b
		}
		return out
	}
	out := make([]byte, maxLen)
	for i := 0; i < maxLen; i++ {
		var acc byte
		first := true
		for _, s := range srcs {
			var b byte
			if i < len(s) {
				b = s[i]
			}
			if first {
				acc = b
				first = false
				continue
			}
			switch op {
			case "AND":
				acc &= b
			case "OR":
				acc |= b
			case "XOR":
				acc ^= b
			}
		}
		out[i] = acc
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
