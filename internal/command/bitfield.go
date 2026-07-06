package command

// bitfield.go implements BITFIELD key [GET type offset | SET type offset value |
// INCRBY type offset increment | OVERFLOW WRAP|SAT|FAIL]...  Each GET/SET/INCRBY
// yields one reply element (an integer, or the null bulk when a FAIL-overflow op
// is skipped); OVERFLOW only changes the mode for the ops that follow it. Signed
// fields are i1..i64, unsigned u1..u63. Offsets are bit offsets, or "#n" for the
// n-th field of the given width. Arithmetic uses math/big so every width's
// wrap/saturate/fail boundary is exact.

import (
	"context"
	"math/big"
	"strings"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

type bitfieldOp struct {
	kind     string // "GET" | "SET" | "INCRBY"
	signed   bool
	nbits    int
	offset   int64
	arg      *big.Int // SET value / INCRBY increment
	overflow string   // WRAP | SAT | FAIL, resolved at parse time
}

// bfResult is one reply element: an integer value, or nil (FAIL overflow skip).
type bfResult struct {
	nilResult bool
	value     int64
}

func (r *Router) handleBitField(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())

	ops, errMsg := parseBitfieldOps(args[2:])
	if errMsg != "" {
		w.Error(errMsg)
		return
	}

	hasWrite := false
	for _, op := range ops {
		if op.kind != "GET" {
			hasWrite = true
			break
		}
	}

	pk := encodePK(c.DB(), args[1])

	var results []bfResult
	if !hasWrite {
		// Read-only: no write, no type creation beyond the WRONGTYPE check.
		cur, _, wrongType, err := r.readCurrentString(ctx, pk)
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if wrongType {
			w.Error(resp.ErrWrongType)
			return
		}
		results = applyBitfield(cur, ops, nil)
	} else {
		// Reject an over-large result BEFORE bfSet grows the buffer: the largest write op's
		// (offset+width) determines the resulting byte length. Without this, a valid-but-huge
		// offset (up to 2^32 bits = 512MB) would allocate hundreds of MB before the write-size
		// guard ran. Mirrors handleSetRange's CheckValueSize-before-alloc.
		var maxBytes int64
		for _, op := range ops {
			if op.kind == "GET" {
				continue
			}
			if nb := (op.offset + int64(op.nbits) + 7) / 8; nb > maxBytes {
				maxBytes = nb
			}
		}
		if gerr := guard.CheckValueSize(maxBytes); gerr != nil {
			r.writeStoreError(c, gerr)
			return
		}
		_, err := r.rmwString(ctx, pk, func(base []byte) ([]byte, error) {
			next := append([]byte(nil), base...)
			// Grow to the highest write offset UP FRONT — Redis' lookupStringForBitCommand
			// sizes (and zero-fills) the string to the top write bit before applying any op,
			// so the key is created/extended even when every write op overflow-FAILs and
			// stores nothing. Without this, an all-FAIL command (e.g. `OVERFLOW FAIL SET u1
			// 0 2`) left `applied` empty, and persisting an empty value to DynamoDB errored
			// ("backend error") instead of creating the zero-filled key Redis leaves behind.
			for int64(len(next)) < maxBytes {
				next = append(next, 0)
			}
			var applied []byte
			results = applyBitfield(next, ops, &applied)
			if gerr := guard.CheckWrite(args[1], nil, [][]byte{applied}); gerr != nil {
				return nil, gerr
			}
			return applied, nil
		})
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
	}

	buf := resp.AppendArrayHeader(nil, len(results))
	for _, res := range results {
		if res.nilResult {
			buf = resp.AppendNullBulk(buf)
		} else {
			buf = resp.AppendInt(buf, res.value)
		}
	}
	c.Redcon().WriteRaw(buf)
}

// parseBitfieldOps parses the BITFIELD operation list. The OVERFLOW mode in effect
// is resolved onto each subsequent write op. Returns a RESP error body on a
// malformed op.
func parseBitfieldOps(toks [][]byte) ([]bitfieldOp, string) {
	var ops []bitfieldOp
	overflow := "WRAP"
	i := 0
	for i < len(toks) {
		switch strings.ToUpper(string(toks[i])) {
		case "OVERFLOW":
			if i+1 >= len(toks) {
				return nil, resp.ErrSyntax
			}
			mode := strings.ToUpper(string(toks[i+1]))
			if mode != "WRAP" && mode != "SAT" && mode != "FAIL" {
				return nil, "ERR Invalid OVERFLOW type specified"
			}
			overflow = mode
			i += 2
		case "GET":
			if i+2 >= len(toks) {
				return nil, resp.ErrSyntax
			}
			signed, nbits, ok := parseBitfieldType(toks[i+1])
			if !ok {
				return nil, errBitfieldType
			}
			off, ok := parseBitfieldOffset(toks[i+2], nbits)
			if !ok {
				return nil, errBitOffset
			}
			ops = append(ops, bitfieldOp{kind: "GET", signed: signed, nbits: nbits, offset: off})
			i += 3
		case "SET", "INCRBY":
			if i+3 >= len(toks) {
				return nil, resp.ErrSyntax
			}
			kind := strings.ToUpper(string(toks[i]))
			signed, nbits, ok := parseBitfieldType(toks[i+1])
			if !ok {
				return nil, errBitfieldType
			}
			off, ok := parseBitfieldOffset(toks[i+2], nbits)
			if !ok {
				return nil, errBitOffset
			}
			// Redis parses the SET value / INCRBY increment via getLongLongFromObjectOrReply
			// -> string2ll: an int64. big.Int.SetString would accept a leading '+' and an
			// arbitrary-precision magnitude that string2ll rejects (silently wrapping an
			// int64-overflowing value), so parse strictly and widen to big.Int.
			n, perr := ParseInt(toks[i+3])
			if perr != nil {
				return nil, resp.ErrNotInteger
			}
			val := big.NewInt(n)
			ops = append(ops, bitfieldOp{kind: kind, signed: signed, nbits: nbits, offset: off, arg: val, overflow: overflow})
			i += 4
		default:
			return nil, resp.ErrSyntax
		}
	}
	return ops, ""
}

const errBitfieldType = "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."

func parseBitfieldType(tok []byte) (signed bool, nbits int, ok bool) {
	if len(tok) < 2 {
		return false, 0, false
	}
	switch tok[0] {
	case 'i':
		signed = true
	case 'u':
		signed = false
	default:
		return false, 0, false
	}
	// Redis getBitfieldTypeFromArgument parses the width via string2ll, which rejects
	// a leading '+' and leading zeros ("u08"/"u+8" are invalid types); strconv.Atoi
	// would wrongly accept them.
	n64, err := ParseInt(tok[1:])
	if err != nil {
		return false, 0, false
	}
	n := int(n64)
	if signed {
		if n < 1 || n > 64 {
			return false, 0, false
		}
	} else {
		if n < 1 || n > 63 {
			return false, 0, false
		}
	}
	return signed, n, true
}

func parseBitfieldOffset(tok []byte, nbits int) (int64, bool) {
	// Redis getBitOffsetFromArgument parses the offset (and the "#n" field index) via
	// string2ll, which rejects a leading '+' and leading zeros; ParseInt mirrors that.
	if len(tok) > 0 && tok[0] == '#' {
		idx, err := ParseInt(tok[1:])
		if err != nil || idx < 0 {
			return 0, false
		}
		// Guard idx*nbits against int64 overflow (which produced a NEGATIVE bit offset and a
		// negative slice index -> process panic), then bound the resulting offset like SETBIT.
		if idx > (maxBitOffset-int64(nbits))/int64(nbits) {
			return 0, false
		}
		off := idx * int64(nbits)
		if off+int64(nbits) > maxBitOffset {
			return 0, false
		}
		return off, true
	}
	off, err := ParseInt(tok)
	// Bound offset+width at maxBitOffset (2^32), matching SETBIT; an unbounded offset let bfSet
	// grow the value to terabytes (OOM) before the write-size guard ran.
	if err != nil || off < 0 || off+int64(nbits) > maxBitOffset {
		return 0, false
	}
	return off, true
}

// applyBitfield runs the ops against val. When out is non-nil (write path) the
// resulting (possibly grown) byte slice is stored through *out.
func applyBitfield(val []byte, ops []bitfieldOp, out *[]byte) []bfResult {
	cur := val
	results := make([]bfResult, 0, len(ops))
	for _, op := range ops {
		switch op.kind {
		case "GET":
			results = append(results, bfResult{value: bfGet(cur, op.offset, op.nbits, op.signed)})
		case "SET":
			old := bfGet(cur, op.offset, op.nbits, op.signed)
			stored, ok := bfClampToWidth(op.overflow, op.signed, op.nbits, op.arg)
			if !ok {
				results = append(results, bfResult{nilResult: true})
				continue
			}
			cur = bfSet(cur, op.offset, op.nbits, stored)
			results = append(results, bfResult{value: old})
		case "INCRBY":
			old := bfGet(cur, op.offset, op.nbits, op.signed)
			sum := new(big.Int).Add(big.NewInt(old), op.arg)
			stored, ok := bfClampToWidth(op.overflow, op.signed, op.nbits, sum)
			if !ok {
				results = append(results, bfResult{nilResult: true})
				continue
			}
			cur = bfSet(cur, op.offset, op.nbits, stored)
			results = append(results, bfResult{value: bfDecode(stored, op.nbits, op.signed)})
		}
	}
	if out != nil {
		*out = cur
	}
	return results
}

// bfGet reads nbits at bit offset from val (0 past the end), sign-extending when
// signed.
func bfGet(val []byte, offset int64, nbits int, signed bool) int64 {
	var v uint64
	for i := 0; i < nbits; i++ {
		bitIndex := offset + int64(i)
		byteIdx := int(bitIndex / 8)
		var bit uint64
		if byteIdx < len(val) {
			bit = uint64((val[byteIdx] >> (7 - uint(bitIndex%8))) & 1)
		}
		v = (v << 1) | bit
	}
	return bfDecode(v, nbits, signed)
}

// bfDecode interprets the low nbits of v as a signed/unsigned integer.
func bfDecode(v uint64, nbits int, signed bool) int64 {
	if signed && nbits < 64 && v&(uint64(1)<<uint(nbits-1)) != 0 {
		v |= ^uint64(0) << uint(nbits)
	}
	return int64(v)
}

// bfSet writes the low nbits of v at bit offset, growing val with zero bytes as
// needed.
func bfSet(val []byte, offset int64, nbits int, v uint64) []byte {
	needBytes := int((offset + int64(nbits) + 7) / 8)
	for len(val) < needBytes {
		val = append(val, 0)
	}
	for i := 0; i < nbits; i++ {
		bit := (v >> uint(nbits-1-i)) & 1
		bitIndex := offset + int64(i)
		byteIdx := int(bitIndex / 8)
		mask := byte(1 << (7 - uint(bitIndex%8)))
		if bit == 1 {
			val[byteIdx] |= mask
		} else {
			val[byteIdx] &^= mask
		}
	}
	return val
}

// bfClampToWidth applies the overflow mode to want and returns the nbits-wide
// stored representation. ok is false only for FAIL when want is out of range.
func bfClampToWidth(mode string, signed bool, nbits int, want *big.Int) (uint64, bool) {
	min, max := bfBounds(signed, nbits)
	inRange := want.Cmp(min) >= 0 && want.Cmp(max) <= 0

	switch mode {
	case "FAIL":
		if !inRange {
			return 0, false
		}
		return bfEncode(want, nbits), true
	case "SAT":
		if want.Cmp(min) < 0 {
			return bfEncode(min, nbits), true
		}
		if want.Cmp(max) > 0 {
			return bfEncode(max, nbits), true
		}
		return bfEncode(want, nbits), true
	default: // WRAP
		mod := new(big.Int).Lsh(big.NewInt(1), uint(nbits))
		w := new(big.Int).Mod(want, mod) // math/big Mod is non-negative
		return w.Uint64() & bfMask(nbits), true
	}
}

func bfBounds(signed bool, nbits int) (min, max *big.Int) {
	if signed {
		half := new(big.Int).Lsh(big.NewInt(1), uint(nbits-1))
		min = new(big.Int).Neg(half)
		max = new(big.Int).Sub(half, big.NewInt(1))
	} else {
		min = big.NewInt(0)
		max = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), uint(nbits)), big.NewInt(1))
	}
	return min, max
}

// bfEncode returns the nbits-wide two's-complement representation of v.
func bfEncode(v *big.Int, nbits int) uint64 {
	mod := new(big.Int).Lsh(big.NewInt(1), uint(nbits))
	w := new(big.Int).Mod(v, mod)
	return w.Uint64() & bfMask(nbits)
}

func bfMask(nbits int) uint64 {
	if nbits >= 64 {
		return ^uint64(0)
	}
	return (uint64(1) << uint(nbits)) - 1
}
