package command

// hll.go implements the HyperLogLog command family (PFADD / PFCOUNT / PFMERGE).
// A HyperLogLog is a Redis String holding a "HYLL" blob, so — like the Bit family —
// these handlers live purely in the command layer over the binary String value;
// no redimo change is needed.
//
// The register math is a faithful port of Redis 3.2 hll.c: MurmurHash64A (seed
// 0xadc83b19), p=14 → 16384 registers of 6 bits, and Redis 3.2's LEGACY cardinality
// estimator (harmonic mean + LINEARCOUNTING + bias-correction polynomial — NOT the
// newer Ertl-2017 hllSigma/hllTau estimator, which Redis only adopted in 4.0).
// redimos always writes the DENSE encoding, so the stored blob is not byte-identical
// to Redis (which starts sparse), but because the registers AND the estimator match,
// PFCOUNT returns the same value as redis:3.2 for the same set of elements.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

const (
	hllP            = 14
	hllRegisters    = 1 << hllP // 16384
	hllPMask        = hllRegisters - 1
	hllBits         = 6
	hllRegisterMax  = (1 << hllBits) - 1 // 63
	hllQ            = 64 - hllP           // 50
	hllHdrSize      = 16
	hllRegBytes     = (hllRegisters*hllBits + 7) / 8 // 12288
	hllDenseEncoding = 0
)

// errHLLWrongType is the sentinel for "the key is a String but not a valid HLL".
var errHLLWrongType = errors.New("WRONGTYPE Key is not a valid HyperLogLog string value.")

const errHLLWrongTypeText = "WRONGTYPE Key is not a valid HyperLogLog string value."

func (r *Router) registerHLL() {
	r.reg("PFADD", -2, true, r.handlePFAdd)
	r.reg("PFCOUNT", -2, false, r.handlePFCount)
	r.reg("PFMERGE", -2, true, r.handlePFMerge)
	r.reg("PFDEBUG", -3, false, r.handlePFDebug)
}

// handlePFDebug implements PFDEBUG <subcommand> <key> (a port of Redis 3.2's
// pfdebugCommand). It inspects the stored "HYLL" String blob:
//   - GETREG   -> the 16384 dense register values (byte-identical to Redis, since
//     the registers are representation-independent).
//   - ENCODING -> "dense" (redimos always stores the dense encoding).
//   - TODENSE  -> :0 (already dense; no conversion happens).
//   - DECODE   -> the sparse-opcode dump, which only exists for the sparse
//     encoding; on redimos' always-dense blob it errors like Redis on a dense HLL.
//
// ENCODING/TODENSE/DECODE are therefore only approximately Redis-compatible: Redis
// keeps small-cardinality HLLs sparse, so for those it would report "sparse" /
// convert / decode where redimos reports dense. GETREG matches exactly.
func (r *Router) handlePFDebug(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	sub := toLower(string(args[1]))
	pk := r.encodePK(c.DB(), args[2])

	cur, found, wrongType, err := r.readCurrentString(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if wrongType {
		w.Error(resp.ErrWrongType)
		return
	}
	if !found {
		w.Error("ERR The specified key does not exist")
		return
	}
	if !isHLL(cur) {
		w.Error(errHLLWrongTypeText)
		return
	}
	regs := cur[hllHdrSize:]

	switch sub {
	case "getreg":
		if len(args) != 3 {
			w.Error(pfdebugArityErr(args[1]))
			return
		}
		buf := resp.AppendArrayHeader(nil, hllRegisters)
		for j := 0; j < hllRegisters; j++ {
			buf = resp.AppendInt(buf, int64(denseGetRegister(regs, j)))
		}
		c.Redcon().WriteRaw(buf)
	case "encoding":
		if len(args) != 3 {
			w.Error(pfdebugArityErr(args[1]))
			return
		}
		// redimos always writes the dense encoding.
		w.SimpleString("dense")
	case "todense":
		if len(args) != 3 {
			w.Error(pfdebugArityErr(args[1]))
			return
		}
		// Already dense: zero conversions.
		w.Int(0)
	case "decode":
		if len(args) != 3 {
			w.Error(pfdebugArityErr(args[1]))
			return
		}
		// DECODE only applies to the sparse encoding; the stored blob is dense.
		w.Error("ERR HLL encoding is not sparse")
	default:
		w.Error(pfdebugUnknownErr(args[1]))
	}
}

// pfdebugCommand splits its errors: an UNKNOWN subcommand echoes the name, while a
// KNOWN subcommand invoked with the wrong argument count reports an arity error.
// Both echo the subcommand as the client sent it (case preserved).
func pfdebugUnknownErr(sub []byte) string {
	return fmt.Sprintf("ERR Unknown PFDEBUG subcommand '%s'", string(sub))
}

func pfdebugArityErr(sub []byte) string {
	return fmt.Sprintf("ERR Wrong number of arguments for the '%s' subcommand", string(sub))
}

// handlePFAdd implements PFADD key [element ...]. Replies :1 if at least one
// register changed (or the key was created), else :0.
func (r *Router) handlePFAdd(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key := args[1]
	elements := args[2:]
	pk := r.encodePK(c.DB(), key)

	var changed bool
	_, err := r.rmwString(ctx, pk, func(base []byte) ([]byte, error) {
		var blob []byte
		if len(base) == 0 {
			blob = newDenseHLL()
			changed = true // creation counts as a change
		} else {
			if !isHLL(base) {
				return nil, errHLLWrongType
			}
			blob = append([]byte(nil), base...)
		}
		regs := blob[hllHdrSize:]
		for _, e := range elements {
			if hllAdd(regs, e) {
				changed = true
			}
		}
		if gerr := guard.CheckWrite(key, nil, [][]byte{blob}); gerr != nil {
			return nil, gerr
		}
		return blob, nil
	})
	if errors.Is(err, errHLLWrongType) {
		w.Error(errHLLWrongTypeText)
		return
	}
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	if changed {
		w.Int(1)
	} else {
		w.Int(0)
	}
}

// handlePFCount implements PFCOUNT key [key ...]. Replies the estimated cardinality
// of the union of the given HyperLogLogs.
func (r *Router) handlePFCount(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())

	merged := make([]byte, hllRegBytes)
	any := false
	for _, key := range args[1:] {
		cur, found, wrongType, err := r.readCurrentString(ctx, r.encodePK(c.DB(), key))
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if wrongType {
			w.Error(resp.ErrWrongType)
			return
		}
		if !found {
			continue
		}
		if !isHLL(cur) {
			w.Error(errHLLWrongTypeText)
			return
		}
		any = true
		hllMergeInto(merged, cur[hllHdrSize:])
	}

	if !any {
		w.Int(0)
		return
	}
	w.Int(hllCount(merged))
}

// handlePFMerge implements PFMERGE destkey [sourcekey ...]. Merges the sources
// (and the current destination) into destkey and replies +OK.
func (r *Router) handlePFMerge(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	destKey := args[1]

	// Pre-read the source HLLs (multi-key; the dest write below is not atomic
	// across these reads, matching redimos' other multi-key writes).
	srcRegs := make([][]byte, 0, len(args)-2)
	for _, key := range args[2:] {
		cur, found, wrongType, err := r.readCurrentString(ctx, r.encodePK(c.DB(), key))
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if wrongType {
			w.Error(resp.ErrWrongType)
			return
		}
		if !found {
			continue
		}
		if !isHLL(cur) {
			w.Error(errHLLWrongTypeText)
			return
		}
		srcRegs = append(srcRegs, cur[hllHdrSize:])
	}

	pk := r.encodePK(c.DB(), destKey)
	_, err := r.rmwString(ctx, pk, func(base []byte) ([]byte, error) {
		var blob []byte
		if len(base) == 0 {
			blob = newDenseHLL()
		} else {
			if !isHLL(base) {
				return nil, errHLLWrongType
			}
			blob = append([]byte(nil), base...)
		}
		regs := blob[hllHdrSize:]
		for _, s := range srcRegs {
			hllMergeInto(regs, s)
		}
		if gerr := guard.CheckWrite(destKey, nil, [][]byte{blob}); gerr != nil {
			return nil, gerr
		}
		return blob, nil
	})
	if errors.Is(err, errHLLWrongType) {
		w.Error(errHLLWrongTypeText)
		return
	}
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.SimpleString("OK")
}

// --- HLL core (port of Redis 3.2 hll.c, dense encoding) --------------------

func newDenseHLL() []byte {
	blob := make([]byte, hllHdrSize+hllRegBytes)
	copy(blob, "HYLL")
	blob[4] = hllDenseEncoding
	return blob
}

func isHLL(blob []byte) bool {
	return len(blob) >= hllHdrSize && blob[0] == 'H' && blob[1] == 'Y' && blob[2] == 'L' && blob[3] == 'L' &&
		blob[4] == hllDenseEncoding && len(blob) == hllHdrSize+hllRegBytes
}

// hllAdd updates the register for ele, returning true if the register grew.
func hllAdd(regs []byte, ele []byte) bool {
	patlen, index := hllPatLen(ele)
	if patlen > denseGetRegister(regs, index) {
		denseSetRegister(regs, index, patlen)
		return true
	}
	return false
}

// hllMergeInto merges src registers into dst by taking the per-register max.
func hllMergeInto(dst, src []byte) {
	for i := 0; i < hllRegisters; i++ {
		if s := denseGetRegister(src, i); s > denseGetRegister(dst, i) {
			denseSetRegister(dst, i, s)
		}
	}
}

// hllPatLen returns the register index and the position of the first set bit
// (from bit HLL_P upward) plus one, exactly as Redis' hllPatLen.
func hllPatLen(ele []byte) (patlen int, index int) {
	hash := murmur64A(ele, 0xadc83b19)
	index = int(hash & hllPMask)
	hash >>= hllP
	hash |= uint64(1) << hllQ // bound the loop
	bit := uint64(1)
	count := 1
	for hash&bit == 0 {
		count++
		bit <<= 1
	}
	return count, index
}

func denseGetRegister(regs []byte, regnum int) int {
	b := regnum * hllBits / 8
	fb := uint(regnum*hllBits) & 7
	b0 := uint64(regs[b])
	var b1 uint64
	if b+1 < len(regs) {
		b1 = uint64(regs[b+1])
	}
	return int(((b0 >> fb) | (b1 << (8 - fb))) & hllRegisterMax)
}

func denseSetRegister(regs []byte, regnum, val int) {
	b := regnum * hllBits / 8
	fb := uint(regnum*hllBits) & 7
	fb8 := 8 - fb
	v := uint64(val)
	regs[b] &= ^(byte(hllRegisterMax) << fb)
	regs[b] |= byte(v << fb)
	if b+1 < len(regs) {
		regs[b+1] &= ^(byte(hllRegisterMax) >> fb8)
		regs[b+1] |= byte(v >> fb8)
	}
}

// hllCount estimates the cardinality from the dense registers using Redis 3.2's
// LEGACY estimator (hyperloglog.c hllCount): the harmonic-mean raw estimate with
// alpha bias, then LINEARCOUNTING for small cardinalities and a polynomial bias
// correction in the mid-range. NOTE: this is deliberately NOT the newer Ertl-2017
// estimator (hllSigma/hllTau) — that one was adopted in Redis 4.0, and using it
// makes PFCOUNT differ from redis:3.2 by a unit or two for the same registers. The
// result is truncated (uint64_t cast), not rounded, matching Redis.
func hllCount(regs []byte) int64 {
	m := float64(hllRegisters)
	alpha := 0.7213 / (1 + 1.079/m)

	// E = SUM(2^-register); ez counts zero registers (each contributing 2^0 = 1).
	var e float64
	ez := 0
	for j := 0; j < hllRegisters; j++ {
		reg := denseGetRegister(regs, j)
		if reg == 0 {
			ez++
		} else {
			e += 1.0 / float64(uint64(1)<<uint(reg))
		}
	}
	e += float64(ez) // the zero registers: 2^0 each

	e = (1.0 / e) * alpha * m * m

	if e < m*2.5 && ez != 0 {
		// LINEARCOUNTING for small cardinalities.
		e = m * math.Log(m/float64(ez))
	} else if m == 16384 && e < 72000 {
		// Bias-correction polynomial for the p=14 (16384-register) case.
		bias := 5.9119e-18*(e*e*e*e) -
			1.4253e-12*(e*e*e) +
			1.2940e-7*(e*e) -
			5.2921e-3*e +
			83.3216
		e -= e * (bias / 100)
	}
	return int64(e) // truncate, matching Redis' (uint64_t) cast
}

// murmur64A is MurmurHash64A over data with the given seed (little-endian block
// reads), matching Redis' hll.c.
func murmur64A(data []byte, seed uint64) uint64 {
	const m = 0xc6a4a7935bd1e995
	const rsh = 47
	l := len(data)
	h := seed ^ (uint64(l) * m)

	n := l / 8
	for i := 0; i < n; i++ {
		k := binary.LittleEndian.Uint64(data[i*8:])
		k *= m
		k ^= k >> rsh
		k *= m
		h ^= k
		h *= m
	}

	tail := data[n*8:]
	switch len(tail) {
	case 7:
		h ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(tail[0])
		h *= m
	}

	h ^= h >> rsh
	h *= m
	h ^= h >> rsh
	return h
}
