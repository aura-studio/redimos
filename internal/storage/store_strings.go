package storage

import (
	"context"
	"math"
	"strconv"
	"strings"

	redimo "github.com/aura-studio/redimo"
)

func (s *redimoStore) GetString(ctx context.Context, pk string) ([]byte, bool, error) {
	rv, err := s.client.GET(pk)
	if err != nil || rv.Empty() {
		return nil, false, err
	}

	return rvBytes(rv), true, nil
}

func (s *redimoStore) MGetStrings(ctx context.Context, pks []string) (map[string][]byte, error) {
	// rv1 MGET is a TransactGetItems-backed multi-get. It is limited to 25 keys per
	// call (and 4 MB), so chunk the request here and merge the maps (the former
	// v2 BatchGET chunked at 100 internally; on the v1 line the adapter chunks). A
	// value is returned only for pks that have a value item; missing keys are simply
	// absent from the map. Per-key existence/type filtering is the caller's job (the
	// MGET handler passes only live String pks). Duplicate pks are fetched once.
	if len(pks) == 0 {
		return map[string][]byte{}, nil
	}

	// De-duplicate pks. rv1 MGET is TransactGetItems, which REJECTS a request that
	// references the same item twice ("Transaction request cannot include multiple
	// operations on one item"), and Redis MGET permits repeated keys (MGET s s). The
	// result is a pk-keyed map, so fetching each distinct pk once is sufficient — the
	// MGET handler assembles the reply in request order from that map.
	unique := make([]string, 0, len(pks))
	seen := make(map[string]struct{}, len(pks))
	for _, pk := range pks {
		if _, dup := seen[pk]; dup {
			continue
		}
		seen[pk] = struct{}{}
		unique = append(unique, pk)
	}

	const mgetChunk = 25 // TransactGetItems hard limit
	vals := make(map[string][]byte, len(unique))
	for start := 0; start < len(unique); start += mgetChunk {
		end := start + mgetChunk
		if end > len(unique) {
			end = len(unique)
		}

		rvs, err := s.client.MGET(unique[start:end]...)
		if err != nil {
			return nil, err
		}
		for pk, rv := range rvs {
			if !rv.Empty() {
				vals[pk] = rvBytes(rv)
			}
		}
	}

	return vals, nil
}

func (s *redimoStore) SetString(ctx context.Context, pk string, val []byte) error {
	// Store the value as binary to keep Redis' strings binary-safe. The SET is
	// unconditional; NX/XX/type conditions are decided by the caller before this call.
	_, err := s.client.SET(pk, redimo.BytesValue{B: val})
	return err
}

func (s *redimoStore) GetSetString(ctx context.Context, pk string, val []byte) ([]byte, bool, error) {
	old, err := s.client.GETSET(pk, redimo.BytesValue{B: val})
	if err != nil || old.Empty() {
		return nil, false, err
	}

	return rvBytes(old), true, nil
}

func (s *redimoStore) SetStringIfEquals(ctx context.Context, pk string, newVal, oldVal []byte, oldExists bool) (bool, error) {
	// v1 line: rv1 has NO conditional compare-and-set (no SETCAS). This degrades to a
	// best-effort unconditional write — the lost-update safety of the v2 CAS is gone
	// (an accepted v1 tradeoff). It always reports ok=true (the write landed). It is
	// only reached by APPEND/SETRANGE, which are GATED (unregistered) on the v1 line,
	// so in practice this path is never exercised; it exists to satisfy the Store
	// interface.
	_, err := s.client.SET(pk, redimo.BytesValue{B: newVal})
	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *redimoStore) IncrBy(ctx context.Context, pk string, delta int64) (newVal int64, err error) {
	// Read-modify-write reconciliation (see the Store interface doc): read the
	// current binary value, parse it as a Redis integer, apply the delta, and store
	// the decimal result back as the same binary attribute GET reads. The write is
	// a compare-and-set conditional on the value the read observed, retried by
	// casRetry, so two connections incrementing the same key concurrently cannot
	// lose an update — the loser's condition fails and it re-reads and re-applies
	// its delta on top of the winner's value (requirements 16.3, 16.4). A run that
	// exhausts the retry bound surfaces ErrRMWMaxRetries (from casRetry), with
	// newVal left at its zero value.
	cl := s.client
	err = casRetry(func() (bool, error) {
		rv, gerr := cl.GET(pk)
		if gerr != nil {
			return false, gerr
		}

		var cur int64
		oldExists := !rv.Empty()
		if oldExists {
			cur, gerr = parseStoredInt(rvBytes(rv))
			if gerr != nil {
				return false, gerr
			}
		}

		if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
			return false, ErrIncrOverflow
		}
		next := cur + delta

		// v1 line: no SETCAS, so this is an unconditional write (best-effort, not
		// lost-update-safe — the accepted v1 INCR tradeoff). casRetry runs once.
		if _, serr := cl.SET(pk, redimo.BytesValue{B: []byte(strconv.FormatInt(next, 10))}); serr != nil {
			return false, serr
		}
		newVal = next

		return true, nil
	})

	return newVal, err
}

func (s *redimoStore) IncrByFloat(ctx context.Context, pk string, delta float64) (newVal []byte, err error) {
	// Read-modify-write reconciliation as for IncrBy, driven by the same casRetry
	// compare-and-set loop so concurrent INCRBYFLOAT on one key cannot lose an update
	// (requirements 16.3, 16.4).
	cl := s.client
	err = casRetry(func() (bool, error) {
		rv, gerr := cl.GET(pk)
		if gerr != nil {
			return false, gerr
		}

		var cur float64
		oldExists := !rv.Empty()
		if oldExists {
			cur, gerr = parseStoredFloat(rvBytes(rv))
			if gerr != nil {
				return false, gerr
			}
		}

		next := cur + delta
		if math.IsNaN(next) || math.IsInf(next, 0) {
			return false, ErrIncrNaNOrInfinity
		}

		out := formatRedisFloat(next)
		// v1 line: unconditional write (no SETCAS). casRetry runs once.
		if _, serr := cl.SET(pk, redimo.BytesValue{B: out}); serr != nil {
			return false, serr
		}
		newVal = out

		return true, nil
	})

	return newVal, err
}

// parseStoredInt parses a stored String value as a base-10 signed 64-bit integer
// using the same strict rules as the command layer's argument parser (Redis
// string2ll): the empty string, a leading '+', a leading zero on a multi-digit
// number, surrounding/embedded whitespace, and out-of-range values are all
// rejected. On any violation it returns ErrNotInteger so IncrBy can surface the
// Redis "value is not an integer or out of range" reply.
func parseStoredInt(b []byte) (int64, error) {
	n := len(b)
	if n == 0 {
		return 0, ErrNotInteger
	}
	if n == 1 && b[0] == '0' {
		return 0, nil
	}

	i := 0
	negative := false
	if b[0] == '-' {
		negative = true
		i = 1
		if i == n {
			return 0, ErrNotInteger
		}
	}
	if b[i] < '1' || b[i] > '9' {
		return 0, ErrNotInteger
	}

	var v uint64 = uint64(b[i] - '0')
	i++
	for ; i < n; i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, ErrNotInteger
		}
		d := uint64(c - '0')
		if v > (math.MaxUint64-d)/10 {
			return 0, ErrNotInteger
		}
		v = v*10 + d
	}

	const maxAbs = uint64(math.MaxInt64)
	const minAbs = uint64(math.MaxInt64) + 1
	if negative {
		if v > minAbs {
			return 0, ErrNotInteger
		}
		if v == minAbs {
			return math.MinInt64, nil
		}
		return -int64(v), nil
	}
	if v > maxAbs {
		return 0, ErrNotInteger
	}
	return int64(v), nil
}

// parseStoredFloat parses a stored String value as a float64 with Redis'
// INCRBYFLOAT semantics: valid finite or infinite decimals/exponents are
// accepted, NaN is rejected, and any surrounding whitespace or trailing garbage
// is rejected (strconv.ParseFloat enforces full consumption). On failure it
// returns ErrNotFloat.
func parseStoredFloat(b []byte) (float64, error) {
	// A stored empty-string value reads back as 0.0 for INCRBYFLOAT/HINCRBYFLOAT, matching
	// Redis' strtod (SET k ""; INCRBYFLOAT k 1 -> 1). Mirrors command.ParseFloat's empty
	// short-circuit so an increment argument and a stored value validate identically.
	if len(b) == 0 {
		return 0, nil
	}
	s := string(b)
	// Match Redis' strtod, not Go's strconv: reject Go-style underscore digit separators
	// ("1_000") that strtod does not accept, and accept a hex integer constant without the
	// binary 'p' exponent that strtod does ("0x10" -> 16). This must mirror the command-layer
	// ParseFloat so a stored value and an increment argument are validated identically.
	if strings.IndexByte(s, '_') >= 0 {
		return 0, ErrNotFloat
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		if hf, ok := parseHexNoExpStored(s); ok {
			f = hf
		} else {
			return 0, ErrNotFloat
		}
	}
	if math.IsNaN(f) {
		return 0, ErrNotFloat
	}
	return f, nil
}

// parseHexNoExpStored accepts a strtod-style hex float constant lacking the binary 'p'
// exponent Go requires — both hex integers ("0x1f") and hex fractions ("0x1.8" -> 1.5) —
// by appending "p0" and re-parsing; ok=false only when a 'p'/'P' exponent is already
// present or it is not a hex constant. Mirrors command.parseHexNoExp for the stored-value
// INCRBYFLOAT/HINCRBYFLOAT read path.
func parseHexNoExpStored(s string) (float64, bool) {
	body := s
	if len(body) > 0 && (body[0] == '+' || body[0] == '-') {
		body = body[1:]
	}
	if len(body) < 3 || body[0] != '0' || (body[1] != 'x' && body[1] != 'X') {
		return 0, false
	}
	if strings.ContainsAny(body, "pP") {
		return 0, false
	}
	f, err := strconv.ParseFloat(s+"p0", 64)
	return f, err == nil
}

// formatRedisFloat renders f the way Redis formats an INCRBYFLOAT / HINCRBYFLOAT
// reply (and the value it stores): ld2string(LD_STR_HUMAN) = "%.17Lf" on a *long
// double* accumulator, with trailing zeros and any trailing '.' trimmed.
//
// redimos accumulates in float64, not long double, so a straight float64 "%.17f"
// surfaces float64's ~16-significant-digit binary noise at the 17th decimal place
// ("0.1" -> "0.10000000000000001", "3.3" -> "3.29999999999999982") whereas the long
// double's representation error lies beyond 17 decimals and trims clean ("0.1",
// "3.3"). The shortest round-tripping float64 form reproduces the long double's
// trimmed "%.17Lf" output for every value whose two representations agree to <= 17
// fractional digits — i.e. all human-entered decimals — so we prefer it. Only for
// sub-1e-17 magnitudes does the shortest form print MORE than 17 fractional digits
// (1e-20 -> "0.00000000000000000001"), where Redis instead rounds to 17 fixed
// decimals (1e-20 -> "0", 9e-18 -> "0.00000000000000001"); there we fall back to
// "%.17f" + trim to reproduce that rounding.
//
// Residual float64-vs-long-double divergence survives only for genuinely
// >17-significant-digit values (1234.5678 -> long double "...79999999999999997"),
// accumulation drift (0.1+0.1+0.1 float64 = 0.30000000000000004), and the exact
// 17th/18th-decimal rounding boundary (5e-18 -> long double "0" vs float64
// "0.00000000000000001"). Those are the accepted §4.1 floor. This is distinct from
// ZSCORE's "%.17g" significant-digit formatting (see formatScore).
func formatRedisFloat(f float64) []byte {
	// Non-finite results: Redis' ld2string special-cases +Inf/-Inf to "inf"/"-inf",
	// and a NaN falls through its "%.17Lf" to glibc's "-nan". Only HINCRBYFLOAT ever
	// reaches here with a non-finite value — INCRBYFLOAT rejects those before formatting
	// (its store guard stays), matching Redis' incrbyfloatCommand isnan/isinf check that
	// hincrbyfloatCommand lacks.
	if math.IsInf(f, 1) {
		return []byte("inf")
	}
	if math.IsInf(f, -1) {
		return []byte("-inf")
	}
	if math.IsNaN(f) {
		return []byte("-nan")
	}
	if short := strconv.FormatFloat(f, 'f', -1, 64); fractionalDigits(short) <= 17 {
		return []byte(short)
	}
	s := strconv.FormatFloat(f, 'f', 17, 64)
	if strings.ContainsRune(s, '.') {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	return []byte(s)
}

// fractionalDigits counts the digits after the decimal point in a plain (non-
// exponent) decimal string such as strconv.FormatFloat(_, 'f', ...) produces; it
// returns 0 when there is no fractional part.
func fractionalDigits(s string) int {
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		return len(s) - dot - 1
	}
	return 0
}
