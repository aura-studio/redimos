package storage

import (
	"context"
	"math"
	"strconv"
	"strings"

	redimo "github.com/aura-studio/redimo/v2"
)

func (s *redimoStore) GetString(ctx context.Context, pk string) ([]byte, bool, error) {
	rv, err := s.client.WithContext(ctx).GET(pk)
	if err != nil || rv.Empty() {
		return nil, false, err
	}

	return rv.Bytes(), true, nil
}

func (s *redimoStore) MGetStrings(ctx context.Context, pks []string) (map[string][]byte, error) {
	// BatchGET issues one DynamoDB BatchGetItem per 100 partition keys (chunking and
	// UnprocessedKeys retry are handled inside the fork), de-duplicates keys, and
	// returns a value only for pks that have a value item — so this is a true batched
	// read, not the former per-key GET fan-out (one RPC per key). Per-key existence /
	// expiry / type filtering is the caller's job (the MGET handler passes only live
	// String pks). Missing keys are simply absent from the map. It is NOT the
	// transactional MGET (TransactGetItems): a plain multi-get needs no cross-key
	// atomicity and BatchGetItem is cheaper and chunk-friendly.
	rvs, err := s.client.WithContext(ctx).BatchGET(pks...)
	if err != nil {
		return nil, err
	}

	vals := make(map[string][]byte, len(rvs))
	for pk, rv := range rvs {
		if !rv.Empty() {
			vals[pk] = rv.Bytes()
		}
	}

	return vals, nil
}

func (s *redimoStore) SetString(ctx context.Context, pk string, val []byte) error {
	// Store the value as binary to keep Redis' strings binary-safe. The SET is
	// unconditional; NX/XX/type conditions are decided by the meta layer before this
	// call.
	_, err := s.client.WithContext(ctx).SET(pk, redimo.BytesValue{B: val})
	return err
}

func (s *redimoStore) GetSetString(ctx context.Context, pk string, val []byte) ([]byte, bool, error) {
	old, err := s.client.WithContext(ctx).GETSET(pk, redimo.BytesValue{B: val})
	if err != nil || old.Empty() {
		return nil, false, err
	}

	return old.Bytes(), true, nil
}

func (s *redimoStore) SetStringIfEquals(ctx context.Context, pk string, newVal, oldVal []byte, oldExists bool) (bool, error) {
	// The compare-and-set is delegated to the fork's SETCAS, whose DynamoDB
	// conditional expression asserts the value item still equals oldVal (or is still
	// absent), so a concurrent writer's change makes the condition fail and SETCAS
	// returns ok=false without writing.
	return s.client.WithContext(ctx).SETCAS(pk, redimo.BytesValue{B: newVal}, redimo.BytesValue{B: oldVal}, oldExists)
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
	cl := s.client.WithContext(ctx)
	err = casRetry(func() (bool, error) {
		rv, gerr := cl.GET(pk)
		if gerr != nil {
			return false, gerr
		}

		var cur int64
		oldExists := !rv.Empty()
		var oldVal []byte
		if oldExists {
			oldVal = rv.Bytes()
			cur, gerr = parseStoredInt(oldVal)
			if gerr != nil {
				return false, gerr
			}
		}

		if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
			return false, ErrIncrOverflow
		}
		next := cur + delta

		ok, serr := cl.SETCAS(pk, redimo.BytesValue{B: []byte(strconv.FormatInt(next, 10))}, redimo.BytesValue{B: oldVal}, oldExists)
		if serr != nil {
			return false, serr
		}
		if ok {
			newVal = next
		}

		return ok, nil
	})

	return newVal, err
}

func (s *redimoStore) IncrByFloat(ctx context.Context, pk string, delta float64) (newVal []byte, err error) {
	// Read-modify-write reconciliation as for IncrBy, driven by the same casRetry
	// compare-and-set loop so concurrent INCRBYFLOAT on one key cannot lose an update
	// (requirements 16.3, 16.4).
	cl := s.client.WithContext(ctx)
	err = casRetry(func() (bool, error) {
		rv, gerr := cl.GET(pk)
		if gerr != nil {
			return false, gerr
		}

		var cur float64
		oldExists := !rv.Empty()
		var oldVal []byte
		if oldExists {
			oldVal = rv.Bytes()
			cur, gerr = parseStoredFloat(oldVal)
			if gerr != nil {
				return false, gerr
			}
		}

		next := cur + delta
		if math.IsNaN(next) || math.IsInf(next, 0) {
			return false, ErrIncrNaNOrInfinity
		}

		out := formatRedisFloat(next)
		ok, serr := cl.SETCAS(pk, redimo.BytesValue{B: out}, redimo.BytesValue{B: oldVal}, oldExists)
		if serr != nil {
			return false, serr
		}
		if ok {
			newVal = out
		}

		return ok, nil
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
// reply (and the value it stores): ld2string(LD_STR_HUMAN) = "%.17Lf" with trailing
// zeros and any trailing decimal point trimmed. That is 17 FIXED decimal places, NOT
// the shortest round-tripping form — the difference shows for tiny magnitudes, where
// the shortest form prints far more digits (1e-20 -> "0" here, not
// "0.00000000000000000001"; 9e-18 -> "0.00000000000000001"). This is distinct from
// ZSCORE's "%.17g" significant-digit formatting (see formatScore).
func formatRedisFloat(f float64) []byte {
	s := strconv.FormatFloat(f, 'f', 17, 64)
	if strings.ContainsRune(s, '.') {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	return []byte(s)
}
