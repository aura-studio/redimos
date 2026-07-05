package storage

import (
	"context"
	"math"
	"strconv"

	redimo "github.com/aura-studio/redimo/v3"
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
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil || math.IsNaN(f) {
		return 0, ErrNotFloat
	}
	return f, nil
}

// formatRedisFloat renders f the way Redis formats an INCRBYFLOAT reply: the
// shortest decimal that round-trips, in plain (non-exponent) notation, with
// trailing zeros and any trailing decimal point trimmed (so 5.0 -> "5"). Using
// the 'f' verb with precision -1 yields the trimmed, exponent-free form directly.
func formatRedisFloat(f float64) []byte {
	return []byte(strconv.FormatFloat(f, 'f', -1, 64))
}
