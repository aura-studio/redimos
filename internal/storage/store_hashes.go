package storage

import (
	"context"
	"math"
	"strconv"

	redimo "github.com/aura-studio/redimo/v3"
)

// --- Hash data operations (task 13.1) --------------------------------------
//
// Field values are stored and read back as opaque binary (BytesValue / .Bytes()),
// exactly like the String family, so HGET round-trips arbitrary bytes and the
// HINCRBY/HINCRBYFLOAT read-modify-write can reconcile the numeric decimal form
// with the same byte encoding HGET reads. Whole-partition reads
// (HGetAll/HKeys/HVals) exclude the reserved meta item (sk == redimo.MetaSK) so
// it is never surfaced as a hash field.

func (s *redimoStore) HSet(ctx context.Context, pk string, fields []HField) (int, error) {
	// Build a field->Value map (binary values) and hand it to the fork's HSET,
	// which reports the fields that were newly created via ReturnValue ALL_OLD
	// (an item with no prior attributes was new). The count of newly-created
	// fields is the net cnt delta the caller applies to meta.
	if len(fields) == 0 {
		return 0, nil
	}

	m := make(map[string]redimo.Value, len(fields))
	for _, f := range fields {
		m[f.Field] = redimo.BytesValue{B: f.Value}
	}

	newly, err := s.client.WithContext(ctx).HSET(pk, m)
	if err != nil {
		return 0, err
	}

	return len(newly), nil
}

func (s *redimoStore) HSetNX(ctx context.Context, pk, field string, val []byte) (bool, error) {
	// HSETNX conditions on attribute_not_exists(pk), so ok reports whether the field
	// was created.
	return s.client.WithContext(ctx).HSETNX(pk, field, redimo.BytesValue{B: val})
}

func (s *redimoStore) HGet(ctx context.Context, pk, field string) ([]byte, bool, error) {
	rv, err := s.client.WithContext(ctx).HGET(pk, field)
	if err != nil || rv.Empty() {
		return nil, false, err
	}

	return rv.Bytes(), true, nil
}

func (s *redimoStore) HMGet(ctx context.Context, pk string, fields []string) (map[string][]byte, error) {
	// Only present fields are returned; the caller renders a missing field as a null
	// bulk string in request order.
	if len(fields) == 0 {
		return map[string][]byte{}, nil
	}

	rvs, err := s.client.WithContext(ctx).HMGET(pk, fields...)
	if err != nil {
		return nil, err
	}

	out := make(map[string][]byte, len(rvs))
	for f, rv := range rvs {
		if !rv.Empty() {
			out[f] = rv.Bytes()
		}
	}

	return out, nil
}

func (s *redimoStore) HGetAll(ctx context.Context, pk string) ([]HField, error) {
	// The fork's HGETALL queries the whole partition, which includes the reserved
	// meta item; filter it out so it is never surfaced as a field.
	all, err := s.client.WithContext(ctx).HGETALL(pk)
	if err != nil {
		return nil, err
	}

	out := make([]HField, 0, len(all))
	for field, rv := range all {
		if field == redimo.MetaSK {
			continue
		}
		out = append(out, HField{Field: field, Value: rv.Bytes()})
	}

	return out, nil
}

func (s *redimoStore) HKeys(ctx context.Context, pk string) ([]string, error) {
	// Derived from HGetAll so the meta-item filtering is applied once.
	fields, err := s.HGetAll(ctx, pk)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(fields))
	for _, f := range fields {
		keys = append(keys, f.Field)
	}

	return keys, nil
}

func (s *redimoStore) HVals(ctx context.Context, pk string) ([][]byte, error) {
	// Derived from HGetAll so the meta-item filtering is applied once.
	fields, err := s.HGetAll(ctx, pk)
	if err != nil {
		return nil, err
	}

	vals := make([][]byte, 0, len(fields))
	for _, f := range fields {
		vals = append(vals, f.Value)
	}

	return vals, nil
}

func (s *redimoStore) HDel(ctx context.Context, pk string, fields []string) (int, error) {
	// The fork's HDEL returns the fields that actually existed and were removed (a
	// field deleted twice counts once), so its length is the removal count the caller
	// negates into the cnt delta.
	if len(fields) == 0 {
		return 0, nil
	}

	deleted, err := s.client.WithContext(ctx).HDEL(pk, fields...)
	if err != nil {
		return 0, err
	}

	return len(deleted), nil
}

func (s *redimoStore) HExists(ctx context.Context, pk, field string) (bool, error) {
	return s.client.WithContext(ctx).HEXISTS(pk, field)
}

func (s *redimoStore) HStrlen(ctx context.Context, pk, field string) (int, error) {
	// Length is derived from the stored bytes; a missing field is length 0.
	rv, err := s.client.WithContext(ctx).HGET(pk, field)
	if err != nil || rv.Empty() {
		return 0, err
	}

	return len(rv.Bytes()), nil
}

func (s *redimoStore) HIncrBy(ctx context.Context, pk, field string, delta int64) (newVal int64, isNew bool, err error) {
	// Read-modify-write reconciliation driven by a compare-and-set retry loop,
	// mirroring the String INCR family (IncrBy): HGET the current binary field
	// value, parse it as a Redis integer, apply the delta, and conditionally HSET
	// the decimal result on the value the read observed. Two connections
	// incrementing the same field concurrently cannot lose an update — the loser's
	// HSETCAS condition fails and casRetry re-reads and re-applies its delta on the
	// winner's value (requirements 16.3, 16.4). isNew reports whether this call
	// created the field so the caller bumps cnt only for a brand-new field; it
	// reflects the pre-state observed on the winning attempt. A run that exhausts
	// the retry bound surfaces ErrRMWMaxRetries.
	cl := s.client.WithContext(ctx)
	err = casRetry(func() (bool, error) {
		rv, gerr := cl.HGET(pk, field)
		if gerr != nil {
			return false, gerr
		}

		existed := !rv.Empty()
		var (
			cur    int64
			oldVal []byte
		)
		if existed {
			oldVal = rv.Bytes()
			cur, gerr = parseStoredInt(oldVal)
			if gerr != nil {
				return false, ErrHashNotInteger
			}
		}

		if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
			return false, ErrIncrOverflow
		}
		next := cur + delta

		ok, serr := cl.HSETCAS(pk, field, redimo.BytesValue{B: []byte(strconv.FormatInt(next, 10))}, redimo.BytesValue{B: oldVal}, existed)
		if serr != nil {
			return false, serr
		}
		if ok {
			newVal = next
			isNew = !existed
		}

		return ok, nil
	})

	return newVal, isNew, err
}

func (s *redimoStore) HIncrByFloat(ctx context.Context, pk, field string, delta float64) (newVal []byte, isNew bool, err error) {
	// Read-modify-write reconciliation as for HIncrBy: the HGET → HSETCAS
	// compare-and-set loop makes concurrent HINCRBYFLOAT on one field lose no update
	// (requirements 16.3, 16.4).
	cl := s.client.WithContext(ctx)
	err = casRetry(func() (bool, error) {
		rv, gerr := cl.HGET(pk, field)
		if gerr != nil {
			return false, gerr
		}

		existed := !rv.Empty()
		var (
			cur    float64
			oldVal []byte
		)
		if existed {
			oldVal = rv.Bytes()
			cur, gerr = parseStoredFloat(oldVal)
			if gerr != nil {
				return false, ErrHashNotFloat
			}
		}

		next := cur + delta
		if math.IsNaN(next) || math.IsInf(next, 0) {
			return false, ErrIncrNaNOrInfinity
		}

		out := formatRedisFloat(next)
		ok, serr := cl.HSETCAS(pk, field, redimo.BytesValue{B: out}, redimo.BytesValue{B: oldVal}, existed)
		if serr != nil {
			return false, serr
		}
		if ok {
			newVal = out
			isNew = !existed
		}

		return ok, nil
	})

	return newVal, isNew, err
}
