package storage

import (
	"context"

	redimo "github.com/aura-studio/redimo/v2"
)

// --- List data operations (task 16.1) --------------------------------------
//
// Elements are handed to LPUSH/RPUSH as binary-safe redimo.BytesValue (as the
// String/Hash/Set families do); redimo v2.1 accepts either BytesValue or
// StringValue for list elements (valueBytes) and stores the value as a DynamoDB
// Binary attribute. They are read back with ReturnValue.String() — a Go string is
// byte-safe, so an element round-trips its exact bytes.
//
// The fork's list reads normalize indices against its own LLEN, which counts the
// whole partition and therefore also counts the reserved meta item (sk =
// "#meta"), inflating the length by one. That inflation is harmless for LPUSH/
// RPUSH (their return value is ignored — the length the command replies comes
// from meta.cnt) and for LPOP/RPOP (which fetch a single element from the
// score-ordered index, which never includes the meta item). It WOULD, however,
// skew negative-index normalization for arbitrary LRANGE/LINDEX. To stay exact,
// LRange/LIndex read the full element list in order via the fork's LRANGE(0, -1)
// — which reliably returns every real element because the index it queries
// excludes the meta item — and apply Redis' range/index semantics in process via
// the shared ZNormalizeRankRange helper, the same approach the Sorted Set reads
// use.

func (s *redimoStore) LPush(_ context.Context, pk string, elements [][]byte) (int, error) {
	// ctx is accepted by the seam but not yet threaded down: redimo v1.7 uses
	// context.TODO() internally. Elements are passed as binary-safe BytesValue (as
	// the String/Hash/Set families do), matching redimo v2.1's binary-tolerant list
	// element handling; every element is pushed, so the net cnt delta is len(elements).
	if len(elements) == 0 {
		return 0, nil
	}

	vals := make([]any, len(elements))
	for i, e := range elements {
		vals[i] = redimo.BytesValue{B: e}
	}

	if _, err := s.client.LPUSH(pk, vals...); err != nil {
		return 0, err
	}

	return len(elements), nil
}

func (s *redimoStore) RPush(_ context.Context, pk string, elements [][]byte) (int, error) {
	// ctx is accepted by the seam but not yet threaded down: redimo v1.7 uses
	// context.TODO() internally. Elements are passed as BytesValue as for LPush.
	if len(elements) == 0 {
		return 0, nil
	}

	vals := make([]any, len(elements))
	for i, e := range elements {
		vals[i] = redimo.BytesValue{B: e}
	}

	if _, err := s.client.RPUSH(pk, vals...); err != nil {
		return 0, err
	}

	return len(elements), nil
}

func (s *redimoStore) LPop(_ context.Context, pk string) ([]byte, bool, error) {
	// ctx is accepted by the seam but not yet threaded down: redimo v1.7 uses
	// context.TODO() internally. An empty ReturnValue means the list is empty.
	rv, err := s.client.LPOP(pk)
	if err != nil || rv.Empty() {
		return nil, false, err
	}

	return []byte(rv.String()), true, nil
}

func (s *redimoStore) RPop(_ context.Context, pk string) ([]byte, bool, error) {
	// ctx is accepted by the seam but not yet threaded down: redimo v1.7 uses
	// context.TODO() internally.
	rv, err := s.client.RPOP(pk)
	if err != nil || rv.Empty() {
		return nil, false, err
	}

	return []byte(rv.String()), true, nil
}

// lAll reads every element of the list at pk in head-to-tail order. It relies on
// the fork's LRANGE(0, -1): the -1 stop resolves against the meta-inflated length,
// but because the underlying score-index query never includes the meta item, the
// call still returns exactly the real elements in order (the inflated stop only
// raises the query limit). It is the base LRange/LIndex slice in process.
func (s *redimoStore) lAll(pk string) ([][]byte, error) {
	rvs, err := s.client.LRANGE(pk, 0, -1)
	if err != nil {
		return nil, err
	}

	out := make([][]byte, len(rvs))
	for i, rv := range rvs {
		out[i] = []byte(rv.String())
	}

	return out, nil
}

func (s *redimoStore) LRange(_ context.Context, pk string, start, stop int) ([][]byte, error) {
	// Push the range to the fork's bounded LRANGE (a score-index Query limited to the
	// requested window) instead of reading the whole list and slicing in process:
	// LRANGE key 0 9 on a million-element list now reads ~10 elements, not a million.
	// Lists have no tie-break subtlety (elements are ordered by their insertion-time
	// score index, not by member bytes), so the fork's negative-index + clamp rules
	// match Redis exactly — this is a pure cost win with identical results.
	rvs, err := s.client.LRANGE(pk, int64(start), int64(stop))
	if err != nil {
		return nil, err
	}

	// List elements are stored Binary but the fork re-wraps them as String-typed
	// ReturnValues (see redimo listElement), so the raw bytes come back via String(),
	// not Bytes() — []byte(rv.String()) is lossless. This mirrors lAll's decode.
	out := make([][]byte, len(rvs))
	for i, rv := range rvs {
		out[i] = []byte(rv.String())
	}

	return out, nil
}

func (s *redimoStore) LIndex(_ context.Context, pk string, index int) ([]byte, bool, error) {
	all, err := s.lAll(pk)
	if err != nil {
		return nil, false, err
	}

	n := len(all)
	if index < 0 {
		index += n
	}
	if index < 0 || index >= n {
		return nil, false, nil
	}

	return all[index], true, nil
}

func (s *redimoStore) LRangeAll(_ context.Context, pk string) ([][]byte, error) {
	// The whole list in head-to-tail order, the base slice the command layer's
	// LSET/LTRIM/LREM/LINSERT combined implementation reads before rewriting.
	return s.lAll(pk)
}

func (s *redimoStore) LReplaceAll(ctx context.Context, pk string, elements [][]byte) (int, error) {
	// Combined read-modify-write rewrite (see the interface doc): clear every
	// existing element item, then re-push the new sequence in head-to-tail order.
	// DeleteMembers removes all data-member items (the list's elements) but leaves
	// the reserved meta item intact, so the length counter is maintained by the
	// caller via the meta layer. RPush appends in order, so the resulting
	// head-to-tail order equals elements (passed as redimo.BytesValue by RPush).
	// This is not atomic across concurrent connections: unlike the single-item
	// String read-modify-write commands (which task 20.1 made safe with a
	// compare-and-set + retry), a multi-item list rebuild would need a DynamoDB
	// transaction across all element items for true cross-connection atomicity, so
	// it remains best-effort in P0.
	if _, err := s.DeleteMembers(ctx, pk); err != nil {
		return 0, err
	}
	if len(elements) == 0 {
		return 0, nil
	}

	return s.RPush(ctx, pk, elements)
}
