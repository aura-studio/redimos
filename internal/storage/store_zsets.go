package storage

import (
	"context"
	"math"
	"sort"

	redimo "github.com/aura-studio/redimo/v2"
)

// zsetScoreMaxMagnitude / zsetScoreMinMagnitude mirror the DynamoDB Number domain
// bounds the command layer enforces on a directly-supplied score
// (command.checkScoreDomain). A ZINCRBY / ZADD INCR *result* outside them is
// unstorable, so ZIncrBy rejects it up front (ErrScoreOutOfRange) rather than letting
// the native ADD fail with a misleading retryable "backend error". See doc §4.1.
const (
	zsetScoreMaxMagnitude = 9.9999999999999999999999999999999999999e+125
	zsetScoreMinMagnitude = 1e-130
)

// scoreOutOfDomain reports whether a computed score cannot be persisted as a
// DynamoDB Number (non-finite, or a finite magnitude above the ceiling / below the
// non-zero floor).
func scoreOutOfDomain(f float64) bool {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return true
	}
	m := math.Abs(f)
	return m > zsetScoreMaxMagnitude || (f != 0 && m < zsetScoreMinMagnitude)
}

// --- Sorted Set data operations (task 15.1) --------------------------------
//
// The redimo fork stores each member as an item under the key's pk with the
// member as the sort key and the score in the numeric sort-key attribute (skN),
// which the score index orders on. The map-returning fork range helpers lose
// order, so the ordered reads here build on the fork's ZMembersOrdered primitive
// (a single score-ordered Query over the partition) and then apply Redis' rank /
// score-bound semantics in process via the shared ZReverse / ZNormalizeRankRange
// / ZScoreInRange helpers — the same helpers the command-layer test fake uses, so
// the two implementations stay behaviourally identical. Scores round-trip as the
// fork's 17-significant-digit N encoding.

func (s *redimoStore) ZAdd(ctx context.Context, pk string, members []ZMember) (int, error) {
	// The fork's ZADD sets skN = score unconditionally (updating an existing member's
	// score) and returns the members that did not already exist, so its length is the
	// net cnt delta the caller applies. A member repeated in the input collapses in
	// the map (last score wins) and is counted at most once.
	if len(members) == 0 {
		return 0, nil
	}

	m := make(map[string]float64, len(members))
	for _, zm := range members {
		m[zm.Member] = zm.Score
	}

	added, err := s.client.WithContext(ctx).ZADD(pk, m, redimo.Flags{})
	if err != nil {
		return 0, err
	}

	return len(added), nil
}

func (s *redimoStore) ZRem(ctx context.Context, pk string, members []string) (int, error) {
	// The fork's ZREM returns the members that actually existed and were removed (a
	// member listed twice counts once), so its length is the removal count the caller
	// negates into the cnt delta.
	if len(members) == 0 {
		return 0, nil
	}

	removed, err := s.client.WithContext(ctx).ZREM(pk, members...)
	if err != nil {
		return 0, err
	}

	return len(removed), nil
}

func (s *redimoStore) ZScore(ctx context.Context, pk, member string) (float64, bool, error) {
	return s.client.WithContext(ctx).ZSCORE(pk, member)
}

func (s *redimoStore) ZIncrBy(ctx context.Context, pk, member string, delta float64) (float64, bool, error) {
	// The fork's ZINCRBY does a native ADD on skN, initialising a missing member to
	// 0; a prior ZSCORE tells us whether the member was brand-new so the caller bumps
	// cnt only then.
	cl := s.client.WithContext(ctx)
	old, found, err := cl.ZSCORE(pk, member)
	if err != nil {
		return 0, false, err
	}

	// Pre-check the result against the storable Number domain using the score we just
	// read (a missing member starts at 0), so an out-of-domain result is rejected
	// deterministically before the native ADD touches the backend. No extra read.
	result := delta
	if found {
		result = old + delta
	}
	if scoreOutOfDomain(result) {
		return 0, false, ErrScoreOutOfRange
	}

	newScore, err := cl.ZINCRBY(pk, member, delta)
	if err != nil {
		return 0, false, err
	}

	return newScore, !found, nil
}

// zAscending reads every member of the sorted set at pk in ascending score order
// (ties by member) via the fork's ordered-read primitive, converting to the
// storage seam's ZMember. It is the base the rank-RANGE and score reads layer on.
//
// Cost note: ZRangeByRank / ZRangeByScore / ZCount read the WHOLE set here on
// purpose. A rank RANGE needs the full ordered sequence to slice with Redis' exact
// tie order; a score RANGE / COUNT needs redimos' ScoreBound semantics — exclusive
// "(" bounds and ±inf — which the fork's float-only, inclusive ZRANGEBYSCORE /
// ZCOUNT cannot express without changing results. The correctness of those exact
// semantics is worth the read; the reply memory is separately bounded by the
// command layer's --max-collection-result cap (rangeResultCount). Reads that CAN be
// bounded without losing semantics are: single-member ZRank/ZRevRank (fork ZRANK,
// count-by-score + tie group) and list LRange (fork LRANGE, windowed Query).
func (s *redimoStore) zAscending(ctx context.Context, pk string) ([]ZMember, error) {
	ms, err := s.client.WithContext(ctx).ZMembersOrdered(pk, true)
	if err != nil {
		return nil, err
	}

	out := make([]ZMember, len(ms))
	for i, m := range ms {
		out[i] = ZMember{Member: m.Member, Score: m.Score}
	}

	// Redis orders a sorted set by ascending score, breaking ties by ascending
	// byte-lexicographic member. The score index (LSI on skN) orders by score, but
	// its tie-break among equal scores is the backend's and is not guaranteed to be
	// the member order (DynamoDB Local, for one, does not tie-break by the table
	// sort key). Re-sort in process with the exact Redis comparator so ties are
	// member-lexicographic — members round-trip as exact bytes, so a Go string
	// compare is a byte compare.
	SortZMembers(out)

	return out, nil
}

func (s *redimoStore) ZRangeByRank(ctx context.Context, pk string, start, stop int, rev bool) ([]ZMember, error) {
	asc, err := s.zAscending(ctx, pk)
	if err != nil {
		return nil, err
	}

	ordered := asc
	if rev {
		ordered = ZReverse(asc)
	}

	lo, hi, ok := ZNormalizeRankRange(len(ordered), start, stop)
	if !ok {
		return []ZMember{}, nil
	}

	return append([]ZMember(nil), ordered[lo:hi+1]...), nil
}

func (s *redimoStore) ZRangeByScore(ctx context.Context, pk string, min, max ScoreBound, rev bool) ([]ZMember, error) {
	asc, err := s.zAscending(ctx, pk)
	if err != nil {
		return nil, err
	}

	filtered := make([]ZMember, 0, len(asc))
	for _, m := range asc {
		if ZScoreInRange(m.Score, min, max) {
			filtered = append(filtered, m)
		}
	}

	if rev {
		filtered = ZReverse(filtered)
	}

	return filtered, nil
}

func (s *redimoStore) ZCount(ctx context.Context, pk string, min, max ScoreBound) (int, error) {
	asc, err := s.zAscending(ctx, pk)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, m := range asc {
		if ZScoreInRange(m.Score, min, max) {
			count++
		}
	}

	return count, nil
}

func (s *redimoStore) ZRank(ctx context.Context, pk, member string, rev bool) (int, bool, error) {
	// Delegate to the fork's bounded ZRANK/ZREVRANK: it counts the "before" side by
	// score with a score-index Query and resolves the lexical tie-break within ONLY
	// the equal-score group — not by reading and re-sorting the whole set here. The
	// fork's tie-break is byte-lexical (identical to SortZMembers), so the rank is the
	// same as the former whole-set scan; this is a pure cost win for a single-member
	// rank on a large key.
	var (
		rank  int32
		found bool
		err   error
	)
	cl := s.client.WithContext(ctx)
	if rev {
		rank, found, err = cl.ZREVRANK(pk, member)
	} else {
		rank, found, err = cl.ZRANK(pk, member)
	}
	if err != nil {
		return 0, false, err
	}

	return int(rank), found, nil
}

func (s *redimoStore) ZRemRangeByRank(ctx context.Context, pk string, start, stop int) (int, error) {
	victims, err := s.ZRangeByRank(ctx, pk, start, stop, false)
	if err != nil {
		return 0, err
	}

	return s.zRemMembers(ctx, pk, victims)
}

func (s *redimoStore) ZRemRangeByScore(ctx context.Context, pk string, min, max ScoreBound) (int, error) {
	victims, err := s.ZRangeByScore(ctx, pk, min, max, false)
	if err != nil {
		return 0, err
	}

	return s.zRemMembers(ctx, pk, victims)
}

// zRemMembers removes the given members from pk and returns how many were removed,
// the shared tail of ZREMRANGEBYRANK / ZREMRANGEBYSCORE.
func (s *redimoStore) zRemMembers(ctx context.Context, pk string, victims []ZMember) (int, error) {
	if len(victims) == 0 {
		return 0, nil
	}

	names := make([]string, len(victims))
	for i, m := range victims {
		names[i] = m.Member
	}

	removed, err := s.client.WithContext(ctx).ZREM(pk, names...)
	if err != nil {
		return 0, err
	}

	return len(removed), nil
}

// ZReverse returns a new slice with the members of in in reverse order. It maps an
// ascending score ordering onto the descending ordering ZREVRANGE / ZREVRANGEBYSCORE
// require (ties are reversed too, matching Redis).
func ZReverse(in []ZMember) []ZMember {
	out := make([]ZMember, len(in))
	for i, m := range in {
		out[len(in)-1-i] = m
	}

	return out
}

// ZNormalizeRankRange resolves a Redis rank range [start, stop] against a set of n
// members: negative indices count from the end (-1 is the last element), a start
// past the end (or a start greater than stop) yields ok=false (empty range), and a
// stop past the end is clamped to the last index. On ok=true, lo/hi are the
// inclusive slice bounds. It backs ZRANGE / ZREVRANGE / ZREMRANGEBYRANK.
func ZNormalizeRankRange(n, start, stop int) (lo, hi int, ok bool) {
	if n == 0 {
		return 0, 0, false
	}

	if start < 0 {
		start += n
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += n
	}

	if stop >= n {
		stop = n - 1
	}

	if start >= n || stop < 0 || start > stop {
		return 0, 0, false
	}

	return start, stop, true
}

// ZScoreInRange reports whether score falls within the interval [min, max],
// honouring each bound's Exclusive flag. A -Inf min or +Inf max makes that side
// unbounded. It backs ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZCOUNT / ZREMRANGEBYSCORE.
func ZScoreInRange(score float64, min, max ScoreBound) bool {
	if min.Exclusive {
		if score <= min.Value {
			return false
		}
	} else if score < min.Value {
		return false
	}

	if max.Exclusive {
		if score >= max.Value {
			return false
		}
	} else if score > max.Value {
		return false
	}

	return true
}

// SortZMembers orders members in place by ascending score, breaking ties by
// member value, matching the score index's ordering. It is the exported form of
// the order the fork's ordered read produces, used by the command-layer test fake
// so its in-memory sorted set ranks members identically to the redimo-backed
// store.
func SortZMembers(members []ZMember) {
	sort.Slice(members, func(i, j int) bool {
		if members[i].Score != members[j].Score {
			return members[i].Score < members[j].Score
		}

		return members[i].Member < members[j].Member
	})
}
