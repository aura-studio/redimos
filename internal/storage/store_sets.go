package storage

import (
	"context"
	"math"
	"math/rand"

	redimo "github.com/aura-studio/redimo/v2"
)

// --- Set data operations (task 14.1) ---------------------------------------
//
// Members are DynamoDB sort keys, so they are string-typed. The fork's SADD /
// SREM report which members were actually added / removed (via ReturnValue
// ALL_OLD), whose lengths are the net cnt deltas the caller applies to meta.
// Whole-partition reads (SMEMBERS) include the reserved meta item (sk ==
// redimo.MetaSK); it is filtered out here so it is never surfaced as a member.
// SPop / SRandMember build on the filtered member list and select in-process so
// the reserved item can never be popped or returned, and so Redis' count
// semantics (distinct vs with-repeats) are honoured exactly.

func (s *redimoStore) SAdd(ctx context.Context, pk string, members []string) (int, error) {
	// The fork's SADD returns the members that did not already exist, so its length
	// is the net cnt delta the caller applies.
	if len(members) == 0 {
		return 0, nil
	}

	added, err := s.client.WithContext(ctx).SADD(pk, members...)
	if err != nil {
		return 0, err
	}

	return len(added), nil
}

func (s *redimoStore) SRem(ctx context.Context, pk string, members []string) (int, error) {
	// The fork's SREM returns the members that actually existed and were removed (a
	// member listed twice counts once), so its length is the removal count the caller
	// negates into the cnt delta.
	if len(members) == 0 {
		return 0, nil
	}

	removed, err := s.client.WithContext(ctx).SREM(pk, members...)
	if err != nil {
		return 0, err
	}

	return len(removed), nil
}

func (s *redimoStore) SIsMember(ctx context.Context, pk, member string) (bool, error) {
	return s.client.WithContext(ctx).SISMEMBER(pk, member)
}

func (s *redimoStore) SMembers(ctx context.Context, pk string) ([]string, error) {
	// The fork's SMEMBERS queries the whole partition, which includes the reserved
	// meta item; filter it out so it is never surfaced as a member.
	all, err := s.client.WithContext(ctx).SMEMBERS(pk)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(all))
	for _, m := range all {
		if m == redimo.MetaSK {
			continue
		}
		out = append(out, m)
	}

	return out, nil
}

func (s *redimoStore) SPop(ctx context.Context, pk string, count int) ([]string, error) {
	// SPOP removes up to count DISTINCT random members. Read the filtered member
	// list, select a random distinct subset in-process (so the reserved meta item
	// can never be popped), then delete exactly that subset and return the members
	// the delete confirmed removed — their count is the cnt delta the caller
	// negates.
	if count <= 0 {
		return nil, nil
	}

	members, err := s.SMembers(ctx, pk)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}

	chosen := randomDistinct(members, count)
	if len(chosen) == 0 {
		return nil, nil
	}

	removed, err := s.client.WithContext(ctx).SREM(pk, chosen...)
	if err != nil {
		return nil, err
	}

	return removed, nil
}

func (s *redimoStore) SRandMember(ctx context.Context, pk string, count int) ([]string, error) {
	// SRANDMEMBER never removes. A non-negative count returns up to that many
	// distinct members; a negative count returns exactly -count members with
	// possible repeats (Redis semantics). Selection is in-process over the
	// filtered member list so the reserved meta item is never returned.
	members, err := s.SMembers(ctx, pk)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}

	if count < 0 {
		// -count members WITH repeats. Guard the -MinInt64 overflow (which stays negative and
		// panics make([]string,0,n)) and clamp the magnitude so a huge negative count cannot
		// OOM or crash the process. A -count above the clamp is served as clamp members.
		mag := int64(count)
		if mag == math.MinInt64 {
			mag = maxSRandRepeats
		} else {
			mag = -mag
		}
		if mag > maxSRandRepeats {
			mag = maxSRandRepeats
		}
		out := make([]string, 0, mag)
		for i := int64(0); i < mag; i++ {
			out = append(out, members[rand.Intn(len(members))])
		}

		return out, nil
	}

	return randomDistinct(members, count), nil
}

// maxSRandRepeats bounds SRANDMEMBER's WITH-REPEATS reply so a huge (or int64-overflowing)
// negative count cannot allocate an unbounded slice and OOM/panic the process.
const maxSRandRepeats = 1 << 20

// randomDistinct returns up to count distinct elements chosen uniformly at random
// from members. When count >= len(members) every member is returned (shuffled).
// It shuffles a copy so the caller's slice is left untouched.
func randomDistinct(members []string, count int) []string {
	if count >= len(members) {
		count = len(members)
	}
	if count <= 0 {
		return nil
	}

	pool := make([]string, len(members))
	copy(pool, members)
	rand.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })

	return pool[:count]
}
