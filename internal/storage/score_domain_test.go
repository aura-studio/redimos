package storage

import (
	"math"
	"testing"
)

// TestScoreOutOfDomain covers the round-10 fix: a ZINCRBY / ZADD INCR result whose
// magnitude leaves the storable DynamoDB Number domain (or is non-finite) is caught by
// scoreOutOfDomain, so ZIncrBy can reject it with ErrScoreOutOfRange before the native
// ADD — mirroring the command layer's checkScoreDomain for a directly-supplied score.
func TestScoreOutOfDomain(t *testing.T) {
	cases := []struct {
		name string
		f    float64
		want bool
	}{
		{"zero", 0, false},
		{"one", 1, false},
		{"negative", -12345.6789, false},
		{"at ceiling", zsetScoreMaxMagnitude, false},
		{"just over ceiling", 1.8e126, true}, // 9e125 + 9e125
		{"way over ceiling", 1e200, true},
		{"negative over ceiling", -1.8e126, true},
		{"at floor", zsetScoreMinMagnitude, false},
		{"below floor positive", 1e-200, true},
		{"below floor negative", -1e-200, true},
		{"positive infinity", math.Inf(1), true},
		{"negative infinity", math.Inf(-1), true},
		{"nan", math.NaN(), true},
	}
	for _, c := range cases {
		if got := scoreOutOfDomain(c.f); got != c.want {
			t.Errorf("scoreOutOfDomain(%v) [%s] = %v, want %v", c.f, c.name, got, c.want)
		}
	}
}
