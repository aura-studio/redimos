package command

import (
	"math"
	"testing"
)

// TestCheckScoreDomain locks in the DynamoDB Number-domain guard that keeps ZADD /
// ZINCRBY / *STORE writes atomic: a score redimos cannot persist (non-finite, or a
// finite magnitude below the ~1e-130 floor or above the ~1e125 ceiling) is rejected
// deterministically BEFORE any write, rather than reaching the backend and tearing.
// These scores are the documented §4.1 platform divergence — Redis stores them,
// redimos cannot — so they are asserted here, not against the differential oracle.
func TestCheckScoreDomain(t *testing.T) {
	cases := []struct {
		name    string
		f       float64
		wantErr string
	}{
		{"zero", 0, ""},
		{"one", 1, ""},
		{"negative small", -3.5, ""},
		{"at ceiling", 9e125, ""},
		{"at floor", 1e-130, ""},
		{"+inf", math.Inf(1), errScoreNotFinite},
		{"-inf", math.Inf(-1), errScoreNotFinite},
		{"over ceiling", 1e130, errScoreOutOfRange},
		{"below floor positive", 1e-200, errScoreOutOfRange},
		{"below floor negative", -1e-200, errScoreOutOfRange},
	}
	for _, tc := range cases {
		if got := checkScoreDomain(tc.f); got != tc.wantErr {
			t.Errorf("checkScoreDomain(%g) = %q, want %q", tc.f, got, tc.wantErr)
		}
	}
}

// TestStoreScoreRejectsOutOfDomain confirms storeScore surfaces the same rejections
// from a wire argument (so a multi-member ZADD fails all-or-nothing on the first bad
// score, leaving no orphan member behind).
func TestStoreScoreRejectsOutOfDomain(t *testing.T) {
	cases := []struct {
		arg     string
		wantErr string
	}{
		{"1", ""},
		{"1e-200", errScoreOutOfRange},
		{"1e130", errScoreOutOfRange},
		{"inf", errScoreNotFinite},
		{"notafloat", errNotValidFloat},
	}
	for _, tc := range cases {
		if _, got := storeScore([]byte(tc.arg)); got != tc.wantErr {
			t.Errorf("storeScore(%q) err = %q, want %q", tc.arg, got, tc.wantErr)
		}
	}
}
