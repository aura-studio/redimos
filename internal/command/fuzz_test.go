package command

// Go fuzz targets for the pure parsers/matchers that consume UNTRUSTED client
// bytes: the SCAN glob matcher and the numeric argument/score parsers. Every
// target's core invariant is "never panics on any input"; where it is cheap they
// also assert a reference agreement or a round-trip so the fuzzer catches wrong
// answers, not just crashes.
//
// These run as ordinary tests too: `go test` executes each Fuzz function's SEED
// CORPUS (the f.Add cases) as a normal subtest, so they gate in CI without a
// dedicated fuzzing run. Reach them with `go test -run Fuzz ./internal/command/`
// and actually fuzz with e.g. `go test -run x -fuzz FuzzGlobMatch -fuzztime 30s`.

import (
	"math"
	"strconv"
	"testing"
)

// FuzzGlobMatch fuzzes the Redis-style glob matcher (globMatch / stringMatchLen)
// with an arbitrary pattern and string. Invariants:
//   - it never panics on any pattern/string bytes (the parser walks raw bytes and
//     must be robust to unterminated classes, trailing escapes, empty inputs, ...);
//   - for a "literal" pattern (no glob metacharacters at all) the match result must
//     equal plain byte equality, a cheap reference the matcher must agree with.
func FuzzGlobMatch(f *testing.F) {
	seeds := []struct{ pattern, s string }{
		{"", ""},
		{"", "x"},
		{"*", ""},
		{"*", "anything"},
		{"h?llo", "hello"},
		{"h[a-z]llo", "hello"},
		{"h[^a-z]llo", "hAllo"},
		{"h[a-", "ha"},   // unterminated class
		{"foo\\", "foo"}, // trailing escape
		{"\\*", "*"},     // escaped literal star
		{"a*b*c", "axxbxxc"},
		{"literal", "literal"},
		{"literal", "different"},
		{"[]", "x"},
		{"[^]", "x"},
	}
	for _, s := range seeds {
		f.Add([]byte(s.pattern), []byte(s.s))
	}

	f.Fuzz(func(t *testing.T, pattern, s []byte) {
		got := globMatch(pattern, s) // must not panic

		// Cheap reference: a pattern with no glob metacharacters is a plain literal,
		// so the match must equal exact byte equality.
		if !containsGlobMeta(pattern) {
			if want := string(pattern) == string(s); got != want {
				t.Fatalf("literal glob mismatch: pattern=%q s=%q got=%v want=%v", pattern, s, got, want)
			}
		}
	})
}

// containsGlobMeta reports whether b holds any byte the glob matcher
// treats specially (*, ?, [, or the \ escape). Only when NONE are present is the
// pattern a plain literal whose match reduces to byte equality.
func containsGlobMeta(b []byte) bool {
	for _, c := range b {
		switch c {
		case '*', '?', '[', '\\':
			return true
		}
	}
	return false
}

// FuzzParseInt fuzzes the strict base-10 integer parser (ParseInt) with arbitrary
// bytes. Invariants:
//   - it never panics;
//   - a successful parse round-trips: re-parsing strconv's decimal rendering of the
//     returned value yields the same value (the accepted grammar is canonical
//     decimal, so this always holds for a genuinely-accepted input).
func FuzzParseInt(f *testing.F) {
	for _, s := range []string{
		"", "0", "-0", "+1", "1", "-1", "10", "007", " 1", "1 ",
		"9223372036854775807", "9223372036854775808",
		"-9223372036854775808", "-9223372036854775809",
		"18446744073709551616", "999999999999999999999999", "-", "12a", "0x1f",
	} {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, arg []byte) {
		v, err := ParseInt(arg) // must not panic
		if err != nil {
			return
		}
		// The canonical decimal rendering must re-parse to the same value.
		canon := strconv.FormatInt(v, 10)
		v2, err2 := ParseInt([]byte(canon))
		if err2 != nil || v2 != v {
			t.Fatalf("ParseInt round-trip failed: arg=%q v=%d re=%q err=%v v2=%d", arg, v, canon, err2, v2)
		}
	})
}

// FuzzParseFloat fuzzes the float argument parser (ParseFloat) with arbitrary
// bytes. Invariants:
//   - it never panics;
//   - a successful parse never yields NaN (ParseFloat rejects NaN by contract);
//   - the parsed value is finite-or-infinite and re-parsing its canonical rendering
//     yields an equal value (or an equal infinity), a sane round-trip.
func FuzzParseFloat(f *testing.F) {
	for _, s := range []string{
		"", "0", "1.5", "-1.5", "1e10", "-1e-10", "3.14159265358979",
		"inf", "+inf", "-inf", "Inf", "nan", "NaN", "1.0.0", "abc", " 1.5", "0x1p4",
	} {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, arg []byte) {
		v, err := ParseFloat(arg) // must not panic
		if err != nil {
			return
		}
		if math.IsNaN(v) {
			t.Fatalf("ParseFloat returned NaN for %q", arg)
		}
		// Round-trip: formatting then re-parsing yields an equal value. Use the same
		// 17-significant-digit 'g' rendering the reply path uses, which ParseFloat
		// (via strconv.ParseFloat) accepts and round-trips exactly for finite values.
		canon := strconv.FormatFloat(v, 'g', 17, 64)
		v2, err2 := ParseFloat([]byte(canon))
		if err2 != nil || v2 != v {
			t.Fatalf("ParseFloat round-trip failed: arg=%q v=%v re=%q err=%v v2=%v", arg, v, canon, err2, v2)
		}
	})
}

// FuzzParseScore fuzzes the ZADD score / ZINCRBY increment parser (parseScore),
// which layers Redis' inf/+inf/-inf spellings on top of ParseFloat. Invariants:
//   - it never panics;
//   - a successful parse never yields NaN;
//   - formatScore(score) re-parses back to the same score (the encode/decode pair
//     the ZSet handlers rely on must round-trip).
func FuzzParseScore(f *testing.F) {
	for _, s := range []string{
		"", "0", "1.5", "-1.5", "inf", "+inf", "-inf", "Inf", "-Inf", "INF",
		"nan", "1e309", "-1e309", "3", "3.0", "abc", "(", "(1",
	} {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, arg []byte) {
		score, ok := parseScore(arg) // must not panic
		if !ok {
			return
		}
		if math.IsNaN(score) {
			t.Fatalf("parseScore returned NaN for %q", arg)
		}
		// formatScore is the inverse used on the reply path; it must round-trip.
		back, ok2 := parseScore(formatScore(score))
		if !ok2 || back != score {
			t.Fatalf("parseScore/formatScore round-trip failed: arg=%q score=%v formatted=%q ok2=%v back=%v",
				arg, score, formatScore(score), ok2, back)
		}
	})
}

// FuzzParseScoreBound fuzzes the ZRANGEBYSCORE / ZCOUNT bound parser
// (parseScoreBound), which accepts an optional leading '(' exclusive marker before
// a parseScore value. Invariants:
//   - it never panics;
//   - a successful parse never yields a NaN bound;
//   - the Exclusive flag matches the presence of a leading '(' byte.
func FuzzParseScoreBound(f *testing.F) {
	for _, s := range []string{
		"", "0", "1.5", "(1.5", "(inf", "-inf", "(-inf", "(", "((1", "abc", "(abc",
	} {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, arg []byte) {
		b, ok := parseScoreBound(arg) // must not panic
		if !ok {
			return
		}
		if math.IsNaN(b.Value) {
			t.Fatalf("parseScoreBound returned NaN bound for %q", arg)
		}
		if wantExclusive := len(arg) > 0 && arg[0] == '('; b.Exclusive != wantExclusive {
			t.Fatalf("parseScoreBound exclusive mismatch: arg=%q got=%v want=%v", arg, b.Exclusive, wantExclusive)
		}
	})
}
