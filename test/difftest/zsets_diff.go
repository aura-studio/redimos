package difftest

import "fmt"

// zsets_diff.go supplies the Sorted Set score-PRECISION differential sequences
// for task 15.3 (需求 9.6). Redis/Pika store zset scores as IEEE754 doubles
// while DynamoDB Number is a 38-digit decimal, so a naive score path that let
// the backend's extra decimal precision leak back out would diverge from the
// Pika v3.2.2 oracle at extreme / high-precision values. These sequences drive
// the byte-level assertion engine (harness.go) over exactly those extreme values
// to PIN that redimos reproduces Pika's double semantics — parse-to-double,
// format-as-double — rather than the backend's wider decimal.
//
// They are exposed via ZSetDiffSequences() / ZSetDiffSequenceNames() and wired
// into a dedicated live entry point (TestDiffZSets in zsets_diff_test.go)
// WITHOUT touching the shared Matrix() / difftest_test.go, keeping this file
// conflict-free with concurrent work there.
//
// Determinism discipline (so a byte-for-byte comparison is meaningful):
//
//   - Every member in a key is given a DISTINCT score, so score-ordered reads
//     (ZRANGE/ZREVRANGE WITHSCORES) have ONE unambiguous order — no reliance on
//     the equal-score tie-break, which is an implementation detail.
//   - Extreme / high-precision FORMATTING is asserted mostly via ZSCORE on a
//     SINGLE member, isolating one score's rendering rather than a whole array.
//   - No operation is allowed to produce NaN (e.g. +inf + -inf); ZINCRBY
//     overflow deliberately drives a finite score to +inf, which both endpoints
//     render "inf".
//
// Extreme score values chosen (the divergence-prone points):
//
//   - Max finite double            1.7976931348623157e+308  and its negation
//   - Min positive normal double   2.2250738585072014e-308
//   - Min positive subnormal       5e-324  (and its negation -5e-324)
//   - 2^53 and 2^53+1              9007199254740992 / 9007199254740993
//     (2^53+1 is NOT exactly representable — surfaces double rounding)
//   - High-precision decimals      1.000000000000000000000001,
//     3.141592653589793238462643383279  (extra digits a 38-digit decimal could
//     keep but a double collapses)
//   - Scientific-notation input    1e100, 1.5e-10, 3.0e3
//   - Infinities                   +inf / -inf (ZADD, ZSCORE, ZRANGEBYSCORE
//     bounds, ZINCRBY overflow)
//   - Signed zero                  0 vs -0 (both must render "0")
//
// Also included, per task 15.3, are ZRANGE WITHSCORES (to surface score
// FORMATTING in array position) over distinct scores, and ZCARD / ZRANK
// deterministic integer replies.
//
// Validates: 需求 9.6 (score precision differential); exercises 需求 9.1/9.2/9.7
// reply shapes. Env-guarded so `go test ./...` needs no live infrastructure.

// Extreme score literals, as the exact argument bytes sent on the wire. Keeping
// them as shared constants makes the same literal appear identically on both
// endpoints.
const (
	maxDouble       = "1.7976931348623157e+308"
	negMaxDouble    = "-1.7976931348623157e+308"
	minNormalDouble = "2.2250738585072014e-308"
	minSubnormal    = "5e-324"
	negMinSubnormal = "-5e-324"
	pow53           = "9007199254740992" // 2^53, exactly representable
	pow53Plus1      = "9007199254740993" // 2^53+1, NOT exactly representable
	highPrecOne     = "1.000000000000000000000001"
	highPrecPi      = "3.141592653589793238462643383279"
)

// ZSetDiffSequences returns the Sorted Set score-precision differential
// sequences.
func ZSetDiffSequences() []Sequence {
	return []Sequence{
		zsetExtremeScoreSequence(),
		zsetHighPrecisionSequence(),
		zsetScientificNotationSequence(),
		zsetInfinitySequence(),
		zsetSignedZeroSequence(),
		zsetIncrByExtremeSequence(),
		zsetWithScoresOrderedSequence(),
		zsetRankCardDeterministicSequence(),
	}
}

// ZSetDiffSequenceNames returns the sequence names, for logging / subtest names.
func ZSetDiffSequenceNames() []string {
	seqs := ZSetDiffSequences()
	names := make([]string, len(seqs))
	for i, s := range seqs {
		names[i] = s.Name
	}
	return names
}

// zsetExtremeScoreSequence adds one member per extreme finite double (each a
// DISTINCT score) and reads each back with ZSCORE, isolating each extreme
// value's double formatting. Requirement 9.6.
func zsetExtremeScoreSequence() Sequence {
	k := "difftest:zset:extreme"
	return Sequence{
		Name: "zset-extreme-scores",
		Commands: []Command{
			Cmd("DEL", k),

			// Each member gets a distinct extreme score.
			Cmd("ZADD", k, maxDouble, "max"),
			Cmd("ZADD", k, negMaxDouble, "negmax"),
			Cmd("ZADD", k, minNormalDouble, "minnorm"),
			Cmd("ZADD", k, minSubnormal, "minsub"),
			Cmd("ZADD", k, negMinSubnormal, "negminsub"),
			Cmd("ZADD", k, pow53, "p53"),
			Cmd("ZADD", k, pow53Plus1, "p53p1"),

			// Single-member ZSCORE reads: one score's rendering each.
			Cmd("ZSCORE", k, "max"),
			Cmd("ZSCORE", k, "negmax"),
			Cmd("ZSCORE", k, "minnorm"),
			Cmd("ZSCORE", k, "minsub"),
			Cmd("ZSCORE", k, "negminsub"),
			Cmd("ZSCORE", k, "p53"),
			Cmd("ZSCORE", k, "p53p1"),

			// Count is deterministic regardless of formatting.
			Cmd("ZCARD", k),

			Cmd("DEL", k),
		},
	}
}

// zsetHighPrecisionSequence stores decimals with more significant digits than a
// double can hold and reads them back with ZSCORE. The reply must be the
// double-rounded form (what Pika returns), NOT the wider decimal a 38-digit
// backend could preserve — this is the core double-vs-38-digit divergence point.
// Requirement 9.6.
func zsetHighPrecisionSequence() Sequence {
	k := "difftest:zset:highprec"
	return Sequence{
		Name: "zset-high-precision",
		Commands: []Command{
			Cmd("DEL", k),

			// A value one ulp beyond what a double can distinguish from 1.0.
			Cmd("ZADD", k, highPrecOne, "one"),
			Cmd("ZSCORE", k, "one"), // must render as the double-rounded value

			// Pi to 30 digits; double keeps ~17 significant digits.
			Cmd("ZADD", k, highPrecPi, "pi"),
			Cmd("ZSCORE", k, "pi"),

			// 2^53+1 collapses onto 2^53 in double; ZSCORE must reflect that.
			Cmd("ZADD", k, pow53Plus1, "big"),
			Cmd("ZSCORE", k, "big"),

			Cmd("ZCARD", k),
			Cmd("DEL", k),
		},
	}
}

// zsetScientificNotationSequence feeds scores in scientific notation and reads
// them back, pinning that both endpoints accept the exponent form and render the
// same double. Requirement 9.6.
func zsetScientificNotationSequence() Sequence {
	k := "difftest:zset:scinot"
	return Sequence{
		Name: "zset-scientific-notation",
		Commands: []Command{
			Cmd("DEL", k),

			Cmd("ZADD", k, "1e100", "big"),
			Cmd("ZSCORE", k, "big"),

			Cmd("ZADD", k, "1.5e-10", "small"),
			Cmd("ZSCORE", k, "small"),

			Cmd("ZADD", k, "3.0e3", "int"), // 3000, integral -> renders "3000"
			Cmd("ZSCORE", k, "int"),

			Cmd("ZADD", k, "-2.5e2", "neg"), // -250
			Cmd("ZSCORE", k, "neg"),

			Cmd("DEL", k),
		},
	}
}

// zsetInfinitySequence covers the +inf / -inf scores: ZADD acceptance, ZSCORE
// rendering ("inf" / "-inf"), and ZRANGEBYSCORE with infinite bounds. Distinct
// finite members sit between the infinities so the WITHSCORES order is
// unambiguous. Requirement 9.6.
func zsetInfinitySequence() Sequence {
	k := "difftest:zset:inf"
	return Sequence{
		Name: "zset-infinity",
		Commands: []Command{
			Cmd("DEL", k),

			Cmd("ZADD", k, "-inf", "lo"),
			Cmd("ZADD", k, "0", "mid"),
			Cmd("ZADD", k, "+inf", "hi"),

			// Single-member score rendering.
			Cmd("ZSCORE", k, "lo"), // "-inf"
			Cmd("ZSCORE", k, "hi"), // "inf"

			// Full-range read by infinite bounds, WITHSCORES (distinct scores ->
			// one unambiguous order).
			Cmd("ZRANGEBYSCORE", k, "-inf", "+inf", "WITHSCORES"),

			// Only the finite-and-above members.
			Cmd("ZRANGEBYSCORE", k, "0", "+inf", "WITHSCORES"),

			Cmd("ZCARD", k),
			Cmd("DEL", k),
		},
	}
}

// zsetSignedZeroSequence pins that positive and negative zero both render "0"
// (Redis normalises -0.0). The two members carry the same numeric value, so the
// sequence reads them with per-member ZSCORE, never relying on their relative
// order. Requirement 9.6.
func zsetSignedZeroSequence() Sequence {
	k := "difftest:zset:zero"
	return Sequence{
		Name: "zset-signed-zero",
		Commands: []Command{
			Cmd("DEL", k),

			Cmd("ZADD", k, "0", "pos"),
			Cmd("ZADD", k, "-0", "neg"),

			Cmd("ZSCORE", k, "pos"), // "0"
			Cmd("ZSCORE", k, "neg"), // "0" (negative zero normalised)

			Cmd("ZCARD", k), // :2 (distinct members, equal score)
			Cmd("DEL", k),
		},
	}
}

// zsetIncrByExtremeSequence covers ZINCRBY at extreme magnitudes: a large
// increment that overflows a finite score to +inf (both endpoints -> "inf"), a
// tiny increment lost to double precision, and building a score up to a finite
// double. No inf + (-inf) is performed, so no NaN can arise. Requirement 9.6.
func zsetIncrByExtremeSequence() Sequence {
	k := "difftest:zset:incr"
	return Sequence{
		Name: "zset-incrby-extreme",
		Commands: []Command{
			Cmd("DEL", k),

			// Overflow to +inf: max double + max double -> inf.
			Cmd("ZADD", k, maxDouble, "of"),
			Cmd("ZINCRBY", k, maxDouble, "of"), // "inf"
			Cmd("ZSCORE", k, "of"),             // "inf"

			// Underflow to -inf: -max + -max -> -inf.
			Cmd("ZADD", k, negMaxDouble, "uf"),
			Cmd("ZINCRBY", k, negMaxDouble, "uf"), // "-inf"
			Cmd("ZSCORE", k, "uf"),                // "-inf"

			// Tiny increment lost to precision: 1 + 5e-324 stays 1 in double.
			Cmd("ZADD", k, "1", "tiny"),
			Cmd("ZINCRBY", k, minSubnormal, "tiny"), // "1"
			Cmd("ZSCORE", k, "tiny"),                // "1"

			// ZINCRBY on a fresh member starts from 0.
			Cmd("ZINCRBY", k, highPrecPi, "fresh"), // double-rounded pi
			Cmd("ZSCORE", k, "fresh"),

			Cmd("DEL", k),
		},
	}
}

// zsetWithScoresOrderedSequence surfaces score FORMATTING in array position via
// ZRANGE / ZREVRANGE WITHSCORES. Every score is DISTINCT and well separated, so
// the ascending/descending order is unambiguous and independent of the
// equal-score tie-break. Requirement 9.6 (and 9.1 reply shape).
func zsetWithScoresOrderedSequence() Sequence {
	k := "difftest:zset:withscores"
	return Sequence{
		Name: "zset-withscores-ordered",
		Commands: []Command{
			Cmd("DEL", k),

			// Distinct, well-separated scores including negatives and a fraction.
			Cmd("ZADD", k, "-100.5", "a"),
			Cmd("ZADD", k, "-1", "b"),
			Cmd("ZADD", k, "0", "c"),
			Cmd("ZADD", k, "2.5", "d"),
			Cmd("ZADD", k, "1000000", "e"),

			// Ascending and descending, with scores interleaved.
			Cmd("ZRANGE", k, "0", "-1", "WITHSCORES"),
			Cmd("ZREVRANGE", k, "0", "-1", "WITHSCORES"),

			// A score sub-range, WITHSCORES.
			Cmd("ZRANGEBYSCORE", k, "-1", "2.5", "WITHSCORES"),

			Cmd("DEL", k),
		},
	}
}

// zsetRankCardDeterministicSequence pins the deterministic integer replies:
// ZCARD (member count), ZRANK / ZREVRANK (0-based ranks), and ZCOUNT (score-range
// cardinality). Distinct scores make every rank unambiguous. Requirements 9.2,
// 9.7 (and 9.1 reply shape).
func zsetRankCardDeterministicSequence() Sequence {
	k := "difftest:zset:rank"
	miss := k + ":missing"
	return Sequence{
		Name: "zset-rank-card-deterministic",
		Commands: []Command{
			Cmd("DEL", k, miss),

			// Absent key: ZCARD 0, ZRANK null bulk, ZSCORE null bulk.
			Cmd("ZCARD", miss),
			Cmd("ZRANK", miss, "x"),
			Cmd("ZSCORE", miss, "x"),

			Cmd("ZADD", k, "1", "a", "2", "b", "3", "c", "4", "d"),
			Cmd("ZCARD", k),         // :4
			Cmd("ZRANK", k, "a"),    // :0
			Cmd("ZRANK", k, "d"),    // :3
			Cmd("ZREVRANK", k, "a"), // :3
			Cmd("ZREVRANK", k, "d"), // :0
			Cmd("ZRANK", k, "absent"),
			Cmd("ZCOUNT", k, "2", "3"),       // :2
			Cmd("ZCOUNT", k, "(1", "4"),      // :3
			Cmd("ZCOUNT", k, "-inf", "+inf"), // :4

			Cmd("DEL", k),
		},
	}
}

// describeZSetDiff is a small diagnostics helper mirroring describeKeysDiff.
func describeZSetDiff() string {
	return fmt.Sprintf("zset difftest: %d sequences %v",
		len(ZSetDiffSequences()), ZSetDiffSequenceNames())
}
