package integration

import "testing"

// Empty-string dimension: an empty string "" is a valid key / value / set member / hash
// field / zset member, is parsed as 0.0 by the float commands (strtod), and is accepted
// as cursor 0 by the SCAN family (strtoull("")==0) — while the integer-argument path
// still rejects it. These tests sweep "" through every role and byte-diff against the
// live redis:3.2 oracle. (This is the systematic dimension whose SCAN-cursor and float
// cells earlier rounds' sampling had missed; see doc §2 v1.52.0.)

// TestEmptyStringAsKey: "" is a normal key across the command families.
func TestEmptyStringAsKey(t *testing.T) {
	d := newDiffer(t)
	// "" cannot be nonce-namespaced, so clear it on both sides before each sub-case.
	d.eq("SET \"\" then GET", bs("SET"), bs(""), bs("EV"))
	d.eq("GET \"\"", bs("GET"), bs(""))
	d.eq("EXISTS \"\"", bs("EXISTS"), bs(""))
	d.eq("TYPE \"\"", bs("TYPE"), bs(""))
	d.eq("EXPIRE \"\"", bs("EXPIRE"), bs(""), bs("100"))
	d.eq("DEL \"\"", bs("DEL"), bs(""))
	t.Logf("compared %d empty-key replies vs Redis 3.2", d.n)
}

// TestEmptyStringAsValue: "" as a stored value round-trips as an empty bulk ($0).
func TestEmptyStringAsValue(t *testing.T) {
	d := newDiffer(t)
	k := d.k("v")
	d.eq("SET empty value", bs("SET"), k, bs(""))
	d.eq("GET empty value -> $0", bs("GET"), k)
	d.eq("STRLEN empty -> 0", bs("STRLEN"), k)
	d.eq("APPEND \"\" to new -> 0", bs("APPEND"), d.k("v2"), bs(""))
	k3 := d.k("v3")
	d.eq("SET abc", bs("SET"), k3, bs("abc"))
	d.eq("APPEND \"\" to abc -> 3", bs("APPEND"), k3, bs(""))
	k4 := d.k("v4")
	d.eq("SET old", bs("SET"), k4, bs("old"))
	d.eq("GETSET \"\"", bs("GETSET"), k4, bs(""))
	k5 := d.k("v5")
	d.eq("RPUSH empty elem", bs("RPUSH"), k5, bs(""))
	d.eq("LINDEX empty elem -> $0", bs("LINDEX"), k5, bs("0"))
	k6 := d.k("v6")
	d.eq("SET hello", bs("SET"), k6, bs("hello"))
	d.eq("SETRANGE off0 \"\" (no-op)", bs("SETRANGE"), k6, bs("0"), bs(""))
	t.Logf("compared %d empty-value replies vs Redis 3.2", d.n)
}

// TestEmptyStringAsMemberField: "" is a real set member / hash field / zset member (the
// phantom-empty-member encoding fix; see doc §2 v1.41.0).
func TestEmptyStringAsMemberField(t *testing.T) {
	d := newDiffer(t)
	// set member
	s := d.k("s")
	d.eq("SADD \"\" -> 1", bs("SADD"), s, bs(""))
	d.eq("SISMEMBER \"\" -> 1", bs("SISMEMBER"), s, bs(""))
	d.eq("SCARD -> 1", bs("SCARD"), s)
	d.eqSorted("SMEMBERS -> [\"\"]", bs("SMEMBERS"), s)
	d.eq("SREM \"\" -> 1", bs("SREM"), s, bs(""))
	// zset member
	z := d.k("z")
	d.eq("ZADD 1 \"\" -> 1", bs("ZADD"), z, bs("1"), bs(""))
	d.eq("ZSCORE \"\" -> 1", bs("ZSCORE"), z, bs(""))
	d.eq("ZRANK \"\" -> 0", bs("ZRANK"), z, bs(""))
	d.eq("ZREM \"\" -> 1", bs("ZREM"), z, bs(""))
	// hash field
	h := d.k("h")
	d.eq("HSET \"\" v -> 1", bs("HSET"), h, bs(""), bs("v"))
	d.eq("HGET \"\" -> v", bs("HGET"), h, bs(""))
	d.eq("HEXISTS \"\" -> 1", bs("HEXISTS"), h, bs(""))
	d.eqSorted("HKEYS -> [\"\"]", bs("HKEYS"), h)
	d.eq("HDEL \"\" -> 1", bs("HDEL"), h, bs(""))
	// empty-string score parses as 0
	ze := d.k("ze")
	d.eq("ZADD \"\" m (score 0) -> 1", bs("ZADD"), ze, bs(""), bs("m"))
	d.eq("ZSCORE m -> 0", bs("ZSCORE"), ze, bs("m"))
	t.Logf("compared %d empty-member/field replies vs Redis 3.2", d.n)
}

// TestEmptyStringAsNumericArg: the float path reads "" as 0 (strtod), the integer path
// rejects it (string2ll), and a single space is rejected on both.
func TestEmptyStringAsNumericArg(t *testing.T) {
	d := newDiffer(t)
	d.eq("INCRBYFLOAT \"\" -> 0", bs("INCRBYFLOAT"), d.k("f1"), bs(""))
	kf := d.k("f2")
	d.eq("SET 5", bs("SET"), kf, bs("5"))
	d.eq("INCRBYFLOAT \"\" on 5 -> 5", bs("INCRBYFLOAT"), kf, bs(""))
	kf2 := d.k("f3")
	d.eq("SET empty", bs("SET"), kf2, bs(""))
	d.eq("INCRBYFLOAT 1 on stored-empty -> 1", bs("INCRBYFLOAT"), kf2, bs("1"))
	hf := d.k("hf")
	d.eq("HSET empty field", bs("HSET"), hf, bs("g"), bs(""))
	d.eq("HINCRBYFLOAT 1 on empty field -> 1", bs("HINCRBYFLOAT"), hf, bs("g"), bs("1"))
	d.eq("GEOADD empty lon -> 1 (0,0)", bs("GEOADD"), d.k("geo"), bs(""), bs("0"), bs("m"))
	// integer path + single space still rejected on both
	d.eq("INCR on stored-empty -> not-integer err", bs("INCR"), kf2)
	d.eq("INCRBY \"\" -> not-integer err", bs("INCRBY"), d.k("i1"), bs(""))
	d.eq("INCRBYFLOAT single space -> not-a-float err", bs("INCRBYFLOAT"), d.k("i2"), bs(" "))
	d.eq("EXPIRE \"\" -> not-integer err", bs("EXPIRE"), kf, bs(""))
	t.Logf("compared %d empty-numeric-arg replies vs Redis 3.2", d.n)
}

// TestEmptyStringAsCursor: the SCAN family treats an empty cursor "" as cursor 0
// (strtoull("")==0), returning a valid scan reply rather than an "invalid cursor" error.
// (The reply itself is opaque-cursor-dependent, so we assert only that it does not error
// — via a small hash that scans to completion in one page.)
func TestEmptyStringAsCursor(t *testing.T) {
	d := newDiffer(t)
	h := d.k("hs")
	d.eq("HSET seed", bs("HSET"), h, bs("f"), bs("1"))
	// HSCAN of a one-field hash completes in a single page on both, so the reply is
	// deterministic (cursor "0" + the one field) and safe to byte-compare.
	d.eq("HSCAN \"\" cursor -> full page", bs("HSCAN"), h, bs(""))
	d.eq("HSCAN 0 cursor (baseline)", bs("HSCAN"), h, bs("0"))
	t.Logf("compared %d empty-cursor replies vs Redis 3.2", d.n)
}
