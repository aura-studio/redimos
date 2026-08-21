package command

import "testing"

// TestGlobMatch pins the Redis-style glob semantics SCAN's MATCH relies on:
// literals, *, ?, character classes ([...] / [^...]), ranges, and \ escapes,
// all case-sensitive. Each case is asserted in both directions (match / no match).
func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern string
		input   string
		want    bool
	}{
		// literals
		{"", "", true},
		{"", "a", false},
		{"abc", "abc", true},
		{"abc", "abd", false},
		{"abc", "ab", false},

		// '*'
		{"*", "", true},
		{"*", "anything", true},
		{"a*", "a", true},
		{"a*", "abc", true},
		{"a*", "bac", false},
		{"*c", "abc", true},
		{"a*c", "ac", true},
		{"a*c", "abbbc", true},
		{"a*c", "abbb", false},
		{"**", "xy", true}, // collapsed stars
		{"a**b", "aXYb", true},

		// '?'
		{"?", "a", true},
		{"?", "", false},
		{"?", "ab", false},
		{"a?c", "abc", true},
		{"a?c", "ac", false},
		{"user:?", "user:1", true},
		{"user:?", "user:10", false},

		// character classes
		{"[abc]", "a", true},
		{"[abc]", "c", true},
		{"[abc]", "d", false},
		{"[^abc]", "d", true},
		{"[^abc]", "a", false},
		{"[a-c]", "b", true},
		{"[a-c]", "d", false},
		{"[c-a]", "b", true}, // reversed range is normalized
		{"h[ae]llo", "hello", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"key[0-9]", "key7", true},
		{"key[0-9]", "keyx", false},

		// case sensitivity
		{"ABC", "abc", false},
		{"[A-Z]", "a", false},
		{"[A-Z]", "A", true},

		// escapes
		{`a\*c`, "a*c", true},
		{`a\*c`, "abc", false},
		{`\?`, "?", true},
		{`\?`, "a", false},
		{`\[abc\]`, "[abc]", true},
	}

	for _, tc := range cases {
		if got := globMatch([]byte(tc.pattern), []byte(tc.input)); got != tc.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", tc.pattern, tc.input, got, tc.want)
		}
	}
}

// TestDecodePK verifies decodePK reverses encodePK. It covers BOTH modes:
// multi-db (the uniform "{n}:" prefix scheme with db filtering and collision
// safety) and single-db (raw keys, no prefix, every pk belongs to the shared
// keyspace).
func TestDecodePK(t *testing.T) {
	// --- Multi-DB mode: pks carry the "{db}:" prefix and are filtered by db. ---
	rm := &Router{Config: Config{MultiDB: true}}

	// db 0 uses the "0:" prefix.
	if k, ok := rm.decodePK(0, "0:foo"); !ok || k != "foo" {
		t.Errorf("decodePK(0, \"0:foo\") = (%q, %v), want (\"foo\", true)", k, ok)
	}
	// A key containing ':' round-trips verbatim.
	if k, ok := rm.decodePK(0, "0:a:b:c"); !ok || k != "a:b:c" {
		t.Errorf("decodePK(0, \"0:a:b:c\") = (%q, %v), want (\"a:b:c\", true)", k, ok)
	}
	// db 3 uses the "3:" prefix.
	if k, ok := rm.decodePK(3, "3:bar"); !ok || k != "bar" {
		t.Errorf("decodePK(3, \"3:bar\") = (%q, %v), want (\"bar\", true)", k, ok)
	}
	// A pk from a different database is filtered out.
	if _, ok := rm.decodePK(0, "3:bar"); ok {
		t.Error("decodePK(0, \"3:bar\") should report ok=false (different db)")
	}
	if _, ok := rm.decodePK(3, "0:foo"); ok {
		t.Error("decodePK(3, \"0:foo\") should report ok=false (different db)")
	}
	// Collision-safety: "1:" is not a prefix of "12:" (the ':' terminates the db
	// number), so db 1 does not swallow db 12's keys and vice versa.
	if _, ok := rm.decodePK(1, rm.encodePK(12, []byte("x"))); ok {
		t.Error("decodePK(1, encodePK(12, \"x\")) should report ok=false")
	}
	if _, ok := rm.decodePK(12, rm.encodePK(1, []byte("x"))); ok {
		t.Error("decodePK(12, encodePK(1, \"x\")) should report ok=false")
	}
	// A db-0 key that itself looks like a db-1 prefix ("1:foo") stays distinct from
	// db 1's real "foo": encodePK(0,"1:foo") = "0:1:foo" != encodePK(1,"foo") = "1:foo".
	if k, ok := rm.decodePK(0, rm.encodePK(0, []byte("1:foo"))); !ok || k != "1:foo" {
		t.Errorf("db-0 key \"1:foo\" round-trip = (%q, %v), want (\"1:foo\", true)", k, ok)
	}
	if _, ok := rm.decodePK(1, rm.encodePK(0, []byte("1:foo"))); ok {
		t.Error("decodePK(1, encodePK(0, \"1:foo\")) should report ok=false")
	}

	// --- Single-DB mode: pks are RAW keys, no prefix; every pk belongs to the ---
	// shared keyspace, so decodePK returns it verbatim with ok=true regardless of db.
	rs := &Router{Config: Config{MultiDB: false}}
	if got := rs.encodePK(3, []byte("foo")); got != "foo" {
		t.Errorf("single-db encodePK(3, foo) = %q, want %q (raw, no prefix)", got, "foo")
	}
	if k, ok := rs.decodePK(7, "foo"); !ok || k != "foo" {
		t.Errorf("single-db decodePK(7, \"foo\") = (%q, %v), want (\"foo\", true)", k, ok)
	}
	// A raw key that literally contains a ':' is still returned verbatim (no db strip).
	if k, ok := rs.decodePK(0, "3:bar"); !ok || k != "3:bar" {
		t.Errorf("single-db decodePK(0, \"3:bar\") = (%q, %v), want (\"3:bar\", true)", k, ok)
	}
}

// TestNormalizeMatchPattern pins the auto-wrap convention (bugfix
// v1-scan-substring-match): patterns WITHOUT glob metacharacters become
// *pattern* (substring), everything else is returned byte-identically —
// including the empty pattern, one-sided stars, and every metacharacter class.
func TestNormalizeMatchPattern(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		// plain text → substring wrap
		{"plain digits", "52023464", "*52023464*"},
		{"plain word", "foo", "*foo*"},
		{"dotted name", "Game.SettleQueueReadSN", "*Game.SettleQueueReadSN*"},
		{"utf8 text", "订单", "*订单*"},
		{"binary byte", "a\x00b", "*a\x00b*"},

		// metacharacters → unchanged
		{"empty stays empty", "", ""},
		{"leading star", "*52023464", "*52023464"},
		{"trailing star", "52023464*", "52023464*"},
		{"both stars", "*52023464*", "*52023464*"},
		{"collapsed stars", "**", "**"},
		{"question mark", "user:?", "user:?"},
		{"char class", "k[AB]", "k[AB]"},
		{"negated class", "k[^A]*", "k[^A]*"},
		{"unterminated class", "k[AB", "k[AB"},
		{"escape", `a\*b`, `a\*b`},
		{"lone backslash", `a\`, `a\`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := normalizeMatchPattern([]byte(c.in))
			if string(got) != c.want {
				t.Errorf("normalizeMatchPattern(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestNormalizeMatchPatternWrapsForGlob proves the wrapped pattern actually
// drives the substring behavior through globMatch: the auto-wrapped form of a
// GUI-typed string hits keys that merely CONTAIN it (prefix/suffix/infix),
// while strict exact semantics would have missed all three.
func TestNormalizeMatchPatternWrapsForGlob(t *testing.T) {
	p := normalizeMatchPattern([]byte("52023464"))
	for _, key := range []string{
		"Game.SettleQueueReadSN[52023464]", // infix, bracketed
		"52023464",                         // the whole key
		"52023464:extra",                   // prefix
		"prefix:52023464",                  // suffix
	} {
		if !globMatch(p, []byte(key)) {
			t.Errorf("wrapped pattern %q should match %q", p, key)
		}
	}
	if globMatch(p, []byte("5202346")) {
		t.Errorf("wrapped pattern %q must not match shorter string %q", p, "5202346")
	}
}

// TestGlobMatchSettleQueuePin pins the exact scenario from the bug report
// (bugfix v1-scan-substring-match): the GUI-issued substring pattern must hit
// the real production-style key name, and the auto-wrapped plain-text form
// must behave identically (task 6.2).
func TestGlobMatchSettleQueuePin(t *testing.T) {
	const key = "Game.SettleQueueReadSN[52023464]"
	if !globMatch([]byte("*52023464*"), []byte(key)) {
		t.Errorf("globMatch(*52023464*, %q) = false, want true", key)
	}
	// The normalized plain-text pattern matches the same key byte-for-byte.
	if !globMatch(normalizeMatchPattern([]byte("52023464")), []byte(key)) {
		t.Errorf("globMatch(normalize(52023464), %q) = false, want true", key)
	}
	// A sibling key with a different queue id must NOT match.
	if globMatch([]byte("*52023464*"), []byte("Game.SettleQueueReadSN[52023465]")) {
		t.Error("globMatch(*52023464*, ...[52023465]) = true, want false")
	}
}
