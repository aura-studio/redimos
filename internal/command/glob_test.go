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

// TestDecodePK verifies decodePK reverses encodePK and filters by database.
func TestDecodePK(t *testing.T) {
	// db 0 uses the "0:" prefix.
	if k, ok := decodePK(0, "0:foo"); !ok || k != "foo" {
		t.Errorf("decodePK(0, \"0:foo\") = (%q, %v), want (\"foo\", true)", k, ok)
	}
	// A key containing ':' round-trips verbatim.
	if k, ok := decodePK(0, "0:a:b:c"); !ok || k != "a:b:c" {
		t.Errorf("decodePK(0, \"0:a:b:c\") = (%q, %v), want (\"a:b:c\", true)", k, ok)
	}
	// db 3 uses the "d3:" prefix.
	if k, ok := decodePK(3, "d3:bar"); !ok || k != "bar" {
		t.Errorf("decodePK(3, \"d3:bar\") = (%q, %v), want (\"bar\", true)", k, ok)
	}
	// A pk from a different database is filtered out.
	if _, ok := decodePK(0, "d3:bar"); ok {
		t.Error("decodePK(0, \"d3:bar\") should report ok=false (different db)")
	}
	if _, ok := decodePK(3, "0:foo"); ok {
		t.Error("decodePK(3, \"0:foo\") should report ok=false (different db)")
	}
}
