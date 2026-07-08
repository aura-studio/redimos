package command

import "testing"

// TestParseScanCursor covers the round-9 fix: Redis' parseScanCursorOrReply uses
// strtoul, which tolerates a single leading '+' ("SCAN +0" == "SCAN 0") that Go's
// ParseUint rejects, while still rejecting leading whitespace, a bare sign, and
// negative values (redimos cursors are opaque, so wrap-around cannot resolve anyway).
func TestParseScanCursor(t *testing.T) {
	cases := []struct {
		in     string
		want   uint64
		wantOK bool
	}{
		{"0", 0, true},
		{"+0", 0, true},
		{"42", 42, true},
		{"+42", 42, true},
		{"18446744073709551615", 18446744073709551615, true}, // max uint64
		{"-0", 0, true},                                       // -0 wraps to 0 (valid start/end cursor)
		{"-00", 0, true},                                      // any zero spelling
		{"+", 0, false},                                       // bare sign
		{"-", 0, false},                                       // bare sign
		{"++0", 0, false},                                     // only one leading sign
		{" 0", 0, false},                                      // leading whitespace
		{"-1", 0, false},                                      // negative non-zero not reproduced
		{"-42", 0, false},                                     // negative non-zero not reproduced
		{"", 0, true},                                         // strtoull("") -> 0 : SCAN "" == SCAN 0
		{" ", 0, false},                                       // a single space still fails
		{"0x10", 0, false},                                    // no hex
		{"3.5", 0, false},                                     // no float
	}
	for _, c := range cases {
		got, ok := parseScanCursor([]byte(c.in))
		if ok != c.wantOK || (ok && got != c.want) {
			t.Errorf("parseScanCursor(%q) = (%d, %v), want (%d, %v)", c.in, got, ok, c.want, c.wantOK)
		}
	}
}
