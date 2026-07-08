package command

// This file implements the case-sensitive Redis-style glob matcher used by
// SCAN's MATCH filter (and, via the shared cursor mechanism, HSCAN/SSCAN/ZSCAN).
//
// Redis applies MATCH on the proxy/server side after the keys are read (design
// "SCAN 游标设计": `MATCH：代理侧 glob 后过滤`), so the pattern language must match
// Redis' own glob, not a shell glob or a regexp. It is a direct port of Redis'
// stringmatchlen (util.c) operating on raw bytes so key names remain binary-safe.
//
// Supported syntax (case-sensitive):
//
//	*        matches any sequence of bytes, including the empty sequence
//	?        matches exactly one byte
//	[...]    character class: any one of the listed bytes
//	[^...]   negated class: any one byte NOT listed
//	[a-z]    range inside a class (start/end swapped if reversed)
//	\x       escapes the next byte (so a literal *, ?, [, or \ can be matched),
//	         both at top level and inside a class
//
// tidwall/match is deliberately NOT used: it does not implement the [...]
// character-class syntax Redis clients rely on.

// globMatch reports whether the Redis-style glob pattern matches s. Both are raw
// byte slices; comparison is byte-exact (case-sensitive) so binary-safe key names
// round-trip. A nil/empty pattern matches only the empty string.
func globMatch(pattern, s []byte) bool {
	return stringMatchLen(pattern, s)
}

// stringMatchLen is the byte-slice port of Redis' stringmatchlen with nocase=0.
// It walks pattern and str in lockstep, recursing at '*' to try every split
// point. The index bookkeeping mirrors the C original's pointer/length pair.
func stringMatchLen(pattern, str []byte) bool {
	p, s := 0, 0
	for p < len(pattern) && s < len(str) {
		switch pattern[p] {
		case '*':
			// Collapse runs of '*' into one.
			for p+1 < len(pattern) && pattern[p+1] == '*' {
				p++
			}
			// A trailing '*' matches the rest of the string unconditionally.
			if p+1 == len(pattern) {
				return true
			}
			// Try to match the remainder of the pattern against every suffix of
			// str (including the empty suffix, so '*' may match zero bytes).
			for i := s; i <= len(str); i++ {
				if stringMatchLen(pattern[p+1:], str[i:]) {
					return true
				}
			}
			return false
		case '?':
			s++
		case '[':
			p++
			negate := false
			if p < len(pattern) && pattern[p] == '^' {
				negate = true
				p++
			}
			match := false
			for {
				if p >= len(pattern) {
					// Unterminated class: back up one so the trailing bookkeeping
					// below does not over-advance (mirrors the C original).
					p--
					break
				}
				if pattern[p] == '\\' {
					if len(pattern)-p < 2 {
						// Trailing bare backslash with no byte to escape: Redis'
						// stringmatchlen does not register it as a class member, so end the
						// class scan without matching (a fall-through to the literal branch
						// would wrongly match a literal backslash for a pattern like "[a\").
						break
					}
					p++
					if pattern[p] == str[s] {
						match = true
					}
				} else if pattern[p] == ']' {
					break
				} else if len(pattern)-p >= 3 && pattern[p+1] == '-' {
					lo, hi := pattern[p], pattern[p+2]
					if lo > hi {
						lo, hi = hi, lo
					}
					c := str[s]
					p += 2
					if c >= lo && c <= hi {
						match = true
					}
				} else {
					if pattern[p] == str[s] {
						match = true
					}
				}
				p++
			}
			if negate {
				match = !match
			}
			if !match {
				return false
			}
			s++
		case '\\':
			// Escape: consume the backslash and match the next byte literally.
			if p+1 < len(pattern) {
				p++
			}
			if pattern[p] != str[s] {
				return false
			}
			s++
		default:
			if pattern[p] != str[s] {
				return false
			}
			s++
		}
		p++
		if s == len(str) {
			// String exhausted: any remaining pattern must be all '*' to match.
			for p < len(pattern) && pattern[p] == '*' {
				p++
			}
			break
		}
	}
	// Trailing '*' (including when the string was empty from the start) matches the
	// empty remainder of the string.
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern) && s == len(str)
}
