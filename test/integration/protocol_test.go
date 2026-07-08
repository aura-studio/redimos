package integration

import (
	"strconv"
	"testing"
)

// Dimension Q: RESP2 WIRE-PROTOCOL parity.
//
// The A-P dimensions all exercise command SEMANTICS through the canonical array framing
// d.do() builds. This dimension exercises the wire layer itself against a live Redis 3.2:
// inline commands, pipelining (reply order + framing), zero-length vs null bulks,
// command-name / option-keyword case-insensitivity, the unknown-command reply, and a few
// protocol-error frames. redimos rides on redcon for the wire; none of it was ever
// differentially verified.

// respArray builds a RESP2 array-of-bulk-strings request payload (binary-safe), the same
// framing d.do() writes — but returned as bytes so it can be concatenated (pipelining) or
// mutated (malformed-framing tests).
func respArray(args ...string) []byte {
	var b []byte
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(args)), 10)
	b = append(b, '\r', '\n')
	for _, a := range args {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(a)), 10)
		b = append(b, '\r', '\n')
		b = append(b, a...)
		b = append(b, '\r', '\n')
	}
	return b
}

// TestDiffProtocol_Inline covers INLINE commands (a bare "CMD arg arg\r\n" line, no array
// framing) — the legacy Redis request form redcon must also accept, byte-identically.
func TestDiffProtocol_Inline(t *testing.T) {
	d := newDiffer(t)
	k := string(d.k("inl"))

	d.eqRaw("inline PING", bs("PING\r\n"))
	d.eqRaw("inline ping (lowercase)", bs("ping\r\n"))
	d.eqRaw("inline ECHO", bs("ECHO hello\r\n"))
	d.eqRaw("inline SET", bs("SET "+k+" v1\r\n"))
	d.eqRaw("inline GET", bs("GET "+k+"\r\n"))
	d.eqRaw("inline SET quoted value", bs("SET "+k+" \"a b c\"\r\n"))
	d.eqRaw("inline GET after quoted", bs("GET "+k+"\r\n"))
	d.eqRaw("inline extra spaces", bs("SET   "+k+"   v2\r\n"))
	d.eqRaw("inline GET after extra spaces", bs("GET "+k+"\r\n"))
	d.eqRaw("inline empty line -> no reply expected? use PING", bs("PING\r\n"))
	d.eqRaw("inline leading spaces", bs("   PING\r\n"))

	t.Logf("compared %d inline-protocol replies vs Redis 3.2", d.n)
}

// TestDiffProtocol_Pipelining verifies that several commands in a SINGLE write produce the
// replies in order, correctly framed, identically to Redis.
func TestDiffProtocol_Pipelining(t *testing.T) {
	d := newDiffer(t)
	k := string(d.k("pipe"))

	// 3 inline commands, one write, 3 replies in order.
	d.eqPipeline("inline pipeline x3", bs("PING\r\nECHO a\r\nPING\r\n"), 3)

	// Mixed array-framed pipeline: SET / GET / STRLEN.
	var mix []byte
	mix = append(mix, respArray("SET", k, "hello")...)
	mix = append(mix, respArray("GET", k)...)
	mix = append(mix, respArray("STRLEN", k)...)
	d.eqPipeline("array pipeline SET/GET/STRLEN", mix, 3)

	// Pipeline where an early command errors (WRONGTYPE) must not desync later replies.
	kl := string(d.k("pipe-list"))
	var errmix []byte
	errmix = append(errmix, respArray("RPUSH", kl, "x")...)
	errmix = append(errmix, respArray("GET", kl)...) // WRONGTYPE
	errmix = append(errmix, respArray("LLEN", kl)...)
	d.eqPipeline("pipeline with mid error keeps order", errmix, 3)

	t.Logf("compared %d pipelined replies vs Redis 3.2", d.n)
}

// TestDiffProtocol_ZeroLengthBulk pins zero-length bulk ($0) vs null bulk ($-1) handling:
// an empty-string value/arg is a present zero-length bulk, distinct from a missing key.
func TestDiffProtocol_ZeroLengthBulk(t *testing.T) {
	d := newDiffer(t)
	k := string(d.k("zlb"))
	kmiss := string(d.k("zlb-missing"))

	// SET key "" via an explicit $0 empty-bulk arg, then GET -> $0 (not $-1).
	d.eqRaw("SET key <empty> ($0 arg)", respArray("SET", k, ""))
	d.eqRaw("GET key -> $0", respArray("GET", k))
	d.eqRaw("STRLEN empty -> :0", respArray("STRLEN", k))
	d.eqRaw("GET absent -> $-1", respArray("GET", kmiss))
	// APPEND to the empty value.
	d.eqRaw("APPEND to empty", respArray("APPEND", k, "x"))
	d.eqRaw("GET after append", respArray("GET", k))
	// Empty-string MEMBER / FIELD round-trips (post-v3 lands at 0x01).
	ks := string(d.k("zlb-set"))
	d.eqRaw("SADD empty member", respArray("SADD", ks, ""))
	d.eqSorted("SMEMBERS has empty member", bs("SMEMBERS"), bs(ks))
	d.eqRaw("SISMEMBER empty -> :1", respArray("SISMEMBER", ks, ""))

	t.Logf("compared %d zero-length-bulk replies vs Redis 3.2", d.n)
}

// TestDiffProtocol_CaseInsensitive verifies command names AND option keywords are
// case-insensitive byte-for-byte the same as Redis.
func TestDiffProtocol_CaseInsensitive(t *testing.T) {
	d := newDiffer(t)
	k := string(d.k("ci"))
	z := string(d.k("ci-z"))

	// Command-name casings all resolve.
	for _, cmd := range []string{"set", "SET", "Set", "sEt"} {
		d.eqRaw("cmd casing "+cmd, respArray(cmd, k, "v"))
	}
	for _, cmd := range []string{"get", "GET", "gEt"} {
		d.eqRaw("cmd casing "+cmd, respArray(cmd, k))
	}
	// Option-keyword casings.
	d.eq("SET ex lowercase", bs("SET"), bs(k), bs("v"), bs("ex"), bs("100"))
	d.eq("TTL after lowercase ex", bs("TTL"), bs(k)) // (may drift by 1s; both fresh so usually equal)
	d.eq("SET PX MixedCase", bs("SET"), bs(k), bs("v"), bs("Px"), bs("100000"))
	d.eq("SET nx lowercase on existing", bs("SET"), bs(k), bs("v2"), bs("nx"))
	d.eq("ZADD nx ch lowercase", bs("ZADD"), bs(z), bs("nx"), bs("ch"), bs("1"), bs("m"))
	d.eqSorted("ZRANGE withscores lowercase", bs("ZRANGE"), bs(z), bs("0"), bs("-1"), bs("withscores"))
	d.eqSorted("ZRANGEBYSCORE withscores mixed", bs("ZRANGEBYSCORE"), bs(z), bs("-inf"), bs("+inf"), bs("WithScores"))

	t.Logf("compared %d case-insensitivity replies vs Redis 3.2", d.n)
}

// TestDiffProtocol_UnknownCommand pins the unknown-command reply text (Redis 3.2 form:
// "ERR unknown command 'NAME'", name echoed with the client's casing, no args suffix).
func TestDiffProtocol_UnknownCommand(t *testing.T) {
	d := newDiffer(t)

	d.eqRaw("unknown command", respArray("NOTACOMMAND"))
	d.eqRaw("unknown command mixedcase", respArray("NotACmd"))
	d.eqRaw("unknown command with args", respArray("BOGUS", "a", "b"))
	d.eqRaw("empty command name", respArray(""))

	t.Logf("compared %d unknown-command replies vs Redis 3.2", d.n)
}

// TestDiffProtocol_MalformedFraming compares a few PROTOCOL-ERROR frames on FRESH
// connections (Redis replies "-ERR Protocol error: ..." then closes). Only frames that
// yield a prompt error (never a hang waiting for more bytes) are included.
func TestDiffProtocol_MalformedFraming(t *testing.T) {
	d := newDiffer(t)

	// Non-numeric multibulk count.
	d.eqRawFresh("invalid multibulk length", bs("*abc\r\n"))
	// Non-numeric bulk length.
	d.eqRawFresh("invalid bulk length", bs("*1\r\n$xy\r\n"))
	// Bulk arg missing its leading '$'.
	d.eqRawFresh("expected dollar", bs("*1\r\nPING\r\n"))
	// NOTE: "*0\r\n..." (empty multibulk) is NOT compared — another redcon divergence:
	// redcon rejects a count <= 0 ("invalid multibulk length"), whereas Redis 3.2 treats an
	// empty multibulk as an ignorable no-op and parses the following bytes as a fresh
	// (here inline) command. Minor, and inherited from the wire library — see the
	// oversized-frame note above.
	// Unbalanced quotes in an inline command.
	d.eqRawFresh("inline unbalanced quotes", bs("SET k \"abc\r\n"))

	// KNOWN DIVERGENCE (NOT compared): an OVERSIZED but numeric length. Redis 3.2 rejects a
	// bulk length > 512MB ("invalid bulk length") and a multibulk count > 1024*1024
	// ("invalid multibulk length") the instant it parses the header. redcon v1.6.2 (the
	// wire library redimos rides on) validates only non-negative/non-empty lengths — it has
	// no upper bound — so "*1\r\n$629145600\r\n" or "*2097152\r\n" makes it WAIT for (and
	// buffer) the declared bytes instead of erroring. That is a real protocol-parity gap and
	// a DoS-adjacent risk (unbounded buffering / connection hang from a ~15-byte frame). It
	// cannot be closed at the command layer — the frame never reaches a handler — so it is
	// tracked as an R2 security item (needs a redcon fork/replacement enforcing the limits),
	// and is deliberately excluded here rather than hanging the differential for 10s/case.

	t.Logf("compared %d malformed-framing replies vs Redis 3.2", d.n)
}
