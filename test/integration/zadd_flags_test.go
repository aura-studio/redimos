package integration

import "testing"

// ZADD NX/XX/CH/INCR flag parity (Redis 3.2). This locks in the flag support added after the
// dimension-L audit found ZADD rejecting `CH`/`NX`/`XX`/`INCR` with a syntax error. Every case
// is compared byte-for-byte with Redis 3.2, including the nil-bulk INCR gating and the flag
// combination errors.

func TestDiffZAddFlags_CH(t *testing.T) {
	d := newDiffer(t)
	k := d.k("z")
	d.eq("CH both new -> 2", bs("ZADD"), k, bs("CH"), bs("1"), bs("a"), bs("2"), bs("b"))
	d.eq("CH one changed -> 1", bs("ZADD"), k, bs("CH"), bs("1"), bs("a"), bs("5"), bs("b"))
	d.eq("CH none changed -> 0", bs("ZADD"), k, bs("CH"), bs("1"), bs("a"))
	d.eq("CH add+change -> 2", bs("ZADD"), k, bs("CH"), bs("9"), bs("a"), bs("1"), bs("c"))
	d.eq("ZRANGE WITHSCORES", bs("ZRANGE"), k, bs("0"), bs("-1"), bs("WITHSCORES"))
}

func TestDiffZAddFlags_NX(t *testing.T) {
	d := newDiffer(t)
	k := d.k("z")
	d.eq("seed", bs("ZADD"), k, bs("1"), bs("a"), bs("2"), bs("b"))
	d.eq("NX existing -> 0", bs("ZADD"), k, bs("NX"), bs("100"), bs("a"))
	d.eq("NX new -> 1", bs("ZADD"), k, bs("NX"), bs("3"), bs("c"))
	d.eq("NX score of a unchanged", bs("ZSCORE"), k, bs("a"))
	d.eq("NX CH new -> 1", bs("ZADD"), k, bs("NX"), bs("CH"), bs("4"), bs("dd"))
	d.eq("NX CH existing -> 0", bs("ZADD"), k, bs("NX"), bs("CH"), bs("9"), bs("a"))
}

func TestDiffZAddFlags_XX(t *testing.T) {
	d := newDiffer(t)
	k := d.k("z")
	d.eq("seed", bs("ZADD"), k, bs("1"), bs("a"))
	d.eq("XX existing update -> 0 added", bs("ZADD"), k, bs("XX"), bs("9"), bs("a"))
	d.eq("XX score updated", bs("ZSCORE"), k, bs("a"))
	d.eq("XX CH existing -> 1 changed", bs("ZADD"), k, bs("XX"), bs("CH"), bs("50"), bs("a"))
	d.eq("XX missing -> 0 added", bs("ZADD"), k, bs("XX"), bs("1"), bs("nope"))
	d.eq("XX did not create nope", bs("ZSCORE"), k, bs("nope"))
	// XX on a wholly missing key must not create the key.
	mk := d.k("missing")
	d.eq("XX on missing key -> 0", bs("ZADD"), mk, bs("XX"), bs("1"), bs("a"))
	d.eq("missing key still absent", bs("EXISTS"), mk)
}

func TestDiffZAddFlags_INCR(t *testing.T) {
	d := newDiffer(t)
	k := d.k("z")
	d.eq("INCR new -> 5", bs("ZADD"), k, bs("INCR"), bs("5"), bs("a"))
	d.eq("INCR again -> 8", bs("ZADD"), k, bs("INCR"), bs("3"), bs("a"))
	d.eq("INCR negative -> 6", bs("ZADD"), k, bs("INCR"), bs("-2"), bs("a"))
	// NX INCR on an existing member is blocked -> nil bulk.
	d.eq("NX INCR existing -> nil", bs("ZADD"), k, bs("NX"), bs("INCR"), bs("1"), bs("a"))
	// XX INCR on a missing member is blocked -> nil bulk.
	d.eq("XX INCR missing -> nil", bs("ZADD"), k, bs("XX"), bs("INCR"), bs("1"), bs("nope"))
	// NX INCR on a brand-new member succeeds.
	d.eq("NX INCR new -> 7", bs("ZADD"), k, bs("NX"), bs("INCR"), bs("7"), bs("fresh"))
	d.eq("ZSCORE fresh", bs("ZSCORE"), k, bs("fresh"))
}

func TestDiffZAddFlags_Dedup(t *testing.T) {
	d := newDiffer(t)
	// A member repeated in one call collapses to the last score; it counts as added once.
	k := d.k("z")
	d.eq("CH dup last-wins -> 1", bs("ZADD"), k, bs("CH"), bs("1"), bs("x"), bs("2"), bs("x"))
	d.eq("score is last (2)", bs("ZSCORE"), k, bs("x"))
	d.eq("plain dup added once", bs("ZADD"), d.k("z2"), bs("1"), bs("y"), bs("9"), bs("y"))
	d.eq("score is last (9)", bs("ZSCORE"), d.k("z2"), bs("y"))
}

func TestDiffZAddFlags_Errors(t *testing.T) {
	d := newDiffer(t)
	k := d.k("z")
	d.eq("NX XX incompatible", bs("ZADD"), k, bs("NX"), bs("XX"), bs("1"), bs("a"))
	d.eq("INCR multi-pair", bs("ZADD"), k, bs("INCR"), bs("1"), bs("a"), bs("2"), bs("b"))
	d.eq("odd args after flag", bs("ZADD"), k, bs("CH"), bs("1"))
	d.eq("bad score after flag", bs("ZADD"), k, bs("CH"), bs("notafloat"), bs("a"))
}
