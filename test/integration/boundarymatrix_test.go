package integration

import "testing"

// Value×position boundary matrix: a cross-cutting sweep of the special integer boundary
// values {MinInt64, MaxInt64, MaxInt64+1 (int64 overflow), 0, -1} through every position
// that takes an integer argument (count / index / offset / TTL / bit-value / increment /
// DB), plus binary keys/members and inf/nan scores. Each case byte-diffs against the live
// redis:3.2 oracle. This is the orthogonal "value × position" dimension that per-command
// adversarial sampling structurally misses (see doc §2 v1.52.1).
//
// Deliberately EXCLUDED (documented platform divergences / DoS inputs, NOT asserted here):
//   - SRANDMEMBER key <negative-huge>  — OOMs real Redis 3.2 (§4.6; redimos caps it)
//   - SELECT <MinInt64>                — Redis (int)INT64_MIN==0 C-UB replies +OK (§4.6)
//   - ZADD/ZINCRBY inf                 — DynamoDB Number has no inf (§4.1)
//   - SETBIT/SETRANGE offset that grows the value past ~390KB — §4.1 (and crashes oracle)

const (
	bMin   = "-9223372036854775808" // MinInt64
	bMax   = "9223372036854775807"  // MaxInt64
	bMaxP1 = "9223372036854775808"  // MaxInt64+1 (overflows int64)
)

// TestBoundaryCountPosition: SPOP / SRANDMEMBER / SCAN COUNT count arguments.
func TestBoundaryCountPosition(t *testing.T) {
	d := newDiffer(t)
	for _, v := range []string{"0", "-1", bMin, bMaxP1} {
		k := d.k("sp")
		d.eq("SADD", bs("SADD"), k, bs("a"), bs("b"), bs("c"))
		d.eq("SPOP count "+v, bs("SPOP"), k, bs(v))
	}
	kAll := d.k("spall")
	d.eq("SADD", bs("SADD"), kAll, bs("a"), bs("b"), bs("c"))
	d.eqSorted("SPOP MAX (pops all, unordered)", bs("SPOP"), kAll, bs(bMax))
	d.eq("SCARD after SPOP-all -> 0", bs("SCARD"), kAll)
	for _, v := range []string{"0", "-1"} { // NOT bMin (OOMs the oracle)
		k := d.k("sr")
		d.eq("SADD", bs("SADD"), k, bs("a"))
		d.eqSorted("SRANDMEMBER count "+v, bs("SRANDMEMBER"), k, bs(v))
	}
	for _, v := range []string{"0", "-1", bMin, "abc"} {
		d.eq("SCAN COUNT "+v+" -> err", bs("SCAN"), bs("0"), bs("COUNT"), bs(v))
	}
	t.Logf("compared %d boundary-count replies vs Redis 3.2", d.n)
}

// TestBoundaryIndexPosition: LINDEX / LSET / LRANGE / LTRIM / LREM index arguments.
func TestBoundaryIndexPosition(t *testing.T) {
	d := newDiffer(t)
	for _, v := range []string{bMin, bMax, "0", "-1", bMaxP1} {
		k := d.k("li")
		d.eq("RPUSH", bs("RPUSH"), k, bs("a"), bs("b"), bs("c"))
		d.eq("LINDEX "+v, bs("LINDEX"), k, bs(v))
	}
	for _, v := range []string{bMin, bMaxP1} {
		k := d.k("ls")
		d.eq("RPUSH", bs("RPUSH"), k, bs("a"), bs("b"), bs("c"))
		d.eq("LSET "+v+" -> err", bs("LSET"), k, bs(v), bs("x"))
	}
	kr := d.k("lr")
	d.eq("RPUSH", bs("RPUSH"), kr, bs("a"), bs("b"), bs("c"))
	d.eq("LRANGE MIN MAX -> full", bs("LRANGE"), kr, bs(bMin), bs(bMax))
	kr2 := d.k("lr2")
	d.eq("RPUSH", bs("RPUSH"), kr2, bs("a"))
	d.eq("LRANGE MAXP1 0 -> err", bs("LRANGE"), kr2, bs(bMaxP1), bs("0"))
	klr := d.k("lrem")
	d.eq("RPUSH", bs("RPUSH"), klr, bs("a"), bs("a"), bs("b"))
	d.eq("LREM MIN a", bs("LREM"), klr, bs(bMin), bs("a"))
	klt := d.k("lt")
	d.eq("RPUSH", bs("RPUSH"), klt, bs("a"), bs("b"), bs("c"))
	d.eq("LTRIM MIN MAX", bs("LTRIM"), klt, bs(bMin), bs(bMax))
	d.eq("LLEN after LTRIM MIN MAX -> 3", bs("LLEN"), klt)
	t.Logf("compared %d boundary-index replies vs Redis 3.2", d.n)
}

// TestBoundaryOffsetPosition: GETRANGE / SETRANGE / SETBIT / GETBIT / BITCOUNT / BITPOS
// offset arguments (the in-int64 overflow ones; huge-value-growing offsets are §4.1).
func TestBoundaryOffsetPosition(t *testing.T) {
	d := newDiffer(t)
	kg := d.k("gr")
	d.eq("SET hello", bs("SET"), kg, bs("hello"))
	d.eq("GETRANGE MIN MAX -> full", bs("GETRANGE"), kg, bs(bMin), bs(bMax))
	kg2 := d.k("gr2")
	d.eq("SET hello", bs("SET"), kg2, bs("hello"))
	d.eq("GETRANGE MAXP1 0 -> err", bs("GETRANGE"), kg2, bs(bMaxP1), bs("0"))
	for _, v := range []string{bMin, bMaxP1, "-1"} {
		ks := d.k("sr")
		d.eq("SET hello", bs("SET"), ks, bs("hello"))
		d.eq("SETRANGE "+v+" -> err", bs("SETRANGE"), ks, bs(v), bs("x"))
	}
	for _, v := range []string{bMin, bMaxP1, "-1", bMax} { // bMax offset > 2^32 -> both reject
		d.eq("SETBIT "+v+" 1 -> err", bs("SETBIT"), d.k("sb"), bs(v), bs("1"))
	}
	for _, v := range []string{bMin, bMax} {
		kb := d.k("gb")
		d.eq("SET x", bs("SET"), kb, bs("x"))
		d.eq("GETBIT "+v+" -> err", bs("GETBIT"), kb, bs(v))
	}
	kbc := d.k("bc")
	d.eq("SET foobar", bs("SET"), kbc, bs("foobar"))
	d.eq("BITCOUNT MIN MAX", bs("BITCOUNT"), kbc, bs(bMin), bs(bMax))
	kbp := d.k("bp")
	d.eq("SET x", bs("SET"), kbp, bs("x"))
	d.eq("BITPOS 1 MAXP1 -> err", bs("BITPOS"), kbp, bs("1"), bs(bMaxP1))
	t.Logf("compared %d boundary-offset replies vs Redis 3.2", d.n)
}

// TestBoundaryTTLPosition: EXPIRE / PEXPIRE / SETEX / SET EX / EXPIREAT TTL arguments
// (excluding the overflow-into-ms-domain C-UB zone, which is §4.6).
func TestBoundaryTTLPosition(t *testing.T) {
	d := newDiffer(t)
	for _, cmd := range []string{"EXPIRE", "PEXPIRE"} {
		for _, v := range []string{bMin, "0", "-1", bMaxP1} {
			k := d.k("ex")
			d.eq("SET v", bs("SET"), k, bs("v"))
			d.eq(cmd+" "+v+" then EXISTS", bs("EXISTS"), applyThenExists(d, cmd, k, v))
		}
	}
	for _, v := range []string{bMin, "0", bMaxP1} {
		d.eq("SETEX "+v+" -> err", bs("SETEX"), d.k("se"), bs(v), bs("vv"))
		d.eq("SET EX "+v+" -> err", bs("SET"), d.k("sx"), bs("v"), bs("EX"), bs(v))
	}
	ka := d.k("at")
	d.eq("SET v", bs("SET"), ka, bs("v"))
	d.eq("EXPIREAT MIN (past->del)", bs("EXPIREAT"), ka, bs(bMin))
	d.eq("EXISTS after EXPIREAT MIN -> 0", bs("EXISTS"), ka)
	t.Logf("compared %d boundary-TTL replies vs Redis 3.2", d.n)
}

// applyThenExists issues the TTL command (ignoring its reply, compared elsewhere) and
// returns the key so the caller can probe EXISTS — keeping the matrix rows compact.
func applyThenExists(d *differ, cmd string, key []byte, v string) []byte {
	d.eq(cmd+" apply", bs(cmd), key, bs(v))
	return key
}

// TestBoundaryBitAndIncrementPosition: SETBIT bit-value + INCRBY/HINCRBY/ZADD increments.
func TestBoundaryBitAndIncrementPosition(t *testing.T) {
	d := newDiffer(t)
	for _, v := range []string{"2", "-1", bMin} { // bit value must be 0/1
		d.eq("SETBIT bit-value "+v+" -> err", bs("SETBIT"), d.k("bv"), bs("0"), bs(v))
	}
	ki := d.k("ib")
	d.eq("SET 0", bs("SET"), ki, bs("0"))
	d.eq("INCRBY MAX", bs("INCRBY"), ki, bs(bMax))
	d.eq("INCRBY MAXP1 -> err", bs("INCRBY"), d.k("ib2"), bs(bMaxP1))
	ki3 := d.k("ib3")
	d.eq("SET 0", bs("SET"), ki3, bs("0"))
	d.eq("INCRBY MIN", bs("INCRBY"), ki3, bs(bMin))
	kh := d.k("hi")
	d.eq("HSET 0", bs("HSET"), kh, bs("f"), bs("0"))
	d.eq("HINCRBY MAXP1 -> err", bs("HINCRBY"), kh, bs("f"), bs(bMaxP1))
	kh2 := d.k("hi2")
	d.eq("HSET 0", bs("HSET"), kh2, bs("f"), bs("0"))
	d.eq("HINCRBY MAX", bs("HINCRBY"), kh2, bs("f"), bs(bMax))
	kz := d.k("z")
	d.eq("ZADD MAXP1 m", bs("ZADD"), kz, bs(bMaxP1), bs("m"))
	d.eq("ZSCORE m (MAXP1 as float)", bs("ZSCORE"), kz, bs("m"))
	t.Logf("compared %d boundary-bit/increment replies vs Redis 3.2", d.n)
}

// TestBoundaryDBAndBinaryAndInf: SELECT bounds (excluding the MinInt64 C-UB), binary
// keys/members, and the inf/nan cases that DO agree (score-bound ±inf, INCRBYFLOAT/
// HINCRBYFLOAT inf, GEOADD nan).
func TestBoundaryDBAndBinaryAndInf(t *testing.T) {
	d := newDiffer(t)
	// SELECT out-of-range (MinInt64 excluded — Redis (int)-cast C-UB, §4.6).
	for _, v := range []string{bMax, bMaxP1, "16", "-1"} {
		d.eq("SELECT "+v+" -> err", bs("SELECT"), bs(v))
	}
	// binary key: embedded NUL must not collide/truncate; \xff value round-trips.
	kn := d.k("bin")
	d.eq("SET nul-key", bs("SET"), bs(string([]byte{0x00})+"bk"), bs("bv"))
	d.eq("GET nul-key", bs("GET"), bs(string([]byte{0x00})+"bk"))
	_ = kn
	d.eq("SET ab", bs("SET"), bs("ab"), bs("V1"))
	d.eq("SET a\\x00b (no collide)", bs("SET"), bs("a\x00b"), bs("V2"))
	d.eq("GET a\\x00b -> V2", bs("GET"), bs("a\x00b"))
	kff := d.k("ff")
	d.eq("SET \\xff value", bs("SET"), kff, bs(string([]byte{0xff})+"end"))
	d.eq("GET \\xff value round-trips", bs("GET"), kff)
	sm := d.k("bmem")
	d.eq("SADD nul-member", bs("SADD"), sm, bs("a\x00c"))
	d.eq("SISMEMBER nul-member -> 1", bs("SISMEMBER"), sm, bs("a\x00c"))
	// inf/nan cases that agree with the oracle
	kzi := d.k("zi")
	d.eq("ZADD 1 a 2 b", bs("ZADD"), kzi, bs("1"), bs("a"), bs("2"), bs("b"))
	d.eq("ZRANGEBYSCORE -inf +inf -> full", bs("ZRANGEBYSCORE"), kzi, bs("-inf"), bs("+inf"))
	d.eq("INCRBYFLOAT inf -> both reject", bs("INCRBYFLOAT"), d.k("ibf"), bs("inf"))
	d.eq("HINCRBYFLOAT inf -> both accept (inf)", bs("HINCRBYFLOAT"), d.k("hbf"), bs("f"), bs("inf"))
	d.eq("GEOADD nan -> both reject", bs("GEOADD"), d.k("geo"), bs("nan"), bs("20"), bs("m"))
	t.Logf("compared %d boundary-DB/binary/inf replies vs Redis 3.2", d.n)
}
