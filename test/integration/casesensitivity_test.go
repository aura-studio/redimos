package integration

import "testing"

// Case-sensitivity dimension: Redis 3.2 lowercases command names before lookup, and
// compares almost every option keyword / subcommand with strcasecmp (case-insensitive)
// — but a few argument values are matched case-SENSITIVELY (notably the GEO distance
// unit, which 3.2 checks with a manual lowercase-only comparison). These tests assert
// redimos handles the case of each position exactly as the live redis:3.2 oracle does,
// by sending the mixed-case form (the discriminating case) and byte-diffing the reply.
// Gated on REDIMOS_PROXY_ADDR + REDIMOS_REDIS_ORACLE like the rest of the differential
// suite.

// TestCommandNameCaseInsensitive: command names resolve regardless of case.
func TestCommandNameCaseInsensitive(t *testing.T) {
	d := newDiffer(t)
	d.eq("SeT mixed case", bs("SeT"), d.k("a"), bs("v"))
	d.eq("pInG mixed case", bs("pInG"))
	d.eq("iNcR mixed case", bs("iNcR"), d.k("i"))
	k := d.k("g")
	d.eq("seed", bs("SET"), k, bs("vv"))
	d.eq("GeT mixed case", bs("GeT"), k)
	d.eq("HsEt mixed case", bs("HsEt"), d.k("h"), bs("f"), bs("v"))
	e := d.k("e")
	d.eq("seed e", bs("SET"), e, bs("v"))
	d.eq("ExPiRe mixed case", bs("ExPiRe"), e, bs("100"))
	t.Logf("compared %d mixed-case command names vs Redis 3.2", d.n)
}

// TestOptionKeywordCaseInsensitive: option keywords / flags / subcommands are matched
// with strcasecmp, so the mixed-case form behaves identically to the canonical one.
func TestOptionKeywordCaseInsensitive(t *testing.T) {
	d := newDiffer(t)

	// SET options EX/PX/NX/XX.
	d.eq("SET Ex", bs("SET"), d.k("so"), bs("v"), bs("Ex"), bs("100"))
	d.eq("SET nX (new)", bs("SET"), d.k("so2"), bs("v"), bs("nX"))
	d.eq("SET Xx (missing -> nil)", bs("SET"), d.k("so3"), bs("v"), bs("Xx"))
	d.eq("SET pX", bs("SET"), d.k("so4"), bs("v"), bs("pX"), bs("100000"))

	// ZADD flags NX/XX/CH/INCR.
	d.eq("ZADD Nx", bs("ZADD"), d.k("z"), bs("Nx"), bs("1"), bs("m"))
	z2 := d.k("z2")
	d.eq("ZADD seed", bs("ZADD"), z2, bs("1"), bs("m"))
	d.eq("ZADD cH", bs("ZADD"), z2, bs("cH"), bs("2"), bs("m"))
	d.eq("ZADD InCr", bs("ZADD"), d.k("z3"), bs("InCr"), bs("5"), bs("m"))

	// ZRANGEBYSCORE WITHSCORES / LIMIT.
	zr := d.k("zr")
	d.eq("ZADD seed zr", bs("ZADD"), zr, bs("1"), bs("a"))
	d.eq("ZRANGEBYSCORE WiThScOrEs", bs("ZRANGEBYSCORE"), zr, bs("0"), bs("10"), bs("WiThScOrEs"))
	zr2 := d.k("zr2")
	d.eq("ZADD seed zr2", bs("ZADD"), zr2, bs("1"), bs("a"), bs("2"), bs("b"))
	d.eq("ZRANGEBYSCORE LiMiT", bs("ZRANGEBYSCORE"), zr2, bs("0"), bs("10"), bs("LiMiT"), bs("0"), bs("1"))

	// ZUNIONSTORE WEIGHTS / AGGREGATE SUM|MIN|MAX.
	zu := d.k("zu")
	d.eq("ZADD seed zu", bs("ZADD"), zu, bs("1"), bs("a"))
	d.eq("ZUNIONSTORE WeIgHtS", bs("ZUNIONSTORE"), d.k("zud"), bs("1"), zu, bs("WeIgHtS"), bs("2"))
	d.eq("ZUNIONSTORE AgGrEgAtE mIn", bs("ZUNIONSTORE"), d.k("zud2"), bs("1"), zu, bs("AgGrEgAtE"), bs("mIn"))

	// BITOP AND/OR/XOR/NOT.
	b1, b2 := d.k("bo"), d.k("bo")
	d.eq("SET b1", bs("SET"), b1, bs("abc"))
	d.eq("SET b2", bs("SET"), b2, bs("xyz"))
	d.eq("BITOP aNd", bs("BITOP"), bs("aNd"), d.k("bod"), b1, b2)
	d.eq("BITOP nOt", bs("BITOP"), bs("nOt"), d.k("bod2"), b1)

	// BITFIELD GET/SET/INCRBY/OVERFLOW/WRAP/SAT.
	d.eq("BITFIELD GeT", bs("BITFIELD"), d.k("bf"), bs("GeT"), bs("u8"), bs("0"))
	d.eq("BITFIELD sEt oVeRfLoW wRaP iNcRbY", bs("BITFIELD"), d.k("bf2"),
		bs("sEt"), bs("u8"), bs("0"), bs("255"), bs("oVeRfLoW"), bs("wRaP"), bs("iNcRbY"), bs("u8"), bs("0"), bs("10"))

	// LINSERT BEFORE/AFTER.
	li := d.k("li")
	d.eq("RPUSH li", bs("RPUSH"), li, bs("a"), bs("b"))
	d.eq("LINSERT BeFoRe", bs("LINSERT"), li, bs("BeFoRe"), bs("a"), bs("x"))
	li2 := d.k("li2")
	d.eq("RPUSH li2", bs("RPUSH"), li2, bs("a"), bs("b"))
	d.eq("LINSERT aFtEr", bs("LINSERT"), li2, bs("aFtEr"), bs("a"), bs("x"))

	// HSCAN MATCH/COUNT.
	hs := d.k("hs")
	d.eq("HSET hs", bs("HSET"), hs, bs("f"), bs("1"))
	d.eq("HSCAN MaTcH CoUnT", bs("HSCAN"), hs, bs("0"), bs("MaTcH"), bs("*"), bs("CoUnT"), bs("100"))

	// GEORADIUS WITHCOORD / ASC / COUNT.
	gr := d.k("gr")
	d.eq("GEOADD gr", bs("GEOADD"), gr, bs("13.361389"), bs("38.115556"), bs("P"))
	d.eqSorted("GEORADIUS WiThCoOrD", bs("GEORADIUS"), gr, bs("13.361389"), bs("38.115556"), bs("1"), bs("km"), bs("WiThCoOrD"))
	d.eq("GEORADIUS aSc CoUnT", bs("GEORADIUS"), gr, bs("13.361389"), bs("38.115556"), bs("1"), bs("km"), bs("aSc"), bs("CoUnT"), bs("1"))

	// CLIENT / CONFIG subcommands. (CLIENT SETNAME/GETNAME are §4.5 stubs that do not
	// persist the name, so we assert only that the mixed-case subcommand is accepted —
	// SETNAME replies +OK; GETNAME-after-SETNAME would expose the stub, not a case bug.)
	d.eq("CLIENT SeTnAmE", bs("CLIENT"), bs("SeTnAmE"), bs("x"))
	d.eq("CONFIG gEt", bs("CONFIG"), bs("gEt"), bs("maxmemory"))
	t.Logf("compared %d mixed-case option keywords vs Redis 3.2", d.n)
}

// TestGeoUnitCaseSensitive: the GEO distance unit is matched case-SENSITIVELY in Redis
// 3.2 (a manual lowercase-only check, not strcasecmp), so only "m"/"km"/"mi"/"ft" are
// accepted; "Km"/"KM" are rejected with the same error on both.
func TestGeoUnitCaseSensitive(t *testing.T) {
	d := newDiffer(t)
	k := d.k("gu")
	d.eq("GEOADD P", bs("GEOADD"), k, bs("13.361389"), bs("38.115556"), bs("P"))
	d.eq("GEOADD C", bs("GEOADD"), k, bs("15.087269"), bs("37.502669"), bs("C"))
	d.eq("GEODIST km lowercase -> distance", bs("GEODIST"), k, bs("P"), bs("C"), bs("km"))
	d.eq("GEODIST Km mixed -> error (case-sensitive)", bs("GEODIST"), k, bs("P"), bs("C"), bs("Km"))
	d.eq("GEODIST KM upper -> error", bs("GEODIST"), k, bs("P"), bs("C"), bs("KM"))
	d.eq("GEORADIUS KM upper -> error", bs("GEORADIUS"), k, bs("13.361389"), bs("38.115556"), bs("1"), bs("KM"))
	t.Logf("compared %d GEO-unit case replies vs Redis 3.2", d.n)
}
