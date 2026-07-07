package integration

import "testing"

// Regression tests for the 2026-07-07 round-10 adversarial pass, whose star dimension
// was pika-migrate compatibility. Most pika findings were migration-limitation docs;
// the code fixes here are the shared Redis-3.2 alignment bugs the sweep surfaced. Each
// byte-diffs against the live redis:3.2 oracle. (The ZINCRBY/ZADD-INCR out-of-domain
// error-shape fix and the SCAN "-0" cursor fix diverge from / can't be compared to the
// oracle by design — they are covered by unit tests: storage.TestScoreOutOfDomain and
// command.TestParseScanCursor.)

// TestFixEmptyStringFloat: Redis' strtod treats an EMPTY string as 0.0, so INCRBYFLOAT /
// HINCRBYFLOAT with an empty increment (or applied to a stored empty-string value), and
// GEOADD / GEORADIUS with an empty coordinate / radius, all succeed as 0. Exact "" only:
// a single space still fails, and the integer path (INCRBY / HINCRBY "") still rejects.
func TestFixEmptyStringFloat(t *testing.T) {
	d := newDiffer(t)
	d.eq("INCRBYFLOAT empty (new) -> 0", bs("INCRBYFLOAT"), d.k("f1"), bs(""))

	k2 := d.k("f2")
	d.eq("SET 5", bs("SET"), k2, bs("5"))
	d.eq("INCRBYFLOAT empty on 5 -> 5", bs("INCRBYFLOAT"), k2, bs(""))

	k3 := d.k("f3")
	d.eq("SET empty", bs("SET"), k3, bs(""))
	d.eq("INCRBYFLOAT 1 on stored-empty -> 1", bs("INCRBYFLOAT"), k3, bs("1"))

	k4 := d.k("f4")
	d.eq("HSET empty field", bs("HSET"), k4, bs("f"), bs(""))
	d.eq("HINCRBYFLOAT 1 on empty field -> 1", bs("HINCRBYFLOAT"), k4, bs("f"), bs("1"))
	d.eq("HINCRBYFLOAT empty incr (new) -> 0", bs("HINCRBYFLOAT"), d.k("f5"), bs("g"), bs(""))

	d.eq("GEOADD empty lon -> 1 (0,0)", bs("GEOADD"), d.k("g1"), bs(""), bs("0"), bs("m"))
	g2 := d.k("g2")
	d.eq("GEOADD seed", bs("GEOADD"), g2, bs("13.36"), bs("38.11"), bs("x"))
	d.eqSorted("GEORADIUS empty radius -> empty", bs("GEORADIUS"), g2, bs("15"), bs("37"), bs(""), bs("km"))

	// Must-stay rejections: the integer path and a single space.
	d.eq("INCRBY empty still rejected", bs("INCRBY"), d.k("i1"), bs(""))
	d.eq("INCRBYFLOAT single space still rejected", bs("INCRBYFLOAT"), d.k("i2"), bs(" "))
	t.Logf("compared %d empty-string-float replies vs Redis 3.2", d.n)
}
