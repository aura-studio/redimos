package integration

import "testing"

// Regression test for the 2026-07-07 round-13 all-dimensions sweep. The 11-lens sweep
// (regression-audit of round-8..12 + fresh A/C/E/AC/M/Q/bitops/geo/hll/zset/string
// passes) surfaced exactly one real code gap, in GEODIST's absent-case reply shape;
// every other candidate was already-covered, platform-bound, or (GEO unit
// case-insensitivity) a finder misconception that oracle verification refuted (Redis
// 3.2 GEO units are case-sensitive too — both reject "KM"). Byte-diffs vs redis:3.2.

// TestFixGeoDistAbsentReplyShape: GEODIST distinguishes a missing KEY (Redis'
// lookupKeyReadOrReply(...emptybulk) -> "$0" empty bulk) from a missing MEMBER of a
// present key (shared.nullbulk -> "$-1" nil), matching GEOPOS/GEOHASH's live-vs-not
// split. redimos previously conflated both to "$-1".
func TestFixGeoDistAbsentReplyShape(t *testing.T) {
	d := newDiffer(t)
	d.eq("GEODIST missing key -> empty bulk", bs("GEODIST"), d.k("nokey"), bs("a"), bs("b"))
	d.eq("GEODIST missing key with unit -> empty bulk", bs("GEODIST"), d.k("nokey2"), bs("a"), bs("b"), bs("km"))

	k := d.k("g")
	d.eq("GEOADD seed", bs("GEOADD"), k, bs("13.361389"), bs("38.115556"), bs("P"))
	d.eq("GEODIST missing member -> nil", bs("GEODIST"), k, bs("P"), bs("Zmiss"))
	d.eq("GEODIST both present -> distance", bs("GEODIST"), func() []byte { return k }(), bs("P"), bs("P"))

	// Units stay case-sensitive (Redis 3.2 rejects uppercase too — verified vs oracle).
	k2 := d.k("g2")
	d.eq("GEOADD P", bs("GEOADD"), k2, bs("13.361389"), bs("38.115556"), bs("P"))
	d.eq("GEOADD C", bs("GEOADD"), k2, bs("15.087269"), bs("37.502669"), bs("C"))
	d.eq("GEODIST km lowercase works", bs("GEODIST"), k2, bs("P"), bs("C"), bs("km"))
	d.eq("GEODIST KM uppercase still rejected", bs("GEODIST"), k2, bs("P"), bs("C"), bs("KM"))
	t.Logf("compared %d GEODIST replies vs Redis 3.2", d.n)
}
