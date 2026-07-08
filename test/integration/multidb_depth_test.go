package integration

import (
	"strconv"
	"testing"
)

// Dimension H (depth): multi-DB isolation and SELECT bounds.
//
// The base file (multidb_test.go) proves a string key written in one DB is invisible
// in another. This file DEEPENS that along the gaps enumerated for dimension H:
//
//   - SELECT bounds parity: out-of-range (>= databases), negative, and the int64
//     extremes must be REJECTED with the same error Redis 3.2 emits, and a
//     non-integer index must produce Redis' "invalid DB index" message. redimos now
//     bounds SELECT n to [0, databases) exactly like Redis' selectDb, so these are
//     asserted differentially (GAP 1, GAP 7).
//   - Per-DB isolation of the COLLECTION and KEY-MANAGEMENT command families, not
//     just string GET/SET: EXPIRE/TTL/PERSIST, DEL/EXISTS/TYPE, and SCAN must each
//     operate on the currently-selected DB only, leaving a same-named key in another
//     DB untouched (GAP 4, GAP 5, GAP 6).
//
// DELIBERATELY NOT ASSERTED HERE (they would diverge by design, not a bug — see the
// handlers): DBSIZE is a client-probe stub that always replies :0 (stub.go
// handleDBSize), so a per-DB count comparison would trivially fail on the proxy;
// FLUSHDB / FLUSHALL / MOVE are first-class proxy REJECTS (keys.go / rejected.go
// errMoveUnsupported), so a differential case would only re-observe the intended
// rejection, not a semantic. Those gaps (GAP 2, GAP 3) are architectural stubs, not
// alignment bugs, so no differential case is added for them.
//
// Every SELECT/mutation goes through d.eq so BOTH endpoints advance their per-connection
// DB pointer in lockstep; the subsequent SCAN helpers reuse the same d.p/d.o connections
// and therefore inherit that same selected DB on each side.

// selectBack returns to DB 0 on both endpoints; every test ends here so a leaked
// non-zero DB pointer cannot bleed into a later test sharing the run nonce.
func (d *differ) selectBack() {
	d.eq("SELECT 0 restore", bs("SELECT"), bs("0"))
}

// TestDiffSelectBounds exercises the SELECT index validation boundary against the
// oracle: the last in-range index, the first out-of-range index, negatives, and the
// int64 extremes must all reply byte-identically (GAP 1, GAP 7). Redis 3.2 defaults
// to 16 logical DBs, so [0,16) is valid and 16+ is "-ERR DB index is out of range".
func TestDiffSelectBounds(t *testing.T) {
	d := newDiffer(t)

	// In-range endpoints: 0 and the last valid index (15) both reply +OK.
	d.eq("SELECT 0 -> OK", bs("SELECT"), bs("0"))
	d.eq("SELECT 15 (last valid) -> OK", bs("SELECT"), bs("15"))
	d.selectBack()

	// First out-of-range index: 16 is rejected as out of range on both sides.
	d.eq("SELECT 16 (first out-of-range)", bs("SELECT"), bs("16"))
	// A little past the edge.
	d.eq("SELECT 17", bs("SELECT"), bs("17"))
	d.eq("SELECT 100", bs("SELECT"), bs("100"))

	// Negative indices are out of range (never a valid DB).
	d.eq("SELECT -1", bs("SELECT"), bs("-1"))
	d.eq("SELECT -16", bs("SELECT"), bs("-16"))

	// int64-max: parses as an integer, then fails the range check — NOT an overflow/parse
	// error. Confirms redimos validates the bound after parsing (GAP 7). Both reject:
	// redimos because it is >= 16, and Redis because selectCommand passes the parsed long
	// to selectDb's `int id`, and (int)INT64_MAX == -1 < 0.
	d.eq("SELECT int64-max", bs("SELECT"), bs(strconv.FormatInt(9223372036854775807, 10)))
	// NOTE: "SELECT -9223372036854775808" (int64-min) is deliberately NOT compared. It is a
	// Redis 3.2 C-truncation quirk: selectDb takes an `int`, and (int)INT64_MIN == 0, so
	// Redis replies +OK (silently selecting DB 0!) for the most-negative index — verified
	// against the live oracle. redimos parses the full int64 and correctly rejects it as out
	// of range; matching Redis here would mean reproducing an integer-truncation bug, so
	// this is an accepted (redimos-is-stricter) divergence, not a shortcoming.

	// One past int64-max does not parse as int64 -> Redis' non-numeric "invalid DB
	// index" path, distinct from the out-of-range message.
	d.eq("SELECT 9223372036854775808 (int64 overflow)", bs("SELECT"), bs("9223372036854775808"))

	d.selectBack()
}

// TestDiffSelectNonInteger covers the non-numeric SELECT argument, which Redis 3.2
// reports as "invalid DB index" (not the generic not-an-integer error), including
// binary and whitespace-padded forms.
func TestDiffSelectNonInteger(t *testing.T) {
	d := newDiffer(t)

	d.eq("SELECT abc (non-numeric)", bs("SELECT"), bs("abc"))
	d.eq("SELECT empty arg", bs("SELECT"), bs(""))
	d.eq("SELECT float 1.5", bs("SELECT"), bs("1.5"))
	d.eq("SELECT hex 0x1", bs("SELECT"), bs("0x1"))
	d.eq("SELECT '1 ' (trailing space)", bs("SELECT"), bs("1 "))
	d.eq("SELECT ' 1' (leading space)", bs("SELECT"), bs(" 1"))
	d.eq("SELECT '+1' (leading plus)", bs("SELECT"), bs("+1"))
	d.eq("SELECT binary NUL", bs("SELECT"), bs("\x001"))

	d.selectBack()
}

// TestDiffMultiDBExpireIsolation verifies EXPIRE/TTL/PTTL/PERSIST are per-key per-DB:
// a set named the same in DB 1 and DB 0 carries independent expirations, so reading
// TTL after switching DBs must report THIS DB's value, never the other's (GAP 5). The
// TTLs (10 vs 20) are far enough apart that eqIntClose's 1s tolerance would catch any
// cross-DB leak while still allowing the countdown to straddle a second boundary.
func TestDiffMultiDBExpireIsolation(t *testing.T) {
	d := newDiffer(t)
	k := d.k("expiso")

	d.eq("SELECT 1", bs("SELECT"), bs("1"))
	d.eq("SADD in db1", bs("SADD"), k, bs("m"))
	d.eq("EXPIRE db1 -> 10", bs("EXPIRE"), k, bs("10"))

	d.eq("SELECT 0", bs("SELECT"), bs("0"))
	d.eq("SADD same key in db0", bs("SADD"), k, bs("m"))
	d.eq("EXPIRE db0 -> 20", bs("EXPIRE"), k, bs("20"))
	d.eqIntClose("TTL in db0 ~20 (not db1's 10)", 1, bs("TTL"), k)
	d.eqIntClose("PTTL in db0 ~20000ms", 1500, bs("PTTL"), k)

	d.eq("SELECT 1 again", bs("SELECT"), bs("1"))
	d.eqIntClose("TTL in db1 still ~10 (not db0's 20)", 1, bs("TTL"), k)

	// PERSIST in db1 clears only db1's TTL; db0's must survive.
	d.eq("PERSIST db1 -> 1", bs("PERSIST"), k)
	d.eq("TTL db1 after PERSIST -> -1", bs("TTL"), k)

	d.eq("SELECT 0 recheck", bs("SELECT"), bs("0"))
	d.eqIntClose("TTL db0 unchanged ~20 after db1 PERSIST", 1, bs("TTL"), k)

	// A TTL sentinel is per-DB too: an absent same-named key in db2 reports -2.
	d.eq("SELECT 2", bs("SELECT"), bs("2"))
	d.eq("TTL absent key in db2 -> -2", bs("TTL"), k)
	d.eq("PERSIST absent in db2 -> 0", bs("PERSIST"), k)

	d.selectBack()
}

// TestDiffMultiDBKeyMgmtIsolation seeds the SAME key name into DBs 0..5, then deletes
// it in ONE DB and checks EXISTS/TYPE/GET are unaffected in the others (GAP 6). This
// pushes past the base file's DBs 0,1,2 to the full low range and exercises DEL's
// delete-in-one-DB path plus TYPE reporting the right per-DB type.
func TestDiffMultiDBKeyMgmtIsolation(t *testing.T) {
	d := newDiffer(t)
	k := d.k("keymgmt")

	// Seed a string value that encodes its DB index in every DB 0..5.
	for i := 0; i <= 5; i++ {
		si := strconv.Itoa(i)
		d.eq("SELECT "+si, bs("SELECT"), bs(si))
		d.eq("SET in db"+si, bs("SET"), k, bs("val-db"+si))
		d.eq("EXISTS in db"+si+" -> 1", bs("EXISTS"), k)
		d.eq("TYPE in db"+si+" -> string", bs("TYPE"), k)
	}

	// DEL the key in DB 3 only.
	d.eq("SELECT 3", bs("SELECT"), bs("3"))
	d.eq("DEL in db3 -> 1", bs("DEL"), k)
	d.eq("EXISTS in db3 after DEL -> 0", bs("EXISTS"), k)
	d.eq("GET in db3 after DEL -> nil", bs("GET"), k)
	d.eq("TYPE in db3 after DEL -> none", bs("TYPE"), k)
	d.eq("DEL again in db3 -> 0 (already gone)", bs("DEL"), k)

	// Every OTHER DB is untouched: value, EXISTS and TYPE all intact.
	for _, i := range []int{0, 1, 2, 4, 5} {
		si := strconv.Itoa(i)
		d.eq("SELECT "+si, bs("SELECT"), bs(si))
		d.eq("db"+si+" GET unaffected by db3 DEL", bs("GET"), k)
		d.eq("db"+si+" EXISTS still 1", bs("EXISTS"), k)
		d.eq("db"+si+" TYPE still string", bs("TYPE"), k)
	}

	d.selectBack()
}

// TestDiffMultiDBTypeIsolation checks that the SAME key name may hold DIFFERENT types
// in different DBs and TYPE reports each independently — and that a data op in one DB
// does not raise WRONGTYPE from a differently-typed same-named key in another DB
// (a subtle isolation bug if the pk prefix ever leaked).
func TestDiffMultiDBTypeIsolation(t *testing.T) {
	d := newDiffer(t)
	k := d.k("typeiso")

	d.eq("SELECT 1", bs("SELECT"), bs("1"))
	d.eq("SET string in db1", bs("SET"), k, bs("s"))
	d.eq("TYPE db1 -> string", bs("TYPE"), k)

	d.eq("SELECT 2", bs("SELECT"), bs("2"))
	d.eq("LPUSH list in db2", bs("LPUSH"), k, bs("e1"))
	d.eq("TYPE db2 -> list", bs("TYPE"), k)
	// A list op in db2 must succeed despite db1 holding a string under the same name.
	d.eq("LPUSH again in db2 (no cross-DB WRONGTYPE)", bs("LPUSH"), k, bs("e2"))
	d.eq("LLEN db2 -> 2", bs("LLEN"), k)

	d.eq("SELECT 3", bs("SELECT"), bs("3"))
	d.eq("SADD set in db3", bs("SADD"), k, bs("x"))
	d.eq("TYPE db3 -> set", bs("TYPE"), k)

	d.eq("SELECT 4", bs("SELECT"), bs("4"))
	d.eq("HSET hash in db4", bs("HSET"), k, bs("f"), bs("v"))
	d.eq("TYPE db4 -> hash", bs("TYPE"), k)

	// Re-read each earlier DB: its own type is intact, unshadowed by later DBs.
	d.eq("SELECT 1 recheck", bs("SELECT"), bs("1"))
	d.eq("TYPE db1 still string", bs("TYPE"), k)
	d.eq("GET db1 still 's'", bs("GET"), k)

	d.eq("SELECT 2 recheck", bs("SELECT"), bs("2"))
	d.eq("TYPE db2 still list", bs("TYPE"), k)

	d.selectBack()
}

// TestDiffMultiDBScanIsolation verifies SCAN over the keyspace returns only the
// currently-selected DB's keys — a same-named-but-independent key living in another
// DB must NOT appear (GAP 4). SCAN cursors are opaque and differ between the two
// endpoints, so we iterate each side to completion (scanMatchEq) and compare the
// matched key multisets; the per-run nonce MATCH pattern isolates this run's keys.
func TestDiffMultiDBScanIsolation(t *testing.T) {
	d := newDiffer(t)

	kShared := d.k("scaniso:shared") // exists in BOTH db1 and db0 (must not cross over)
	kOnly1 := d.k("scaniso:only1")   // exists only in db1
	kOnly0 := d.k("scaniso:only0")   // exists only in db0

	d.eq("SELECT 1", bs("SELECT"), bs("1"))
	d.eq("SET shared in db1", bs("SET"), kShared, bs("v1"))
	d.eq("SET only1 in db1", bs("SET"), kOnly1, bs("v"))

	d.eq("SELECT 0", bs("SELECT"), bs("0"))
	d.eq("SET shared in db0", bs("SET"), kShared, bs("v0"))
	d.eq("SET only0 in db0", bs("SET"), kOnly0, bs("v"))

	// Match only this run's scaniso keys. In db0: {shared, only0}, no only1.
	pat := "dt:" + d.prefix + ":scaniso:*"
	d.scanMatchEq("SCAN db0 sees only db0 keys", [][]byte{bs("SCAN")}, pat)

	d.eq("SELECT 1 for scan", bs("SELECT"), bs("1"))
	// In db1: {shared, only1}, no only0.
	d.scanMatchEq("SCAN db1 sees only db1 keys", [][]byte{bs("SCAN")}, pat)

	d.selectBack()
}
