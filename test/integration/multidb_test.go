package integration

import "testing"

// Dimension H: multi-DB isolation. redimos maps SELECT n onto a pk prefix ("<n>:"); a key
// written in one DB must be invisible in another, matching Redis' logical-DB isolation.
// This requires the proxy to be started with -multi-db (the harness does so); it compares
// SELECT/GET/EXISTS across DBs byte-for-byte with Redis 3.2. If the proxy runs without
// -multi-db, SELECT 1 is rejected and this test's first compare fails fast — that is the
// intended signal to enable it.

func TestDiffMultiDBIsolation(t *testing.T) {
	d := newDiffer(t)

	k := d.k("iso")

	d.eq("SELECT 1", bs("SELECT"), bs("1"))
	d.eq("SET in db1", bs("SET"), k, bs("v1"))
	d.eq("GET in db1", bs("GET"), k)
	d.eq("EXISTS in db1 -> 1", bs("EXISTS"), k)

	d.eq("SELECT 0", bs("SELECT"), bs("0"))
	d.eq("GET in db0 -> nil (isolated)", bs("GET"), k)
	d.eq("EXISTS in db0 -> 0", bs("EXISTS"), k)

	// A different value under the same key name in db0 stays independent of db1.
	d.eq("SET same key in db0", bs("SET"), k, bs("v0"))
	d.eq("GET in db0", bs("GET"), k)

	d.eq("SELECT 1 again", bs("SELECT"), bs("1"))
	d.eq("db1 value unchanged", bs("GET"), k)

	d.eq("SELECT 2", bs("SELECT"), bs("2"))
	d.eq("GET in db2 -> nil", bs("GET"), k)

	d.eq("SELECT 0 restore", bs("SELECT"), bs("0"))
}
