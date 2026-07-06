package command

import "testing"

// Deterministic (fixedNow) coverage for the 2026-07-06 sub-second / overflow expiry fixes.
// The integration differential can't pin these without timing flake (second-granularity
// storage vs a wall clock), so they're asserted here against the stored meta.exp.

// TestSubSecondTTLDoesNotInstantExpire: a POSITIVE sub-second TTL must lift the expiry to
// now+1 (key survives ~1s) instead of truncating to now (which read as already-expired and
// deleted the key immediately). Covers PEXPIRE, PSETEX and SET PX.
func TestSubSecondTTLDoesNotInstantExpire(t *testing.T) {
	cases := []struct {
		name  string
		setup []string
		cmd   string
	}{
		{"PEXPIRE 200", []string{"SET k v"}, "PEXPIRE k 200"},
		{"SET PX 300", nil, "SET k v PX 300"},
		{"PSETEX 200", nil, "PSETEX k 200 v"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStringStore()
			conn, r := startKeysServer(t, store, fixedNow(1000), nil)
			for _, s := range tc.setup {
				sendRead(t, conn, r, s)
			}
			sendRead(t, conn, r, tc.cmd)
			// exp must be now+1 (1001), NOT now (1000, which meta.IsExpired treats as expired).
			if got := store.metas["0:k"].Exp; got != 1001 {
				t.Errorf("%s: meta.exp = %d, want 1001 (now+1, key survives — not instant-expired)", tc.name, got)
			}
			// And the key reads as live: GET returns the value, PTTL is 1000 (not -2).
			if got, want := sendRead(t, conn, r, "GET k"), "$v"; got != want {
				t.Errorf("%s: GET k = %q, want %q (key must survive)", tc.name, got, want)
			}
			if got, want := sendRead(t, conn, r, "PTTL k"), ":1000"; got != want {
				t.Errorf("%s: PTTL k = %q, want %q (second-precision, key alive)", tc.name, got, want)
			}
		})
	}
}

// TestExpiryOverflowImmediatelyExpires: an expire time so large it overflows the ms domain
// must resolve deterministically to now (created-then-expired), not overflow now+n into a
// bogus permanent/negative-TTL key. Covers SET EX, SETEX, SET PX, PSETEX.
func TestExpiryOverflowImmediatelyExpires(t *testing.T) {
	const maxInt64 = "9223372036854775807"
	cases := []struct {
		name string
		cmd  string
	}{
		{"SET EX overflow", "SET k v EX " + maxInt64},
		{"SET EX 9.3e15", "SET k v EX 9300000000000000"},
		{"SETEX overflow", "SETEX k " + maxInt64 + " v"},
		{"SET PX overflow", "SET k v PX " + maxInt64},
		{"PSETEX overflow", "PSETEX k " + maxInt64 + " v"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStringStore()
			conn, r := startKeysServer(t, store, fixedNow(1000), nil)
			sendRead(t, conn, r, tc.cmd) // +OK
			// exp resolved to now (1000) => not a bogus future/negative epoch.
			if got := store.metas["0:k"].Exp; got != 1000 {
				t.Errorf("%s: meta.exp = %d, want 1000 (now, immediately expired — no overflow)", tc.name, got)
			}
			// The key reads as gone.
			if got, want := sendRead(t, conn, r, "TTL k"), ":-2"; got != want {
				t.Errorf("%s: TTL k = %q, want %q (created-then-expired)", tc.name, got, want)
			}
		})
	}
}
