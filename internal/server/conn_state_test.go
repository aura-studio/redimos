package server

import "testing"

func TestNewConnDefaults(t *testing.T) {
	c := newConn(nil, "inst-abc")

	if c.Authed() {
		t.Errorf("new connection should start unauthenticated")
	}
	if got := c.DB(); got != 0 {
		t.Errorf("new connection should default to db 0, got %d", got)
	}
	if got := c.InstID(); got != "inst-abc" {
		t.Errorf("InstID = %q, want %q", got, "inst-abc")
	}
	if got := c.RemoteAddr(); got != "" {
		t.Errorf("RemoteAddr with nil redcon conn = %q, want empty", got)
	}
	if c.Redcon() != nil {
		t.Errorf("Redcon() should be nil when constructed with nil")
	}
}

func TestConnAuthedToggle(t *testing.T) {
	c := newConn(nil, "inst-1")

	c.SetAuthed(true)
	if !c.Authed() {
		t.Errorf("Authed() = false after SetAuthed(true)")
	}

	c.SetAuthed(false)
	if c.Authed() {
		t.Errorf("Authed() = true after SetAuthed(false)")
	}
}

func TestConnSelectDB(t *testing.T) {
	c := newConn(nil, "inst-1")

	c.SetDB(3)
	if got := c.DB(); got != 3 {
		t.Errorf("DB() = %d after SetDB(3), want 3", got)
	}

	c.SetDB(0)
	if got := c.DB(); got != 0 {
		t.Errorf("DB() = %d after SetDB(0), want 0", got)
	}
}

func TestConnInstIDStable(t *testing.T) {
	// The instID identifies the owning proxy instance for SCAN cursor
	// ownership; it must not change over the connection's lifetime.
	c := newConn(nil, "inst-xyz")
	first := c.InstID()
	c.SetAuthed(true)
	c.SetDB(1)
	if c.InstID() != first {
		t.Errorf("InstID changed over connection lifetime: %q -> %q", first, c.InstID())
	}
}
