// Package server hosts the redcon protocol shell: TCP listening, connection
// lifecycle management, and strictly serial per-connection pipelining.
package server

import "github.com/tidwall/redcon"

// Conn is the per-connection state carried alongside a redcon connection for
// the lifetime of a single client connection. It is attached to the underlying
// redcon connection via SetContext when the connection is accepted and is
// handed to the command dispatcher on every command.
//
// Because redcon serves each connection on its own goroutine and invokes the
// command callback strictly one command at a time (see ServeConn in the
// design's algorithm 4), the fields below are only ever read or written from
// that single serving goroutine. No additional synchronization is required as
// long as the proxy does not introduce concurrency within a connection.
type Conn struct {
	rc redcon.Conn

	// authed reports whether the connection has successfully AUTHed. When
	// requirepass is unset the connection is considered authed from the start
	// (the handshake layer, task 6.1, decides the initial value); the shell
	// only stores the flag.
	authed bool

	// db is the currently SELECTed logical database index. P0 fixes this to 0;
	// the value maps to the pk prefix when multi-DB is enabled.
	db int

	// instID identifies the proxy instance that owns this connection. SCAN
	// cursors handed out on this connection belong to instID, so a cursor
	// replayed against a different instance can be rejected with
	// "-ERR invalid cursor, restart scan".
	instID string
}

// newConn builds the initial per-connection state. The connection starts
// unauthenticated on db 0; the handshake layer adjusts authed based on whether
// requirepass is configured.
func newConn(rc redcon.Conn, instID string) *Conn {
	return &Conn{
		rc:     rc,
		authed: false,
		db:     0,
		instID: instID,
	}
}

// Redcon returns the underlying redcon connection used to write RESP2 replies.
func (c *Conn) Redcon() redcon.Conn { return c.rc }

// Authed reports whether the connection has authenticated.
func (c *Conn) Authed() bool { return c.authed }

// SetAuthed records the authentication state of the connection.
func (c *Conn) SetAuthed(authed bool) { c.authed = authed }

// DB returns the currently selected logical database index.
func (c *Conn) DB() int { return c.db }

// SetDB records the selected logical database index.
func (c *Conn) SetDB(db int) { c.db = db }

// InstID returns the identifier of the proxy instance that owns this
// connection. It is used to validate SCAN cursor ownership.
func (c *Conn) InstID() string { return c.instID }

// RemoteAddr returns the remote address of the client, or "" if the underlying
// connection is not set (e.g. in unit tests).
func (c *Conn) RemoteAddr() string {
	if c.rc == nil {
		return ""
	}
	return c.rc.RemoteAddr()
}
