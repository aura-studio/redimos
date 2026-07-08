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

	// wrapped is the observing redcon.Conn returned by Redcon(): it flips errored
	// when an error reply is written, so the dispatcher can label per-command
	// metrics without the handlers reporting back. It is nil in tests that build a
	// Conn without a real redcon connection.
	wrapped redcon.Conn

	// errored records whether the current command wrote an error reply. It is reset
	// per command by the observing dispatcher (ResetErrored) and read after the
	// handler returns (Errored).
	errored bool

	// errClass records the RESP error code of the current command's error reply —
	// the first whitespace-delimited token after the leading '-', e.g. "WRONGTYPE",
	// "ERR", "NOAUTH". Empty when the command did not error. Reset per command
	// alongside errored and read via ErrorClass so the dispatcher can label the
	// error metric by class without the handlers reporting back.
	errClass string

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
	c := &Conn{
		rc:     rc,
		authed: false,
		db:     0,
		instID: instID,
	}
	if rc != nil {
		c.wrapped = observingConn{Conn: rc, errored: &c.errored, errClass: &c.errClass}
	}
	return c
}

// Redcon returns the redcon connection used to write RESP2 replies. It is the
// observing wrapper when a real connection is present (so error replies flip the
// per-command errored flag), or the raw connection in tests.
func (c *Conn) Redcon() redcon.Conn {
	if c.wrapped != nil {
		return c.wrapped
	}
	return c.rc
}

// Errored reports whether the current command has written an error reply.
func (c *Conn) Errored() bool { return c.errored }

// ErrorClass returns the RESP error code of the current command's error reply
// (e.g. "WRONGTYPE", "ERR", "NOAUTH"), or "" when the command did not error.
func (c *Conn) ErrorClass() string { return c.errClass }

// ResetErrored clears the error flag and class; the dispatcher calls it before each
// command.
func (c *Conn) ResetErrored() {
	c.errored = false
	c.errClass = ""
}

// observingConn wraps a redcon.Conn and, when an error reply is written — either the
// raw "-...\r\n" bytes that resp.Writer flushes via WriteRaw, or a direct redcon
// WriteError — flips *errored and records the error code in *errClass. All other
// methods are promoted unchanged.
type observingConn struct {
	redcon.Conn
	errored  *bool
	errClass *string
}

func (o observingConn) WriteRaw(p []byte) {
	if len(p) > 0 && p[0] == '-' {
		*o.errored = true
		*o.errClass = errCodeFromReply(p[1:])
	}
	o.Conn.WriteRaw(p)
}

func (o observingConn) WriteError(msg string) {
	*o.errored = true
	*o.errClass = errCodeFromReply([]byte(msg))
	o.Conn.WriteError(msg)
}

// errCodeFromReply extracts the error code — the first whitespace/CRLF-delimited
// token — from a RESP error payload (the bytes after the leading '-', or a
// WriteError message), e.g. "WRONGTYPE Operation ..." -> "WRONGTYPE". Redis error
// codes are a small fixed set, so this keeps the error-class label low-cardinality.
// It returns "ERR" as a safe default when no token is present.
func errCodeFromReply(b []byte) string {
	end := len(b)
	for i := 0; i < len(b); i++ {
		if ch := b[i]; ch == ' ' || ch == '\r' || ch == '\n' {
			end = i
			break
		}
	}
	if end == 0 {
		return "ERR"
	}
	return string(b[:end])
}

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
