package server

import (
	"net"
	"sync"

	"github.com/tidwall/redcon"
)

// ServeConn serves exactly one already-established net.Conn using the SAME redcon
// callbacks (onAccept / onCommand / onClosed) as the TCP path, so an embedded,
// in-process client (see the root redimos.NewClient) drives the proxy over
// an in-memory connection with byte-for-byte the same command handling, per-command
// serial pipelining, MaxCommandBytes gate and drain/closing semantics as a real TCP
// client. It is purely additive: the TCP serving path (New + ListenAndServe /
// ListenServeAndSignal) is untouched.
//
// It blocks until conn is closed (by either peer) and then returns nil, mirroring
// redcon's own "clean close returns nil" behaviour; a genuine listener/accept error
// is surfaced. Each call serves one conn, so the caller spawns one goroutine per
// dialed connection (matching go-redis's independent pooled connections).
func (s *Server) ServeConn(conn net.Conn) error {
	ln := newOneShotListener(conn)
	// redcon.Serve drives the same accept/command/closed callbacks the TCP server
	// wires in New; reusing them is what guarantees identical behaviour. The one-shot
	// listener yields conn exactly once, then blocks until conn/Close so Serve's accept
	// loop parks (never spins) and returns cleanly once the conn is done.
	return redcon.Serve(ln, s.onCommand, s.onAccept, s.onClosed)
}

// oneShotListener is a net.Listener that yields a single, pre-established conn from
// its first Accept and then blocks subsequent Accepts until the conn (or the
// listener) is closed, at which point Accept returns net.ErrClosed so redcon's serve
// loop exits with nil. It exists only to bridge a raw net.Conn into redcon.Serve,
// which is listener-oriented; no kernel networking is involved.
type oneShotListener struct {
	conn net.Conn

	mu       sync.Mutex
	handed   bool          // conn already returned by Accept
	closed   bool          // Close called
	closeCh  chan struct{} // closed exactly once when the listener closes
	closeOne sync.Once
}

func newOneShotListener(conn net.Conn) *oneShotListener {
	return &oneShotListener{conn: conn, closeCh: make(chan struct{})}
}

// Accept returns the wrapped conn on the first call. On every later call it blocks
// until Close is invoked and then returns net.ErrClosed, so redcon's accept loop
// parks after the single conn is handed out and unwinds cleanly on close.
func (l *oneShotListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, net.ErrClosed
	}
	if !l.handed {
		l.handed = true
		c := l.conn
		l.mu.Unlock()
		return c, nil
	}
	l.mu.Unlock()

	// Single conn already handed out: block until the listener is closed rather than
	// return an error (which redcon would treat as an accept error and spin on).
	<-l.closeCh
	return nil, net.ErrClosed
}

// Close marks the listener closed and wakes any parked Accept. It does NOT close the
// wrapped conn itself: the conn's lifetime is owned by redcon's per-connection handler
// (which closes it on EOF) and by the embedding's Closer.
func (l *oneShotListener) Close() error {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	l.closeOne.Do(func() { close(l.closeCh) })
	return nil
}

// Addr returns the wrapped conn's local address (a dummy in-process address).
func (l *oneShotListener) Addr() net.Addr { return l.conn.LocalAddr() }
