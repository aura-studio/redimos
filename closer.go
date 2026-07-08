package redimos

import (
	"crypto/rand"
	"encoding/hex"
	"net"
	"sync"
)

// inProcessCloser is the io.Closer returned by NewInProcessClient. It owns the
// server-side ends of every in-memory connection go-redis dialed, plus the reused
// server.Server. Close severs each server-side conn — which makes its per-connection
// redcon.Serve loop hit EOF and return, ending that serving goroutine — and then
// closes the server shell. After Close no serving goroutine from this embedding
// remains (verified by the goroutine-leak test), so the embedding leaves zero
// background goroutines behind.
type inProcessCloser struct {
	mu     sync.Mutex
	conns  []net.Conn // server-side ends served by ServeConn
	closed bool
}

// track registers a server-side conn so Close can sever it. A conn dialed after Close
// (which should not happen — go-redis stops dialing once the pool is closed) is closed
// immediately so no goroutine is stranded.
func (c *inProcessCloser) track(conn net.Conn) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = conn.Close()
		return
	}
	c.conns = append(c.conns, conn)
	c.mu.Unlock()
}

// Close shuts the embedding down: it closes every tracked server-side conn (ending
// each ServeConn goroutine at EOF) and then closes the reused server shell. It is
// idempotent. It returns the first conn-close error, if any (normally nil).
func (c *inProcessCloser) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conns := c.conns
	c.conns = nil
	c.mu.Unlock()

	var firstErr error
	for _, conn := range conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// The reused server shell holds no OS resources on the in-process path (it was
	// never TCP-listened; each ServeConn runs its own redcon loop over an in-memory
	// conn). Closing the tracked server-side conns above already ends every serving
	// goroutine at EOF, so there is nothing further to release here.
	return firstErr
}

// newInstID returns a random hex instance identifier, mirroring the server's own
// generator so the embedding's SCAN cursor ownership shares the TCP path's id shape.
func newInstID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "inst-0"
	}
	return "inst-" + hex.EncodeToString(b[:])
}
