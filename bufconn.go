package redimos

import (
	"bytes"
	"io"
	"net"
	"sync"
	"time"
)

// bufConn is one end of a buffered, bidirectional, in-memory connection pair. It
// implements net.Conn WITHOUT any kernel networking: bytes written to one end appear
// on the other end's read buffer.
//
// Why not net.Pipe: go-redis pipelines its connection handshake (it writes multiple
// commands before reading the first reply). net.Pipe is synchronous and unbuffered —
// a Write blocks until a matching Read — so a pipelined write with no concurrent
// reader DEADLOCKS. bufConn's Write is non-blocking: it appends to an unbounded
// in-memory buffer and signals a waiting reader, so pipelining never blocks.
//
// Each direction is a *pipeBuffer (an unbounded bytes.Buffer guarded by a Mutex +
// Cond). A bufConn reads from one direction and writes to the other; its peer reads
// and writes the opposite pair, so the two ends form a full-duplex connection.
type bufConn struct {
	rd *pipeBuffer // this end reads from here (peer writes into it)
	wr *pipeBuffer // this end writes into here (peer reads from it)

	// Deadlines. A zero time means "no deadline". They are guarded by their own mutex
	// and consulted by Read/Write via the shared pipeBuffer wakeups.
	dmu       sync.Mutex
	readDead  time.Time
	writeDead time.Time
}

// newBufConnPair returns the two ends of a buffered in-memory connection. Writes on
// one end are readable on the other. Either end may be closed independently; closing
// either wakes blocked reads/writes on both ends (a closed pipeBuffer surfaces EOF to
// its reader and ErrClosed to its writer).
func newBufConnPair() (client, server *bufConn) {
	a := newPipeBuffer() // client -> server bytes
	b := newPipeBuffer() // server -> client bytes
	client = &bufConn{rd: b, wr: a}
	server = &bufConn{rd: a, wr: b}
	return client, server
}

// pipeBuffer is an unbounded byte buffer with blocking reads. Write never blocks
// (append + signal); Read blocks until data is available, the buffer is closed, or
// the supplied deadline passes.
type pipeBuffer struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    bytes.Buffer
	closed bool
}

func newPipeBuffer() *pipeBuffer {
	p := &pipeBuffer{}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// write appends p to the buffer and wakes any waiting reader. It never blocks.
// It returns ErrClosed if the buffer is already closed.
func (b *pipeBuffer) write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, net.ErrClosed
	}
	n, _ := b.buf.Write(p) // bytes.Buffer.Write never returns an error
	b.cond.Broadcast()
	return n, nil
}

// read copies available bytes into p, blocking until data arrives, the buffer is
// closed (returns io.EOF once drained), or the deadline passes (returns a timeout
// error). A zero deadline means block indefinitely.
func (b *pipeBuffer) read(p []byte, deadline time.Time) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Deadline wakeup: a Cond has no timeout, so when a deadline is set we spawn a
	// one-shot timer that broadcasts at the deadline; the wait loop below then
	// re-checks the (now-passed) deadline and returns a timeout error.
	var timer *time.Timer
	if !deadline.IsZero() {
		d := time.Until(deadline)
		if d <= 0 {
			return 0, timeoutErr{}
		}
		timer = time.AfterFunc(d, func() {
			b.mu.Lock()
			b.cond.Broadcast()
			b.mu.Unlock()
		})
		defer timer.Stop()
	}

	for {
		if b.buf.Len() > 0 {
			return b.buf.Read(p) // never returns an error while Len()>0
		}
		if b.closed {
			return 0, io.EOF
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return 0, timeoutErr{}
		}
		b.cond.Wait()
	}
}

// close marks the buffer closed and wakes all waiters. Buffered-but-unread bytes
// remain readable (a reader drains them before seeing EOF), matching a half-closed
// connection whose peer already flushed its final reply.
func (b *pipeBuffer) close() {
	b.mu.Lock()
	b.closed = true
	b.cond.Broadcast()
	b.mu.Unlock()
}

// --- net.Conn implementation ------------------------------------------------

// Read reads bytes written by the peer, honouring the read deadline.
func (c *bufConn) Read(p []byte) (int, error) {
	c.dmu.Lock()
	dl := c.readDead
	c.dmu.Unlock()
	return c.rd.read(p, dl)
}

// Write sends bytes to the peer. It never blocks (the peer's read buffer is
// unbounded), so a pipelined burst of writes with no concurrent reader cannot
// deadlock. A past write deadline fails fast without writing.
func (c *bufConn) Write(p []byte) (int, error) {
	c.dmu.Lock()
	dl := c.writeDead
	c.dmu.Unlock()
	if !dl.IsZero() && !time.Now().Before(dl) {
		return 0, timeoutErr{}
	}
	return c.wr.write(p)
}

// Close closes BOTH directions of this end, waking any blocked Read/Write on this end
// and surfacing EOF to the peer's reader. Idempotent-safe: closing an already-closed
// pipeBuffer just re-broadcasts.
func (c *bufConn) Close() error {
	c.rd.close()
	c.wr.close()
	return nil
}

// LocalAddr returns a dummy in-process address.
func (c *bufConn) LocalAddr() net.Addr { return inprocAddr{} }

// RemoteAddr returns a dummy in-process address.
func (c *bufConn) RemoteAddr() net.Addr { return inprocAddr{} }

// SetDeadline sets both the read and write deadlines.
func (c *bufConn) SetDeadline(t time.Time) error {
	c.dmu.Lock()
	c.readDead = t
	c.writeDead = t
	c.dmu.Unlock()
	// Wake any currently-blocked Read so it re-evaluates the new deadline.
	c.rd.wake()
	return nil
}

// SetReadDeadline sets the deadline for future and pending Read calls. A blocked Read
// wakes when the deadline passes and returns a timeout error.
func (c *bufConn) SetReadDeadline(t time.Time) error {
	c.dmu.Lock()
	c.readDead = t
	c.dmu.Unlock()
	c.rd.wake()
	return nil
}

// SetWriteDeadline sets the deadline for future Write calls.
func (c *bufConn) SetWriteDeadline(t time.Time) error {
	c.dmu.Lock()
	c.writeDead = t
	c.dmu.Unlock()
	return nil
}

// wake broadcasts to any blocked reader so it re-checks its (possibly changed)
// deadline. Used when a deadline is set/cleared on an in-progress Read.
func (b *pipeBuffer) wake() {
	b.mu.Lock()
	b.cond.Broadcast()
	b.mu.Unlock()
}

// inprocAddr is the dummy address reported by both ends of an in-process conn.
type inprocAddr struct{}

func (inprocAddr) Network() string { return "inproc" }
func (inprocAddr) String() string  { return "redimos" }

// timeoutErr is a net.Error with Timeout()==true, so callers (go-redis) classify a
// deadline expiry as a timeout exactly as they would for a real socket.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// compile-time assertions.
var (
	_ net.Conn  = (*bufConn)(nil)
	_ net.Error = timeoutErr{}
)
