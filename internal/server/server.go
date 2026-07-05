// Package server hosts the redcon protocol shell: TCP listening, connection
// lifecycle management, and strictly serial per-connection pipelining.
//
// The shell owns no business semantics. It assembles a redcon server, manages
// the connection lifecycle (accept/close), attaches per-connection state, and
// forwards each parsed command to a Dispatcher seam. The command router
// (task 5.x) plugs into that seam without the shell depending on it, avoiding
// an import cycle: this package defines Conn and Dispatcher; the command
// package imports this package and implements Dispatcher.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"net"

	"github.com/tidwall/redcon"
)

// Dispatcher is the seam the command router plugs into. Implementations receive
// the per-connection state and the raw command arguments, and are responsible
// for writing a RESP2 reply via c.Redcon().
//
// The shell invokes Dispatch strictly one command at a time per connection, so
// implementations must not assume they may be called concurrently for the same
// Conn, and must not spawn concurrency that would reorder replies on a single
// connection.
type Dispatcher interface {
	Dispatch(ctx context.Context, c *Conn, args [][]byte)
}

// DispatchFunc adapts a plain function to the Dispatcher interface.
type DispatchFunc func(ctx context.Context, c *Conn, args [][]byte)

// Dispatch calls f(ctx, c, args).
func (f DispatchFunc) Dispatch(ctx context.Context, c *Conn, args [][]byte) {
	f(ctx, c, args)
}

// Options configures the server shell.
type Options struct {
	// Addr is the TCP listen address for the RESP2 endpoint, e.g. ":6379".
	Addr string

	// InstID identifies this proxy instance for SCAN cursor ownership. When
	// empty a random identifier is generated so every instance is distinct.
	InstID string

	// MaxCommandBytes rejects a single command whose raw wire size exceeds it,
	// bounding the work (and reply/allocation) one command can drive. 0 disables the
	// check. Note: redcon buffers the command before this callback, so this caps
	// processing rather than the transient read buffer.
	MaxCommandBytes int
}

// Server is the redcon protocol shell. It is safe to construct with New and
// then run with ListenAndServe or ListenServeAndSignal.
type Server struct {
	opts       Options
	dispatcher Dispatcher
	rc         *redcon.Server
}

// New assembles a redcon server that forwards commands to d. The dispatcher
// must be non-nil; command handling is entirely delegated to it.
func New(opts Options, d Dispatcher) *Server {
	if opts.InstID == "" {
		opts.InstID = newInstID()
	}
	s := &Server{opts: opts, dispatcher: d}
	s.rc = redcon.NewServer(opts.Addr, s.onCommand, s.onAccept, s.onClosed)
	return s
}

// onAccept runs when a new connection is established. It attaches fresh
// per-connection state and admits the connection. Returning true admits the
// connection; false would reject it.
func (s *Server) onAccept(rc redcon.Conn) bool {
	rc.SetContext(newConn(rc, s.opts.InstID))
	return true
}

// onCommand is the redcon command callback. redcon invokes it strictly one
// command at a time per connection, which is exactly the serial pipelining
// guarantee required by the design (algorithm 4 / requirement 2.10). The shell
// preserves that guarantee by handling the command inline and never spawning
// per-command goroutines.
func (s *Server) onCommand(rc redcon.Conn, cmd redcon.Command) {
	c, ok := rc.Context().(*Conn)
	if !ok || c == nil {
		// Defensive: a connection should always carry state set in onAccept,
		// but recover gracefully if it does not.
		c = newConn(rc, s.opts.InstID)
		rc.SetContext(c)
	}

	if len(cmd.Args) == 0 {
		// redcon does not deliver empty commands, but guard anyway so the
		// dispatcher can assume args[0] exists.
		return
	}

	if s.opts.MaxCommandBytes > 0 && len(cmd.Raw) > s.opts.MaxCommandBytes {
		rc.WriteError("ERR command exceeds the configured maximum size")
		return
	}

	s.dispatcher.Dispatch(context.Background(), c, cmd.Args)
}

// onClosed runs when a connection is torn down. The shell keeps no per-server
// connection registry, so cleanup is limited to observability.
func (s *Server) onClosed(rc redcon.Conn, err error) {
	if err != nil {
		log.Printf("redimos: connection %s closed: %v", rc.RemoteAddr(), err)
	}
}

// ListenAndServe binds the listen address and serves connections until Close.
func (s *Server) ListenAndServe() error {
	return s.rc.ListenAndServe()
}

// ListenServeAndSignal binds the listen address, sends nil (or the bind error)
// on signal once listening has started, then serves connections until Close.
// This is useful for tests that must wait until the listener is ready.
func (s *Server) ListenServeAndSignal(signal chan error) error {
	return s.rc.ListenServeAndSignal(signal)
}

// Addr returns the address the server is listening on, or nil before it binds.
func (s *Server) Addr() net.Addr {
	return s.rc.Addr()
}

// InstID returns this instance's identifier used for SCAN cursor ownership.
func (s *Server) InstID() string { return s.opts.InstID }

// Close stops the server and closes all active connections.
func (s *Server) Close() error {
	return s.rc.Close()
}

// newInstID returns a random hex instance identifier. It falls back to a fixed
// string only if the system randomness source is unavailable, which is
// effectively never on supported platforms.
func newInstID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "inst-0"
	}
	return "inst-" + hex.EncodeToString(b[:])
}
