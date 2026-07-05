package server

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewGeneratesInstID(t *testing.T) {
	s := New(Options{Addr: "127.0.0.1:0"}, DispatchFunc(func(context.Context, *Conn, [][]byte) {}))
	if s.InstID() == "" {
		t.Fatalf("New should generate a non-empty instID when none is provided")
	}

	other := New(Options{Addr: "127.0.0.1:0"}, DispatchFunc(func(context.Context, *Conn, [][]byte) {}))
	if s.InstID() == other.InstID() {
		t.Errorf("distinct servers should have distinct instIDs: %q", s.InstID())
	}
}

func TestNewHonorsProvidedInstID(t *testing.T) {
	s := New(Options{Addr: "127.0.0.1:0", InstID: "inst-fixed"}, DispatchFunc(func(context.Context, *Conn, [][]byte) {}))
	if got := s.InstID(); got != "inst-fixed" {
		t.Errorf("InstID = %q, want %q", got, "inst-fixed")
	}
}

// startTestServer boots a server on an ephemeral port and returns its address.
func startTestServer(t *testing.T, d Dispatcher) *Server {
	t.Helper()
	s := New(Options{Addr: "127.0.0.1:0"}, d)
	signal := make(chan error, 1)
	go func() { _ = s.ListenServeAndSignal(signal) }()
	if err := <-signal; err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestServerRejectsOversizedCommand verifies the MaxCommandBytes guard: a command whose
// raw wire size exceeds the cap gets an error reply and is never dispatched, while a small
// command passes through normally.
func TestServerRejectsOversizedCommand(t *testing.T) {
	d := DispatchFunc(func(_ context.Context, c *Conn, _ [][]byte) {
		c.Redcon().WriteString("OK") // only reached when a command IS dispatched
	})

	s := New(Options{Addr: "127.0.0.1:0", MaxCommandBytes: 40}, d)
	signal := make(chan error, 1)
	go func() { _ = s.ListenServeAndSignal(signal) }()
	if err := <-signal; err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	r := bufio.NewReader(conn)

	// A small command dispatches normally.
	if _, err := conn.Write([]byte("PING\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if line, _ := r.ReadString('\n'); strings.TrimRight(line, "\r\n") != "+OK" {
		t.Fatalf("small-command reply = %q, want +OK", line)
	}

	// A command whose raw size exceeds the cap is rejected (error reply, not dispatched).
	if _, err := conn.Write([]byte("ECHO " + strings.Repeat("x", 200) + "\r\n")); err != nil {
		t.Fatalf("write big: %v", err)
	}
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read big reply: %v", err)
	}
	if len(line) == 0 || line[0] != '-' {
		t.Fatalf("oversized-command reply = %q, want an error", line)
	}
}

// TestServerSerialPipelining verifies that pipelined commands on a single
// connection are dispatched strictly in order and replies come back in order.
func TestServerSerialPipelining(t *testing.T) {
	var mu sync.Mutex
	var order []string
	var seenInstID string

	d := DispatchFunc(func(_ context.Context, c *Conn, args [][]byte) {
		mu.Lock()
		order = append(order, strings.ToUpper(string(args[0])))
		seenInstID = c.InstID()
		mu.Unlock()
		// Echo a distinct simple-string reply per command to confirm ordering.
		c.Redcon().WriteString(strings.ToUpper(string(args[0])))
	})

	s := startTestServer(t, d)

	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))

	// Send three inline commands in one write (a pipeline).
	if _, err := conn.Write([]byte("PING\r\nECHO hi\r\nSELECT 0\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Expect three simple-string replies in order.
	r := bufio.NewReader(conn)
	want := []string{"+PING", "+ECHO", "+SELECT"}
	for i, w := range want {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read reply %d: %v", i, err)
		}
		if strings.TrimRight(line, "\r\n") != w {
			t.Errorf("reply %d = %q, want %q", i, strings.TrimRight(line, "\r\n"), w)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	wantOrder := []string{"PING", "ECHO", "SELECT"}
	if len(order) != len(wantOrder) {
		t.Fatalf("dispatched %d commands, want %d (%v)", len(order), len(wantOrder), order)
	}
	for i := range wantOrder {
		if order[i] != wantOrder[i] {
			t.Errorf("dispatch order[%d] = %q, want %q", i, order[i], wantOrder[i])
		}
	}
	if seenInstID != s.InstID() {
		t.Errorf("dispatched Conn.InstID = %q, want server instID %q", seenInstID, s.InstID())
	}
}

// TestServerConnStatePersists verifies that per-connection state set by the
// dispatcher survives across commands on the same connection (redcon context).
func TestServerConnStatePersists(t *testing.T) {
	var mu sync.Mutex
	var authedSecondCmd bool
	cmdCount := 0

	d := DispatchFunc(func(_ context.Context, c *Conn, args [][]byte) {
		mu.Lock()
		cmdCount++
		n := cmdCount
		mu.Unlock()

		switch n {
		case 1:
			c.SetAuthed(true) // first command mutates connection state
		case 2:
			mu.Lock()
			authedSecondCmd = c.Authed() // second command observes it
			mu.Unlock()
		}
		c.Redcon().WriteString("OK")
	})

	s := startTestServer(t, d)

	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))

	if _, err := conn.Write([]byte("AUTH x\r\nPING\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}

	r := bufio.NewReader(conn)
	for i := 0; i < 2; i++ {
		if _, err := r.ReadString('\n'); err != nil {
			t.Fatalf("read reply %d: %v", i, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if !authedSecondCmd {
		t.Errorf("connection state did not persist across commands: authed=false on second command")
	}
}
