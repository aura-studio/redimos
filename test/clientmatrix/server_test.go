package clientmatrix

import (
	"testing"

	"github.com/aura-studio/redimos/v2/internal/command"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// startServer boots an in-process redimos server on an ephemeral port using a
// command.Router (which registers the handshake / connection-management
// commands) built from cfg, and returns the "host:port" address a real client
// can dial. The server is torn down via t.Cleanup when the test finishes.
//
// This mirrors the pattern already used by the connection unit tests
// (internal/command/connection_test.go) but exposes the address so a full
// client library (go-redis) can connect over TCP, exercising the real
// handshake path rather than a hand-rolled RESP reader.
func startServer(t *testing.T, cfg command.Config) string {
	t.Helper()

	r := command.NewRouter(cfg)
	s := server.New(server.Options{Addr: "127.0.0.1:0"}, r)

	signal := make(chan error, 1)
	go func() { _ = s.ListenServeAndSignal(signal) }()
	if err := <-signal; err != nil {
		t.Fatalf("failed to start redimos server: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	return s.Addr().String()
}
