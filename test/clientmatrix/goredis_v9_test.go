package clientmatrix

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aura-studio/redimos/internal/command"
	goredisv9 "github.com/redis/go-redis/v9"
)

// go-redis v9 is a protocol-3 (RESP3) capable client: on the first command over
// a fresh connection it issues HELLO to negotiate RESP3. redimos answers HELLO
// with "-ERR unknown command 'HELLO'" (requirement 2.1) precisely so that v9
// silently falls back to RESP2 and continues. These smoke tests assert that the
// fall-back succeeds end-to-end: PING, ECHO, and the AUTH flow all work through
// the real client after the failed HELLO.

func newV9Client(addr, password string) *goredisv9.Client {
	return goredisv9.NewClient(&goredisv9.Options{
		Addr:     addr,
		Password: password,
		// Keep the pool tiny and the timeouts short so a hung handshake fails
		// fast instead of stalling the suite.
		PoolSize:     2,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
}

// TestGoRedisV9_HandshakeFallsBackToRESP2 verifies requirement 2.1: after HELLO
// returns unknown-command, go-redis v9 must fall back to RESP2 and PING must
// succeed on the same connection.
func TestGoRedisV9_HandshakeFallsBackToRESP2(t *testing.T) {
	addr := startServer(t, command.Config{})
	client := newV9Client(addr, "")
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// PING drives connection initialization (HELLO -> unknown command -> RESP2
	// fallback) and then the actual PING. A successful PONG proves the handshake
	// recovered from the HELLO error.
	if got, err := client.Ping(ctx).Result(); err != nil {
		t.Fatalf("PING after HELLO fallback failed: %v", err)
	} else if got != "PONG" {
		t.Fatalf("PING = %q, want %q", got, "PONG")
	}
}

// TestGoRedisV9_EchoRoundTrips verifies requirement 2.4 through the real client
// once the RESP2 fallback has completed.
func TestGoRedisV9_EchoRoundTrips(t *testing.T) {
	addr := startServer(t, command.Config{})
	client := newV9Client(addr, "")
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if got, err := client.Echo(ctx, "hello-redimos").Result(); err != nil {
		t.Fatalf("ECHO failed: %v", err)
	} else if got != "hello-redimos" {
		t.Fatalf("ECHO = %q, want %q", got, "hello-redimos")
	}
}

// TestGoRedisV9_AuthFlowSucceeds verifies requirement 2.5/2.6: with a
// requirepass configured, a client that supplies the correct password (via the
// Options.Password field, which go-redis sends as AUTH during handshake)
// authenticates and can then run business commands.
func TestGoRedisV9_AuthFlowSucceeds(t *testing.T) {
	const password = "s3cret-v9"
	addr := startServer(t, command.Config{RequirePass: password})
	client := newV9Client(addr, password)
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// The handshake performs AUTH s3cret-v9 (requirement 2.5); a subsequent
	// business command must succeed on the authenticated connection.
	if got, err := client.Echo(ctx, "authed").Result(); err != nil {
		t.Fatalf("ECHO after AUTH failed: %v", err)
	} else if got != "authed" {
		t.Fatalf("ECHO = %q, want %q", got, "authed")
	}
}

// TestGoRedisV9_WrongPasswordRejected verifies requirement 2.5/2.6: a wrong
// password must not authenticate the connection, and business commands must be
// rejected. go-redis surfaces the server error during connection init.
func TestGoRedisV9_WrongPasswordRejected(t *testing.T) {
	addr := startServer(t, command.Config{RequirePass: "correct-horse"})
	client := newV9Client(addr, "wrong-password")
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Echo(ctx, "should-fail").Result()
	if err == nil {
		t.Fatalf("ECHO with wrong password unexpectedly succeeded")
	}
	// The error originates from the server's "-ERR invalid password" reply
	// raised during handshake AUTH; we only assert that the client observed a
	// failure (the exact text is asserted byte-for-byte by the connection unit
	// tests). A context deadline would indicate a hang rather than a rejection.
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ECHO with wrong password timed out instead of being rejected: %v", err)
	}
}

// TestGoRedisV9_PreAuthBusinessCommandRejected verifies requirement 2.6: an
// unauthenticated connection (no password supplied while requirepass is set)
// must be rejected with NOAUTH for business commands.
func TestGoRedisV9_PreAuthBusinessCommandRejected(t *testing.T) {
	addr := startServer(t, command.Config{RequirePass: "need-auth"})
	// No password supplied: the connection never authenticates.
	client := newV9Client(addr, "")
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Echo(ctx, "should-fail").Result()
	if err == nil {
		t.Fatalf("pre-auth ECHO unexpectedly succeeded")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pre-auth ECHO timed out instead of being rejected: %v", err)
	}
}
