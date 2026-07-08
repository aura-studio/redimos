package clientmatrix

import (
	"context"
	"errors"
	"testing"
	"time"

	goredisv8 "github.com/go-redis/redis/v8"

	"github.com/aura-studio/redimos/internal/command"
)

// go-redis v8 is a RESP2-only client: it never issues HELLO, so it connects to
// redimos directly over RESP2 without any protocol negotiation. These smoke
// tests confirm that the "legacy" (pre-HELLO) client path also works, which is
// the baseline redimos must always support alongside the v9 fallback path.

func newV8Client(addr, password string) *goredisv8.Client {
	return goredisv8.NewClient(&goredisv8.Options{
		Addr:         addr,
		Password:     password,
		PoolSize:     2,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
}

// TestGoRedisV8_PingWorks verifies the RESP2-only client connects and PINGs
// without any HELLO negotiation (requirement 2.2).
func TestGoRedisV8_PingWorks(t *testing.T) {
	addr := startServer(t, command.Config{})
	client := newV8Client(addr, "")
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if got, err := client.Ping(ctx).Result(); err != nil {
		t.Fatalf("PING failed: %v", err)
	} else if got != "PONG" {
		t.Fatalf("PING = %q, want %q", got, "PONG")
	}
}

// TestGoRedisV8_EchoRoundTrips verifies ECHO through the v8 client
// (requirement 2.4).
func TestGoRedisV8_EchoRoundTrips(t *testing.T) {
	addr := startServer(t, command.Config{})
	client := newV8Client(addr, "")
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if got, err := client.Echo(ctx, "hello-v8").Result(); err != nil {
		t.Fatalf("ECHO failed: %v", err)
	} else if got != "hello-v8" {
		t.Fatalf("ECHO = %q, want %q", got, "hello-v8")
	}
}

// TestGoRedisV8_AuthFlowSucceeds verifies requirement 2.5/2.6 for the RESP2-only
// client: the correct password authenticates and business commands then work.
func TestGoRedisV8_AuthFlowSucceeds(t *testing.T) {
	const password = "s3cret-v8"
	addr := startServer(t, command.Config{RequirePass: password})
	client := newV8Client(addr, password)
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if got, err := client.Echo(ctx, "authed-v8").Result(); err != nil {
		t.Fatalf("ECHO after AUTH failed: %v", err)
	} else if got != "authed-v8" {
		t.Fatalf("ECHO = %q, want %q", got, "authed-v8")
	}
}

// TestGoRedisV8_WrongPasswordRejected verifies requirement 2.5/2.6: a wrong
// password must not authenticate the connection.
func TestGoRedisV8_WrongPasswordRejected(t *testing.T) {
	addr := startServer(t, command.Config{RequirePass: "correct-v8"})
	client := newV8Client(addr, "wrong-v8")
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Echo(ctx, "should-fail").Result()
	if err == nil {
		t.Fatalf("ECHO with wrong password unexpectedly succeeded")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ECHO with wrong password timed out instead of being rejected: %v", err)
	}
}

// TestGoRedisV8_PreAuthBusinessCommandRejected verifies requirement 2.6: an
// unauthenticated connection is rejected with NOAUTH for business commands.
func TestGoRedisV8_PreAuthBusinessCommandRejected(t *testing.T) {
	addr := startServer(t, command.Config{RequirePass: "need-auth-v8"})
	client := newV8Client(addr, "")
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
