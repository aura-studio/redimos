package command

import (
	"context"
	"testing"
	"time"

	"github.com/aura-studio/redimos/internal/server"
	"github.com/aura-studio/redimos/internal/storage"
)

func TestBreakerDispatcher_NilBreakerReturnsInner(t *testing.T) {
	inner := &capturingDispatcher{}
	if got := NewBreakerDispatcher(inner, nil); got != server.Dispatcher(inner) {
		t.Fatal("nil breaker must return the inner dispatcher unwrapped")
	}
}

func TestBreakerDispatcher_ClosedForwards(t *testing.T) {
	inner := &capturingDispatcher{}
	b := storage.NewCircuitBreaker(1, time.Hour) // closed until a throttle is recorded
	d := NewBreakerDispatcher(inner, b)

	d.Dispatch(context.Background(), nil, [][]byte{[]byte("GET"), []byte("k")})
	if inner.got == nil {
		t.Fatal("a closed breaker must forward the command to the inner dispatcher")
	}
}

func TestBreakerDispatcher_OpenLetsExemptCommandsThrough(t *testing.T) {
	inner := &capturingDispatcher{}
	b := storage.NewCircuitBreaker(1, time.Hour)
	b.Record(true) // open it (threshold 1)

	d := NewBreakerDispatcher(inner, b)

	// PING is exempt (connection command) — it must be forwarded even while the
	// breaker sheds backend load, so health checks keep working.
	d.Dispatch(context.Background(), nil, [][]byte{[]byte("PING")})
	if inner.got == nil {
		t.Fatal("an open breaker must still forward exempt (connection) commands")
	}
}
