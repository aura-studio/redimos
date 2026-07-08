package command

import (
	"context"
	"testing"
	"time"

	"github.com/aura-studio/redimos/internal/server"
)

// capturingDispatcher records the context it was dispatched with.
type capturingDispatcher struct{ got context.Context }

func (d *capturingDispatcher) Dispatch(ctx context.Context, _ *server.Conn, _ [][]byte) {
	d.got = ctx
}

func TestTimeoutDispatcher_DisabledReturnsInner(t *testing.T) {
	inner := &capturingDispatcher{}
	if got := NewTimeoutDispatcher(inner, 0); got != server.Dispatcher(inner) {
		t.Fatalf("timeout<=0 must return the inner dispatcher unwrapped")
	}
	if got := NewTimeoutDispatcher(inner, -time.Second); got != server.Dispatcher(inner) {
		t.Fatalf("negative timeout must return the inner dispatcher unwrapped")
	}
}

func TestTimeoutDispatcher_AppliesDeadline(t *testing.T) {
	inner := &capturingDispatcher{}
	d := NewTimeoutDispatcher(inner, 50*time.Millisecond)

	d.Dispatch(context.Background(), nil, [][]byte{[]byte("PING")})

	if inner.got == nil {
		t.Fatal("inner dispatcher was not called")
	}
	dl, ok := inner.got.Deadline()
	if !ok {
		t.Fatal("inner ctx has no deadline; timeout was not applied")
	}
	if until := time.Until(dl); until <= 0 || until > 50*time.Millisecond {
		t.Fatalf("deadline is %s away, want in (0, 50ms]", until)
	}
}
