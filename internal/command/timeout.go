package command

import (
	"context"
	"time"

	"github.com/aura-studio/redimos/internal/server"
)

// TimeoutDispatcher wraps a server.Dispatcher and applies a per-command deadline to
// the dispatch context. Because the storage layer threads that context down to the
// DynamoDB calls (via redimo's WithContext), the deadline bounds a command's backend
// work end-to-end: when it elapses, the in-flight DynamoDB call is cancelled, the
// handler observes the error, and it replies an error rather than hanging the
// connection indefinitely on a stalled backend.
//
// It does NOT forcibly preempt a handler that is busy off-backend (the proxy handlers
// are DynamoDB-bound, so this is not a concern), and it never writes the reply itself
// — the handler owns the single reply per command; the timeout only unblocks it.
type TimeoutDispatcher struct {
	inner   server.Dispatcher
	timeout time.Duration
}

var _ server.Dispatcher = (*TimeoutDispatcher)(nil)

// NewTimeoutDispatcher wraps inner so every dispatch runs under a context.WithTimeout
// of the given duration. A timeout <= 0 disables the feature: inner is returned
// unwrapped so there is no per-command allocation on the hot path.
func NewTimeoutDispatcher(inner server.Dispatcher, timeout time.Duration) server.Dispatcher {
	if timeout <= 0 {
		return inner
	}
	return &TimeoutDispatcher{inner: inner, timeout: timeout}
}

// Dispatch runs the inner dispatch under a fresh deadline derived from ctx.
func (d *TimeoutDispatcher) Dispatch(ctx context.Context, c *server.Conn, args [][]byte) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	d.inner.Dispatch(ctx, c, args)
}
