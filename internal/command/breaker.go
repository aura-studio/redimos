package command

import (
	"context"
	"strings"

	"github.com/aura-studio/redimos/internal/resp"
	"github.com/aura-studio/redimos/internal/server"
	"github.com/aura-studio/redimos/internal/storage"
)

// breakerExemptCommands are the connection/admin commands that never touch DynamoDB.
// The circuit breaker must NOT fail these fast during a backend throttle storm, so
// health checks (PING), auth (AUTH), DB selection, and introspection (INFO/SLOWLOG)
// keep working while backend load is shed.
var breakerExemptCommands = map[string]struct{}{
	"ping": {}, "echo": {}, "auth": {}, "select": {}, "hello": {}, "quit": {},
	"command": {}, "info": {}, "slowlog": {}, "config": {}, "client": {},
}

// BreakerDispatcher wraps a server.Dispatcher and, while the storage circuit breaker
// is open (a sustained DynamoDB throttle storm), replies the retryable
// backend-throttled error immediately for backend commands — skipping the handler
// and thus the doomed DynamoDB calls, which both protects the table and gives the
// client a fast signal to back off. Connection/admin commands pass through untouched.
type BreakerDispatcher struct {
	inner   server.Dispatcher
	breaker *storage.CircuitBreaker
}

var _ server.Dispatcher = (*BreakerDispatcher)(nil)

// NewBreakerDispatcher wraps inner. A nil breaker disables the gate: inner is
// returned unwrapped so there is no per-command cost.
func NewBreakerDispatcher(inner server.Dispatcher, breaker *storage.CircuitBreaker) server.Dispatcher {
	if breaker == nil {
		return inner
	}
	return &BreakerDispatcher{inner: inner, breaker: breaker}
}

// Dispatch fails fast for a backend command while the breaker is open; otherwise it
// forwards to the inner dispatcher.
func (d *BreakerDispatcher) Dispatch(ctx context.Context, c *server.Conn, args [][]byte) {
	if len(args) > 0 && d.breaker.Open() {
		name := strings.ToLower(string(args[0]))
		if _, exempt := breakerExemptCommands[name]; !exempt {
			resp.NewWriter(c.Redcon()).Error(resp.ErrBackendThrottled)
			return
		}
	}
	d.inner.Dispatch(ctx, c, args)
}
