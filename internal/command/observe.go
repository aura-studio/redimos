package command

import (
	"context"
	"strings"
	"time"

	"github.com/aura-studio/redimos/v2/internal/metrics"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// ObservedDispatcher wraps a server.Dispatcher and records, per command, the
// Prometheus metrics (count + latency, labelled by command name and whether the
// reply was an error) and a slowlog entry when the command exceeds the threshold.
//
// Without this decorator the *metrics.Metrics and *metrics.SlowLog facilities are
// constructed and exposed via /metrics, INFO and SLOWLOG, but NOTHING on the
// request path ever feeds them — so total_commands_processed, the latency
// histograms and the slowlog all stay empty (dead telemetry). This decorator is
// the missing recording site.
type ObservedDispatcher struct {
	inner     server.Dispatcher
	metrics   *metrics.Metrics
	slowlog   *metrics.SlowLog
	threshold time.Duration
}

// NewObservedDispatcher wraps inner. A nil metrics or slowlog is tolerated: that
// facility is simply not recorded.
func NewObservedDispatcher(inner server.Dispatcher, m *metrics.Metrics, sl *metrics.SlowLog, threshold time.Duration) *ObservedDispatcher {
	return &ObservedDispatcher{inner: inner, metrics: m, slowlog: sl, threshold: threshold}
}

var _ server.Dispatcher = (*ObservedDispatcher)(nil)

// Dispatch times the inner dispatch, then records metrics and (if slow) a slowlog
// entry. errored is read back from the connection, which the observing redcon
// wrapper flips when an error reply is written.
func (o *ObservedDispatcher) Dispatch(ctx context.Context, c *server.Conn, args [][]byte) {
	if len(args) == 0 {
		o.inner.Dispatch(ctx, c, args)
		return
	}

	start := time.Now()
	c.ResetErrored()
	o.inner.Dispatch(ctx, c, args)
	dur := time.Since(start)

	name := strings.ToLower(string(args[0]))
	if o.metrics != nil {
		o.metrics.ObserveCommand(name, dur, c.Errored())
	}
	if o.slowlog != nil && dur >= o.threshold {
		o.slowlog.Record(metrics.SlowlogEntry{
			Time:     start,
			Duration: dur,
			Command:  name,
			Args:     argsToStrings(args),
		})
	}
}

// argsToStrings converts command arguments for a slowlog entry, bounding the count
// so a huge variadic command does not convert thousands of args (SlowLog.Record's
// capArgs bounds the retained entry further).
func argsToStrings(args [][]byte) []string {
	const maxArgs = 32
	n := len(args)
	if n > maxArgs {
		n = maxArgs
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = string(args[i])
	}
	return out
}
