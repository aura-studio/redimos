package command

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/aura-studio/redimos/v2/internal/metrics"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// RequestLogLevel selects which commands the ObservedDispatcher emits a structured
// request log line for.
type RequestLogLevel int

const (
	// LogNone disables per-command request logging (the default; logging every
	// command is expensive and noisy).
	LogNone RequestLogLevel = iota
	// LogErrors logs only commands that returned an error.
	LogErrors
	// LogSlow logs errors plus commands slower than the slowlog threshold.
	LogSlow
	// LogAll logs every command (verbose; for debugging only).
	LogAll
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

	// reqLog, when non-nil, emits a structured (slog) line per command according to
	// logLevel. It is PII-safe: it records the command NAME, family, argument COUNT,
	// approximate payload SIZE, latency, and result class — never the actual key or
	// value bytes.
	reqLog   *slog.Logger
	logLevel RequestLogLevel
}

// NewObservedDispatcher wraps inner. A nil metrics or slowlog is tolerated: that
// facility is simply not recorded.
func NewObservedDispatcher(inner server.Dispatcher, m *metrics.Metrics, sl *metrics.SlowLog, threshold time.Duration) *ObservedDispatcher {
	return &ObservedDispatcher{inner: inner, metrics: m, slowlog: sl, threshold: threshold}
}

// WithRequestLog enables PII-safe structured request logging at the given level and
// returns the dispatcher (builder style). A nil logger or LogNone disables it.
func (o *ObservedDispatcher) WithRequestLog(logger *slog.Logger, level RequestLogLevel) *ObservedDispatcher {
	o.reqLog = logger
	o.logLevel = level
	return o
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
		o.metrics.ObserveCommand(name, dur, c.Errored(), c.ErrorClass())
	}
	if o.slowlog != nil && dur >= o.threshold {
		o.slowlog.Record(metrics.SlowlogEntry{
			Time:     start,
			Duration: dur,
			Command:  name,
			Args:     argsToStrings(args),
		})
	}

	o.logRequest(name, args, dur, c)
}

// logRequest emits a PII-safe structured line for the command when the configured
// level selects it. It logs the command NAME, argument count, approximate payload
// size, latency, result class and connection facts — never the key/value bytes.
func (o *ObservedDispatcher) logRequest(name string, args [][]byte, dur time.Duration, c *server.Conn) {
	if o.reqLog == nil || o.logLevel == LogNone {
		return
	}
	errored := c.Errored()
	slow := dur >= o.threshold
	switch o.logLevel {
	case LogErrors:
		if !errored {
			return
		}
	case LogSlow:
		if !errored && !slow {
			return
		}
	}

	result := "ok"
	if errored {
		result = "error"
	}
	o.reqLog.Info("command",
		slog.String("cmd", name),
		slog.Int("argc", len(args)),
		slog.Int("bytes", approxCommandBytes(args)),
		slog.Int64("latency_us", dur.Microseconds()),
		slog.String("result", result),
		slog.String("error_class", c.ErrorClass()),
		slog.Int("db", c.DB()),
		slog.String("remote", c.RemoteAddr()),
	)
}

// approxCommandBytes sums the argument byte lengths (the on-wire payload size), used
// as a size signal in request logs WITHOUT logging the content itself.
func approxCommandBytes(args [][]byte) int {
	total := 0
	for _, a := range args {
		total += len(a)
	}
	return total
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
