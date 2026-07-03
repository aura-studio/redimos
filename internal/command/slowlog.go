package command

import (
	"context"

	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// slowlog.go implements the SLOWLOG command family as a read-only view over the
// proxy's in-memory slowlog ring buffer (requirement 18.7, design.md 可观测性 /
// 慢日志). The buffer itself (internal/metrics.SlowLog) is populated elsewhere
// (the dispatch path records slow commands); SLOWLOG never mutates data, it only
// serves the ring — except RESET, which clears it, matching Redis.
//
// # Registration & storage
//
// SLOWLOG is registered on the connection path (registerStubs in stub.go), so it
// works even on a connection-only router with no storage backend. The ring is
// always present via Router.ensureObservability: a caller may inject its own
// SlowLog through Storage.Slowlog (the one wired into metrics/main), otherwise a
// fresh default buffer is installed at construction.

// defaultSlowlogCount is the number of entries SLOWLOG GET returns when no count
// argument is supplied, matching Redis' default of 10.
const defaultSlowlogCount = 10

// handleSlowlog dispatches the SLOWLOG subcommands:
//
//   - SLOWLOG GET [count] : reply the ring entries newest-first as an array of
//     4-element entries (Redis 3.2 shape). count defaults to 10 when omitted; a
//     negative count returns every entry. Requirement 18.7.
//   - SLOWLOG LEN         : reply the integer number of entries currently held.
//   - SLOWLOG RESET       : clear the ring and reply "+OK".
//
// An unknown subcommand is a syntax error, matching Redis' minimal handling.
func (r *Router) handleSlowlog(_ context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	switch toLower(string(args[1])) {
	case "get":
		r.slowlogGet(c, args)
	case "len":
		w.Int(int64(r.Storage.Slowlog.Len()))
	case "reset":
		r.Storage.Slowlog.Reset()
		w.SimpleString("OK")
	default:
		w.Error(resp.ErrSyntax)
	}
}

// slowlogGet serves SLOWLOG GET [count]. It parses the optional count (default
// 10; negative means "all"), reads the ring newest-first, and writes the Redis
// 3.2 SLOWLOG GET wire shape: an outer array of entries, each a 4-element array
// [id, timestamp_seconds, duration_micros, [command, arg, ...]]. A non-integer
// count is rejected with the standard integer-parse error.
func (r *Router) slowlogGet(c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())

	count := defaultSlowlogCount
	if len(args) >= 3 {
		n, err := ParseInt(args[2])
		if err != nil {
			w.Error(resp.ErrNotInteger)
			return
		}
		count = int(n)
	}

	entries := r.Storage.Slowlog.Get(count)

	// Build the nested array by hand: resp.Writer exposes only flat helpers, so
	// the multi-level SLOWLOG shape is assembled with the Append* encoders and
	// written in one raw flush for byte-for-byte control.
	buf := resp.AppendArrayHeader(nil, len(entries))
	for _, e := range entries {
		buf = resp.AppendArrayHeader(buf, 4)
		buf = resp.AppendInt(buf, e.ID)
		buf = resp.AppendInt(buf, e.Time.Unix())
		buf = resp.AppendInt(buf, e.Duration.Microseconds())

		// The 4th element is the command with its arguments as a bulk-string
		// array: [command, arg1, arg2, ...].
		buf = resp.AppendArrayHeader(buf, 1+len(e.Args))
		buf = resp.AppendBulkString(buf, []byte(e.Command))
		for _, a := range e.Args {
			buf = resp.AppendBulkString(buf, []byte(a))
		}
	}
	c.Redcon().WriteRaw(buf)
}
