package command

import (
	"context"
	"strconv"
	"strings"

	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// info.go implements the INFO observability command (requirement 18.6,
// design.md "客户端探测兜底 stub" / 可观测性). INFO is a minimal, read-only
// implementation: it MUST report `redis_version:3.2.2` and `role:master` (so
// clients and tooling that probe the server version / replication role behave
// as they would against Pika v3.2.2) plus a small proxy-metrics summary.
//
// # Registration & storage
//
// INFO is registered on the connection path (registerStubs in stub.go) so it
// answers even on a connection-only router with no storage backend. The
// command-totals line is sourced from the injected *metrics.Metrics when
// present; when absent the mandated version/role fields are still returned and
// the totals are simply reported as 0. The slowlog length comes from the
// always-present Storage.Slowlog (see Router.ensureObservability).

// redisVersion is the Redis version redimos reports to clients. It is pinned to
// the Pika v3.2.2 oracle the proxy targets (requirement 18.6) so version-gated
// client behaviour matches the migration source.
const redisVersion = "3.2.2"

// handleInfo implements INFO [section]. It replies a single bulk string in the
// standard Redis INFO text format: CRLF-separated "key:value" lines grouped
// under "# Section" headers. Requirement 18.6.
//
// Section filtering is minimal but Redis-compatible: with no argument (or a
// "default"/"all"/"everything" argument) every section is returned; a specific,
// known section name returns just that section; an unknown section returns an
// empty bulk string, as Redis does.
func (r *Router) handleInfo(_ context.Context, c *server.Conn, args [][]byte) {
	section := ""
	if len(args) >= 2 {
		section = toLower(string(args[1]))
	}
	body := r.buildInfo(section)
	resp.NewWriter(c.Redcon()).BulkString([]byte(body))
}

// buildInfo renders the INFO payload, honouring the requested section (empty /
// "default" / "all" / "everything" mean every section). An unknown section
// yields an empty string, matching Redis.
func (r *Router) buildInfo(section string) string {
	all := section == "" || section == "default" || section == "all" || section == "everything"

	var b strings.Builder
	wrote := false

	writeSection := func(name string, lines func(*strings.Builder)) {
		if !all && section != toLower(name) {
			return
		}
		if wrote {
			b.WriteString("\r\n")
		}
		b.WriteString("# ")
		b.WriteString(name)
		b.WriteString("\r\n")
		lines(&b)
		wrote = true
	}

	writeSection("Server", func(b *strings.Builder) {
		writeInfoField(b, "redis_version", redisVersion)
		writeInfoField(b, "redis_mode", "standalone")
	})

	writeSection("Replication", func(b *strings.Builder) {
		writeInfoField(b, "role", "master")
		writeInfoField(b, "connected_slaves", "0")
	})

	writeSection("Stats", func(b *strings.Builder) {
		writeInfoField(b, "total_commands_processed", strconv.FormatUint(r.totalCommands(), 10))
	})

	writeSection("Slowlog", func(b *strings.Builder) {
		writeInfoField(b, "slowlog_len", strconv.Itoa(r.Storage.Slowlog.Len()))
	})

	return b.String()
}

// writeInfoField appends one "key:value\r\n" line in Redis INFO format.
func writeInfoField(b *strings.Builder, key, value string) {
	b.WriteString(key)
	b.WriteByte(':')
	b.WriteString(value)
	b.WriteString("\r\n")
}

// totalCommands returns the number of commands the proxy has processed, summed
// across the per-command QPS counter exported by the injected metrics registry.
// When no Metrics is wired it returns 0 (the summary degrades gracefully rather
// than failing INFO). The value is gathered lazily here so metrics stays free of
// a read-side accessor and INFO stays optional.
func (r *Router) totalCommands() uint64 {
	m := r.Storage.Metrics
	if m == nil {
		return 0
	}
	families, err := m.Registry().Gather()
	if err != nil {
		return 0
	}
	var total uint64
	for _, mf := range families {
		if mf.GetName() != "redimos_commands_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if ctr := metric.GetCounter(); ctr != nil {
				total += uint64(ctr.GetValue())
			}
		}
	}
	return total
}
