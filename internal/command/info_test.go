package command

import (
	"strings"
	"testing"
	"time"

	"github.com/aura-studio/redimos/internal/metrics"
)

// infoBody sends INFO (optionally with a section) and returns the decoded bulk
// string payload (with the leading "$" render prefix stripped).
func infoBody(t *testing.T, st Storage, cmd string) string {
	t.Helper()
	conn, r := startObsServer(t, st)
	got := sendReadValue(t, r, conn, cmd)
	if !strings.HasPrefix(got, "$") {
		t.Fatalf("%s = %q, want a bulk string", cmd, got)
	}
	return strings.TrimPrefix(got, "$")
}

// --- INFO (requirement 18.6) -------------------------------------------------

func TestInfoContainsVersionAndRole(t *testing.T) {
	body := infoBody(t, Storage{}, "INFO")

	// Requirement 18.6: INFO must report the Pika v3.2.2 version and master role.
	if !strings.Contains(body, "redis_version:3.2.2") {
		t.Errorf("INFO missing redis_version:3.2.2; body=%q", body)
	}
	if !strings.Contains(body, "role:master") {
		t.Errorf("INFO missing role:master; body=%q", body)
	}
}

func TestInfoIsRedisFormatWithSectionHeaders(t *testing.T) {
	body := infoBody(t, Storage{}, "INFO")

	// Standard Redis INFO format: "# Section" headers and CRLF-separated lines.
	for _, want := range []string{"# Server\r\n", "# Replication\r\n", "# Stats\r\n"} {
		if !strings.Contains(body, want) {
			t.Errorf("INFO missing section header %q; body=%q", want, body)
		}
	}
	if !strings.Contains(body, "\r\n") {
		t.Errorf("INFO not CRLF-separated; body=%q", body)
	}
}

func TestInfoSectionFilter(t *testing.T) {
	body := infoBody(t, Storage{}, "INFO replication")

	if !strings.Contains(body, "role:master") {
		t.Errorf("INFO replication missing role:master; body=%q", body)
	}
	// A filtered section must not include unrelated sections.
	if strings.Contains(body, "redis_version") {
		t.Errorf("INFO replication leaked Server section; body=%q", body)
	}
}

func TestInfoUnknownSectionIsEmpty(t *testing.T) {
	body := infoBody(t, Storage{}, "INFO nonesuch")
	if body != "" {
		t.Errorf("INFO nonesuch = %q, want empty bulk string", body)
	}
}

func TestInfoSummarisesCommandTotalsFromMetrics(t *testing.T) {
	m := metrics.New(metrics.Config{})
	m.ObserveCommand("get", time.Millisecond, false, "")
	m.ObserveCommand("set", time.Millisecond, false, "")
	m.ObserveCommand("get", time.Millisecond, true, "ERR")

	body := infoBody(t, Storage{Metrics: m}, "INFO")
	if !strings.Contains(body, "total_commands_processed:3") {
		t.Errorf("INFO missing total_commands_processed:3; body=%q", body)
	}
}

func TestInfoSummarisesSlowlogLen(t *testing.T) {
	body := infoBody(t, Storage{Slowlog: seededSlowlog(t)}, "INFO")
	if !strings.Contains(body, "slowlog_len:2") {
		t.Errorf("INFO missing slowlog_len:2; body=%q", body)
	}
}
