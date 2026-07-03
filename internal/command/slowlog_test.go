package command

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/aura-studio/redimos/v2/internal/metrics"
	"github.com/aura-studio/redimos/v2/internal/server"
)

// startObsServer boots a server whose Router is built with the given Storage
// (nil Store, so only connection + stub commands are registered) so tests can
// inject a pre-seeded slowlog / metrics instance and exercise INFO and SLOWLOG
// over the wire. It returns a connected client and reader.
func startObsServer(t *testing.T, st Storage) (net.Conn, *bufio.Reader) {
	t.Helper()
	r := NewRouterWithStorage(Config{}, st)
	s := server.New(server.Options{Addr: "127.0.0.1:0"}, r)
	signal := make(chan error, 1)
	go func() { _ = s.ListenServeAndSignal(signal) }()
	if err := <-signal; err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	return conn, bufio.NewReader(conn)
}

// seededSlowlog returns a SlowLog pre-populated with two deterministic entries:
// a GET (id 0, t=1000s, 5ms) then a SET (id 1, t=2000s, 12ms). Get() returns
// them newest-first, i.e. SET before GET.
func seededSlowlog(t *testing.T) *metrics.SlowLog {
	t.Helper()
	sl := metrics.NewSlowLog(metrics.SlowlogConfig{})
	sl.Record(metrics.SlowlogEntry{
		Time:     time.Unix(1000, 0),
		Duration: 5 * time.Millisecond,
		Command:  "GET",
		Args:     []string{"foo"},
	})
	sl.Record(metrics.SlowlogEntry{
		Time:     time.Unix(2000, 0),
		Duration: 12 * time.Millisecond,
		Command:  "SET",
		Args:     []string{"k", "v"},
	})
	return sl
}

// --- SLOWLOG GET (requirement 18.7) ------------------------------------------

func TestSlowlogGetReturnsEntriesNewestFirst(t *testing.T) {
	conn, r := startObsServer(t, Storage{Slowlog: seededSlowlog(t)})

	// Redis 3.2 shape: outer array of 4-element entries
	// [id, unix_seconds, duration_micros, [command, arg...]], newest-first.
	// SET (id 1, t=2000, 12000us) precedes GET (id 0, t=1000, 5000us).
	want := "[[:1 :2000 :12000 [$SET $k $v]] [:0 :1000 :5000 [$GET $foo]]]"
	if got := sendReadValue(t, r, conn, "SLOWLOG GET"); got != want {
		t.Errorf("SLOWLOG GET = %q, want %q", got, want)
	}
}

func TestSlowlogGetHonoursCount(t *testing.T) {
	conn, r := startObsServer(t, Storage{Slowlog: seededSlowlog(t)})

	// count=1 returns only the newest entry (SET).
	want := "[[:1 :2000 :12000 [$SET $k $v]]]"
	if got := sendReadValue(t, r, conn, "SLOWLOG GET 1"); got != want {
		t.Errorf("SLOWLOG GET 1 = %q, want %q", got, want)
	}
}

func TestSlowlogGetNegativeCountReturnsAll(t *testing.T) {
	conn, r := startObsServer(t, Storage{Slowlog: seededSlowlog(t)})

	want := "[[:1 :2000 :12000 [$SET $k $v]] [:0 :1000 :5000 [$GET $foo]]]"
	if got := sendReadValue(t, r, conn, "SLOWLOG GET -1"); got != want {
		t.Errorf("SLOWLOG GET -1 = %q, want %q", got, want)
	}
}

func TestSlowlogGetEmptyRing(t *testing.T) {
	conn, r := startObsServer(t, Storage{})
	if got, want := sendReadValue(t, r, conn, "SLOWLOG GET"), "[]"; got != want {
		t.Errorf("SLOWLOG GET (empty) = %q, want %q", got, want)
	}
}

func TestSlowlogGetNonIntegerCount(t *testing.T) {
	conn, r := startObsServer(t, Storage{Slowlog: seededSlowlog(t)})
	want := "-ERR value is not an integer or out of range"
	if got := sendReadValue(t, r, conn, "SLOWLOG GET abc"); got != want {
		t.Errorf("SLOWLOG GET abc = %q, want %q", got, want)
	}
}

// --- SLOWLOG LEN / RESET -----------------------------------------------------

func TestSlowlogLen(t *testing.T) {
	conn, r := startObsServer(t, Storage{Slowlog: seededSlowlog(t)})
	if got, want := sendReadValue(t, r, conn, "SLOWLOG LEN"), ":2"; got != want {
		t.Errorf("SLOWLOG LEN = %q, want %q", got, want)
	}
}

func TestSlowlogResetClearsRing(t *testing.T) {
	conn, r := startObsServer(t, Storage{Slowlog: seededSlowlog(t)})
	if got, want := sendReadValue(t, r, conn, "SLOWLOG RESET"), "+OK"; got != want {
		t.Fatalf("SLOWLOG RESET = %q, want %q", got, want)
	}
	if got, want := sendReadValue(t, r, conn, "SLOWLOG LEN"), ":0"; got != want {
		t.Errorf("SLOWLOG LEN after RESET = %q, want %q", got, want)
	}
}

func TestSlowlogUnknownSubcommandIsSyntaxError(t *testing.T) {
	conn, r := startObsServer(t, Storage{})
	want := "-ERR syntax error"
	if got := sendReadValue(t, r, conn, "SLOWLOG BOGUS"); got != want {
		t.Errorf("SLOWLOG BOGUS = %q, want %q", got, want)
	}
}
