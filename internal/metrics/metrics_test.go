package metrics

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// gatherMetric returns the *dto.MetricFamily with the given fully qualified name
// from the registry, or nil when absent.
func gatherMetric(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() == name {
			return mf
		}
	}
	return nil
}

// counterFor returns the counter value for the metric family with a single
// command label equal to cmd, or -1 when no matching sample exists.
func counterFor(mf *dto.MetricFamily, cmd string) float64 {
	if mf == nil {
		return -1
	}
	for _, m := range mf.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == commandLabel && l.GetValue() == cmd {
				return m.GetCounter().GetValue()
			}
		}
	}
	return -1
}

// histogramFor returns (sampleCount, sampleSum) for the histogram family with a
// single command label equal to cmd, or (0,0) when absent.
func histogramFor(mf *dto.MetricFamily, cmd string) (uint64, float64) {
	if mf == nil {
		return 0, 0
	}
	for _, m := range mf.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == commandLabel && l.GetValue() == cmd {
				h := m.GetHistogram()
				return h.GetSampleCount(), h.GetSampleSum()
			}
		}
	}
	return 0, 0
}

func gaugeValue(mf *dto.MetricFamily) float64 {
	if mf == nil || len(mf.GetMetric()) == 0 {
		return -1
	}
	return mf.GetMetric()[0].GetGauge().GetValue()
}

func TestObserveCommandIncrementsCountersAndHistogram(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(Config{Registry: reg})

	m.ObserveCommand("get", 2*time.Millisecond, false)
	m.ObserveCommand("get", 4*time.Millisecond, false)
	m.ObserveCommand("get", 6*time.Millisecond, true) // one error

	if got := counterFor(gatherMetric(t, reg, "redimos_commands_total"), "get"); got != 3 {
		t.Errorf("commands_total{command=get} = %v, want 3", got)
	}
	if got := counterFor(gatherMetric(t, reg, "redimos_command_errors_total"), "get"); got != 1 {
		t.Errorf("command_errors_total{command=get} = %v, want 1", got)
	}
	count, sum := histogramFor(gatherMetric(t, reg, "redimos_command_duration_seconds"), "get")
	if count != 3 {
		t.Errorf("histogram sample count = %d, want 3", count)
	}
	wantSum := (2 + 4 + 6) * time.Millisecond
	if sum <= 0 || sum != wantSum.Seconds() {
		t.Errorf("histogram sample sum = %v, want %v", sum, wantSum.Seconds())
	}
}

func TestObserveCommandSeparatesLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(Config{Registry: reg})

	m.ObserveCommand("get", time.Millisecond, false)
	m.ObserveCommand("set", time.Millisecond, true)

	total := gatherMetric(t, reg, "redimos_commands_total")
	if got := counterFor(total, "get"); got != 1 {
		t.Errorf("commands_total{command=get} = %v, want 1", got)
	}
	if got := counterFor(total, "set"); got != 1 {
		t.Errorf("commands_total{command=set} = %v, want 1", got)
	}
	errs := gatherMetric(t, reg, "redimos_command_errors_total")
	if got := counterFor(errs, "set"); got != 1 {
		t.Errorf("command_errors_total{command=set} = %v, want 1", got)
	}
	// get had no error, so it must not appear in the error family.
	if got := counterFor(errs, "get"); got != -1 {
		t.Errorf("command_errors_total{command=get} = %v, want absent", got)
	}
}

func TestInterceptionsGaugeFromSetter(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(Config{Registry: reg})

	if got := gaugeValue(gatherMetric(t, reg, "redimos_large_key_interceptions_total")); got != 0 {
		t.Errorf("interceptions gauge initial = %v, want 0", got)
	}
	m.SetInterceptions(7)
	if got := gaugeValue(gatherMetric(t, reg, "redimos_large_key_interceptions_total")); got != 7 {
		t.Errorf("interceptions gauge = %v, want 7", got)
	}
}

func TestInterceptionsGaugeFromFunc(t *testing.T) {
	reg := prometheus.NewRegistry()
	var live uint64
	m := New(Config{Registry: reg, InterceptionsFunc: func() uint64 { return live }})

	live = 42
	if got := gaugeValue(gatherMetric(t, reg, "redimos_large_key_interceptions_total")); got != 42 {
		t.Errorf("interceptions gauge = %v, want 42", got)
	}
	// SetInterceptions must not override the injected authoritative source.
	m.SetInterceptions(100)
	if got := gaugeValue(gatherMetric(t, reg, "redimos_large_key_interceptions_total")); got != 42 {
		t.Errorf("interceptions gauge after SetInterceptions = %v, want 42", got)
	}
}

func TestHandlerServesRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(Config{Registry: reg})
	if m.Handler() == nil {
		t.Fatal("Handler() returned nil")
	}
	if m.Registry() != reg {
		t.Fatal("Registry() did not return the injected registry")
	}
}

// --- slowlog ---

func newTestSlowLog(cap int, threshold time.Duration) *SlowLog {
	base := time.Unix(1_700_000_000, 0)
	var n int64
	return NewSlowLog(SlowlogConfig{
		Capacity:  cap,
		Threshold: threshold,
		Now: func() time.Time {
			n++
			return base.Add(time.Duration(n) * time.Second)
		},
	})
}

func TestSlowlogRecordsAboveThresholdDropsBelow(t *testing.T) {
	s := newTestSlowLog(4, 5*time.Millisecond)

	if s.Record(SlowlogEntry{Command: "get", Duration: 4 * time.Millisecond}) {
		t.Error("sub-threshold command was recorded, want dropped")
	}
	if !s.Record(SlowlogEntry{Command: "hgetall", Duration: 5 * time.Millisecond}) {
		t.Error("at-threshold command was dropped, want recorded")
	}
	if !s.Record(SlowlogEntry{Command: "smembers", Duration: 20 * time.Millisecond}) {
		t.Error("above-threshold command was dropped, want recorded")
	}
	if got := s.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
}

func TestSlowlogGetNewestFirstAndRespectsN(t *testing.T) {
	s := newTestSlowLog(8, 0)
	for _, cmd := range []string{"a", "b", "c"} {
		s.Record(SlowlogEntry{Command: cmd, Duration: time.Millisecond})
	}

	all := s.Get(-1)
	if len(all) != 3 {
		t.Fatalf("Get(-1) len = %d, want 3", len(all))
	}
	// newest-first: c, b, a
	if all[0].Command != "c" || all[1].Command != "b" || all[2].Command != "a" {
		t.Errorf("Get order = %v, want [c b a]", []string{all[0].Command, all[1].Command, all[2].Command})
	}
	// IDs are assigned in record order, so newest has the highest id.
	if all[0].ID != 2 || all[2].ID != 0 {
		t.Errorf("IDs = %d..%d, want newest 2, oldest 0", all[0].ID, all[2].ID)
	}

	limited := s.Get(2)
	if len(limited) != 2 {
		t.Fatalf("Get(2) len = %d, want 2", len(limited))
	}
	if limited[0].Command != "c" || limited[1].Command != "b" {
		t.Errorf("Get(2) = %v, want [c b]", []string{limited[0].Command, limited[1].Command})
	}

	if got := s.Get(0); len(got) != 0 {
		t.Errorf("Get(0) len = %d, want 0", len(got))
	}
}

func TestSlowlogEvictsOldestWhenFull(t *testing.T) {
	s := newTestSlowLog(3, 0)
	for _, cmd := range []string{"a", "b", "c", "d", "e"} {
		s.Record(SlowlogEntry{Command: cmd, Duration: time.Millisecond})
	}

	if got := s.Len(); got != 3 {
		t.Fatalf("Len = %d, want 3 (capacity)", got)
	}
	got := s.Get(-1)
	// Only the last 3 survive, newest-first: e, d, c.
	if len(got) != 3 || got[0].Command != "e" || got[1].Command != "d" || got[2].Command != "c" {
		t.Errorf("Get = %v, want [e d c]", commands(got))
	}
	// The surviving entries keep their original record IDs (c=2, d=3, e=4).
	if got[0].ID != 4 || got[2].ID != 2 {
		t.Errorf("IDs = newest %d oldest %d, want 4 and 2", got[0].ID, got[2].ID)
	}
}

func TestSlowlogAssignsTimeAndCopiesArgs(t *testing.T) {
	s := newTestSlowLog(4, 0)
	args := []string{"key", "val"}
	s.Record(SlowlogEntry{Command: "set", Duration: time.Millisecond, Args: args})
	// Mutate caller's slice after recording; stored entry must be unaffected.
	args[0] = "MUTATED"

	got := s.Get(1)
	if len(got) != 1 {
		t.Fatalf("Get(1) len = %d, want 1", len(got))
	}
	if got[0].Args[0] != "key" {
		t.Errorf("stored arg = %q, want %q (defensive copy failed)", got[0].Args[0], "key")
	}
	if got[0].Time.IsZero() {
		t.Error("Time was not stamped by the ring")
	}
}

func TestSlowlogReset(t *testing.T) {
	s := newTestSlowLog(4, 0)
	s.Record(SlowlogEntry{Command: "a", Duration: time.Millisecond})
	s.Record(SlowlogEntry{Command: "b", Duration: time.Millisecond})
	s.Reset()
	if got := s.Len(); got != 0 {
		t.Fatalf("Len after Reset = %d, want 0", got)
	}
	// IDs restart from 0 after reset.
	s.Record(SlowlogEntry{Command: "c", Duration: time.Millisecond})
	got := s.Get(1)
	if len(got) != 1 || got[0].ID != 0 {
		t.Errorf("post-reset id = %v, want 0", got)
	}
}

func TestSlowlogCapsArgCount(t *testing.T) {
	s := newTestSlowLog(4, 0)
	args := make([]string, MaxSlowlogArgs+10) // 42 args, over the 32 cap
	for i := range args {
		args[i] = fmt.Sprintf("a%d", i)
	}
	s.Record(SlowlogEntry{Command: "mset", Duration: time.Millisecond, Args: args})

	got := s.Get(1)
	if len(got) != 1 {
		t.Fatalf("Get(1) len = %d, want 1", len(got))
	}
	stored := got[0].Args
	if len(stored) != MaxSlowlogArgs {
		t.Fatalf("stored arg count = %d, want %d", len(stored), MaxSlowlogArgs)
	}
	// First MaxSlowlogArgs-1 args are the originals; the last summarises the rest.
	if stored[0] != "a0" || stored[MaxSlowlogArgs-2] != fmt.Sprintf("a%d", MaxSlowlogArgs-2) {
		t.Errorf("leading args not preserved: %v", stored[:MaxSlowlogArgs-1])
	}
	dropped := len(args) - (MaxSlowlogArgs - 1)
	wantTrailer := fmt.Sprintf("... (%d more arguments)", dropped)
	if stored[MaxSlowlogArgs-1] != wantTrailer {
		t.Errorf("trailer = %q, want %q", stored[MaxSlowlogArgs-1], wantTrailer)
	}
}

func TestSlowlogCapsArgBytes(t *testing.T) {
	s := newTestSlowLog(4, 0)
	long := strings.Repeat("x", MaxSlowlogArgBytes+42)
	s.Record(SlowlogEntry{Command: "set", Duration: time.Millisecond, Args: []string{long}})

	got := s.Get(1)
	if len(got) != 1 || len(got[0].Args) != 1 {
		t.Fatalf("unexpected stored entry: %+v", got)
	}
	stored := got[0].Args[0]
	wantPrefix := strings.Repeat("x", MaxSlowlogArgBytes)
	wantSuffix := "... (42 more bytes)"
	if stored != wantPrefix+wantSuffix {
		t.Errorf("stored arg = %q, want prefix of %d bytes + %q", stored, MaxSlowlogArgBytes, wantSuffix)
	}
}

func TestSlowlogArgsExactlyAtCapUnchanged(t *testing.T) {
	s := newTestSlowLog(4, 0)
	args := make([]string, MaxSlowlogArgs)
	for i := range args {
		args[i] = strings.Repeat("y", MaxSlowlogArgBytes) // exactly at the byte cap
	}
	s.Record(SlowlogEntry{Command: "cmd", Duration: time.Millisecond, Args: args})

	got := s.Get(1)
	if len(got[0].Args) != MaxSlowlogArgs {
		t.Fatalf("arg count = %d, want %d (no trailer at exact cap)", len(got[0].Args), MaxSlowlogArgs)
	}
	for i, a := range got[0].Args {
		if len(a) != MaxSlowlogArgBytes {
			t.Errorf("arg %d len = %d, want %d (unchanged at exact cap)", i, len(a), MaxSlowlogArgBytes)
		}
	}
}

func TestSlowlogConcurrentRecordIsRaceFree(t *testing.T) {
	// Small capacity forces frequent wraparound while many goroutines contend,
	// exercising the ring under -race. Correctness here is "no data race and a
	// consistent, capacity-bounded final state".
	s := NewSlowLog(SlowlogConfig{Capacity: 16, Threshold: 0})

	const goroutines = 16
	const perGoroutine = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				s.Record(SlowlogEntry{
					Command:  fmt.Sprintf("cmd-%d", g),
					Duration: time.Millisecond,
					Args:     []string{fmt.Sprintf("arg-%d-%d", g, i)},
				})
				// Interleave reads to contend with writers.
				_ = s.Get(4)
			}
		}(g)
	}
	wg.Wait()

	if got := s.Len(); got != 16 {
		t.Fatalf("Len after concurrent load = %d, want 16 (capacity)", got)
	}
	if got := len(s.Get(-1)); got != 16 {
		t.Fatalf("Get(-1) len = %d, want 16", got)
	}
}

func commands(entries []SlowlogEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Command
	}
	return out
}
