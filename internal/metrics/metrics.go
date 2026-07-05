// Package metrics exposes Prometheus metrics (per-command QPS, latency
// histograms, error counts, large-key interceptions) and an in-memory slowlog
// ring buffer.
//
// # Decoupling
//
// This package sits below the command layer in the dependency graph: the
// command layer imports metrics (to call ObserveCommand and Slowlog.Record),
// but metrics MUST NOT import internal/command. To surface the large-key
// interception count (requirement 18.5) without importing internal/guard
// either, the count is pulled through an injectable source:
//
//   - If Config.InterceptionsFunc is non-nil it is used verbatim. main wires it
//     to guard.Interceptions so the gauge always reflects the live counter.
//   - Otherwise the count is backed by an internal atomic that callers advance
//     via SetInterceptions.
//
// Either way the value is exported as the Prometheus gauge
// redimos_large_key_interceptions_total, keeping metrics free of any
// compile-time dependency on guard or command.
//
// # Registry injection
//
// All collectors register on a caller-supplied *prometheus.Registry. Production
// code passes a shared registry (or lets New create one via a nil Config
// registry); tests pass a fresh registry per test so metric state never leaks
// between cases.
package metrics

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// namespace prefixes every metric name exported by the proxy.
const namespace = "redimos"

// commandLabel is the label carrying the (lowercased) Redis command name on the
// per-command QPS, latency, and error collectors.
const commandLabel = "command"

// DefaultLatencyBuckets are the histogram buckets (in seconds) for command
// latency. They span sub-millisecond in-memory replies through multi-second
// backend stalls so the p99 budget in the design is observable.
var DefaultLatencyBuckets = []float64{
	0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
}

// Config parameterizes New. The zero value is valid and yields a Metrics with a
// fresh registry, default latency buckets, and a settable interception gauge.
type Config struct {
	// Registry is the collector registry to register on. nil creates a fresh
	// *prometheus.Registry (typical in tests and simple assemblies).
	Registry *prometheus.Registry

	// LatencyBuckets overrides the command-latency histogram buckets (seconds).
	// nil uses DefaultLatencyBuckets.
	LatencyBuckets []float64

	// InterceptionsFunc, when non-nil, is the live source for the large-key
	// interception gauge. main wires it to guard.Interceptions. When nil the
	// gauge reads an internal atomic advanced by SetInterceptions.
	InterceptionsFunc func() uint64

	// The following optional funcs are read lazily at scrape time to surface the
	// background reclaimer's health — queue backpressure/drops, orphan-sweep
	// effectiveness — and the read-modify-write CAS exhaustion count, the three
	// signals that predict orphan accumulation and hot-key contention. main wires
	// them to the deleter/sweeper accessors and the storage RMW counter. Each nil
	// func simply omits its collector.
	LazyDeleteDroppedFunc      func() uint64 // pks dropped because the delete queue was full
	LazyDeleteFailuresFunc     func() uint64 // member-reclaim attempts that errored
	LazyDeleteQueueDepthFunc   func() uint64 // current lazy-delete queue length (gauge)
	LazyDeleteIsLiveErrorsFunc func() uint64 // recreate-guard (IsLive) checks that errored
	OrphanSweepRunsFunc      func() uint64 // completed orphan-sweep runs
	OrphanSweepReclaimedFunc func() uint64 // orphan members reclaimed by the sweep
	OrphanSweepFailuresFunc  func() uint64 // orphan-sweep runs that errored
	RMWExhaustedFunc         func() uint64 // RMW/CAS loops that exhausted their retries
}

// Metrics owns the proxy's Prometheus collectors and (optionally) exposes them
// for scraping. It is safe for concurrent use by connection goroutines.
type Metrics struct {
	registry *prometheus.Registry

	qps     *prometheus.CounterVec   // per-command call count (QPS derived by rate())
	latency *prometheus.HistogramVec // per-command latency in seconds
	errors  *prometheus.CounterVec   // per-command error count

	// interceptions backs the large-key gauge when Config.InterceptionsFunc is
	// nil. Advanced by SetInterceptions, read by the registered GaugeFunc.
	interceptions atomic.Uint64
}

// New builds a Metrics, registering all collectors on the configured (or a
// freshly created) registry. It panics only if collectors collide on the given
// registry, which indicates a programming error (double registration).
func New(cfg Config) *Metrics {
	reg := cfg.Registry
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	buckets := cfg.LatencyBuckets
	if buckets == nil {
		buckets = DefaultLatencyBuckets
	}

	m := &Metrics{
		registry: reg,
		qps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "commands_total",
			Help:      "Total number of commands processed, labeled by command name.",
		}, []string{commandLabel}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "command_duration_seconds",
			Help:      "Command handling latency in seconds, labeled by command name.",
			Buckets:   buckets,
		}, []string{commandLabel}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "command_errors_total",
			Help:      "Total number of commands that returned an error, labeled by command name.",
		}, []string{commandLabel}),
	}

	// The large-key interception count is surfaced as a GaugeFunc so it is read
	// lazily at scrape time from the injected source, keeping metrics decoupled
	// from guard/command (requirement 18.5).
	source := cfg.InterceptionsFunc
	if source == nil {
		source = m.interceptions.Load
	}
	interceptionsGauge := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "large_key_interceptions_total",
		Help:      "Number of writes rejected by the size guard for exceeding backend limits.",
	}, func() float64 { return float64(source()) })

	reg.MustRegister(m.qps, m.latency, m.errors, interceptionsGauge)

	// Background-reclaimer and contention gauges, each registered only when its live
	// source was supplied (main wires them to the deleter/sweeper/storage accessors).
	// They are read at scrape time, keeping metrics decoupled from meta/storage.
	for _, g := range []struct {
		src  func() uint64
		name string
		help string
	}{
		{cfg.LazyDeleteDroppedFunc, "lazy_delete_dropped_total", "pks dropped because the lazy-delete queue was full."},
		{cfg.LazyDeleteFailuresFunc, "lazy_delete_failures_total", "lazy-delete member-reclaim attempts that errored."},
		{cfg.LazyDeleteIsLiveErrorsFunc, "lazy_delete_islive_errors_total", "lazy-delete recreate-guard (IsLive) checks that errored."},
		{cfg.LazyDeleteQueueDepthFunc, "lazy_delete_queue_depth", "current lazy-delete queue length."},
		{cfg.OrphanSweepRunsFunc, "orphan_sweep_runs_total", "completed orphan-sweep runs."},
		{cfg.OrphanSweepReclaimedFunc, "orphan_sweep_reclaimed_total", "orphan members reclaimed by the weekly sweep."},
		{cfg.OrphanSweepFailuresFunc, "orphan_sweep_failures_total", "orphan-sweep runs that errored."},
		{cfg.RMWExhaustedFunc, "rmw_max_retries_exhausted_total", "read-modify-write CAS loops that exhausted their retries."},
	} {
		if g.src == nil {
			continue
		}
		src := g.src
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      g.name,
			Help:      g.help,
		}, func() float64 { return float64(src()) }))
	}

	return m
}

// ObserveCommand records one processed command: it increments the per-command
// call counter, observes the latency histogram, and, when isErr is true,
// increments the per-command error counter. name should be the lowercased
// command name so labels stay bounded and consistent with the command table.
func (m *Metrics) ObserveCommand(name string, dur time.Duration, isErr bool) {
	m.qps.WithLabelValues(name).Inc()
	m.latency.WithLabelValues(name).Observe(dur.Seconds())
	if isErr {
		m.errors.WithLabelValues(name).Inc()
	}
}

// SetInterceptions records the current large-key interception count for the
// gauge. It is a no-op source when Config.InterceptionsFunc was supplied (that
// func is authoritative), but remains safe to call. Callers using the internal
// atomic typically pass guard.Interceptions() periodically or on each rejection.
func (m *Metrics) SetInterceptions(n uint64) {
	m.interceptions.Store(n)
}

// Registry returns the underlying collector registry so the assembly layer can
// mount it (for example under an HTTP scrape endpoint).
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler returns an http.Handler that serves the registry in the Prometheus
// text exposition format, suitable for mounting at /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// SlowlogEntry is one recorded slow command. ID is a monotonically increasing
// identifier assigned by the ring on record (matching Redis SLOWLOG semantics);
// Time is when the command completed; Duration is how long it took; Command is
// the command name and Args is a (possibly truncated) summary of its arguments.
type SlowlogEntry struct {
	ID       int64
	Time     time.Time
	Duration time.Duration
	Command  string
	Args     []string
}

// SlowLog is a fixed-capacity, thread-safe ring buffer of slow commands. Only
// commands whose duration is at or above the configured threshold are recorded;
// once the ring is full the oldest entry is overwritten. Get returns the most
// recent entries newest-first, matching Redis SLOWLOG GET ordering.
type SlowLog struct {
	mu        sync.Mutex
	threshold time.Duration
	now       func() time.Time

	buf    []SlowlogEntry // ring storage; len == capacity
	head   int            // index of the next write slot
	size   int            // number of live entries (<= cap)
	nextID int64          // id to assign to the next recorded entry
}

// SlowlogConfig parameterizes NewSlowLog.
type SlowlogConfig struct {
	// Capacity is the maximum number of entries retained. Values <= 0 fall back
	// to DefaultSlowlogCapacity.
	Capacity int
	// Threshold is the minimum duration a command must take to be recorded. A
	// value <= 0 records every command passed to Record.
	Threshold time.Duration
	// Now supplies the current time when an entry omits it; nil uses time.Now.
	// Injectable for deterministic tests.
	Now func() time.Time
}

// DefaultSlowlogCapacity is the ring size used when SlowlogConfig.Capacity is
// unset. It mirrors Redis' default slowlog-max-len of 128.
const DefaultSlowlogCapacity = 128

// Argument caps mirror Redis' slowlog behaviour so stored entries stay bounded
// regardless of how large a command was: at most MaxSlowlogArgs arguments are
// kept, and each argument is truncated to MaxSlowlogArgBytes. When either cap
// trims content the omission is recorded as a synthetic trailer argument, just
// like Redis (e.g. "... (3 more arguments)" / a value suffixed with
// "... (42 more bytes)"). This keeps a single pathological command from pinning
// unbounded memory in the ring.
const (
	// MaxSlowlogArgs is the maximum number of arguments retained per entry.
	MaxSlowlogArgs = 32
	// MaxSlowlogArgBytes is the maximum length (in bytes) retained per argument.
	MaxSlowlogArgBytes = 128
)

// capArgs returns a bounded copy of args following Redis slowlog semantics: no
// more than MaxSlowlogArgs entries (the final slot summarising any surplus) and
// no argument longer than MaxSlowlogArgBytes (the overflow summarised inline).
// The input slice is never mutated. A nil/empty input yields nil.
func capArgs(args []string) []string {
	if len(args) == 0 {
		return nil
	}

	// Decide how many real arguments to keep. When we exceed the cap we keep
	// MaxSlowlogArgs-1 real args and reserve the last slot for the summary.
	keep := len(args)
	truncatedArgs := false
	if keep > MaxSlowlogArgs {
		keep = MaxSlowlogArgs - 1
		truncatedArgs = true
	}

	out := make([]string, 0, keep+1)
	for i := 0; i < keep; i++ {
		out = append(out, capArgBytes(args[i]))
	}
	if truncatedArgs {
		out = append(out, fmt.Sprintf("... (%d more arguments)", len(args)-keep))
	}
	return out
}

// capArgBytes truncates a single argument to MaxSlowlogArgBytes bytes, appending
// a Redis-style summary of the dropped byte count when it overflows.
func capArgBytes(arg string) string {
	if len(arg) <= MaxSlowlogArgBytes {
		return arg
	}
	return arg[:MaxSlowlogArgBytes] + fmt.Sprintf("... (%d more bytes)", len(arg)-MaxSlowlogArgBytes)
}

// NewSlowLog constructs a SlowLog from cfg, applying defaults for unset fields.
func NewSlowLog(cfg SlowlogConfig) *SlowLog {
	if cfg.Capacity <= 0 {
		cfg.Capacity = DefaultSlowlogCapacity
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &SlowLog{
		threshold: cfg.Threshold,
		now:       cfg.Now,
		buf:       make([]SlowlogEntry, cfg.Capacity),
		nextID:    0,
	}
}

// Threshold returns the minimum duration required for Record to retain an entry.
func (s *SlowLog) Threshold() time.Duration { return s.threshold }

// Record adds entry to the ring when entry.Duration is at or above the
// threshold, returning true if it was retained. Sub-threshold commands are
// dropped (false). The ring assigns entry.ID and, when entry.Time is zero,
// stamps it with the configured clock. When the ring is full the oldest entry
// is evicted. The provided Args slice is copied defensively so later mutation by
// the caller cannot corrupt stored entries, and is bounded to MaxSlowlogArgs
// arguments of MaxSlowlogArgBytes each (Redis slowlog semantics) so a single
// oversized command cannot pin unbounded memory.
func (s *SlowLog) Record(entry SlowlogEntry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry.Duration < s.threshold {
		return false
	}
	if entry.Time.IsZero() {
		entry.Time = s.now()
	}
	entry.ID = s.nextID
	s.nextID++
	// capArgs both bounds and copies the arguments, so a later mutation of the
	// caller's slice cannot corrupt the stored entry.
	entry.Args = capArgs(entry.Args)

	s.buf[s.head] = entry
	s.head = (s.head + 1) % len(s.buf)
	if s.size < len(s.buf) {
		s.size++
	}
	return true
}

// Get returns retained entries newest-first. A negative n returns every live
// entry; a non-negative n returns at most n entries (n == 0 yields an empty,
// non-nil slice). The returned slice is freshly allocated and owned by the
// caller.
func (s *SlowLog) Get(n int) []SlowlogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := s.size
	if n >= 0 && n < count {
		count = n
	}
	out := make([]SlowlogEntry, 0, count)
	// The most recently written slot is head-1; walk backwards from there.
	for i := 0; i < count; i++ {
		idx := (s.head - 1 - i + len(s.buf)) % len(s.buf)
		out = append(out, s.buf[idx])
	}
	return out
}

// Len returns the number of entries currently retained in the ring.
func (s *SlowLog) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// Reset clears all entries and resets the id sequence. Primarily for SLOWLOG
// RESET and tests.
func (s *SlowLog) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.buf {
		s.buf[i] = SlowlogEntry{}
	}
	s.head = 0
	s.size = 0
	s.nextID = 0
}
