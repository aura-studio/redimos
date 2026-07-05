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
	"net/http"
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
	OrphanSweepRunsFunc        func() uint64 // completed orphan-sweep runs
	OrphanSweepReclaimedFunc   func() uint64 // orphan members reclaimed by the sweep
	OrphanSweepFailuresFunc    func() uint64 // orphan-sweep runs that errored
	RMWExhaustedFunc           func() uint64 // RMW/CAS loops that exhausted their retries
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
			Help:      "Total number of commands processed, labeled by command name and family.",
		}, []string{commandLabel, familyLabel}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "command_duration_seconds",
			Help:      "Command handling latency in seconds, labeled by command name and family.",
			Buckets:   buckets,
		}, []string{commandLabel, familyLabel}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "command_errors_total",
			Help:      "Total number of commands that returned an error, labeled by command name, family, and RESP error class.",
		}, []string{commandLabel, familyLabel, errorClassLabel}),
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
// call counter, observes the latency histogram (both labeled by command name and
// family), and, when isErr is true, increments the error counter labeled by name,
// family, and the RESP error class (errClass, e.g. "WRONGTYPE"/"ERR"/"NOAUTH"). An
// empty errClass on an error defaults to "ERR". name should be the lowercased
// command name so labels stay bounded and consistent with the command table.
func (m *Metrics) ObserveCommand(name string, dur time.Duration, isErr bool, errClass string) {
	family := commandFamily(name)
	m.qps.WithLabelValues(name, family).Inc()
	m.latency.WithLabelValues(name, family).Observe(dur.Seconds())
	if isErr {
		if errClass == "" {
			errClass = "ERR"
		}
		m.errors.WithLabelValues(name, family, errClass).Inc()
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
