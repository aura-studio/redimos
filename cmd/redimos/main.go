// Command redimos is the RESP2-compatible proxy that maps Redis commands onto
// a DynamoDB single table via the redimo fork.
//
// This is the production entry point (task 23.1). It parses the operational
// flags, assembles every component
//
//	server -> router -> meta / scan / guard / metrics -> storage(redimo fork)
//
// wires them together, exposes the Prometheus metrics endpoint, and serves
// RESP2 over redcon until a shutdown signal arrives, at which point the
// background workers (lazy deleter, orphan sweeper) and the listeners are
// stopped cleanly. Requirements 18.1, 18.2, 18.3.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aura-studio/redimos/v2/internal/command"
	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/metrics"
	"github.com/aura-studio/redimos/v2/internal/scan"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/prometheus/client_golang/prometheus"
)

// appConfig holds the parsed command-line configuration consumed by the
// assembly step. Every field maps to exactly one flag.
type appConfig struct {
	// Endpoint / auth.
	addr                string // listen address for the RESP2 endpoint
	requirepass         string // single-password AUTH; empty disables auth
	multiDB             bool   // permit SELECT n (n != 0)
	maxCollectionResult int    // cap on whole-collection reply/operand size (0 disables)
	maxCommandBytes     int    // reject a single command larger than this (0 disables)

	// DynamoDB / storage.
	table          string // DynamoDB table name
	consistency    string // read consistency: strong|eventual
	region         string // AWS region override (empty uses the default chain)
	dynamoEndpoint string // DynamoDB endpoint override (e.g. local dynamodb)
	retryMax       int    // AWS SDK max attempts (throttle retry/backoff, req 18.8)
	deleteBatch    int    // lazy-delete BatchWriteItem size
	deleteRate     float64

	// SCAN cursor registry.
	instID       string        // proxy instance id (shared with scan registry)
	scanCapacity int           // max live SCAN cursors
	scanTTL      time.Duration // SCAN cursor lifetime
	scanTimeout  time.Duration // max duration of a single SCAN page (0 disables)

	// Background reclamation.
	sweepInterval time.Duration // orphan sweeper cadence

	// Observability.
	metricsAddr      string        // HTTP address for /metrics and /healthz
	slowlogThreshold time.Duration // min duration recorded in the slowlog ring
	slowlogCapacity  int           // slowlog ring size
}

func parseFlags() appConfig {
	var c appConfig

	flag.StringVar(&c.addr, "addr", ":6379", "listen address for the RESP2 endpoint")
	flag.StringVar(&c.requirepass, "requirepass", "", "single-password AUTH (empty disables auth)")
	flag.BoolVar(&c.multiDB, "multi-db", false, "permit SELECT of non-zero DB indexes")
	flag.IntVar(&c.maxCollectionResult, "max-collection-result", 0, "reject a whole-collection reply/operand (HGETALL/SMEMBERS/...) with more than N members (0 disables)")
	flag.IntVar(&c.maxCommandBytes, "max-command-bytes", 0, "reject a single command whose raw wire size exceeds N bytes (0 disables)")

	flag.StringVar(&c.table, "table", "redis-data", "DynamoDB single-table name")
	flag.StringVar(&c.consistency, "consistency", "strong", "default read consistency: strong|eventual")
	flag.StringVar(&c.region, "region", "", "AWS region override (empty uses the default credential/region chain)")
	flag.StringVar(&c.dynamoEndpoint, "dynamo-endpoint", "", "DynamoDB endpoint override (e.g. http://localhost:8000 for dynamodb-local)")
	flag.IntVar(&c.retryMax, "retry-max-attempts", 5, "AWS SDK max attempts for throttling retry/backoff")
	flag.IntVar(&c.deleteBatch, "delete-batch-size", 25, "lazy-delete BatchWriteItem size (1-25)")
	flag.Float64Var(&c.deleteRate, "delete-rate", 50, "lazy-delete pks processed per second (<=0 disables rate limiting)")

	flag.StringVar(&c.instID, "inst-id", "", "proxy instance id for SCAN cursor ownership (empty generates one)")
	flag.IntVar(&c.scanCapacity, "scan-capacity", scan.DefaultCapacity, "maximum number of live SCAN cursors")
	flag.DurationVar(&c.scanTTL, "scan-ttl", scan.DefaultTTL, "SCAN cursor lifetime")
	flag.DurationVar(&c.scanTimeout, "scan-timeout", 5*time.Second, "max duration for a single SCAN page against the backend (0 disables)")

	flag.DurationVar(&c.sweepInterval, "sweep-interval", meta.DefaultSweepInterval, "orphan sweeper interval")

	flag.StringVar(&c.metricsAddr, "metrics-addr", ":9121", "HTTP listen address for /metrics and /healthz")
	flag.DurationVar(&c.slowlogThreshold, "slowlog-threshold", 10*time.Millisecond, "minimum command duration recorded in the slowlog ring")
	flag.IntVar(&c.slowlogCapacity, "slowlog-capacity", metrics.DefaultSlowlogCapacity, "slowlog ring buffer capacity")

	flag.Parse()
	return c
}

func main() {
	if err := run(parseFlags()); err != nil {
		log.Fatalf("redimos: %v", err)
	}
}

// validateConfig fails fast on out-of-range or malformed flags before any resource is
// created, so an operator gets one clear boot error instead of a cryptic failure deep in
// the storage/serving path (e.g. delete-batch-size 100 exceeding BatchWriteItem's 25, or
// a mistyped consistency mode). The defaults are all valid, so a flag-free launch passes.
func validateConfig(cfg appConfig) error {
	if cfg.addr == "" {
		return errors.New("-addr must not be empty")
	}
	if cfg.metricsAddr == "" {
		return errors.New("-metrics-addr must not be empty")
	}
	if cfg.table == "" {
		return errors.New("-table must not be empty")
	}
	if cfg.consistency != "strong" && cfg.consistency != "eventual" {
		return fmt.Errorf("-consistency must be strong|eventual, got %q", cfg.consistency)
	}
	if cfg.deleteBatch < 1 || cfg.deleteBatch > 25 {
		return fmt.Errorf("-delete-batch-size must be in [1,25], got %d", cfg.deleteBatch)
	}
	if cfg.retryMax < 1 {
		return fmt.Errorf("-retry-max-attempts must be >= 1, got %d", cfg.retryMax)
	}
	if cfg.scanCapacity < 1 {
		return fmt.Errorf("-scan-capacity must be >= 1, got %d", cfg.scanCapacity)
	}
	if cfg.slowlogCapacity < 1 {
		return fmt.Errorf("-slowlog-capacity must be >= 1, got %d", cfg.slowlogCapacity)
	}
	if cfg.maxCollectionResult < 0 {
		return fmt.Errorf("-max-collection-result must be >= 0, got %d", cfg.maxCollectionResult)
	}
	if cfg.maxCommandBytes < 0 {
		return fmt.Errorf("-max-command-bytes must be >= 0, got %d", cfg.maxCommandBytes)
	}
	if cfg.scanTimeout < 0 {
		return fmt.Errorf("-scan-timeout must be >= 0, got %s", cfg.scanTimeout)
	}

	return nil
}

// run performs the full assembly and blocks serving until a shutdown signal is
// received, then tears everything down cleanly. It returns a non-nil error only
// for a fatal startup failure.
func run(cfg appConfig) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}

	// A context cancelled by SIGINT/SIGTERM drives the graceful shutdown of the
	// listeners; the background workers are stopped explicitly (below) so they
	// drain their queues rather than aborting on cancellation.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	instID := cfg.instID
	if instID == "" {
		instID = newInstID()
	}

	// --- storage: AWS SDK v2 DynamoDB client -> redimo-backed Store ---------
	//
	// The AWS SDK client owns throttling retry/backoff (requirement 18.8): the
	// standard retryer retries ProvisionedThroughputExceeded with exponential
	// backoff up to retry-max-attempts. The storage seam only classifies the
	// error the SDK ultimately surfaces and fires OnThrottle for alerting.
	loadOpts := []func(*config.LoadOptions) error{
		config.WithRetryMode(aws.RetryModeStandard),
		config.WithRetryMaxAttempts(cfg.retryMax),
	}
	if cfg.region != "" {
		loadOpts = append(loadOpts, config.WithRegion(cfg.region))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	ddb := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if cfg.dynamoEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.dynamoEndpoint)
		}
	})

	// --- metrics: Prometheus registry + collectors + slowlog ring -----------
	//
	// The large-key interception gauge is sourced live from guard.Interceptions
	// so the size-guard rejection count is surfaced without metrics importing
	// guard (requirement 18.5).
	registry := prometheus.NewRegistry()
	slowlog := metrics.NewSlowLog(metrics.SlowlogConfig{
		Capacity:  cfg.slowlogCapacity,
		Threshold: cfg.slowlogThreshold,
	})

	// Throttle alerting hook (requirement 18.8): a dedicated counter on the same
	// registry so a sustained throttle is visible operationally and can feed the
	// DynamoDB ThrottledRequests alert. The hook runs on the request goroutine,
	// so it does only cheap, non-blocking work.
	throttled := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redimos",
		Name:      "dynamodb_throttled_total",
		Help:      "DynamoDB operations that surfaced a throttling error after SDK retries were exhausted.",
	})
	registry.MustRegister(throttled)
	onThrottle := func() { throttled.Inc() }

	store := storage.New(ddb, storage.Options{
		TableName:            cfg.table,
		EventuallyConsistent: strings.EqualFold(cfg.consistency, "eventual"),
		DeleteBatchSize:      cfg.deleteBatch,
		OnThrottle:           onThrottle,
	})

	// --- meta: lazy deleter + meta store + read path + orphan sweeper -------
	//
	// The deleter is the DeletionEnqueuer wired into the MetaStore the router
	// uses, so DEL/expiry hand the pk to the background reclaimer off the
	// request path. The sweeper is the weekly backstop for pks the deleter
	// dropped or failed to reclaim.
	deleter := meta.NewDeleter(store, meta.DeleterConfig{
		RatePerSecond: cfg.deleteRate,
		Logger:        meta.StdLogger{},
		// Skip reclaiming a pk that was recreated after DEL enqueued it (its #meta is live
		// again) — reclaiming would wipe the new incarnation's data (DEL-then-recreate race).
		IsLive: func(ctx context.Context, pk string) (bool, error) {
			_, found, err := store.LoadMeta(ctx, pk)
			return found, err
		},
	})
	metaStore := meta.NewMetaStore(store, deleter)
	reader := meta.NewReader(metaStore, nil)
	sweeper := meta.NewSweeper(store, meta.SweeperConfig{
		Interval: cfg.sweepInterval,
		Logger:   meta.StdLogger{},
	})

	// The command/collector metrics are built here (after the deleter and sweeper
	// exist) so the background-reclaimer health gauges can read their live accessors.
	// The large-key interception gauge is sourced from guard.Interceptions so the
	// size-guard rejection count is surfaced without metrics importing guard (req 18.5).
	m := metrics.New(metrics.Config{
		Registry:                 registry,
		LatencyBuckets:           metrics.DefaultLatencyBuckets,
		InterceptionsFunc:        guard.Interceptions,
		LazyDeleteDroppedFunc:    func() uint64 { return uint64(deleter.Dropped()) },
		LazyDeleteFailuresFunc:   func() uint64 { return uint64(deleter.Failures()) },
		LazyDeleteQueueDepthFunc: func() uint64 { return uint64(deleter.QueueLen()) },
		OrphanSweepRunsFunc:      func() uint64 { return uint64(sweeper.Runs()) },
		OrphanSweepReclaimedFunc: func() uint64 { return uint64(sweeper.Reclaimed()) },
		OrphanSweepFailuresFunc:  func() uint64 { return uint64(sweeper.Failures()) },
		RMWExhaustedFunc:         storage.RMWExhausted,
	})

	// --- scan: per-instance SCAN cursor registry ---------------------------
	//
	// The registry MUST share instID with the server (below) so a SCAN
	// continuation cursor stamped with the owning instance validates against the
	// connection's InstID (see command.Storage.Scan doc comment / requirement
	// 13.6).
	scanReg := scan.New(scan.Config{
		InstID:   instID,
		Capacity: cfg.scanCapacity,
		TTL:      cfg.scanTTL,
	})

	// --- command: storage-backed router ------------------------------------
	router := command.NewRouterWithStorage(
		command.Config{
			RequirePass:         cfg.requirepass,
			MultiDB:             cfg.multiDB,
			MaxCollectionResult: cfg.maxCollectionResult,
			ScanTimeout:         cfg.scanTimeout,
		},
		command.Storage{
			Store:   store,
			Meta:    metaStore,
			Reader:  reader,
			Scan:    scanReg,
			Slowlog: slowlog,
			Metrics: m,
			// Now defaults to the wall clock inside NewRouterWithStorage.
		},
	)

	// --- server: redcon RESP2 shell wired to the router --------------------
	srv := server.New(server.Options{Addr: cfg.addr, InstID: instID, MaxCommandBytes: cfg.maxCommandBytes}, router)

	// Start the background reclaimers on a detached context so a shutdown signal
	// does not abort in-flight deletions; they are drained by the explicit Stop
	// calls during shutdown.
	deleter.Start(context.Background())
	sweeper.Start(context.Background())

	// --- metrics HTTP endpoint (requirement 18.5) --------------------------
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		// Liveness: the process is up. Stays a trivial 200 so a liveness probe never
		// restarts a healthy-but-busy proxy.
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		// Readiness: a cheap, non-blocking snapshot of the background-reclaimer and
		// contention signals (all lock-free atomic reads) an LB/operator can inspect or
		// gate on. Kept separate from /healthz so a readiness policy can evolve without
		// affecting liveness.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w,
			`{"ready":true,"lazy_delete_queue_depth":%d,"lazy_delete_dropped":%d,"lazy_delete_failures":%d,`+
				`"orphan_sweep_runs":%d,"orphan_sweep_failures":%d,"rmw_exhausted":%d,"large_key_interceptions":%d}`+"\n",
			deleter.QueueLen(), deleter.Dropped(), deleter.Failures(),
			sweeper.Runs(), sweeper.Failures(), storage.RMWExhausted(), guard.Interceptions())
	})
	httpSrv := &http.Server{
		Addr:              cfg.metricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("redimos: metrics endpoint error: %v", err)
		}
	}()

	// --- start serving RESP2 ------------------------------------------------
	serveErr := make(chan error, 1)
	ready := make(chan error, 1)
	go func() { serveErr <- srv.ListenServeAndSignal(ready) }()
	if err := <-ready; err != nil {
		return fmt.Errorf("bind %s: %w", cfg.addr, err)
	}

	log.Printf("redimos serving: addr=%s metrics=%s table=%s inst=%s consistency=%s auth=%t multi-db=%t",
		cfg.addr, cfg.metricsAddr, cfg.table, instID, cfg.consistency, cfg.requirepass != "", cfg.multiDB)

	// Block until a signal cancels ctx or the listener fails.
	select {
	case <-ctx.Done():
		log.Printf("redimos: shutdown signal received")
	case err := <-serveErr:
		// A serve error after a clean Close is expected during shutdown; only
		// surface an unexpected one.
		if err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("redimos: serve error: %v", err)
		}
	}

	// --- graceful shutdown --------------------------------------------------
	// Stop accepting connections first, then flush the background reclaimers, then close
	// the metrics endpoint. Each stage is timed so an operator can see which one consumes
	// the shutdown budget; a stage that overruns its budget is logged as a warning.
	shutdownStart := time.Now()
	shutdownStage := func(name string, budget time.Duration, fn func()) {
		start := time.Now()
		fn()
		if elapsed := time.Since(start); elapsed > budget {
			log.Printf("redimos: shutdown stage %q took %s (over %s budget)", name, elapsed.Round(time.Millisecond), budget)
		} else {
			log.Printf("redimos: shutdown stage %q done in %s", name, elapsed.Round(time.Millisecond))
		}
	}

	shutdownStage("server-close", 2*time.Second, func() {
		if err := srv.Close(); err != nil {
			log.Printf("redimos: server close: %v", err)
		}
	})
	shutdownStage("sweeper-stop", 2*time.Second, sweeper.Stop)
	shutdownStage("deleter-drain", 3*time.Second, deleter.Stop)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdownStage("metrics-close", 2*time.Second, func() {
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			log.Printf("redimos: metrics endpoint shutdown: %v", err)
		}
	})

	log.Printf("redimos: shutdown complete in %s", time.Since(shutdownStart).Round(time.Millisecond))
	return nil
}

// newInstID returns a random hex instance identifier, mirroring the server's
// own generator so a caller-provided id and an auto-generated one share a shape.
func newInstID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "inst-0"
	}
	return "inst-" + hex.EncodeToString(b[:])
}

