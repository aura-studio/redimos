package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aura-studio/redimos/internal/command"
	"github.com/aura-studio/redimos/internal/ddbobs"
	"github.com/aura-studio/redimos/internal/guard"
	"github.com/aura-studio/redimos/internal/meta"
	"github.com/aura-studio/redimos/internal/metrics"
	"github.com/aura-studio/redimos/internal/scan"
	"github.com/aura-studio/redimos/internal/server"
	"github.com/aura-studio/redimos/internal/storage"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/prometheus/client_golang/prometheus"
)

// isAddrInUse reports whether err is a listener bind failure because the address is
// already in use, so the caller can auto-select a free port instead of failing.
func isAddrInUse(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		var sysErr *os.SyscallError
		if errors.As(opErr.Err, &sysErr) {
			return errors.Is(sysErr.Err, syscall.EADDRINUSE)
		}
	}
	return false
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

	// DynamoDB connection (rocket-nano style). An endpoint field set installs an
	// endpoint resolver pinning the DynamoDB service to the configured
	// url/partitionID/signingRegion; a credential field set installs a static
	// credentials provider. When neither is set the AWS SDK default credential/
	// region chain is used (env AWS_ACCESS_KEY_ID/SECRET/SESSION_TOKEN, profile,
	// IAM role) — mode ③.
	// Only a custom endpoint URL installs the resolver. A partition id on its own
	// must NOT: it would build an aws.Endpoint whose URL is "", and that resolver
	// shadows the SDK's default AWS endpoint — every request then targets "" and
	// fails ("unsupported protocol scheme"), so the startup backend check would
	// crash-loop. With no URL, fall through to the default AWS/region resolver.
	endpointSet := cfg.endpointURL != ""
	if endpointSet {
		loadOpts = append(loadOpts, config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(
				func(service, _ string, _ ...interface{}) (aws.Endpoint, error) {
					if service == dynamodb.ServiceID {
						return aws.Endpoint{
							URL:           cfg.endpointURL,
							PartitionID:   cfg.endpointPartitionID,
							SigningRegion: cfg.region,
						}, nil
					}
					return aws.Endpoint{}, &aws.EndpointNotFoundError{}
				},
			),
		))
	}

	credSet := cfg.credAccessKeyID != "" || cfg.credSecretAccessKey != "" ||
		cfg.credSessionToken != ""
	// Mode ① convenience: a local dynamodb-local endpoint with no credentials given —
	// inject dummy static credentials so the SDK does not fail with "no credentials"
	// and a bare -endpoint-url just works.
	if endpointSet && !credSet {
		cfg.credAccessKeyID = "dummy"
		cfg.credSecretAccessKey = "dummy"
		credSet = true
	}
	if credSet {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(
			aws.CredentialsProviderFunc(
				func(context.Context) (aws.Credentials, error) {
					return aws.Credentials{
						AccessKeyID:     cfg.credAccessKeyID,
						SecretAccessKey: cfg.credSecretAccessKey,
						SessionToken:    cfg.credSessionToken,
						Source:          "redimos",
					}, nil
				},
			),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	// --- metrics: Prometheus registry + collectors + slowlog ring -----------
	//
	// The registry is created before the DynamoDB client so the backend telemetry
	// observer (SDK retry attempts / throttled retries / consumed capacity units) can
	// be installed as a client middleware. The large-key interception gauge is sourced
	// live from guard.Interceptions so the size-guard rejection count is surfaced
	// without metrics importing guard (requirement 18.5).
	registry := prometheus.NewRegistry()
	dynamoObs := metrics.NewDynamoObserver(registry)

	ddb := dynamodb.NewFromConfig(awsCfg, ddbobs.WithObservability(dynamoObs))

	// Optional (-auto-create-table): create the table with redimo's schema if it is
	// missing, or verify an existing table's schema is compatible — BEFORE the backend
	// check below, which needs the table to exist. Off by default, so a bare launch
	// touches no table-level APIs (DescribeTable/CreateTable) and preserves the prior
	// "operator owns the table" behaviour.
	if cfg.autoCreateTable {
		if err := storage.EnsureTable(ctx, ddb, cfg.table); err != nil {
			return fmt.Errorf("auto-create-table: %w", err)
		}
	}

	// Fail fast: confirm the backend is reachable, the table exists, and credentials
	// are valid BEFORE serving, instead of binding the RESP listener and then erroring
	// on every command. Uses a bounded GetItem on a reserved sentinel key (only the
	// read permission the proxy already holds — no DescribeTable grant needed).
	if err := checkBackend(ctx, ddb, cfg.table, 5*time.Second); err != nil {
		return fmt.Errorf("backend startup check for table %q failed: %w", cfg.table, err)
	}

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

	// Optional load-shedding circuit breaker (opt-in via -circuit-breaker-threshold):
	// after N accumulated throttles it opens, and the command layer fails backend
	// commands fast until it recovers. Its trip count is surfaced as a gauge.
	var breaker *storage.CircuitBreaker
	if cfg.cbThreshold > 0 {
		breaker = storage.NewCircuitBreaker(cfg.cbThreshold, cfg.cbCooldown)
		registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "redimos",
			Name:      "circuit_breaker_trips_total",
			Help:      "Number of times the load-shedding circuit breaker has opened.",
		}, func() float64 { return float64(breaker.Trips()) }))
	}

	store := storage.New(ddb, storage.Options{
		TableName:            cfg.table,
		EventuallyConsistent: strings.EqualFold(cfg.consistency, "eventual"),
		DeleteBatchSize:      cfg.deleteBatch,
		OnThrottle:           onThrottle,
		Breaker:              breaker,
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
		// Run the first sweep a short, jittered delay after startup instead of a full
		// interval later, so a proxy that restarts more often than the (weekly) interval
		// still sweeps orphans each lifetime; the jitter spreads the full-table Scan
		// across a restarting fleet. See meta.SweeperConfig.InitialDelay.
		InitialDelay: sweepInitialDelay(cfg.sweepInterval),
		Logger:       meta.StdLogger{},
	})

	// The command/collector metrics are built here (after the deleter and sweeper
	// exist) so the background-reclaimer health gauges can read their live accessors.
	// The large-key interception gauge is sourced from guard.Interceptions so the
	// size-guard rejection count is surfaced without metrics importing guard (req 18.5).
	m := metrics.New(metrics.Config{
		Registry:                   registry,
		LatencyBuckets:             metrics.DefaultLatencyBuckets,
		InterceptionsFunc:          guard.Interceptions,
		LazyDeleteDroppedFunc:      func() uint64 { return uint64(deleter.Dropped()) },
		LazyDeleteFailuresFunc:     func() uint64 { return uint64(deleter.Failures()) },
		LazyDeleteQueueDepthFunc:   func() uint64 { return uint64(deleter.QueueLen()) },
		LazyDeleteIsLiveErrorsFunc: func() uint64 { return uint64(deleter.IsLiveErrors()) },
		OrphanSweepRunsFunc:        func() uint64 { return uint64(sweeper.Runs()) },
		OrphanSweepReclaimedFunc:   func() uint64 { return uint64(sweeper.Reclaimed()) },
		OrphanSweepFailuresFunc:    func() uint64 { return uint64(sweeper.Failures()) },
		RMWExhaustedFunc:           storage.RMWExhausted,
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
			RequirePass: cfg.requirepass,
			MultiDB:             cfg.multiDB,
			Databases:           cfg.databases,
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
	// Dispatch chain (outermost first): Observed measures every command and feeds the
	// Prometheus metrics + slowlog; Timeout applies the per-command deadline (a no-op
	// wrapper when -command-timeout is 0) which — because the ctx threads to DynamoDB —
	// bounds a command's backend work end-to-end.
	timed := command.NewTimeoutDispatcher(router, cfg.commandTimeout)
	broken := command.NewBreakerDispatcher(timed, breaker)
	dispatcher := command.NewObservedDispatcher(broken, m, slowlog, cfg.slowlogThreshold)
	if level, _ := requestLogLevel(cfg.requestLog); level != command.LogNone {
		// PII-safe structured request logging (JSON to stderr): validated at startup.
		dispatcher = dispatcher.WithRequestLog(slog.New(slog.NewJSONHandler(os.Stderr, nil)), level)
	}
	srv := server.New(server.Options{Addr: cfg.addr, InstID: instID, MaxCommandBytes: cfg.maxCommandBytes}, dispatcher)

	// Start the background reclaimers on a detached context so a shutdown signal
	// does not abort in-flight deletions; they are drained by the explicit Stop
	// calls during shutdown.
	deleter.Start(context.Background())
	sweeper.Start(context.Background())

	// Background backend health prober: caches DynamoDB reachability (a bounded
	// sentinel GetItem every 10s) so /readyz reflects real backend health with a
	// non-blocking atomic read. Seeded healthy since the startup check just passed.
	probe := newBackendProbe(ddb, cfg.table, 10*time.Second, 2*time.Second, true)
	probe.Start(context.Background())

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
		// Readiness now reflects backend health: when the cached DynamoDB probe is
		// unhealthy the proxy cannot serve correctly, so /readyz returns 503 and the LB
		// drains it (liveness /healthz stays 200 so it is not restarted — a transient
		// backend blip should shed traffic, not kill the pod). The reclaimer/contention
		// signals remain in the body for operators; all reads are lock-free atomics.
		ready := probe.Healthy()
		w.Header().Set("Content-Type", "application/json")
		if ready {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		backendErr := probe.LastError()
		fmt.Fprintf(w,
			`{"ready":%t,"backend_healthy":%t,"backend_error":%q,`+
				`"lazy_delete_queue_depth":%d,"lazy_delete_dropped":%d,"lazy_delete_failures":%d,`+
				`"lazy_delete_islive_errors":%d,`+
				`"orphan_sweep_runs":%d,"orphan_sweep_failures":%d,"rmw_exhausted":%d,"large_key_interceptions":%d}`+"\n",
			ready, ready, backendErr,
			deleter.QueueLen(), deleter.Dropped(), deleter.Failures(), deleter.IsLiveErrors(),
			sweeper.Runs(), sweeper.Failures(), storage.RMWExhausted(), guard.Interceptions())
	})
	httpSrv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	// Bind the metrics/health listener SYNCHRONOUSLY so a bind failure (e.g. the port
	// is already in use) is fatal at startup and surfaced to the operator — exactly
	// like the RESP listener below — instead of being logged from a goroutine while the
	// proxy runs on with no /metrics, /healthz or /readyz (a silent observability loss
	// that also hides the readiness gate from the orchestrator). Exception: an
	// address-already-in-use collision (e.g. several instances on one host) auto-falls
	// back to an OS-selected free port (":0") so observability survives; the actual
	// bound port is logged below. Other bind errors (permission, invalid address)
	// remain fatal.
	metricsLn, err := net.Listen("tcp", cfg.metricsAddr)
	if err != nil && isAddrInUse(err) {
		log.Printf("redimos: metrics address %s in use — auto-selecting a free port", cfg.metricsAddr)
		metricsLn, err = net.Listen("tcp", ":0")
	}
	if err != nil {
		return fmt.Errorf("bind metrics endpoint %s: %w", cfg.metricsAddr, err)
	}
	metricsAddr := metricsLn.Addr().String()
	go func() {
		if err := httpSrv.Serve(metricsLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

	log.Printf("redimos serving: addr=%s metrics=%s table=%s inst=%s consistency=%s auth=%t multi-db=%t auto-create-table=%t",
		cfg.addr, metricsAddr, cfg.table, instID, cfg.consistency, cfg.requirepass != "", cfg.multiDB, cfg.autoCreateTable)

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

	// Drain (not Close) so in-flight command dispatches — and any lazy-delete enqueue
	// they make — finish BEFORE the deleter is stopped; otherwise a reclaim enqueued
	// during shutdown is lost until the next orphan sweep.
	shutdownStage("server-drain", 3*time.Second, func() {
		if err := srv.Drain(2 * time.Second); err != nil {
			log.Printf("redimos: server drain: %v", err)
		}
	})
	shutdownStage("probe-stop", 2*time.Second, probe.Stop)
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

// requestLogLevel maps the -request-log flag string to a command.RequestLogLevel.
// An unrecognized value is an error so validateConfig fails fast at startup.
func requestLogLevel(s string) (command.RequestLogLevel, error) {
	switch s {
	case "none", "":
		return command.LogNone, nil
	case "error":
		return command.LogErrors, nil
	case "slow":
		return command.LogSlow, nil
	case "all":
		return command.LogAll, nil
	default:
		return command.LogNone, fmt.Errorf("-request-log must be none|error|slow|all, got %q", s)
	}
}

// sweepInitialDelay returns a short, jittered delay for the FIRST orphan sweep after
// startup (meta.SweeperConfig.InitialDelay). It is capped at a small fraction of the
// interval (and 30m) and jittered so a fleet's full-table Scans spread out instead of
// stampeding at t=0, while still guaranteeing each process lifetime sweeps soon after
// start rather than only after a full (weekly) interval.
func sweepInitialDelay(interval time.Duration) time.Duration {
	maxDelay := interval / 8
	if maxDelay > 30*time.Minute {
		maxDelay = 30 * time.Minute
	}
	if maxDelay <= time.Minute {
		// Very short interval (e.g. a test/override): sweep at ~a quarter interval.
		if d := interval / 4; d > 0 {
			return d
		}
		return interval
	}

	// Jitter uniformly in [1m, maxDelay].
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return maxDelay / 2
	}
	var n uint64
	for _, x := range b {
		n = n<<8 | uint64(x)
	}
	span := uint64(maxDelay - time.Minute)
	return time.Minute + time.Duration(n%span)
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
