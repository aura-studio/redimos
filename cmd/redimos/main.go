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
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/aura-studio/redimos/internal/meta"
	"github.com/aura-studio/redimos/internal/metrics"
	"github.com/aura-studio/redimos/internal/scan"
)

// appConfig holds the parsed command-line configuration consumed by the
// assembly step. Every field maps to exactly one flag.
type appConfig struct {
	// Endpoint / auth.
	addr                string // listen address for the RESP2 endpoint
	requirepass         string // single-password AUTH; empty disables auth
	multiDB             bool   // permit SELECT n (n != 0)
	databases           int    // logical DB count bounding SELECT when multi-DB is on
	maxCollectionResult int    // cap on whole-collection reply/operand size (0 disables)
	maxCommandBytes     int    // reject a single command larger than this (0 disables)

	commandTimeout time.Duration // per-command deadline bounding its backend calls (0 disables)

	// DynamoDB / storage.
	table       string // DynamoDB table name
	consistency string // read consistency: strong|eventual
	region      string // AWS region; also the signing region for -endpoint-url. Empty uses the default chain.
	// DynamoDB connection (rocket-nano style; all optional). Three modes:
	//   ① local dynamodb-local: set -endpoint-url (dummy credentials are auto-injected
	//      when no credential flag is given, so it "just works");
	//   ② local -> cloud: set -access-key-id / -secret-access-key / -session-token;
	//   ③ cloud: set nothing — the AWS SDK default credential/region chain is used
	//      (env AWS_ACCESS_KEY_ID/SECRET/SESSION_TOKEN, shared profile, IAM role).
	// An endpoint field set installs an endpoint resolver for the DynamoDB service
	// (signed with -region); a credential field set installs a static credentials provider.
	endpointURL         string // -endpoint-url
	endpointPartitionID string // -endpoint-partition-id
	credAccessKeyID     string // -access-key-id
	credSecretAccessKey string // -secret-access-key
	credSessionToken    string // -session-token
	retryMax            int    // AWS SDK max attempts (throttle retry/backoff, req 18.8)
	deleteBatch    int    // lazy-delete BatchWriteItem size
	deleteRate     float64

	cbThreshold int           // circuit-breaker throttle threshold (0 disables load shedding)
	cbCooldown  time.Duration // circuit-breaker open duration

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
	requestLog       string        // structured request-log level: none|error|slow|all
}

func parseFlags() appConfig {
	var c appConfig

	flag.StringVar(&c.addr, "addr", ":6379", "listen address for the RESP2 endpoint")
	flag.StringVar(&c.requirepass, "requirepass", "", "single-password AUTH (empty disables auth)")
	flag.BoolVar(&c.multiDB, "multi-db", false, "permit SELECT of non-zero DB indexes")
	flag.IntVar(&c.databases, "databases", 16, "logical DB count bounding SELECT when -multi-db is set (Redis default 16)")
	flag.IntVar(&c.maxCollectionResult, "max-collection-result", 0, "reject a whole-collection reply/operand (HGETALL/SMEMBERS/...) with more than N members (0 disables)")
	flag.IntVar(&c.maxCommandBytes, "max-command-bytes", 0, "reject a single command whose raw wire size exceeds N bytes (0 disables)")
	flag.DurationVar(&c.commandTimeout, "command-timeout", 0, "per-command deadline bounding its DynamoDB calls; a command exceeding it is cancelled and replies an error (0 disables)")

	flag.StringVar(&c.table, "table", "redis-data", "DynamoDB single-table name")
	flag.StringVar(&c.consistency, "consistency", "strong", "default read consistency: strong|eventual")
	flag.StringVar(&c.region, "region", "", "AWS region override (empty uses the default credential/region chain)")
	flag.StringVar(&c.endpointURL, "endpoint-url", "", "DynamoDB endpoint URL override (e.g. http://localhost:8000 for dynamodb-local); installs an endpoint resolver signed with -region")
	flag.StringVar(&c.endpointPartitionID, "endpoint-partition-id", "", "DynamoDB endpoint partition id for the endpoint resolver")
	flag.StringVar(&c.credAccessKeyID, "access-key-id", "", "static AWS access key id (empty uses the default credential chain)")
	flag.StringVar(&c.credSecretAccessKey, "secret-access-key", "", "static AWS secret access key")
	flag.StringVar(&c.credSessionToken, "session-token", "", "static AWS session token (for temporary credentials)")
	flag.IntVar(&c.retryMax, "retry-max-attempts", 5, "AWS SDK max attempts for throttling retry/backoff")
	flag.IntVar(&c.deleteBatch, "delete-batch-size", 25, "lazy-delete BatchWriteItem size (1-25)")
	flag.Float64Var(&c.deleteRate, "delete-rate", 50, "lazy-delete pks processed per second (<=0 disables rate limiting)")
	flag.IntVar(&c.cbThreshold, "circuit-breaker-threshold", 0, "open the load-shedding circuit breaker after N accumulated DynamoDB throttles (0 disables)")
	flag.DurationVar(&c.cbCooldown, "circuit-breaker-cooldown", 5*time.Second, "how long the circuit breaker sheds load once open")

	flag.StringVar(&c.instID, "inst-id", "", "proxy instance id for SCAN cursor ownership (empty generates one)")
	flag.IntVar(&c.scanCapacity, "scan-capacity", scan.DefaultCapacity, "maximum number of live SCAN cursors")
	flag.DurationVar(&c.scanTTL, "scan-ttl", scan.DefaultTTL, "SCAN cursor lifetime")
	flag.DurationVar(&c.scanTimeout, "scan-timeout", 5*time.Second, "max duration for a single SCAN page against the backend (0 disables)")

	flag.DurationVar(&c.sweepInterval, "sweep-interval", meta.DefaultSweepInterval, "orphan sweeper interval")

	flag.StringVar(&c.metricsAddr, "metrics-addr", ":9121", "HTTP listen address for /metrics and /healthz; :0 (or an in-use address) auto-selects a free port, logged at startup")
	flag.DurationVar(&c.slowlogThreshold, "slowlog-threshold", 10*time.Millisecond, "minimum command duration recorded in the slowlog ring")
	flag.IntVar(&c.slowlogCapacity, "slowlog-capacity", metrics.DefaultSlowlogCapacity, "slowlog ring buffer capacity")
	flag.StringVar(&c.requestLog, "request-log", "none", "PII-safe structured (JSON) request logging level: none|error|slow|all")

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
	if _, err := requestLogLevel(cfg.requestLog); err != nil {
		return err
	}
	if cfg.maxCollectionResult < 0 {
		return fmt.Errorf("-max-collection-result must be >= 0, got %d", cfg.maxCollectionResult)
	}
	if cfg.maxCommandBytes < 0 {
		return fmt.Errorf("-max-command-bytes must be >= 0, got %d", cfg.maxCommandBytes)
	}
	if cfg.commandTimeout < 0 {
		return fmt.Errorf("-command-timeout must be >= 0, got %s", cfg.commandTimeout)
	}
	if cfg.databases < 1 {
		return fmt.Errorf("-databases must be >= 1, got %d", cfg.databases)
	}
	if cfg.cbThreshold < 0 {
		return fmt.Errorf("-circuit-breaker-threshold must be >= 0, got %d", cfg.cbThreshold)
	}
	if cfg.cbThreshold > 0 && cfg.cbCooldown <= 0 {
		return fmt.Errorf("-circuit-breaker-cooldown must be > 0 when the breaker is enabled, got %s", cfg.cbCooldown)
	}
	if cfg.scanTimeout < 0 {
		return fmt.Errorf("-scan-timeout must be >= 0, got %s", cfg.scanTimeout)
	}

	return nil
}
