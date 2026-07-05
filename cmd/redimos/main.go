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

	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/metrics"
	"github.com/aura-studio/redimos/v2/internal/scan"
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
