package migrate

import (
	"bytes"
	"context"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ShadowConfig configures the shadow-read hook. The zero value is a disabled
// reader.
type ShadowConfig struct {
	// Enabled turns shadow reads on. When false, NewShadowReader returns a
	// no-op reader that starts no goroutines and samples nothing.
	Enabled bool

	// Rate is the sampling probability in [0,1]: the fraction of eligible read
	// commands that are also read from Pika and compared. 0 never samples; 1
	// always samples. Values outside [0,1] are clamped on construction.
	Rate float64

	// Prefixes is the key-prefix allowlist mirroring dual-write's gating: only
	// keys beginning with one of these prefixes are eligible for shadow reads.
	// An empty list makes every key eligible (no prefix gate). Requirement 17.2
	// pairs with the same gradual-rollout semantics as 17.1.
	Prefixes []string

	// QueueSize bounds the async compare queue. A full queue drops new shadow
	// reads (counted in ShadowStats.Dropped) rather than blocking the caller.
	// Values <= 0 fall back to defaultShadowQueueSize.
	QueueSize int

	// Workers is the number of background goroutines draining the queue. Values
	// <= 0 fall back to defaultShadowWorkers.
	Workers int

	// ReadTimeout bounds each individual Pika shadow read. Values <= 0 mean no
	// per-read timeout. The timeout uses a background context detached from the
	// client's request context so the shadow read never outlives its worker on
	// the client's behalf.
	ReadTimeout time.Duration
}

const (
	defaultShadowQueueSize = 1024
	defaultShadowWorkers   = 2
)

// Diff describes a single shadow-read mismatch: the sampled key, the command
// argv that was compared, and the two replies (primary is the DynamoDB reply
// the client saw; shadow is the reply read from Pika).
type Diff struct {
	Key     string
	Cmd     [][]byte
	Primary []byte
	Shadow  []byte
}

// DiffSink receives shadow-read mismatches. It is the injectable seam that lets
// tests capture diffs without real logging; the default sink logs via the
// standard logger. A sink must be safe for concurrent use — shadow-read workers
// call it from multiple goroutines.
type DiffSink func(Diff)

// defaultDiffSink logs a mismatch through the standard logger. It is used when
// no sink is injected.
func defaultDiffSink(d Diff) {
	log.Printf("shadow-read diff: key=%q cmd=%s primary=%q shadow=%q",
		d.Key, argvString(d.Cmd), d.Primary, d.Shadow)
}

// ShadowStats is a point-in-time snapshot of shadow-read counters.
type ShadowStats struct {
	// Sampled is the number of eligible reads selected by sampling for a shadow
	// comparison (before the Pika read is attempted).
	Sampled uint64
	// Compared is the number of shadow reads that completed a comparison
	// (the Pika read succeeded and its reply was compared to the primary).
	Compared uint64
	// Diffs is the number of comparisons where the Pika reply differed from the
	// primary reply.
	Diffs uint64
	// Errors is the number of shadow reads whose Pika read returned an error
	// (non-fatal; the client already has the authoritative DynamoDB reply).
	Errors uint64
	// Dropped is the number of sampled reads discarded because the async queue
	// was full (drop-on-full policy — the caller is never blocked).
	Dropped uint64
	// Skipped is the number of reads not shadowed because the key did not match
	// the prefix allowlist or sampling did not select them.
	Skipped uint64
}

// shadowJob is one queued shadow read. The argv and primary reply are copied on
// enqueue so the caller may reuse or mutate its buffers after ShadowRead
// returns.
type shadowJob struct {
	key     string
	cmd     [][]byte
	primary []byte
}

// ShadowReader samples read commands and, for the sampled fraction, also reads
// the same key from Pika and compares the two replies, recording a diff when
// they differ. The comparison runs on background workers so it never blocks or
// alters the client's DynamoDB read result (requirement 17.2).
//
// A nil *ShadowReader and a disabled reader are both valid no-ops, so the
// command layer can hold one unconditionally and call ShadowRead without
// branching.
type ShadowReader struct {
	enabled  bool
	rate     float64
	prefixes []string
	timeout  time.Duration

	client PikaClient
	sink   DiffSink

	// rng is guarded by rngMu; math/rand's top-level source is global-locked,
	// but we keep an explicit source so sampling is deterministic in tests via
	// SetRandSource.
	rngMu sync.Mutex
	rng   *rand.Rand

	queue chan shadowJob
	wg    sync.WaitGroup

	// mu guards the stopped transition and serializes it against enqueue so a
	// concurrent ShadowRead can never send on a closed queue.
	mu      sync.RWMutex
	stopped bool

	sampled  atomic.Uint64
	compared atomic.Uint64
	diffs    atomic.Uint64
	errors   atomic.Uint64
	dropped  atomic.Uint64
	skipped  atomic.Uint64
}

// NewShadowReader builds a ShadowReader from cfg. When cfg.Enabled is false, or
// client is nil, it returns a disabled (no-op) reader that starts no goroutines;
// this lets the caller construct one unconditionally from parsed flags. When
// enabled, worker goroutines are started immediately and run until Stop.
//
// If sink is nil, mismatches are logged through the standard logger.
func NewShadowReader(cfg ShadowConfig, client PikaClient, sink DiffSink) *ShadowReader {
	rate := cfg.Rate
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	if sink == nil {
		sink = defaultDiffSink
	}
	r := &ShadowReader{
		enabled:  cfg.Enabled && client != nil,
		rate:     rate,
		prefixes: append([]string(nil), cfg.Prefixes...),
		timeout:  cfg.ReadTimeout,
		client:   client,
		sink:     sink,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	if !r.enabled {
		return r
	}

	qsize := cfg.QueueSize
	if qsize <= 0 {
		qsize = defaultShadowQueueSize
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = defaultShadowWorkers
	}

	r.queue = make(chan shadowJob, qsize)
	r.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go r.worker()
	}
	return r
}

// Enabled reports whether the reader will sample shadow reads. It is nil-safe.
func (r *ShadowReader) Enabled() bool {
	return r != nil && r.enabled
}

// Rate reports the configured sampling probability. It is nil-safe.
func (r *ShadowReader) Rate() float64 {
	if r == nil {
		return 0
	}
	return r.rate
}

// SetRandSource replaces the sampling RNG source, allowing tests to make
// sampling deterministic. It is safe to call before ShadowRead. It is nil-safe.
func (r *ShadowReader) SetRandSource(src rand.Source) {
	if r == nil {
		return
	}
	r.rngMu.Lock()
	r.rng = rand.New(src)
	r.rngMu.Unlock()
}

// shouldSample decides whether this read is selected for a shadow comparison,
// applying the rate. Rate <= 0 never samples; rate >= 1 always samples.
func (r *ShadowReader) shouldSample() bool {
	if r.rate <= 0 {
		return false
	}
	if r.rate >= 1 {
		return true
	}
	r.rngMu.Lock()
	v := r.rng.Float64()
	r.rngMu.Unlock()
	return v < r.rate
}

// ShadowRead is the entry point the command layer's read path calls after it
// obtains the authoritative DynamoDB reply for a read command. When the reader
// is enabled, key matches the prefix allowlist, and sampling selects the read,
// the same command is enqueued for a background worker that reads the key from
// Pika and compares the reply to primaryReply, recording a diff on mismatch.
//
// Behavior (requirement 17.2):
//   - Disabled reader or nil receiver: no-op.
//   - key not in the prefix allowlist, or not selected by sampling: not
//     shadowed (ShadowStats.Skipped++).
//   - Otherwise the command and primary reply are copied and enqueued. If the
//     queue is full the shadow read is dropped (ShadowStats.Dropped++) rather
//     than blocking.
//
// ShadowRead never blocks on Pika, never returns anything, and never alters the
// client's result: primaryReply is what the client already saw and is treated
// as read-only here.
func (r *ShadowReader) ShadowRead(key string, cmd [][]byte, primaryReply []byte) {
	if !r.Enabled() {
		return
	}
	if !matchAnyPrefix(key, r.prefixes) {
		r.skipped.Add(1)
		return
	}
	if !r.shouldSample() {
		r.skipped.Add(1)
		return
	}
	r.sampled.Add(1)

	job := shadowJob{
		key:     key,
		cmd:     cloneArgv(cmd),
		primary: cloneBytes(primaryReply),
	}

	// Hold the read lock across the send so Stop (write lock) cannot close the
	// queue mid-send. The send itself is non-blocking: drop when full.
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.stopped {
		return
	}
	select {
	case r.queue <- job:
	default:
		r.dropped.Add(1)
	}
}

// Stop stops accepting new shadow reads, drains the comparisons already queued,
// and waits for the workers to exit. It is idempotent and nil-safe. After Stop,
// ShadowRead is a no-op.
func (r *ShadowReader) Stop() {
	if r == nil || !r.enabled {
		return
	}

	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	close(r.queue)
	r.mu.Unlock()

	r.wg.Wait()
}

// Stats returns a snapshot of the shadow-read counters. It is nil-safe.
func (r *ShadowReader) Stats() ShadowStats {
	if r == nil {
		return ShadowStats{}
	}
	return ShadowStats{
		Sampled:  r.sampled.Load(),
		Compared: r.compared.Load(),
		Diffs:    r.diffs.Load(),
		Errors:   r.errors.Load(),
		Dropped:  r.dropped.Load(),
		Skipped:  r.skipped.Load(),
	}
}

// worker drains the queue until it is closed and empty.
func (r *ShadowReader) worker() {
	defer r.wg.Done()
	for job := range r.queue {
		r.compare(job)
	}
}

// compare performs one shadow read against Pika on a background context and
// compares the reply to the primary, recording a diff on mismatch. Any error
// from Pika is non-fatal and counted in ShadowStats.Errors.
func (r *ShadowReader) compare(job shadowJob) {
	ctx := context.Background()
	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	shadow, err := r.readShadow(ctx, job.cmd)
	if err != nil {
		r.errors.Add(1)
		return
	}
	r.compared.Add(1)
	if !bytes.Equal(shadow, job.primary) {
		r.diffs.Add(1)
		r.sink(Diff{
			Key:     job.key,
			Cmd:     job.cmd,
			Primary: job.primary,
			Shadow:  shadow,
		})
	}
}

// readShadow reads the same command from Pika. The PikaClient interface (shared
// with dual-write) exposes a Do that executes the argv; shadow reads reuse it
// and the reply is captured via a ReplyRecorder when the client supports it.
//
// To keep the migrate package free of a redis dependency and the interface
// minimal, we let the injected client expose the reply through the optional
// ShadowPikaClient interface. A plain PikaClient (Do only) is still usable: its
// Do result is treated as an empty reply, which suffices for connectivity
// checks but records a diff whenever the primary reply is non-empty.
func (r *ShadowReader) readShadow(ctx context.Context, cmd [][]byte) ([]byte, error) {
	if sc, ok := r.client.(ShadowPikaClient); ok {
		return sc.Read(ctx, cmd)
	}
	if err := r.client.Do(ctx, cmd); err != nil {
		return nil, err
	}
	return nil, nil
}

// ShadowPikaClient is an optional extension of PikaClient for shadow reads that
// need the raw Pika reply bytes to compare against the primary. A real adapter
// around the redis client implements Read to return the serialized reply; the
// dual-write path only needs Do, so PikaClient stays minimal and this extension
// is opt-in.
type ShadowPikaClient interface {
	PikaClient
	Read(ctx context.Context, cmd [][]byte) ([]byte, error)
}

// cloneBytes copies b so a queued job is unaffected by the caller reusing its
// buffer after ShadowRead returns. A nil slice is preserved as nil.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// argvString renders an argv for log output without allocating on the hot path
// of callers (only used by the default diff sink).
func argvString(cmd [][]byte) string {
	parts := make([]string, len(cmd))
	for i, a := range cmd {
		parts[i] = string(a)
	}
	return strings.Join(parts, " ")
}

// ParseShadowReadFlag interprets the --shadow-read flag value into a partial
// ShadowConfig (Enabled + Rate). The recognized values are:
//
//	"", "off", "none", "disabled" -> disabled
//	"sample:<float>"              -> enabled with that sampling rate
//
// The rate is clamped to [0,1]. An unparseable or malformed value yields a
// disabled config (fail-safe: a misconfigured shadow read must never turn into
// an always-on comparison). The caller fills in the remaining config (prefix
// allowlist, queue size, etc.) from its own flags.
func ParseShadowReadFlag(value string) ShadowConfig {
	v := strings.TrimSpace(strings.ToLower(value))
	switch v {
	case "", "off", "none", "disabled":
		return ShadowConfig{Enabled: false}
	}

	const prefix = "sample:"
	if !strings.HasPrefix(v, prefix) {
		return ShadowConfig{Enabled: false}
	}
	rateStr := strings.TrimSpace(v[len(prefix):])
	rate, err := strconv.ParseFloat(rateStr, 64)
	if err != nil {
		return ShadowConfig{Enabled: false}
	}
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	return ShadowConfig{Enabled: true, Rate: rate}
}
