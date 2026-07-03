package migrate

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PikaClient is the minimal seam the dual-writer uses to mirror a write command
// to the legacy Pika backend. A real implementation wraps a redis client (a
// thin adapter around go-redis is wired in during assembly, task 23.1); tests
// inject a fake.
//
// The dual-writer deliberately depends only on this interface, never on a
// concrete client, so the migrate package carries no redis dependency and the
// mirror path can be exercised in isolation. Do receives the full command as
// argv (args[0] is the command name), matching the [][]byte shape used across
// the command layer.
//
// Do may block or fail: the dual-writer runs it on background workers and
// treats any error as non-fatal. The caller's synchronous DynamoDB write is
// always authoritative, so a slow or failing Pika never blocks or fails the
// client (requirement 17.1, "保留回退").
type PikaClient interface {
	Do(ctx context.Context, args [][]byte) error
}

// DualWriteConfig configures the dual-write hook. The zero value is a disabled
// writer.
type DualWriteConfig struct {
	// Enabled turns mirroring on. When false, NewDualWriter returns a no-op
	// writer whose methods do nothing (and start no goroutines).
	Enabled bool

	// Target names the mirror backend (currently only "pika"). It is retained
	// for observability and future multi-target support; it does not change
	// behavior beyond being reported by the flag parser.
	Target string

	// Prefixes is the key-prefix allowlist used for gradual rollout: only keys
	// beginning with one of these prefixes are mirrored. An empty list mirrors
	// every key (no prefix gate). Requirement 17.1 ("按 key 前缀灰度").
	Prefixes []string

	// QueueSize bounds the async mirror queue. A full queue drops new mirror
	// writes (counted in Stats.Dropped) rather than blocking the caller. Values
	// <= 0 fall back to defaultQueueSize.
	QueueSize int

	// Workers is the number of background goroutines draining the queue. Values
	// <= 0 fall back to defaultWorkers.
	Workers int

	// WriteTimeout bounds each individual mirror write. Values <= 0 mean no
	// per-write timeout. The timeout uses a background context, detached from
	// the client's request context, so a mirror write outlives the client reply.
	WriteTimeout time.Duration
}

const (
	defaultQueueSize = 1024
	defaultWorkers   = 2
)

// mirrorJob is one queued mirror write. The argv is copied on enqueue so the
// caller may reuse or mutate its buffers after MirrorWrite returns.
type mirrorJob struct {
	cmd [][]byte
}

// Stats is a point-in-time snapshot of dual-write counters.
type Stats struct {
	// Mirrored is the number of commands successfully written to Pika.
	Mirrored uint64
	// Dropped is the number of mirror writes discarded because the async queue
	// was full (drop-on-full policy — the caller is never blocked).
	Dropped uint64
	// Failed is the number of mirror writes attempted but rejected by the
	// PikaClient (non-fatal; the DynamoDB write already succeeded).
	Failed uint64
	// Skipped is the number of write commands that did not match the prefix
	// allowlist and were therefore not mirrored.
	Skipped uint64
}

// DualWriter mirrors write commands to Pika asynchronously. It is safe for
// concurrent use by many connection goroutines. A nil *DualWriter and a
// disabled writer are both valid no-ops, so the command layer can hold one
// unconditionally and call MirrorWrite without branching.
type DualWriter struct {
	enabled  bool
	target   string
	prefixes []string
	timeout  time.Duration

	client PikaClient
	queue  chan mirrorJob
	wg     sync.WaitGroup

	// mu guards the stopped transition and serializes it against enqueue so a
	// concurrent MirrorWrite can never send on a closed queue. Enqueue takes the
	// read lock (many concurrent sends allowed); Stop takes the write lock
	// before closing the queue.
	mu      sync.RWMutex
	stopped bool

	mirrored atomic.Uint64
	dropped  atomic.Uint64
	failed   atomic.Uint64
	skipped  atomic.Uint64
}

// NewDualWriter builds a DualWriter from cfg. When cfg.Enabled is false, or
// client is nil, it returns a disabled (no-op) writer that starts no goroutines;
// this lets the caller construct one unconditionally from parsed flags. When
// enabled, worker goroutines are started immediately and run until Stop.
func NewDualWriter(cfg DualWriteConfig, client PikaClient) *DualWriter {
	w := &DualWriter{
		enabled:  cfg.Enabled && client != nil,
		target:   cfg.Target,
		prefixes: append([]string(nil), cfg.Prefixes...),
		timeout:  cfg.WriteTimeout,
		client:   client,
	}
	if !w.enabled {
		return w
	}

	qsize := cfg.QueueSize
	if qsize <= 0 {
		qsize = defaultQueueSize
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = defaultWorkers
	}

	w.queue = make(chan mirrorJob, qsize)
	w.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go w.worker()
	}
	return w
}

// Enabled reports whether the writer will mirror writes. It is nil-safe.
func (w *DualWriter) Enabled() bool {
	return w != nil && w.enabled
}

// Target reports the configured mirror target (e.g. "pika"). It is nil-safe.
func (w *DualWriter) Target() string {
	if w == nil {
		return ""
	}
	return w.target
}

// ShouldMirror reports whether a write to key would be mirrored, i.e. the writer
// is enabled and key matches the prefix allowlist. The command layer may call
// this to skip building the argv when a write will not be mirrored. It is
// nil-safe.
func (w *DualWriter) ShouldMirror(key string) bool {
	if !w.Enabled() {
		return false
	}
	return matchAnyPrefix(key, w.prefixes)
}

// MirrorWrite asynchronously mirrors a single write command to Pika, gated by
// the prefix allowlist on key. It is the entry point the command layer's write
// path calls after its synchronous DynamoDB write succeeds.
//
// Behavior (requirement 17.1):
//   - Disabled writer or nil receiver: no-op.
//   - key not in the prefix allowlist: not mirrored (Stats.Skipped++).
//   - Otherwise the command is copied and enqueued for a background worker. If
//     the queue is full the write is dropped (Stats.Dropped++) rather than
//     blocking — the caller (and thus the client) is never blocked by Pika.
//
// MirrorWrite never blocks on Pika and never returns an error: the DynamoDB
// write is authoritative and the mirror is best-effort.
func (w *DualWriter) MirrorWrite(key string, cmd [][]byte) {
	if !w.Enabled() {
		return
	}
	if !matchAnyPrefix(key, w.prefixes) {
		w.skipped.Add(1)
		return
	}

	job := mirrorJob{cmd: cloneArgv(cmd)}

	// Hold the read lock across the send so Stop (write lock) cannot close the
	// queue mid-send. The send itself is non-blocking: drop when full.
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.stopped {
		return
	}
	select {
	case w.queue <- job:
	default:
		w.dropped.Add(1)
	}
}

// Stop stops accepting new mirror writes, drains the writes already queued, and
// waits for the workers to exit. It is idempotent and nil-safe. After Stop,
// MirrorWrite is a no-op.
func (w *DualWriter) Stop() {
	if w == nil || !w.enabled {
		return
	}

	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	close(w.queue)
	w.mu.Unlock()

	// Workers range over the queue and exit once it is closed and drained, so
	// waiting here guarantees all already-queued writes are flushed.
	w.wg.Wait()
}

// Stats returns a snapshot of the dual-write counters. It is nil-safe.
func (w *DualWriter) Stats() Stats {
	if w == nil {
		return Stats{}
	}
	return Stats{
		Mirrored: w.mirrored.Load(),
		Dropped:  w.dropped.Load(),
		Failed:   w.failed.Load(),
		Skipped:  w.skipped.Load(),
	}
}

// worker drains the queue until it is closed and empty.
func (w *DualWriter) worker() {
	defer w.wg.Done()
	for job := range w.queue {
		w.dispatch(job)
	}
}

// dispatch performs one mirror write on a background context, isolating any
// slowness or failure from the client's request path.
func (w *DualWriter) dispatch(job mirrorJob) {
	ctx := context.Background()
	if w.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, w.timeout)
		defer cancel()
	}
	if err := w.client.Do(ctx, job.cmd); err != nil {
		w.failed.Add(1)
		return
	}
	w.mirrored.Add(1)
}

// cloneArgv deep-copies a command argv so a queued job is unaffected by the
// caller reusing its buffers after MirrorWrite returns.
func cloneArgv(cmd [][]byte) [][]byte {
	out := make([][]byte, len(cmd))
	for i, arg := range cmd {
		dup := make([]byte, len(arg))
		copy(dup, arg)
		out[i] = dup
	}
	return out
}

// ParseDualWriteFlag interprets the --dual-write flag value into a partial
// DualWriteConfig (Enabled + Target). The recognized values are:
//
//	"", "off", "none", "disabled" -> disabled
//	"pika"                        -> enabled, target "pika"
//
// The caller fills in the remaining config (prefix allowlist, queue size, etc.)
// from its own flags. Unknown non-empty values are treated as an enabled target
// of that name so future backends need no change here; callers that want strict
// validation can compare Target against their allowlist.
func ParseDualWriteFlag(value string) DualWriteConfig {
	v := strings.TrimSpace(strings.ToLower(value))
	switch v {
	case "", "off", "none", "disabled":
		return DualWriteConfig{Enabled: false}
	default:
		return DualWriteConfig{Enabled: true, Target: v}
	}
}
