package migrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// This file implements the full-import migration tool (task 21.4, milestone M3,
// requirements 17.1/17.2): a resumable importer that SCANs the legacy Pika
// source keyspace and writes every key into the proxy (DynamoDB via the proxy
// write path), plus a sampled consistency check that reports the match rate and
// verifies it against a configurable target (default 99.99%).
//
// The importer depends only on three small seams so it is unit-testable without
// a live Pika or DynamoDB, and so the migrate package carries no redis/dynamo
// dependency:
//
//   - ScanSource — SCAN the Pika keyspace (ScanKeys) and read a key's payload
//     (ReadKey). A real implementation wraps a redis client's SCAN + a per-type
//     dump/read; tests inject a fake.
//   - Sink       — write a key into the proxy (Write). For the consistency
//     check the sink must also be readable (ReadableSink.Read).
//   - Checkpoint — persist/restore the SCAN cursor for resume (Save/Load). A
//     file-backed default (FileCheckpoint) and an in-memory MemoryCheckpoint
//     are provided; tests may inject their own.
//
// Resume design: the importer drives SCAN by cursor. It Loads the last saved
// cursor on start and resumes SCAN from it, then Saves the new cursor after
// each page. Because SCAN is cursor-driven, resuming from a saved cursor
// naturally continues past the keys already imported in prior pages — the
// source simply returns the remaining keys. A cursor of 0 means "start a new
// scan"; a returned next-cursor of 0 means "scan complete".

// DefaultConsistencyTarget is the default sampled-consistency match rate the
// importer verifies against (requirement 17.2 / M3 exit criterion: ≥99.99%).
const DefaultConsistencyTarget = 0.9999

// ScanSource is the seam the importer uses to read the legacy Pika
// source-of-truth keyspace. It is intentionally minimal:
//
//   - ScanKeys returns one page of keys plus the next cursor, mirroring Redis
//     SCAN semantics: pass cursor 0 to begin; a returned next cursor of 0 means
//     the scan is complete. SCAN may return duplicate keys across pages, which
//     the importer tolerates (a re-import is idempotent for a Sink write).
//   - ReadKey reads enough of a key's value to re-create it through the proxy
//     write path. The payload is opaque to the importer; the Sink interprets it.
//
// A nil payload from ReadKey is treated as "key vanished between SCAN and read"
// (a benign race during a live import) and is skipped rather than written.
type ScanSource interface {
	ScanKeys(cursor uint64) (keys []string, next uint64, err error)
	ReadKey(key string) (payload []byte, err error)
}

// Sink writes a key's payload into the proxy (DynamoDB via the proxy write
// path). A real implementation replays the appropriate typed write; tests
// inject a fake that records writes.
type Sink interface {
	Write(ctx context.Context, key string, payload []byte) error
}

// ReadableSink is a Sink that can also read back a key's payload, required by
// the sampled consistency check so it can compare the imported value in the
// sink against the source value. Real proxy sinks implement Read via the proxy
// read path; the pattern mirrors ShadowPikaClient's optional-extension style so
// a write-only Sink stays usable for import even when Verify is not run.
type ReadableSink interface {
	Sink
	Read(ctx context.Context, key string) (payload []byte, err error)
}

// Checkpoint persists and restores the SCAN cursor so an interrupted import can
// resume. Save records the latest cursor; Load returns the saved cursor and
// whether one exists (ok=false on a fresh run with no prior checkpoint).
type Checkpoint interface {
	Save(cursor uint64) error
	Load() (cursor uint64, ok bool, err error)
}

// ImportConfig configures the importer. The zero value is usable: it imports
// every key with no throttling, aborts on the first error, and verifies against
// DefaultConsistencyTarget.
type ImportConfig struct {
	// Prefixes gates which keys are imported, mirroring the dual-write/shadow
	// gradual-rollout semantics (requirement 17.1 "按 key 前缀灰度"). Only keys
	// beginning with one of these prefixes are imported; others are counted in
	// Result.Skipped. An empty list imports every key (no prefix gate).
	Prefixes []string

	// ContinueOnError, when true, makes a per-key read or write failure
	// non-fatal: the failure is counted in Result.Failed and the import
	// proceeds to the next key. When false (the default), the first error
	// aborts Run and is returned.
	ContinueOnError bool

	// SaveEveryPages controls how often the SCAN cursor is checkpointed: the
	// cursor is Saved after every SaveEveryPages pages. Values <= 0 checkpoint
	// after every page (the safest resume granularity).
	SaveEveryPages int

	// PerKeyDelay optionally throttles the import by sleeping after each key
	// write, bounding write throughput against the sink. Zero means no delay.
	PerKeyDelay time.Duration

	// PerPageDelay optionally throttles the import by sleeping after each SCAN
	// page. Zero means no delay.
	PerPageDelay time.Duration

	// ConsistencyTarget is the match rate Verify checks against in
	// VerifyMeetsTarget. Values <= 0 fall back to DefaultConsistencyTarget.
	ConsistencyTarget float64
}

// Result is a summary of an import run.
type Result struct {
	// Imported is the number of keys successfully written to the sink.
	Imported uint64
	// Failed is the number of keys whose read or write failed (only nonzero
	// when ContinueOnError is true; otherwise Run aborts on the first error).
	Failed uint64
	// Skipped is the number of keys not imported because they did not match the
	// prefix allowlist, or vanished between SCAN and read (nil payload).
	Skipped uint64
	// Pages is the number of SCAN pages processed.
	Pages uint64
}

// Importer performs a resumable full import and a sampled consistency check.
// It is not safe for concurrent Run calls; a single Run drives the import
// sequentially so the checkpoint cursor advances monotonically.
type Importer struct {
	cfg    ImportConfig
	source ScanSource
	sink   Sink
	cp     Checkpoint
	target float64

	sleep func(time.Duration)

	rngMu sync.Mutex
	rng   *rand.Rand
}

// NewImporter builds an Importer from cfg and the injected seams. source and
// sink are required; cp may be nil, in which case a NoopCheckpoint is used
// (import runs but cannot resume).
func NewImporter(cfg ImportConfig, source ScanSource, sink Sink, cp Checkpoint) *Importer {
	if cp == nil {
		cp = NoopCheckpoint{}
	}
	target := cfg.ConsistencyTarget
	if target <= 0 {
		target = DefaultConsistencyTarget
	}
	return &Importer{
		cfg:    cfg,
		source: source,
		sink:   sink,
		cp:     cp,
		target: target,
		sleep:  time.Sleep,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SetRandSource replaces the sampling RNG source so tests can make Verify
// sampling deterministic. It is safe to call before Verify.
func (im *Importer) SetRandSource(src rand.Source) {
	im.rngMu.Lock()
	im.rng = rand.New(src)
	im.rngMu.Unlock()
}

// ConsistencyTarget reports the configured (or default) match-rate target.
func (im *Importer) ConsistencyTarget() float64 { return im.target }

// Run performs the full import. It resumes from the checkpointed SCAN cursor if
// one exists, then iterates SCAN pages, writing every eligible key into the
// sink and checkpointing the cursor after each page (or every SaveEveryPages
// pages). It returns a Result summarizing the run.
//
// Errors: a read/write failure aborts Run unless ContinueOnError is set (in
// which case it is counted in Result.Failed and the run continues). A SCAN or
// checkpoint error always aborts. If ctx is cancelled, Run returns the
// partial Result and ctx.Err(); the last saved checkpoint allows a later resume.
func (im *Importer) Run(ctx context.Context) (Result, error) {
	var res Result

	cursor, ok, err := im.cp.Load()
	if err != nil {
		return res, fmt.Errorf("load checkpoint: %w", err)
	}
	if !ok {
		cursor = 0
	}

	saveEvery := im.cfg.SaveEveryPages
	if saveEvery <= 0 {
		saveEvery = 1
	}

	for {
		if err := ctx.Err(); err != nil {
			return res, err
		}

		keys, next, err := im.source.ScanKeys(cursor)
		if err != nil {
			return res, fmt.Errorf("scan cursor %d: %w", cursor, err)
		}

		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return res, err
			}
			if !matchAnyPrefix(key, im.cfg.Prefixes) {
				res.Skipped++
				continue
			}
			if err := im.importKey(ctx, key, &res); err != nil {
				return res, err
			}
			if im.cfg.PerKeyDelay > 0 {
				im.sleep(im.cfg.PerKeyDelay)
			}
		}

		res.Pages++
		cursor = next

		if next == 0 || res.Pages%uint64(saveEvery) == 0 {
			if err := im.cp.Save(cursor); err != nil {
				return res, fmt.Errorf("save checkpoint: %w", err)
			}
		}

		if next == 0 {
			break
		}
		if im.cfg.PerPageDelay > 0 {
			im.sleep(im.cfg.PerPageDelay)
		}
	}

	return res, nil
}

// importKey reads one key from the source and writes it to the sink, updating
// res. A nil payload (key vanished between SCAN and read) is skipped. Read/write
// errors are counted and either swallowed (ContinueOnError) or returned.
func (im *Importer) importKey(ctx context.Context, key string, res *Result) error {
	payload, err := im.source.ReadKey(key)
	if err != nil {
		res.Failed++
		if im.cfg.ContinueOnError {
			return nil
		}
		return fmt.Errorf("read key %q: %w", key, err)
	}
	if payload == nil {
		// The key disappeared between SCAN and read (e.g. expired/deleted
		// during a live import). Nothing to write; count as skipped.
		res.Skipped++
		return nil
	}
	if err := im.sink.Write(ctx, key, payload); err != nil {
		res.Failed++
		if im.cfg.ContinueOnError {
			return nil
		}
		return fmt.Errorf("write key %q: %w", key, err)
	}
	res.Imported++
	return nil
}

// ErrSinkNotReadable is returned by Verify when the configured sink cannot be
// read back for comparison (it does not implement ReadableSink).
var ErrSinkNotReadable = errors.New("migrate: sink is not readable; cannot run consistency check")

// Verify performs a sampled consistency check: it re-SCANs the source keyspace
// and, for each eligible key selected by sampleRate, reads the value from both
// the source and the sink and compares them. It returns the match rate
// (matched/checked), the number of keys checked, and the number of mismatches.
//
// sampleRate is the sampling probability in [0,1]: 0 samples nothing, 1 samples
// every eligible key; values outside the range are clamped. When no keys are
// checked (empty keyspace or sampleRate 0) the match rate is reported as 1.0
// (there is nothing inconsistent).
//
// The sink must implement ReadableSink; otherwise Verify returns
// ErrSinkNotReadable. Read errors from the source or sink abort Verify.
func (im *Importer) Verify(ctx context.Context, sampleRate float64) (matchRate float64, checked int, mismatches int, err error) {
	rs, ok := im.sink.(ReadableSink)
	if !ok {
		return 0, 0, 0, ErrSinkNotReadable
	}

	if sampleRate < 0 {
		sampleRate = 0
	}
	if sampleRate > 1 {
		sampleRate = 1
	}

	var cursor uint64
	for {
		if err := ctx.Err(); err != nil {
			return 0, checked, mismatches, err
		}

		keys, next, scanErr := im.source.ScanKeys(cursor)
		if scanErr != nil {
			return 0, checked, mismatches, fmt.Errorf("verify scan cursor %d: %w", cursor, scanErr)
		}

		for _, key := range keys {
			if !matchAnyPrefix(key, im.cfg.Prefixes) {
				continue
			}
			if !im.shouldSample(sampleRate) {
				continue
			}

			srcVal, rErr := im.source.ReadKey(key)
			if rErr != nil {
				return 0, checked, mismatches, fmt.Errorf("verify read source key %q: %w", key, rErr)
			}
			sinkVal, rErr := rs.Read(ctx, key)
			if rErr != nil {
				return 0, checked, mismatches, fmt.Errorf("verify read sink key %q: %w", key, rErr)
			}

			checked++
			if !bytes.Equal(srcVal, sinkVal) {
				mismatches++
			}
		}

		cursor = next
		if next == 0 {
			break
		}
	}

	if checked == 0 {
		return 1.0, 0, 0, nil
	}
	matchRate = float64(checked-mismatches) / float64(checked)
	return matchRate, checked, mismatches, nil
}

// VerifyMeetsTarget runs Verify and reports whether the observed match rate
// meets or exceeds the configured ConsistencyTarget (default 99.99%). It
// returns the same detail as Verify alongside the ok verdict.
func (im *Importer) VerifyMeetsTarget(ctx context.Context, sampleRate float64) (ok bool, matchRate float64, checked int, mismatches int, err error) {
	matchRate, checked, mismatches, err = im.Verify(ctx, sampleRate)
	if err != nil {
		return false, matchRate, checked, mismatches, err
	}
	return matchRate >= im.target, matchRate, checked, mismatches, nil
}

// shouldSample decides whether a key is selected for the consistency check,
// applying rate. Rate <= 0 never samples; rate >= 1 always samples.
func (im *Importer) shouldSample(rate float64) bool {
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	im.rngMu.Lock()
	v := im.rng.Float64()
	im.rngMu.Unlock()
	return v < rate
}

// NoopCheckpoint is a Checkpoint that persists nothing: Save is a no-op and
// Load always reports no saved cursor. It lets an import run without resume
// support (e.g. a one-shot import) without special-casing a nil checkpoint.
type NoopCheckpoint struct{}

// Save discards the cursor.
func (NoopCheckpoint) Save(uint64) error { return nil }

// Load always reports no saved cursor.
func (NoopCheckpoint) Load() (uint64, bool, error) { return 0, false, nil }

// MemoryCheckpoint is an in-memory Checkpoint, useful for tests and for imports
// that only need resume within a single process lifetime. It is safe for
// concurrent use.
type MemoryCheckpoint struct {
	mu     sync.Mutex
	cursor uint64
	set    bool
}

// Save records the cursor in memory.
func (m *MemoryCheckpoint) Save(cursor uint64) error {
	m.mu.Lock()
	m.cursor = cursor
	m.set = true
	m.mu.Unlock()
	return nil
}

// Load returns the in-memory cursor and whether one has been saved.
func (m *MemoryCheckpoint) Load() (uint64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cursor, m.set, nil
}

// FileCheckpoint is a file-backed Checkpoint: it persists the SCAN cursor to a
// file as decimal text so an import can resume across process restarts. It is
// the default production checkpoint. Save writes atomically via a temp file +
// rename so a crash mid-write cannot corrupt the checkpoint.
type FileCheckpoint struct {
	// Path is the file the cursor is persisted to.
	Path string
}

// NewFileCheckpoint returns a FileCheckpoint persisting to path.
func NewFileCheckpoint(path string) *FileCheckpoint {
	return &FileCheckpoint{Path: path}
}

// Save atomically writes the cursor to the checkpoint file.
func (f *FileCheckpoint) Save(cursor uint64) error {
	if f.Path == "" {
		return errors.New("migrate: FileCheckpoint.Path is empty")
	}
	dir := filepath.Dir(f.Path)
	tmp, err := os.CreateTemp(dir, ".import-checkpoint-*")
	if err != nil {
		return fmt.Errorf("create temp checkpoint: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op if the rename below already moved it

	if _, err := tmp.WriteString(strconv.FormatUint(cursor, 10)); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp checkpoint: %w", err)
	}
	if err := os.Rename(tmpName, f.Path); err != nil {
		return fmt.Errorf("rename checkpoint into place: %w", err)
	}
	return nil
}

// Load reads the cursor from the checkpoint file. A missing file reports no
// saved cursor (ok=false) rather than an error, so a first run starts cleanly.
func (f *FileCheckpoint) Load() (uint64, bool, error) {
	if f.Path == "" {
		return 0, false, errors.New("migrate: FileCheckpoint.Path is empty")
	}
	data, err := os.ReadFile(f.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read checkpoint: %w", err)
	}
	s := strings.TrimSpace(string(data))
	if s == "" {
		return 0, false, nil
	}
	cursor, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse checkpoint %q: %w", s, err)
	}
	return cursor, true, nil
}
