package migrate

import (
	"context"
	"errors"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
)

// scanPage is one page the fake source returns for a given cursor.
type scanPage struct {
	keys []string
	next uint64
}

// fakeImportSource is an injectable ScanSource for tests. Pages are keyed by the
// incoming cursor; ScanKeys(cursor) returns pages[cursor]. Per-key values are
// read from values; optional read/scan errors let a test exercise failures.
type fakeImportSource struct {
	mu       sync.Mutex
	pages    map[uint64]scanPage
	values   map[string][]byte
	readErrs map[string]error
	scanErrs map[uint64]error
	scanned  []uint64 // cursors requested, in order
	reads    []string // keys read, in order
}

func (s *fakeImportSource) ScanKeys(cursor uint64) ([]string, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scanned = append(s.scanned, cursor)
	if err, ok := s.scanErrs[cursor]; ok {
		return nil, 0, err
	}
	p := s.pages[cursor]
	return p.keys, p.next, nil
}

func (s *fakeImportSource) ReadKey(key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads = append(s.reads, key)
	if err, ok := s.readErrs[key]; ok {
		return nil, err
	}
	v, ok := s.values[key]
	if !ok {
		return nil, nil // vanished between SCAN and read
	}
	return append([]byte(nil), v...), nil
}

// fakeSink is an injectable ReadableSink for tests. It records writes, can be
// made to fail specific writes/reads, and can override the value Read returns
// (used to simulate an inconsistency for the verify check).
type fakeSink struct {
	mu           sync.Mutex
	writes       map[string][]byte
	writeErrs    map[string]error
	readErrs     map[string]error
	readOverride map[string][]byte
}

func newFakeSink() *fakeSink {
	return &fakeSink{
		writes:       map[string][]byte{},
		writeErrs:    map[string]error{},
		readErrs:     map[string]error{},
		readOverride: map[string][]byte{},
	}
}

func (k *fakeSink) Write(_ context.Context, key string, payload []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if err, ok := k.writeErrs[key]; ok {
		return err
	}
	k.writes[key] = append([]byte(nil), payload...)
	return nil
}

func (k *fakeSink) Read(_ context.Context, key string) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if err, ok := k.readErrs[key]; ok {
		return nil, err
	}
	if v, ok := k.readOverride[key]; ok {
		return append([]byte(nil), v...), nil
	}
	v, ok := k.writes[key]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), v...), nil
}

func (k *fakeSink) count() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.writes)
}

// threePageSource builds a source with 5 keys spread over 3 SCAN pages:
//
//	cursor 0 -> [k1, k2] next 1
//	cursor 1 -> [k3, k4] next 2
//	cursor 2 -> [k5]     next 0 (done)
func threePageSource() *fakeImportSource {
	return &fakeImportSource{
		pages: map[uint64]scanPage{
			0: {keys: []string{"k1", "k2"}, next: 1},
			1: {keys: []string{"k3", "k4"}, next: 2},
			2: {keys: []string{"k5"}, next: 0},
		},
		values: map[string][]byte{
			"k1": []byte("v1"),
			"k2": []byte("v2"),
			"k3": []byte("v3"),
			"k4": []byte("v4"),
			"k5": []byte("v5"),
		},
		readErrs: map[string]error{},
		scanErrs: map[uint64]error{},
	}
}

func TestImporter_FullImportWritesEveryScannedKey(t *testing.T) {
	src := threePageSource()
	sink := newFakeSink()
	cp := &MemoryCheckpoint{}
	im := NewImporter(ImportConfig{}, src, sink, cp)

	res, err := im.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Imported != 5 || res.Failed != 0 || res.Skipped != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if res.Pages != 3 {
		t.Fatalf("expected 3 pages, got %d", res.Pages)
	}
	if sink.count() != 5 {
		t.Fatalf("expected 5 keys written, got %d", sink.count())
	}
	for _, k := range []string{"k1", "k2", "k3", "k4", "k5"} {
		if got := sink.writes[k]; string(got) != "v"+k[1:] {
			t.Fatalf("key %q: expected value %q, got %q", k, "v"+k[1:], got)
		}
	}
	// Scan completed => final checkpoint cursor is 0.
	if c, ok, _ := cp.Load(); !ok || c != 0 {
		t.Fatalf("expected final checkpoint cursor 0 (done), got cursor=%d ok=%v", c, ok)
	}
}

func TestImporter_ResumeFromCheckpointSkipsImportedKeys(t *testing.T) {
	src := threePageSource()
	sink := newFakeSink()
	// Simulate a prior run that imported page 0 (k1,k2) and checkpointed cursor 1.
	cp := &MemoryCheckpoint{}
	if err := cp.Save(1); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	im := NewImporter(ImportConfig{}, src, sink, cp)

	res, err := im.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only k3, k4, k5 should be imported on resume.
	if res.Imported != 3 {
		t.Fatalf("expected 3 imported on resume, got %d (result %+v)", res.Imported, res)
	}
	if sink.count() != 3 {
		t.Fatalf("expected 3 keys written on resume, got %d", sink.count())
	}
	for _, skipped := range []string{"k1", "k2"} {
		if _, ok := sink.writes[skipped]; ok {
			t.Fatalf("key %q should have been skipped on resume", skipped)
		}
	}
	for _, want := range []string{"k3", "k4", "k5"} {
		if _, ok := sink.writes[want]; !ok {
			t.Fatalf("key %q should have been imported on resume", want)
		}
	}
	// The source must have been scanned starting from the saved cursor 1.
	if len(src.scanned) == 0 || src.scanned[0] != 1 {
		t.Fatalf("expected resume to start SCAN at cursor 1, scanned=%v", src.scanned)
	}
}

func TestImporter_WriteFailureCountedAndDoesNotAbort(t *testing.T) {
	src := threePageSource()
	sink := newFakeSink()
	sink.writeErrs["k3"] = errors.New("sink write failed")
	cp := &MemoryCheckpoint{}
	im := NewImporter(ImportConfig{ContinueOnError: true}, src, sink, cp)

	res, err := im.Run(context.Background())
	if err != nil {
		t.Fatalf("ContinueOnError run must not return an error, got %v", err)
	}
	if res.Failed != 1 {
		t.Fatalf("expected 1 failed, got %d (result %+v)", res.Failed, res)
	}
	if res.Imported != 4 {
		t.Fatalf("expected 4 imported (5 minus the failing key), got %d", res.Imported)
	}
	if _, ok := sink.writes["k3"]; ok {
		t.Fatal("failing key k3 must not be recorded as written")
	}
	// The run continued past the failure: k5 (on a later page) was imported.
	if _, ok := sink.writes["k5"]; !ok {
		t.Fatal("import must continue to later pages after a per-key failure")
	}
}

func TestImporter_WriteFailureAbortsWhenNotContinueOnError(t *testing.T) {
	src := threePageSource()
	sink := newFakeSink()
	sink.writeErrs["k1"] = errors.New("sink write failed")
	im := NewImporter(ImportConfig{ContinueOnError: false}, src, sink, &MemoryCheckpoint{})

	res, err := im.Run(context.Background())
	if err == nil {
		t.Fatal("expected Run to abort on the first write failure")
	}
	if res.Imported != 0 || res.Failed != 1 {
		t.Fatalf("expected 0 imported / 1 failed at abort, got %+v", res)
	}
}

func TestImporter_ReadFailureCountedAndSkipsWrite(t *testing.T) {
	src := threePageSource()
	src.readErrs["k2"] = errors.New("source read failed")
	sink := newFakeSink()
	im := NewImporter(ImportConfig{ContinueOnError: true}, src, sink, &MemoryCheckpoint{})

	res, err := im.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Failed != 1 || res.Imported != 4 {
		t.Fatalf("expected 1 failed / 4 imported, got %+v", res)
	}
	if _, ok := sink.writes["k2"]; ok {
		t.Fatal("k2 read failed; it must not be written")
	}
}

func TestImporter_PrefixGatingSkipsNonMatchingKeys(t *testing.T) {
	src := &fakeImportSource{
		pages: map[uint64]scanPage{
			0: {keys: []string{"user:1", "order:1", "user:2"}, next: 0},
		},
		values: map[string][]byte{
			"user:1":  []byte("a"),
			"order:1": []byte("b"),
			"user:2":  []byte("c"),
		},
		readErrs: map[string]error{},
		scanErrs: map[uint64]error{},
	}
	sink := newFakeSink()
	im := NewImporter(ImportConfig{Prefixes: []string{"user:"}}, src, sink, &MemoryCheckpoint{})

	res, err := im.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Imported != 2 || res.Skipped != 1 {
		t.Fatalf("expected 2 imported / 1 skipped, got %+v", res)
	}
	if _, ok := sink.writes["order:1"]; ok {
		t.Fatal("non-matching key order:1 must be skipped")
	}
}

func TestImporter_NilPayloadSkipped(t *testing.T) {
	src := &fakeImportSource{
		pages: map[uint64]scanPage{
			0: {keys: []string{"gone", "here"}, next: 0},
		},
		values:   map[string][]byte{"here": []byte("v")}, // "gone" has no value => nil payload
		readErrs: map[string]error{},
		scanErrs: map[uint64]error{},
	}
	sink := newFakeSink()
	im := NewImporter(ImportConfig{}, src, sink, &MemoryCheckpoint{})

	res, err := im.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Imported != 1 || res.Skipped != 1 {
		t.Fatalf("expected 1 imported / 1 skipped for vanished key, got %+v", res)
	}
}

func TestImporter_ScanErrorAborts(t *testing.T) {
	src := threePageSource()
	src.scanErrs[1] = errors.New("scan failed")
	sink := newFakeSink()
	im := NewImporter(ImportConfig{}, src, sink, &MemoryCheckpoint{})

	if _, err := im.Run(context.Background()); err == nil {
		t.Fatal("expected a SCAN error to abort Run")
	}
}

func TestImporter_ContextCancellationReturnsPartial(t *testing.T) {
	src := threePageSource()
	sink := newFakeSink()
	im := NewImporter(ImportConfig{}, src, sink, &MemoryCheckpoint{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before running

	res, err := im.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if res.Imported != 0 {
		t.Fatalf("expected no imports after immediate cancel, got %+v", res)
	}
}

func TestImporter_VerifyAllMatch(t *testing.T) {
	src := threePageSource()
	sink := newFakeSink()
	im := NewImporter(ImportConfig{}, src, sink, &MemoryCheckpoint{})
	if _, err := im.Run(context.Background()); err != nil {
		t.Fatalf("import failed: %v", err)
	}

	matchRate, checked, mismatches, err := im.Verify(context.Background(), 1.0)
	if err != nil {
		t.Fatalf("verify error: %v", err)
	}
	if checked != 5 || mismatches != 0 {
		t.Fatalf("expected 5 checked / 0 mismatches, got checked=%d mismatches=%d", checked, mismatches)
	}
	if matchRate != 1.0 {
		t.Fatalf("expected match rate 1.0, got %v", matchRate)
	}
}

func TestImporter_VerifyFlagsMismatch(t *testing.T) {
	src := threePageSource()
	sink := newFakeSink()
	im := NewImporter(ImportConfig{}, src, sink, &MemoryCheckpoint{})
	if _, err := im.Run(context.Background()); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	// Corrupt one key's value in the sink so verify sees a mismatch.
	sink.readOverride["k3"] = []byte("corrupted")

	matchRate, checked, mismatches, err := im.Verify(context.Background(), 1.0)
	if err != nil {
		t.Fatalf("verify error: %v", err)
	}
	if checked != 5 || mismatches != 1 {
		t.Fatalf("expected 5 checked / 1 mismatch, got checked=%d mismatches=%d", checked, mismatches)
	}
	want := float64(4) / float64(5)
	if matchRate != want {
		t.Fatalf("expected match rate %v, got %v", want, matchRate)
	}
}

func TestImporter_VerifyMeetsTarget(t *testing.T) {
	src := threePageSource()
	sink := newFakeSink()
	im := NewImporter(ImportConfig{}, src, sink, &MemoryCheckpoint{})
	if _, err := im.Run(context.Background()); err != nil {
		t.Fatalf("import failed: %v", err)
	}

	// All match => meets the default 0.9999 target.
	ok, rate, _, _, err := im.VerifyMeetsTarget(context.Background(), 1.0)
	if err != nil {
		t.Fatalf("verify error: %v", err)
	}
	if !ok {
		t.Fatalf("expected target met when all keys match, rate=%v target=%v", rate, im.ConsistencyTarget())
	}

	// One mismatch out of five => 0.8, below the 0.9999 target.
	sink.readOverride["k1"] = []byte("corrupted")
	ok, rate, _, mismatches, err := im.VerifyMeetsTarget(context.Background(), 1.0)
	if err != nil {
		t.Fatalf("verify error: %v", err)
	}
	if ok {
		t.Fatalf("expected target NOT met with a mismatch, rate=%v mismatches=%d", rate, mismatches)
	}
}

func TestImporter_VerifyEmptyKeyspaceReportsFullMatch(t *testing.T) {
	src := &fakeImportSource{
		pages:    map[uint64]scanPage{0: {keys: nil, next: 0}},
		values:   map[string][]byte{},
		readErrs: map[string]error{},
		scanErrs: map[uint64]error{},
	}
	sink := newFakeSink()
	im := NewImporter(ImportConfig{}, src, sink, &MemoryCheckpoint{})

	matchRate, checked, mismatches, err := im.Verify(context.Background(), 1.0)
	if err != nil {
		t.Fatalf("verify error: %v", err)
	}
	if checked != 0 || mismatches != 0 || matchRate != 1.0 {
		t.Fatalf("empty keyspace: expected 0 checked / 0 mismatch / rate 1.0, got checked=%d mismatches=%d rate=%v", checked, mismatches, matchRate)
	}
}

func TestImporter_VerifySamplingRateSelectsSubset(t *testing.T) {
	src := threePageSource()
	sink := newFakeSink()
	im := NewImporter(ImportConfig{}, src, sink, &MemoryCheckpoint{})
	if _, err := im.Run(context.Background()); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	// Deterministic RNG so the sampled subset is reproducible.
	im.SetRandSource(rand.NewSource(1))

	_, checked, _, err := im.Verify(context.Background(), 0.5)
	if err != nil {
		t.Fatalf("verify error: %v", err)
	}
	if checked <= 0 || checked >= 5 {
		t.Fatalf("expected a strict subset (0 < checked < 5) at rate 0.5, got %d", checked)
	}
}

// pureWriteSink implements only Sink.
type pureWriteSink struct{}

func (pureWriteSink) Write(context.Context, string, []byte) error { return nil }

func TestImporter_VerifyErrsWhenSinkNotReadable(t *testing.T) {
	src := threePageSource()
	im := NewImporter(ImportConfig{}, src, pureWriteSink{}, &MemoryCheckpoint{})

	_, _, _, err := im.Verify(context.Background(), 1.0)
	if !errors.Is(err, ErrSinkNotReadable) {
		t.Fatalf("expected ErrSinkNotReadable, got %v", err)
	}
}

func TestFileCheckpoint_SaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint")
	cp := NewFileCheckpoint(path)

	// Missing file => no saved cursor, no error.
	if c, ok, err := cp.Load(); err != nil || ok || c != 0 {
		t.Fatalf("missing file: expected (0,false,nil), got (%d,%v,%v)", c, ok, err)
	}

	if err := cp.Save(4242); err != nil {
		t.Fatalf("save: %v", err)
	}
	c, ok, err := cp.Load()
	if err != nil || !ok || c != 4242 {
		t.Fatalf("expected (4242,true,nil), got (%d,%v,%v)", c, ok, err)
	}

	// Overwrite persists the latest cursor.
	if err := cp.Save(0); err != nil {
		t.Fatalf("save 0: %v", err)
	}
	if c, ok, err := cp.Load(); err != nil || !ok || c != 0 {
		t.Fatalf("expected (0,true,nil) after saving 0, got (%d,%v,%v)", c, ok, err)
	}
}

func TestFileCheckpoint_EnablesResumeAcrossInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint")

	// First importer instance: import page 0 only, then simulate interruption
	// by using a file checkpoint and stopping after the first page via a source
	// whose page 0 has next=1 and page 1 errors.
	src := threePageSource()
	sink := newFakeSink()
	cp := NewFileCheckpoint(path)
	if err := cp.Save(1); err != nil { // pretend page 0 done, cursor at 1
		t.Fatalf("seed: %v", err)
	}

	// New importer instance reads the file checkpoint and resumes at cursor 1.
	im := NewImporter(ImportConfig{}, src, sink, cp)
	res, err := im.Run(context.Background())
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if res.Imported != 3 {
		t.Fatalf("expected 3 imported on file-checkpoint resume, got %d", res.Imported)
	}
	if src.scanned[0] != 1 {
		t.Fatalf("expected SCAN to resume at cursor 1, scanned=%v", src.scanned)
	}
}

func TestNoopCheckpoint_NeverResumes(t *testing.T) {
	var cp NoopCheckpoint
	if err := cp.Save(99); err != nil {
		t.Fatalf("noop save: %v", err)
	}
	if c, ok, err := cp.Load(); err != nil || ok || c != 0 {
		t.Fatalf("noop load: expected (0,false,nil), got (%d,%v,%v)", c, ok, err)
	}
}
