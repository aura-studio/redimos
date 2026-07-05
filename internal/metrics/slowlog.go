package metrics

import (
	"fmt"
	"sync"
	"time"
)

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
