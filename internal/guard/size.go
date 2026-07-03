// Package guard enforces size limits on keys, member names, and values before
// they reach the DynamoDB backend. A DynamoDB item is capped at 400KB; the
// proxy reserves headroom for the meta/attribute overhead by allowing values up
// to 390KB and key/member names up to 1KB.
//
// The guard is intentionally decoupled from the RESP and metrics layers: it
// returns the sentinel error ErrSizeExceeded and maintains an atomic
// interception counter that callers (metrics, migration hooks) read through
// Interceptions. The command layer is responsible for translating
// ErrSizeExceeded into the byte-for-byte wire text
// resp.ErrValueExceedsBackendLimit ("ERR value exceeds backend limit (400KB)").
//
// Enforcement happens before any backend write, so a rejected write never
// produces a partial or truncated item (requirement 14.3).
package guard

import (
	"errors"
	"sync/atomic"
)

// Size limits enforced before a backend write. Values are exact byte counts.
const (
	// MaxNameSize is the inclusive upper bound (1KB) for a key or member name.
	// A name of exactly MaxNameSize bytes is accepted; one byte more is
	// rejected. Requirement 14.1.
	MaxNameSize = 1 * 1024

	// MaxValueSize is the inclusive upper bound (390KB) for a value. A value of
	// exactly MaxValueSize bytes is accepted; one byte more is rejected. The
	// 400KB DynamoDB item hard limit minus this bound reserves room for the
	// meta and attribute overhead. Requirement 14.2.
	MaxValueSize = 390 * 1024
)

// ErrSizeExceeded is returned when a key, member name, or value exceeds its
// limit. Callers map it to the RESP error text
// "ERR value exceeds backend limit (400KB)". It is a sentinel; test with
// errors.Is.
var ErrSizeExceeded = errors.New("guard: size exceeds backend limit")

// interceptions counts write attempts rejected by the guard. Each rejected
// write is counted once, regardless of how many individual limits it breached.
// Requirement 14.4.
var interceptions atomic.Uint64

// Interceptions returns the total number of writes rejected by the guard since
// process start (or the last ResetInterceptions). It is safe for concurrent
// use and is the accessor consumed by the metrics and migration-hook layers.
func Interceptions() uint64 {
	return interceptions.Load()
}

// ResetInterceptions zeroes the interception counter. It exists for tests and
// is safe for concurrent use.
func ResetInterceptions() {
	interceptions.Store(0)
}

// nameTooLarge reports whether a key/member name exceeds MaxNameSize.
func nameTooLarge(name []byte) bool {
	return len(name) > MaxNameSize
}

// valueTooLarge reports whether a value exceeds MaxValueSize.
func valueTooLarge(value []byte) bool {
	return len(value) > MaxValueSize
}

// CheckKey validates a key name against MaxNameSize. On violation it counts one
// interception and returns ErrSizeExceeded; otherwise it returns nil.
func CheckKey(key []byte) error {
	if nameTooLarge(key) {
		interceptions.Add(1)
		return ErrSizeExceeded
	}
	return nil
}

// CheckMember validates a member name (hash field, set/zset/list member)
// against MaxNameSize. On violation it counts one interception and returns
// ErrSizeExceeded; otherwise it returns nil.
func CheckMember(member []byte) error {
	if nameTooLarge(member) {
		interceptions.Add(1)
		return ErrSizeExceeded
	}
	return nil
}

// CheckValue validates a value against MaxValueSize. On violation it counts one
// interception and returns ErrSizeExceeded; otherwise it returns nil.
func CheckValue(value []byte) error {
	if valueTooLarge(value) {
		interceptions.Add(1)
		return ErrSizeExceeded
	}
	return nil
}

// CheckValueSize validates a prospective value length in bytes against
// MaxValueSize without materializing the value. Read-modify-write commands such
// as SETRANGE can derive the resulting size from an offset before allocating the
// buffer, so an oversized result is rejected up front without a large
// allocation. On violation it counts one interception and returns
// ErrSizeExceeded; otherwise it returns nil. Requirement 14.2, 14.3.
func CheckValueSize(n int64) error {
	if n > int64(MaxValueSize) {
		interceptions.Add(1)
		return ErrSizeExceeded
	}
	return nil
}

// CheckWrite validates a complete write in one call: the key (name limit), each
// member name (name limit), and each value (value limit). It short-circuits on
// the first violation and counts exactly one interception for the rejected
// write, so it must not be combined with the single-field Check* helpers for
// the same write (that would double count).
//
// nil or empty members/values slices are allowed; a nil entry within a slice is
// treated as a zero-length name/value and always passes.
func CheckWrite(key []byte, members [][]byte, values [][]byte) error {
	if nameTooLarge(key) {
		interceptions.Add(1)
		return ErrSizeExceeded
	}
	for _, m := range members {
		if nameTooLarge(m) {
			interceptions.Add(1)
			return ErrSizeExceeded
		}
	}
	for _, v := range values {
		if valueTooLarge(v) {
			interceptions.Add(1)
			return ErrSizeExceeded
		}
	}
	return nil
}
