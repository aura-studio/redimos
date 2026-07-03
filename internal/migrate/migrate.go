// Package migrate holds the migration-period hooks: dual-write to Pika,
// shadow-read comparison, and read-only fallback with backfill.
//
// The hooks are designed to be composed behind a single Hooks aggregate that
// the command layer consults on its read/write paths. Each hook lives in its
// own file and is independently optional:
//
//   - dualwrite.go — mirror writes to Pika (task 21.1, requirement 17.1).
//   - shadow.go    — sampled shadow reads with diff logging (task 21.2, requirement 17.2).
//   - fallback.go  — read-only source-of-truth fallback + backfill (task 21.3, requirement 17.3).
//
// Hooks is intentionally a thin struct of pointers so later tasks can add their
// component without reshaping the existing dual-write wiring: a nil field means
// that hook is disabled, and every hook exposes nil-safe methods.
package migrate

// Hooks aggregates the optional migration-period components. The command layer
// holds one Hooks value (or none, when no migration flags are set) and calls
// into the relevant hook on its write/read paths. Fields are nil when the
// corresponding flag is not enabled; all hook methods are nil-safe so callers
// need not branch on nil.
//
// Task 21.1 populates DualWriter. Tasks 21.2/21.3 add ShadowReader and Fallback
// fields to this struct without touching the dual-write path.
type Hooks struct {
	// DualWriter mirrors write commands to Pika when --dual-write=pika is set.
	// Nil (or a disabled writer) means writes are not mirrored.
	DualWriter *DualWriter

	// ShadowReader samples read commands and compares against Pika when
	// --shadow-read=sample:<rate> is set (task 21.2, requirement 17.2). Nil (or
	// a disabled reader) means reads are not shadowed. Its methods are nil-safe.
	ShadowReader *ShadowReader

	// Fallback serves read-only source-of-truth fallback with backfill: on a
	// DynamoDB miss it reads the key from Pika and, if present, backfills it
	// into DynamoDB and returns it (task 21.3, requirement 17.3). Nil (or a
	// disabled fallback) means misses are not sourced from Pika. Its methods
	// are nil-safe.
	Fallback *Fallback

	// BigKeys surfaces the big-key / over-limit interception count for
	// migration visibility (task 21.3, requirement 17.4) without coupling
	// migrate to guard or metrics. Task 23.1 wires it to guard.Interceptions.
	// Nil is a valid no-op.
	BigKeys *BigKeyCounter
}

// matchAnyPrefix reports whether key begins with any of the supplied prefixes.
// An empty prefix list means "match everything", which the migration hooks use
// to express "no prefix gate configured — apply to all keys". The helper is
// shared so the shadow-read and fallback hooks (tasks 21.2/21.3) can gate on
// the same prefix-allowlist semantics as dual-write.
func matchAnyPrefix(key string, prefixes []string) bool {
	if len(prefixes) == 0 {
		return true
	}
	for _, p := range prefixes {
		if p == "" {
			// An empty prefix in the allowlist matches every key; treat it as a
			// wildcard so an operator can opt the whole keyspace in explicitly.
			return true
		}
		if len(key) >= len(p) && key[:len(p)] == p {
			return true
		}
	}
	return false
}
