package meta

import (
	"fmt"
	"log"
	"sort"
	"strings"
)

// Logger is an optional structured sink for the background workers' (lazy deleter,
// orphan sweeper) error events. Set it on DeleterConfig/SweeperConfig to receive
// machine-parseable op/pk/duration/error fields instead of the opaque OnError callback.
// When both a Logger and the legacy OnError callback are supplied, the Logger takes
// precedence. Implementations run on the worker goroutine and must not block.
type Logger interface {
	Error(op string, fields map[string]any)
}

// StdLogger is the default Logger: it emits one "redimos: <op> k=v ..." line per event via
// the standard log package, with keys sorted for stable, grep-friendly output.
type StdLogger struct{}

// Error implements Logger.
func (StdLogger) Error(op string, fields map[string]any) {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	fmt.Fprintf(&b, "redimos: %s", op)
	for _, k := range keys {
		fmt.Fprintf(&b, " %s=%v", k, fields[k])
	}

	log.Print(b.String())
}
