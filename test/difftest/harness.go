package difftest

import (
	"bytes"
	"fmt"
	"strings"
	"time"
)

// Command is a single RESP2 command: the command name followed by its
// arguments, all held as raw bytes for binary safety.
type Command struct {
	Args [][]byte
}

// Cmd builds a Command from string arguments (the common case).
func Cmd(args ...string) Command {
	raw := make([][]byte, len(args))
	for i, a := range args {
		raw[i] = []byte(a)
	}
	return Command{Args: raw}
}

// CmdRaw builds a Command from raw byte arguments (for binary-safe values).
func CmdRaw(args ...[]byte) Command {
	return Command{Args: args}
}

// String renders the command for human-readable diff output. Non-printable or
// oversized arguments are summarized so the log stays readable.
func (c Command) String() string {
	parts := make([]string, len(c.Args))
	for i, a := range c.Args {
		parts[i] = quoteArg(a)
	}
	return strings.Join(parts, " ")
}

func quoteArg(a []byte) string {
	const max = 32
	printable := true
	for _, b := range a {
		if b < 0x20 || b > 0x7e {
			printable = false
			break
		}
	}
	if printable && len(a) <= max {
		return string(a)
	}
	if len(a) > max {
		return fmt.Sprintf("<%d bytes>", len(a))
	}
	return fmt.Sprintf("%q", a)
}

// Sequence is an ordered list of commands executed against both endpoints on a
// fresh pair of connections.
type Sequence struct {
	Name     string
	Commands []Command
}

// Diff records a single byte-level mismatch between the two endpoints for one
// command in a sequence.
type Diff struct {
	Sequence string
	Index    int
	Command  Command
	Oracle   []byte // raw reply bytes from the Pika oracle
	Redimos  []byte // raw reply bytes from redimos
	Err      error  // transport-level error, if any (non-nil means comparison aborted)
}

// String renders a Diff for test failure output, showing the raw bytes with
// CRLFs made visible so subtle differences (trailing spaces, $-1 vs *0 vs *-1)
// are obvious.
func (d Diff) String() string {
	if d.Err != nil {
		return fmt.Sprintf("[%s #%d] %s: transport error: %v",
			d.Sequence, d.Index, d.Command, d.Err)
	}
	return fmt.Sprintf("[%s #%d] %s\n  oracle : %s\n  redimos: %s",
		d.Sequence, d.Index, d.Command, visualize(d.Oracle), visualize(d.Redimos))
}

func visualize(b []byte) string {
	s := string(b)
	s = strings.ReplaceAll(s, "\r", "\\r")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

// Endpoints holds the two addresses under differential test.
type Endpoints struct {
	OracleAddr  string        // Pika v3.2.2 oracle (PIKA_ADDR)
	RedimosAddr string        // redimos proxy under test (REDIMOS_ADDR)
	Timeout     time.Duration // per-operation timeout
}

// CompareSequence runs a single sequence against both endpoints on fresh
// connections and returns any byte-level diffs. Each command's reply is read
// from both endpoints and compared with bytes.Equal. A transport error against
// either endpoint is recorded as a Diff with Err set and aborts that sequence.
func CompareSequence(ep Endpoints, seq Sequence) ([]Diff, error) {
	oracle, err := Dial(ep.OracleAddr, ep.Timeout)
	if err != nil {
		return nil, err
	}
	defer oracle.Close()

	proxy, err := Dial(ep.RedimosAddr, ep.Timeout)
	if err != nil {
		return nil, err
	}
	defer proxy.Close()

	var diffs []Diff
	for i, cmd := range seq.Commands {
		oReply, oErr := oracle.DoCmd(cmd)
		pReply, pErr := proxy.DoCmd(cmd)

		if oErr != nil || pErr != nil {
			diffs = append(diffs, Diff{
				Sequence: seq.Name,
				Index:    i,
				Command:  cmd,
				Oracle:   oReply,
				Redimos:  pReply,
				Err:      firstErr(oErr, pErr),
			})
			// A transport error leaves the connections in an unknown state;
			// stop this sequence but let the caller continue with others.
			break
		}

		if !bytes.Equal(oReply, pReply) {
			diffs = append(diffs, Diff{
				Sequence: seq.Name,
				Index:    i,
				Command:  cmd,
				Oracle:   oReply,
				Redimos:  pReply,
			})
		}
	}
	return diffs, nil
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
