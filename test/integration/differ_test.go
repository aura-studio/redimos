package integration

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"
)

// differ is a shared two-endpoint comparison harness for the alignment dimensions: it
// sends the same command to the redimos proxy and to a live Redis 3.2 oracle and asserts
// the replies agree. eq compares raw bytes (exact wire parity); eqSorted compares
// order-unspecified array replies as sorted multisets. It is gated on both
// REDIMOS_PROXY_ADDR and REDIMOS_REDIS_ORACLE.
type differ struct {
	t      *testing.T
	p, o   *respConn
	prefix string
	n      int
}

func newDiffer(t *testing.T) *differ {
	return &differ{
		t:      t,
		p:      dial(t, proxyAddr(t)),
		o:      dial(t, oracleAddr(t)),
		prefix: strconv.FormatInt(time.Now().UnixNano(), 36),
	}
}

// k namespaces a key with a per-run nonce so the proxy, the oracle and successive runs
// never collide (redimos DEL reclaims members asynchronously, so reused keys can retain
// stale data across runs).
func (d *differ) k(name string) []byte {
	return bs(fmt.Sprintf("dt:%s:%s", d.prefix, name))
}

// eq sends args to both endpoints and fails on any byte difference.
func (d *differ) eq(desc string, args ...[]byte) {
	d.n++
	rp := d.p.do(args...)
	ro := d.o.do(args...)
	if !bytes.Equal(rp, ro) {
		d.t.Errorf("%s\n  cmd=%s\n  proxy =%q\n  oracle=%q", desc, joinArgs(args), rp, ro)
	}
}

// eqSorted compares array replies as sorted string multisets, for commands whose element
// order Redis does not specify (SMEMBERS/HKEYS/HVALS/HGETALL/SSCAN...). Identical raw
// replies pass immediately (covers both-error / both-empty). A non-array reply on either
// side is a mismatch.
func (d *differ) eqSorted(desc string, args ...[]byte) {
	d.n++
	rp := d.p.do(args...)
	ro := d.o.do(args...)
	if bytes.Equal(rp, ro) {
		return
	}
	sp, okp := respArrayElements(rp)
	so, oko := respArrayElements(ro)
	if !okp || !oko {
		d.t.Errorf("%s (sorted): non-array reply\n  proxy =%q\n  oracle=%q", desc, rp, ro)
		return
	}
	sort.Strings(sp)
	sort.Strings(so)
	if !reflect.DeepEqual(sp, so) {
		d.t.Errorf("%s (sorted)\n  cmd=%s\n  proxy =%v\n  oracle=%v", desc, joinArgs(args), sp, so)
	}
}

// eqFloatClose compares two bulk-string float replies numerically within a small relative
// tolerance rather than byte-for-byte. It exists for the ACCUMULATING float ops
// (INCRBYFLOAT / ZINCRBY): Redis accumulates in C long double (80-bit extended precision)
// while Go has only float64, so the results legitimately differ near the 17th significant
// digit. Direct score formatting (ZADD/ZSCORE of a set value) still uses eq (byte-for-byte)
// and does match. A gross error (wrong magnitude, NaN, wrong sign) still fails here.
func (d *differ) eqFloatClose(desc string, args ...[]byte) {
	d.n++
	rp := d.p.do(args...)
	ro := d.o.do(args...)
	fp, okp := bulkFloat(rp)
	fo, oko := bulkFloat(ro)
	if !okp || !oko {
		if !bytes.Equal(rp, ro) {
			d.t.Errorf("%s: non-float reply\n  proxy =%q\n  oracle=%q", desc, rp, ro)
		}
		return
	}
	tol := 1e-9 * math.Max(1, math.Abs(fo))
	if math.Abs(fp-fo) > tol {
		d.t.Errorf("%s: values differ beyond tolerance\n  proxy =%q (%g)\n  oracle=%q (%g)", desc, rp, fp, ro, fo)
	}
}

func bulkFloat(reply []byte) (float64, bool) {
	p, ok := bulkPayload(reply)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(string(p), 64)
	return f, err == nil
}

// eqIntClose compares two integer (:N) replies within tol, for values that can drift by a
// small amount between the two endpoints (a countdown TTL straddling a second boundary).
func (d *differ) eqIntClose(desc string, tol int64, args ...[]byte) {
	d.n++
	rp := d.p.do(args...)
	ro := d.o.do(args...)
	ip, okp := intReply(rp)
	io, oko := intReply(ro)
	if !okp || !oko {
		d.t.Errorf("%s: non-integer reply\n  proxy =%q\n  oracle=%q", desc, rp, ro)
		return
	}
	if diff := ip - io; diff > tol || diff < -tol {
		d.t.Errorf("%s: %d vs %d differ by more than %d", desc, ip, io, tol)
	}
}

func intReply(reply []byte) (int64, bool) {
	if len(reply) == 0 || reply[0] != ':' {
		return 0, false
	}
	line, _ := nextLine(reply)
	n, err := strconv.ParseInt(string(line[1:]), 10, 64)
	return n, err == nil
}

// --- Dimension Q: RESP2 wire-protocol parity ---------------------------------
//
// eqRaw/eqPipeline/eqRawFresh send a VERBATIM wire payload (an inline command, a
// hand-framed array, a zero-length bulk, an odd-cased command name, a malformed frame)
// that d.eq's canonical array framing cannot express, and byte-compare the reply against
// Redis 3.2. This is the wire layer the A-P dimensions never exercise.

// eqRaw sends payload verbatim on the shared connections and compares ONE reply. Use for
// WELL-FORMED wire inputs that leave the connection usable (inline commands, zero-length
// bulks, case variants) — NOT protocol errors, which may desync/close the connection.
func (d *differ) eqRaw(desc string, payload []byte) {
	d.n++
	rp := d.p.raw(payload)
	ro := d.o.raw(payload)
	if !bytes.Equal(rp, ro) {
		d.t.Errorf("%s\n  payload=%q\n  proxy =%q\n  oracle=%q", desc, payload, rp, ro)
	}
}

// eqPipeline sends payload (several commands in one write) and compares the n concatenated
// replies — verifying pipelining reply ORDER + framing parity.
func (d *differ) eqPipeline(desc string, payload []byte, n int) {
	d.n++
	rp := d.p.rawReplies(payload, n)
	ro := d.o.rawReplies(payload, n)
	if !bytes.Equal(rp, ro) {
		d.t.Errorf("%s (pipeline x%d)\n  payload=%q\n  proxy =%q\n  oracle=%q", desc, n, payload, rp, ro)
	}
}

// eqRawFresh is eqRaw on a FRESH connection per endpoint (dialed and discarded), for
// PROTOCOL-ERROR inputs: Redis replies "-ERR Protocol error: ..." and then CLOSES the
// connection, so a shared connection would be poisoned for later cases. Comparing the
// single error reply on a throwaway connection isolates the blast radius.
func (d *differ) eqRawFresh(desc string, payload []byte) {
	d.n++
	pp := dial(d.t, proxyAddr(d.t))
	oo := dial(d.t, oracleAddr(d.t))
	rp := pp.raw(payload)
	ro := oo.raw(payload)
	_ = pp.conn.Close()
	_ = oo.conn.Close()
	if !bytes.Equal(rp, ro) {
		d.t.Errorf("%s (fresh)\n  payload=%q\n  proxy =%q\n  oracle=%q", desc, payload, rp, ro)
	}
}

// respArrayElements decodes a RESP2 array-of-bulk-strings reply into its element payloads.
// ok is false when the reply is not an array (e.g. an error or a scalar). A nil element
// ($-1) decodes to "".
func respArrayElements(reply []byte) (elems []string, ok bool) {
	if len(reply) == 0 || reply[0] != '*' {
		return nil, false
	}
	i := 0
	line, rest := nextLine(reply[i:])
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return nil, false
	}
	if n < 0 {
		return nil, true // null array -> empty
	}
	buf := rest
	out := make([]string, 0, n)
	for j := 0; j < n; j++ {
		if len(buf) == 0 || buf[0] != '$' {
			return nil, false
		}
		hdr, after := nextLine(buf)
		l, err := strconv.Atoi(string(hdr[1:]))
		if err != nil {
			return nil, false
		}
		if l < 0 {
			out = append(out, "")
			buf = after
			continue
		}
		if len(after) < l+2 {
			return nil, false
		}
		out = append(out, string(after[:l]))
		buf = after[l+2:]
	}
	return out, true
}

// nextLine splits b at the first CRLF, returning the line (without CRLF) and the rest.
func nextLine(b []byte) (line, rest []byte) {
	idx := bytes.IndexByte(b, '\n')
	if idx < 0 {
		return b, nil
	}
	line = b[:idx]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line, b[idx+1:]
}
