package integration

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"
)

// charsetPayloads returns the binary payloads every redimo-backed value, member,
// field and key must round-trip byte-for-byte: each of the 256 single-byte values,
// the full 0..255 sequence, and adversarial byte sequences (embedded CRLF that
// could break a naive RESP codec, embedded NUL, a RESP-injection lookalike).
func charsetPayloads() [][]byte {
	var ps [][]byte
	for i := 0; i < 256; i++ {
		ps = append(ps, []byte{byte(i)})
	}
	all := make([]byte, 256)
	for i := range all {
		all[i] = byte(i)
	}
	ps = append(ps, all)
	ps = append(ps,
		[]byte("a\r\nb"),
		[]byte("\r\n"),
		[]byte("a\x00b"),
		[]byte("$5\r\nhello\r\n"), // RESP-injection lookalike
		[]byte("\xff\xfe\xfd\x00\x01"),
		[]byte(""), // empty
	)
	return ps
}

// TestCharsetBinarySafety verifies that on the real redimo->DynamoDB path, every
// command family stores and returns arbitrary bytes losslessly — as VALUES, as
// collection MEMBERS/FIELDS, and as KEY names. This is a self-validating round-trip
// (what goes in comes back out), so it needs only the proxy (no oracle).
func TestCharsetBinarySafety(t *testing.T) {
	addr := proxyAddr(t)
	c := dial(t, addr)
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	payloads := charsetPayloads()

	key := func(fam string, i int) []byte {
		return []byte(fmt.Sprintf("cs:%s:%s:%d", nonce, fam, i))
	}
	wantBulk := func(t *testing.T, what string, reply, want []byte) {
		t.Helper()
		got, ok := bulkPayload(reply)
		if !ok {
			t.Errorf("%s: reply %q is not a bulk string", what, reply)
			return
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s: round-trip mismatch\n got=%q\nwant=%q", what, got, want)
		}
	}

	t.Run("string-value", func(t *testing.T) {
		for i, p := range payloads {
			k := key("strv", i)
			c.do(bs("SET"), k, p)
			wantBulk(t, fmt.Sprintf("GET value#%d", i), c.do(bs("GET"), k), p)
		}
	})

	t.Run("string-key-name", func(t *testing.T) {
		for i, p := range payloads {
			if len(p) == 0 {
				continue // empty key name is not meaningful
			}
			k := append(key("strk", i), p...) // key name embeds the binary payload
			c.do(bs("SET"), k, bs("v"))
			wantBulk(t, fmt.Sprintf("GET binkey#%d", i), c.do(bs("GET"), k), bs("v"))
		}
	})

	t.Run("hash-field-and-value", func(t *testing.T) {
		for i, p := range payloads {
			k := key("hash", i)
			// binary VALUE under a fixed field
			c.do(bs("HSET"), k, bs("f"), p)
			wantBulk(t, fmt.Sprintf("HGET value#%d", i), c.do(bs("HGET"), k, bs("f")), p)
			// binary FIELD name under a fixed value
			if len(p) > 0 {
				c.do(bs("HSET"), k, p, bs("V"))
				wantBulk(t, fmt.Sprintf("HGET binfield#%d", i), c.do(bs("HGET"), k, p), bs("V"))
			}
		}
	})

	t.Run("set-member", func(t *testing.T) {
		for i, p := range payloads {
			k := key("set", i)
			c.do(bs("SADD"), k, p)
			if got := c.do(bs("SISMEMBER"), k, p); !bytes.Equal(got, bs(":1\r\n")) {
				t.Errorf("SISMEMBER member#%d = %q, want :1", i, got)
			}
		}
	})

	t.Run("zset-member", func(t *testing.T) {
		for i, p := range payloads {
			k := key("zset", i)
			c.do(bs("ZADD"), k, bs("1"), p)
			wantBulk(t, fmt.Sprintf("ZSCORE member#%d", i), c.do(bs("ZSCORE"), k, p), bs("1"))
		}
	})

	t.Run("list-element", func(t *testing.T) {
		for i, p := range payloads {
			k := key("list", i)
			c.do(bs("RPUSH"), k, p)
			wantBulk(t, fmt.Sprintf("LINDEX element#%d", i), c.do(bs("LINDEX"), k, bs("0")), p)
		}
	})
}
