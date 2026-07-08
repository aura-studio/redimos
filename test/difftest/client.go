package difftest

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

// Client is a minimal RESP2 client over a raw TCP connection. It intentionally
// does NOT use a full Redis client library: the whole point of the harness is
// to capture the exact reply bytes, so we speak the wire protocol directly and
// return raw frames via ReadReply.
type Client struct {
	addr    string
	conn    net.Conn
	reader  *bufio.Reader
	timeout time.Duration
}

// Dial opens a connection to a RESP2 endpoint (Pika oracle or redimos). The
// timeout bounds both the dial and each subsequent read/write so a stuck
// endpoint cannot hang the test suite.
func Dial(addr string, timeout time.Duration) (*Client, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("difftest: dial %s: %w", addr, err)
	}
	return &Client{
		addr:    addr,
		conn:    conn,
		reader:  bufio.NewReader(conn),
		timeout: timeout,
	}, nil
}

// Addr returns the endpoint address this client is connected to.
func (c *Client) Addr() string { return c.addr }

// Do sends a single command (encoded as a RESP array of bulk strings) and
// returns the raw bytes of exactly one reply.
func (c *Client) Do(args ...[]byte) ([]byte, error) {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return nil, err
	}
	if _, err := c.conn.Write(EncodeCommand(args...)); err != nil {
		return nil, fmt.Errorf("difftest: write to %s: %w", c.addr, err)
	}
	if err := c.conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return nil, err
	}
	reply, err := ReadReply(c.reader)
	if err != nil {
		return nil, fmt.Errorf("difftest: read from %s: %w", c.addr, err)
	}
	return reply, nil
}

// DoCmd is a convenience wrapper for a Command value.
func (c *Client) DoCmd(cmd Command) ([]byte, error) {
	return c.Do(cmd.Args...)
}

// Close releases the underlying connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
