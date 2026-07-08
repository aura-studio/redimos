package redimos

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// TestBufConn_RoundTrip checks that bytes written on one end are read on the other,
// in order, across both directions.
func TestBufConn_RoundTrip(t *testing.T) {
	a, b := newBufConnPair()

	if _, err := a.Write([]byte("ping")); err != nil {
		t.Fatalf("a.Write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(b, buf); err != nil {
		t.Fatalf("b.Read: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("b read %q; want ping", buf)
	}

	if _, err := b.Write([]byte("pong")); err != nil {
		t.Fatalf("b.Write: %v", err)
	}
	if _, err := io.ReadFull(a, buf); err != nil {
		t.Fatalf("a.Read: %v", err)
	}
	if string(buf) != "pong" {
		t.Fatalf("a read %q; want pong", buf)
	}
}

// TestBufConn_PipelinedWritesDoNotDeadlock is the property net.Pipe fails: many writes
// with NO concurrent reader must all return without blocking (the buffer is unbounded),
// then a later reader drains them all. go-redis pipelines its handshake this way.
func TestBufConn_PipelinedWritesDoNotDeadlock(t *testing.T) {
	a, b := newBufConnPair()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			if _, err := a.Write([]byte("0123456789")); err != nil {
				t.Errorf("write %d: %v", i, err)
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("pipelined writes deadlocked (no concurrent reader)")
	}

	// Drain everything the peer buffered.
	want := 1000 * 10
	got := 0
	buf := make([]byte, 4096)
	_ = b.SetReadDeadline(time.Now().Add(2 * time.Second))
	for got < want {
		n, err := b.Read(buf)
		got += n
		if err != nil {
			t.Fatalf("read after %d bytes: %v", got, err)
		}
	}
	if got != want {
		t.Fatalf("drained %d bytes; want %d", got, want)
	}
}

// TestBufConn_ConcurrentReadWrite streams data through the pair from a writer goroutine
// while a reader goroutine consumes it, and checks the bytes arrive intact.
func TestBufConn_ConcurrentReadWrite(t *testing.T) {
	a, b := newBufConnPair()
	payload := bytes.Repeat([]byte("abcdefgh"), 4096) // 32 KiB

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer a.Close()
		if _, err := a.Write(payload); err != nil {
			t.Errorf("write: %v", err)
		}
	}()

	var got []byte
	go func() {
		defer wg.Done()
		buf := make([]byte, 1000)
		for {
			n, err := b.Read(buf)
			got = append(got, buf[:n]...)
			if err == io.EOF {
				return
			}
			if err != nil {
				t.Errorf("read: %v", err)
				return
			}
		}
	}()

	wg.Wait()
	if !bytes.Equal(got, payload) {
		t.Fatalf("received %d bytes, want %d; equal=%v", len(got), len(payload), bytes.Equal(got, payload))
	}
}

// TestBufConn_ReadDeadlineWakesBlockedRead is the property a real socket has and that
// go-redis relies on for timeouts: a Read blocked with no data must wake and return a
// timeout error once the read deadline passes.
func TestBufConn_ReadDeadlineWakesBlockedRead(t *testing.T) {
	a, _ := newBufConnPair()

	_ = a.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	start := time.Now()
	buf := make([]byte, 8)
	_, err := a.Read(buf)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("blocked Read returned nil error; want timeout")
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("Read err = %v; want a net.Error with Timeout()==true", err)
	}
	if elapsed < 80*time.Millisecond {
		t.Fatalf("Read returned after %s; want it to block until ~the 100ms deadline", elapsed)
	}
}

// TestBufConn_CloseWakesBlockedRead checks that closing a conn unblocks a Read that is
// waiting for data, surfacing EOF (so redcon's per-connection loop returns on close).
func TestBufConn_CloseWakesBlockedRead(t *testing.T) {
	a, _ := newBufConnPair()

	errc := make(chan error, 1)
	go func() {
		buf := make([]byte, 8)
		_, err := a.Read(buf)
		errc <- err
	}()

	time.Sleep(50 * time.Millisecond) // let the Read block
	_ = a.Close()

	select {
	case err := <-errc:
		if err != io.EOF {
			t.Fatalf("Read after Close = %v; want io.EOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake the blocked Read")
	}
}

// TestBufConn_WriteAfterCloseFails checks a write to a closed direction reports an
// error rather than silently succeeding.
func TestBufConn_WriteAfterCloseFails(t *testing.T) {
	a, _ := newBufConnPair()
	_ = a.Close()
	if _, err := a.Write([]byte("x")); err == nil {
		t.Fatal("Write after Close returned nil error; want a closed error")
	}
}
