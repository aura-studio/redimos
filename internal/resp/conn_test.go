package resp

import (
	"testing"

	"github.com/tidwall/redcon"
)

// fakeConn embeds redcon.Conn so it satisfies the full interface; only WriteRaw
// is implemented, capturing the exact bytes the Writer emits. Any other method
// call would panic on the nil embedded interface, which keeps the test honest:
// the Writer must go through WriteRaw exclusively.
type fakeConn struct {
	redcon.Conn
	written []byte
}

func (f *fakeConn) WriteRaw(data []byte) {
	f.written = append(f.written, data...)
}

func TestWriter(t *testing.T) {
	cases := []struct {
		name string
		fn   func(w *Writer)
		want string
	}{
		{"SimpleString", func(w *Writer) { w.SimpleString("OK") }, "+OK\r\n"},
		{"Error", func(w *Writer) { w.Error(ErrSyntax) }, "-ERR syntax error\r\n"},
		{"Int", func(w *Writer) { w.Int(-2) }, ":-2\r\n"},
		{"BulkString", func(w *Writer) { w.BulkString([]byte("hi")) }, "$2\r\nhi\r\n"},
		{"NullBulk", func(w *Writer) { w.NullBulk() }, "$-1\r\n"},
		{"EmptyArray", func(w *Writer) { w.EmptyArray() }, "*0\r\n"},
		{"NullArray", func(w *Writer) { w.NullArray() }, "*-1\r\n"},
		{"BulkArray", func(w *Writer) { w.BulkArray([][]byte{[]byte("a")}) }, "*1\r\n$1\r\na\r\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fc := &fakeConn{}
			w := NewWriter(fc)
			c.fn(w)
			if got := string(fc.written); got != c.want {
				t.Errorf("%s wrote %q, want %q", c.name, got, c.want)
			}
		})
	}
}

// TestWriterBufferReuse confirms sequential writes on the same Writer produce
// correct, non-corrupted output despite the shared internal buffer.
func TestWriterBufferReuse(t *testing.T) {
	fc := &fakeConn{}
	w := NewWriter(fc)
	w.SimpleString("OK")
	w.Int(1)
	w.NullBulk()
	if got, want := string(fc.written), "+OK\r\n:1\r\n$-1\r\n"; got != want {
		t.Errorf("sequential writes = %q, want %q", got, want)
	}
}
