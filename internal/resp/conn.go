package resp

import "github.com/tidwall/redcon"

// Writer emits RESP2 replies to a redcon connection using the Append* encoders
// as the source of truth, then flushes the exact bytes through Conn.WriteRaw.
//
// Going through WriteRaw (rather than redcon's typed Write* methods) keeps
// byte-for-byte control over the null bulk ($-1), empty array (*0), and null
// array (*-1) distinctions that clients rely on, while still letting redcon own
// connection lifecycle and buffering.
type Writer struct {
	conn redcon.Conn
	// buf is reused across writes on the same connection to avoid per-reply
	// allocations. redcon copies the bytes during WriteRaw, so reuse is safe.
	buf []byte
}

// NewWriter wraps a redcon connection.
func NewWriter(conn redcon.Conn) *Writer {
	return &Writer{conn: conn}
}

// flush writes the staged buffer as raw bytes and resets it for reuse.
func (w *Writer) flush() {
	w.conn.WriteRaw(w.buf)
	w.buf = w.buf[:0]
}

// SimpleString writes "+<s>\r\n".
func (w *Writer) SimpleString(s string) {
	w.buf = AppendSimpleString(w.buf[:0], s)
	w.flush()
}

// Error writes "-<msg>\r\n". msg is an error body such as an ErrXxx constant.
func (w *Writer) Error(msg string) {
	w.buf = AppendError(w.buf[:0], msg)
	w.flush()
}

// Int writes ":<n>\r\n".
func (w *Writer) Int(n int64) {
	w.buf = AppendInt(w.buf[:0], n)
	w.flush()
}

// BulkString writes "$<len>\r\n<bytes>\r\n".
func (w *Writer) BulkString(b []byte) {
	w.buf = AppendBulkString(w.buf[:0], b)
	w.flush()
}

// NullBulk writes the null bulk string "$-1\r\n".
func (w *Writer) NullBulk() {
	w.buf = AppendNullBulk(w.buf[:0])
	w.flush()
}

// EmptyArray writes the empty array "*0\r\n".
func (w *Writer) EmptyArray() {
	w.buf = AppendEmptyArray(w.buf[:0])
	w.flush()
}

// NullArray writes the null array "*-1\r\n".
func (w *Writer) NullArray() {
	w.buf = AppendNullArray(w.buf[:0])
	w.flush()
}

// BulkArray writes an array of bulk strings. A nil elems slice writes the null
// array "*-1\r\n"; a non-nil empty slice writes "*0\r\n".
func (w *Writer) BulkArray(elems [][]byte) {
	w.buf = AppendBulkArray(w.buf[:0], elems)
	w.flush()
}

// OptBulkArray writes a RESP2 array whose element i is a bulk string when
// present[i] is true and the null bulk string "$-1" otherwise. It backs MGET's
// reply, where missing / wrong-type / expired keys appear as nulls interleaved
// with the present values.
func (w *Writer) OptBulkArray(values [][]byte, present []bool) {
	w.buf = AppendOptBulkArray(w.buf[:0], values, present)
	w.flush()
}
