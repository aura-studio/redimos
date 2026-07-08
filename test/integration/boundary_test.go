package integration

import (
	"strconv"
	"testing"
)

// Dimension D: range/index boundary semantics. Negative indices, out-of-range, start>stop
// and empty ranges are classic off-by-one territory (this session already found the LREM
// numeric-order bug). These compare LRANGE / LINDEX / GETRANGE / ZRANGE / ZRANGEBYSCORE /
// LSET boundary behavior byte-for-byte with Redis 3.2.

func TestDiffListBoundaries(t *testing.T) {
	d := newDiffer(t)

	lk := d.k("l")
	for _, v := range []string{"a", "b", "c", "d", "e"} {
		d.eq("RPUSH "+v, bs("RPUSH"), lk, bs(v))
	}

	for _, r := range [][2]string{
		{"0", "-1"}, {"0", "0"}, {"-3", "-1"}, {"2", "1"}, {"-100", "100"},
		{"5", "10"}, {"-1", "-1"}, {"3", "2"}, {"0", "100"}, {"-100", "-1"},
	} {
		d.eq("LRANGE "+r[0]+" "+r[1], bs("LRANGE"), lk, bs(r[0]), bs(r[1]))
	}

	for _, i := range []string{"0", "-1", "4", "5", "-5", "-6", "100", "-100"} {
		d.eq("LINDEX "+i, bs("LINDEX"), lk, bs(i))
	}

	// LSET out-of-range is an error; a valid LSET mutates in place.
	d.eq("LSET oob positive", bs("LSET"), lk, bs("100"), bs("x"))
	d.eq("LSET oob negative", bs("LSET"), lk, bs("-100"), bs("x"))
	d.eq("LSET valid", bs("LSET"), lk, bs("0"), bs("A"))
	d.eq("LSET valid negative", bs("LSET"), lk, bs("-1"), bs("E"))
	d.eq("LRANGE after LSET", bs("LRANGE"), lk, bs("0"), bs("-1"))
}

func TestDiffStringRangeBoundaries(t *testing.T) {
	d := newDiffer(t)

	sk := d.k("s")
	d.eq("SET", bs("SET"), sk, bs("Hello World"))

	for _, r := range [][2]string{
		{"0", "4"}, {"-5", "-1"}, {"0", "-1"}, {"6", "100"}, {"-100", "-1"},
		{"5", "2"}, {"0", "0"}, {"-1", "-1"}, {"100", "200"},
	} {
		d.eq("GETRANGE "+r[0]+" "+r[1], bs("GETRANGE"), sk, bs(r[0]), bs(r[1]))
	}

	// SETRANGE past the end zero-fills the gap.
	sr := d.k("sr")
	d.eq("SETRANGE past end", bs("SETRANGE"), sr, bs("5"), bs("XY"))
	d.eq("GET after SETRANGE", bs("GET"), sr)
	d.eq("STRLEN after SETRANGE", bs("STRLEN"), sr)
}

func TestDiffZSetRangeBoundaries(t *testing.T) {
	d := newDiffer(t)

	zk := d.k("z")
	for i, m := range []string{"a", "b", "c", "d", "e"} {
		d.eq("ZADD "+m, bs("ZADD"), zk, bs(strconv.Itoa(i)), bs(m))
	}

	for _, r := range [][2]string{
		{"0", "-1"}, {"1", "3"}, {"-2", "-1"}, {"5", "10"}, {"2", "1"}, {"-100", "100"},
	} {
		d.eq("ZRANGE "+r[0]+" "+r[1], bs("ZRANGE"), zk, bs(r[0]), bs(r[1]))
		d.eq("ZREVRANGE "+r[0]+" "+r[1], bs("ZREVRANGE"), zk, bs(r[0]), bs(r[1]))
	}

	// Score-range bounds including the infinities and exclusive "(" bounds.
	d.eq("ZRANGEBYSCORE -inf +inf", bs("ZRANGEBYSCORE"), zk, bs("-inf"), bs("+inf"))
	d.eq("ZRANGEBYSCORE 1 3", bs("ZRANGEBYSCORE"), zk, bs("1"), bs("3"))
	d.eq("ZRANGEBYSCORE (1 3", bs("ZRANGEBYSCORE"), zk, bs("(1"), bs("3"))
	d.eq("ZRANGEBYSCORE (1 (3", bs("ZRANGEBYSCORE"), zk, bs("(1"), bs("(3"))
	d.eq("ZCOUNT -inf +inf", bs("ZCOUNT"), zk, bs("-inf"), bs("+inf"))
	d.eq("ZCOUNT (1 3", bs("ZCOUNT"), zk, bs("(1"), bs("3"))

	// Lex ranges over an equal-score set.
	lk := d.k("lex")
	for _, m := range []string{"a", "b", "c", "d"} {
		d.eq("ZADD lex "+m, bs("ZADD"), lk, bs("0"), bs(m))
	}
	d.eq("ZRANGEBYLEX - +", bs("ZRANGEBYLEX"), lk, bs("-"), bs("+"))
	d.eq("ZRANGEBYLEX [b (d", bs("ZRANGEBYLEX"), lk, bs("[b"), bs("(d"))
	d.eq("ZLEXCOUNT - +", bs("ZLEXCOUNT"), lk, bs("-"), bs("+"))
}
