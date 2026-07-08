package integration

import (
	"fmt"
	"strconv"
	"testing"
)

// Dimension O: encoding-threshold invariance. Redis switches a collection's internal encoding
// at size/value thresholds (hash/zset ziplist->hashtable/skiplist at 128 entries; set
// intset->hashtable at 512 int entries or on the first non-int member). The OBSERVABLE
// behaviour must be identical across the threshold. redimos has no encodings, so this both
// confirms no size-dependent divergence AND exercises its DynamoDB pagination (a collection
// this large spans multiple Query pages / BatchWriteItem batches) against real Redis 3.2.

const bigN = 150 // > 128, crosses the hash/zset ziplist threshold

func TestDiffEncoding_Hash(t *testing.T) {
	d := newDiffer(t)
	k := d.k("hash")
	args := [][]byte{bs("HMSET"), k}
	for i := 0; i < bigN; i++ {
		args = append(args, bs(fmt.Sprintf("f%d", i)), bs(fmt.Sprintf("v%d", i)))
	}
	d.eq("HMSET bigN", args...)
	d.eq("HLEN", bs("HLEN"), k)
	d.eqSorted("HKEYS", bs("HKEYS"), k)
	d.eqSorted("HVALS", bs("HVALS"), k)
	d.eqSorted("HGETALL", bs("HGETALL"), k)
	d.eq("HGET f0", bs("HGET"), k, bs("f0"))
	d.eq("HGET f149", bs("HGET"), k, bs("f149"))
	d.eq("HGET missing", bs("HGET"), k, bs("nope"))
	d.eq("HEXISTS f77", bs("HEXISTS"), k, bs("f77"))
}

func TestDiffEncoding_ZSet(t *testing.T) {
	d := newDiffer(t)
	k := d.k("zset")
	args := [][]byte{bs("ZADD"), k}
	for i := 0; i < bigN; i++ {
		args = append(args, bs(strconv.Itoa(i)), bs(fmt.Sprintf("m%d", i)))
	}
	d.eq("ZADD bigN", args...)
	d.eq("ZCARD", bs("ZCARD"), k)
	// Ordered range replies must match byte-for-byte (score then lex ordering).
	d.eq("ZRANGE 0 -1 WITHSCORES", bs("ZRANGE"), k, bs("0"), bs("-1"), bs("WITHSCORES"))
	d.eq("ZRANGE 10 20", bs("ZRANGE"), k, bs("10"), bs("20"))
	d.eq("ZREVRANGE 0 5 WITHSCORES", bs("ZREVRANGE"), k, bs("0"), bs("5"), bs("WITHSCORES"))
	d.eq("ZRANGEBYSCORE 50 60", bs("ZRANGEBYSCORE"), k, bs("50"), bs("60"))
	d.eq("ZSCORE m100", bs("ZSCORE"), k, bs("m100"))
	d.eq("ZRANK m100", bs("ZRANK"), k, bs("m100"))
	d.eq("ZCOUNT 0 149", bs("ZCOUNT"), k, bs("0"), bs("149"))
}

func TestDiffEncoding_SetStrings(t *testing.T) {
	d := newDiffer(t)
	k := d.k("set")
	args := [][]byte{bs("SADD"), k}
	for i := 0; i < bigN; i++ {
		args = append(args, bs(fmt.Sprintf("m%d", i)))
	}
	d.eq("SADD bigN strings (hashtable)", args...)
	d.eq("SCARD", bs("SCARD"), k)
	d.eqSorted("SMEMBERS", bs("SMEMBERS"), k)
	d.eq("SISMEMBER m0", bs("SISMEMBER"), k, bs("m0"))
	d.eq("SISMEMBER m149", bs("SISMEMBER"), k, bs("m149"))
	d.eq("SISMEMBER missing", bs("SISMEMBER"), k, bs("nope"))
}

func TestDiffEncoding_SetInts(t *testing.T) {
	d := newDiffer(t)
	k := d.k("intset")
	args := [][]byte{bs("SADD"), k}
	for i := 0; i < bigN; i++ {
		args = append(args, bs(strconv.Itoa(i*7)))
	}
	d.eq("SADD bigN ints (intset)", args...)
	d.eq("SCARD", bs("SCARD"), k)
	d.eqSorted("SMEMBERS", bs("SMEMBERS"), k)
	// Adding a non-int member forces intset->hashtable in Redis; behaviour must not change.
	d.eq("SADD non-int (force hashtable)", bs("SADD"), k, bs("notanint"))
	d.eq("SCARD after", bs("SCARD"), k)
	d.eqSorted("SMEMBERS after", bs("SMEMBERS"), k)
}

func TestDiffEncoding_List(t *testing.T) {
	d := newDiffer(t)
	k := d.k("list")
	args := [][]byte{bs("RPUSH"), k}
	for i := 0; i < bigN; i++ {
		args = append(args, bs(fmt.Sprintf("e%d", i)))
	}
	d.eq("RPUSH bigN", args...)
	d.eq("LLEN", bs("LLEN"), k)
	d.eq("LRANGE 0 -1", bs("LRANGE"), k, bs("0"), bs("-1"))
	d.eq("LRANGE 100 149", bs("LRANGE"), k, bs("100"), bs("149"))
	d.eq("LINDEX 0", bs("LINDEX"), k, bs("0"))
	d.eq("LINDEX -1", bs("LINDEX"), k, bs("-1"))
	d.eq("LINDEX 75", bs("LINDEX"), k, bs("75"))
}
