package metrics

// familyLabel carries the coarse command family (string / hash / list / set /
// zset / key / bit / hll / geo / connection / server / other) on the per-command
// metrics, so an operator can roll QPS / latency / errors up by family with
// `sum by (family)` without maintaining a recording rule. Family is a pure
// function of the command name (1:1), so labelling with it adds no series beyond
// what the command label already produces.
const familyLabel = "family"

// errorClassLabel carries the RESP error code (WRONGTYPE / ERR / NOAUTH / ...) on
// the error counter, so distinct failure modes — a user type error vs a generic
// backend error vs an auth failure — are distinguishable. The code set is small
// and fixed, keeping the label low-cardinality.
const errorClassLabel = "error_class"

// commandFamilies groups the commands the proxy registers under their Redis
// family. commandFamily inverts it. A command not listed here classifies as
// "other" — a safe fallback that never errors, so the table need not be exhaustive.
var commandFamilies = map[string][]string{
	"string":     {"get", "set", "setnx", "setex", "psetex", "getset", "mget", "mset", "msetnx", "append", "strlen", "setrange", "getrange", "substr", "incr", "decr", "incrby", "decrby", "incrbyfloat"},
	"hash":       {"hget", "hset", "hsetnx", "hmget", "hmset", "hdel", "hexists", "hgetall", "hkeys", "hvals", "hlen", "hstrlen", "hincrby", "hincrbyfloat", "hscan"},
	"list":       {"lpush", "rpush", "lpushx", "rpushx", "lpop", "rpop", "llen", "lrange", "lindex", "lset", "lrem", "ltrim", "linsert", "rpoplpush"},
	"set":        {"sadd", "srem", "sismember", "smembers", "scard", "spop", "srandmember", "sscan", "sunion", "sinter", "sdiff", "sunionstore", "sinterstore", "sdiffstore", "smove"},
	"zset":       {"zadd", "zrem", "zscore", "zincrby", "zcard", "zcount", "zrange", "zrevrange", "zrangebyscore", "zrevrangebyscore", "zrangebylex", "zrevrangebylex", "zrank", "zrevrank", "zremrangebyrank", "zremrangebyscore", "zremrangebylex", "zlexcount", "zpopmin", "zpopmax", "zscan", "zunionstore", "zinterstore"},
	"key":        {"del", "exists", "expire", "expireat", "pexpire", "pexpireat", "ttl", "pttl", "persist", "type", "rename", "renamenx", "keys", "scan", "randomkey", "dbsize", "touch", "object"},
	"bit":        {"setbit", "getbit", "bitcount", "bitpos", "bitop", "bitfield"},
	"hll":        {"pfadd", "pfcount", "pfmerge", "pfselftest"},
	"geo":        {"geoadd", "geodist", "geopos", "geohash", "georadius", "georadiusbymember"},
	"connection": {"ping", "echo", "auth", "select", "hello", "quit"},
	"server":     {"info", "slowlog", "command", "config", "client", "flushdb", "flushall"},
}

// commandFamilyIndex is the inverted commandFamilies map, built once at init.
var commandFamilyIndex = buildCommandFamilyIndex()

func buildCommandFamilyIndex() map[string]string {
	idx := make(map[string]string)
	for fam, cmds := range commandFamilies {
		for _, c := range cmds {
			idx[c] = fam
		}
	}
	return idx
}

// commandFamily maps a lowercased command name to its Redis family, or "other".
func commandFamily(name string) string {
	if fam, ok := commandFamilyIndex[name]; ok {
		return fam
	}
	return "other"
}
