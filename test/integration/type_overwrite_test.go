package integration

import "testing"

// Dimension P: type-overwrite & key-creation semantics. Two related Redis contracts: (1) SET
// unconditionally overwrites a key of ANY prior type into a string (redimos does this via its
// overwriteAnyType path, which must clear the old collection); (2) a write to a missing key
// creates it with the RIGHT type (HSET->hash, SADD->set, RPUSH->list, ZADD->zset, INCR/APPEND/
// SETRANGE/SETBIT->string). Both are compared with Redis 3.2.

func TestDiffSetOverwritesAnyType(t *testing.T) {
	d := newDiffer(t)

	// SET over a hash / set / list / zset -> becomes a plain string of the new value, and the
	// old collection is gone (a collection op on it now returns WRONGTYPE identically).
	cases := []struct {
		name    string
		create  [][]byte // command creating the non-string key
		wrongOp [][]byte // a collection op that must now be WRONGTYPE (key filled in per-run)
	}{
		{"hash", [][]byte{bs("HSET"), nil, bs("f"), bs("v")}, [][]byte{bs("HGET"), nil, bs("f")}},
		{"set", [][]byte{bs("SADD"), nil, bs("m")}, [][]byte{bs("SMEMBERS"), nil}},
		{"list", [][]byte{bs("RPUSH"), nil, bs("e")}, [][]byte{bs("LRANGE"), nil, bs("0"), bs("-1")}},
		{"zset", [][]byte{bs("ZADD"), nil, bs("1"), bs("m")}, [][]byte{bs("ZRANGE"), nil, bs("0"), bs("-1")}},
	}
	for _, c := range cases {
		k := d.k(c.name)
		create := append([][]byte{}, c.create...)
		create[1] = k
		d.eq("create "+c.name, create...)
		d.eq("SET over "+c.name+" -> +OK", bs("SET"), k, bs("nowstring"))
		d.eq("TYPE now string", bs("TYPE"), k)
		d.eq("GET new value", bs("GET"), k)
		wrong := append([][]byte{}, c.wrongOp...)
		wrong[1] = k
		d.eq(c.name+" op now WRONGTYPE", wrong...)
	}
}

func TestDiffKeyCreationTypes(t *testing.T) {
	d := newDiffer(t)

	// Each creating command on a fresh key yields the expected TYPE.
	d.eq("HSET creates hash", bs("HSET"), d.k("h"), bs("f"), bs("v"))
	d.eq("TYPE hash", bs("TYPE"), d.k("h"))
	d.eq("SADD creates set", bs("SADD"), d.k("s"), bs("m"))
	d.eq("TYPE set", bs("TYPE"), d.k("s"))
	d.eq("RPUSH creates list", bs("RPUSH"), d.k("l"), bs("e"))
	d.eq("TYPE list", bs("TYPE"), d.k("l"))
	d.eq("ZADD creates zset", bs("ZADD"), d.k("z"), bs("1"), bs("m"))
	d.eq("TYPE zset", bs("TYPE"), d.k("z"))

	// String-creating writes on missing keys.
	d.eq("INCR creates string=1", bs("INCR"), d.k("i"))
	d.eq("TYPE incr", bs("TYPE"), d.k("i"))
	d.eq("GET incr", bs("GET"), d.k("i"))

	d.eq("APPEND creates string", bs("APPEND"), d.k("a"), bs("hello"))
	d.eq("TYPE append", bs("TYPE"), d.k("a"))
	d.eq("GET append", bs("GET"), d.k("a"))

	d.eq("SETRANGE creates zero-filled string", bs("SETRANGE"), d.k("sr"), bs("3"), bs("XY"))
	d.eq("GET setrange", bs("GET"), d.k("sr"))
	d.eq("STRLEN setrange", bs("STRLEN"), d.k("sr"))

	d.eq("SETBIT creates string", bs("SETBIT"), d.k("sb"), bs("5"), bs("1"))
	d.eq("TYPE setbit", bs("TYPE"), d.k("sb"))
	d.eq("STRLEN setbit", bs("STRLEN"), d.k("sb"))
}

func TestDiffGetSetAndTypeErrors(t *testing.T) {
	d := newDiffer(t)

	// GETSET over a string returns the old value and installs the new.
	s := d.k("s")
	d.eq("SET s", bs("SET"), s, bs("old"))
	d.eq("GETSET -> old", bs("GETSET"), s, bs("new"))
	d.eq("GET -> new", bs("GET"), s)
	// GETSET on a missing key returns nil and creates the string.
	m := d.k("m")
	d.eq("GETSET missing -> nil", bs("GETSET"), m, bs("v"))
	d.eq("GET after GETSET missing", bs("GET"), m)
	// GETSET / INCR / APPEND on a wrong-type key are WRONGTYPE.
	h := d.k("h")
	d.eq("HSET h", bs("HSET"), h, bs("f"), bs("v"))
	d.eq("GETSET on hash -> WRONGTYPE", bs("GETSET"), h, bs("x"))
	d.eq("INCR on hash -> WRONGTYPE", bs("INCR"), h)
	d.eq("APPEND on hash -> WRONGTYPE", bs("APPEND"), h, bs("x"))
	// INCR on a non-integer string is an error (not WRONGTYPE).
	ns := d.k("ns")
	d.eq("SET non-int", bs("SET"), ns, bs("abc"))
	d.eq("INCR non-int -> error", bs("INCR"), ns)
}
