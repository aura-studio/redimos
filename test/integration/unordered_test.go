package integration

import (
	"reflect"
	"testing"
)

// Dimension F: order-unspecified replies. The main differential deliberately skips
// SMEMBERS/HKEYS/HVALS/HGETALL and the set-algebra commands because Redis does not specify
// their element order, so a byte-for-byte compare would false-fail. Here they are compared
// as sorted multisets (and HGETALL as sorted field/value pairs), closing that coverage gap
// while still catching a wrong or missing element.

func TestDiffUnorderedSets(t *testing.T) {
	d := newDiffer(t)

	sk := d.k("set")
	for _, m := range []string{"a", "bb", "ccc", "dddd", "e"} {
		d.eq("SADD "+m, bs("SADD"), sk, bs(m))
	}
	d.eqSorted("SMEMBERS", bs("SMEMBERS"), sk)

	// Set algebra: build a second set and compare union/inter/diff as sorted multisets.
	sk2 := d.k("set2")
	for _, m := range []string{"ccc", "dddd", "x", "y"} {
		d.eq("SADD2 "+m, bs("SADD"), sk2, bs(m))
	}
	d.eqSorted("SUNION", bs("SUNION"), sk, sk2)
	d.eqSorted("SINTER", bs("SINTER"), sk, sk2)
	d.eqSorted("SDIFF", bs("SDIFF"), sk, sk2)
}

func TestDiffUnorderedHashes(t *testing.T) {
	d := newDiffer(t)

	hk := d.k("hash")
	pairs := [][2]string{{"f1", "v1"}, {"f2", "vv2"}, {"f3", "vvv3"}, {"f4", "v4"}}
	for _, p := range pairs {
		d.eq("HSET "+p[0], bs("HSET"), hk, bs(p[0]), bs(p[1]))
	}
	d.eqSorted("HKEYS", bs("HKEYS"), hk)
	d.eqSorted("HVALS", bs("HVALS"), hk)
	d.eqHGetAll("HGETALL", hk)
}

// eqHGetAll compares HGETALL replies as field->value maps (field order is unspecified, but
// each field must pair with the same value on both endpoints).
func (d *differ) eqHGetAll(desc string, key []byte) {
	d.n++
	mp, okp := respPairs(d.p.do(bs("HGETALL"), key))
	mo, oko := respPairs(d.o.do(bs("HGETALL"), key))
	if !okp || !oko {
		d.t.Errorf("%s: HGETALL not a flat array on one side", desc)
		return
	}
	if !reflect.DeepEqual(mp, mo) {
		d.t.Errorf("%s field/value maps differ\n  proxy =%v\n  oracle=%v", desc, mp, mo)
	}
}

// respPairs decodes a flat [f,v,f,v,...] array reply into a field->value map.
func respPairs(reply []byte) (map[string]string, bool) {
	elems, ok := respArrayElements(reply)
	if !ok || len(elems)%2 != 0 {
		return nil, false
	}
	m := make(map[string]string, len(elems)/2)
	for i := 0; i < len(elems); i += 2 {
		m[elems[i]] = elems[i+1]
	}
	return m, true
}
