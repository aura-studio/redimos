package integration

// Dimension F (unordered set-algebra) + J (coverage) DEPTH.
//
// The base unordered/coverage files exercise the set-algebra commands with a single
// happy-path shape. This file drills into the Redis 3.2 edge cases those files leave
// untested, all compared byte-for-byte (or sorted, where element order is unspecified)
// against a live Redis 3.2 oracle:
//
//   - SINTER short-circuits on the FIRST empty operand and does NOT type-check the rest,
//     while SUNION/SDIFF type-check every operand (GAP 1).
//   - SRANDMEMBER count=0 / count==card / count>card / negative count (GAP 2).
//   - Set-algebra *STORE that yields an empty result must DELETE the dest key (GAP 3).
//   - SMOVE type-check order: source first, dest second, membership last (GAP 4).
//   - SDIFFSTORE operand order matters and the empty-result-deletes-dest path (GAP 5).
//
// Only proxy-registered commands are used (SINTER/SUNION/SDIFF/S*STORE/SRANDMEMBER/SMOVE,
// confirmed in internal/command/sets.go). Mutations run through d.eq so both endpoints
// reach identical state before each assertion.

import "testing"

// TestDiffSInterShortCircuit covers GAP 1: SINTER short-circuits on the first empty
// operand without type-checking later operands, whereas SUNION/SDIFF type-check them all.
func TestDiffSInterShortCircuit(t *testing.T) {
	d := newDiffer(t)

	s1 := d.k("si1")
	s2 := d.k("si2")
	s3 := d.k("si3")
	d.eq("SADD s1", bs("SADD"), s1, bs("a"), bs("b"), bs("c"))
	d.eq("SADD s2", bs("SADD"), s2, bs("c"), bs("d"))
	d.eq("SADD s3", bs("SADD"), s3, bs("a"), bs("c"), bs("e"))

	// Three-operand intersection: {a,b,c} ^ {c,d} ^ {a,c,e} = {c}.
	d.eqSorted("SINTER 3 operands", bs("SINTER"), s1, s2, s3)
	// Order of operands must not change the result.
	d.eqSorted("SINTER 3 operands reordered", bs("SINTER"), s3, s2, s1)

	// A wrong-type operand: set a string key.
	wrong := d.k("si_str")
	d.eq("SET wrong", bs("SET"), wrong, bs("notaset"))
	absent := d.k("si_absent") // never created -> empty

	// SINTER absent wrongstring: the FIRST operand is empty, so Redis 3.2 short-circuits
	// to an empty reply WITHOUT ever type-checking the wrong-type second operand. This is
	// the load-bearing divergence risk: a naive implementation type-checks all operands and
	// wrongly returns WRONGTYPE here.
	d.eqSorted("SINTER absent then wrongtype (short-circuit, no WRONGTYPE)", bs("SINTER"), absent, wrong)
	// Same, three operands, empty first: still short-circuits before the wrong-type operand.
	d.eqSorted("SINTER absent, wrong, set", bs("SINTER"), absent, wrong, s1)

	// A present set intersected with an absent set => empty, and the absent operand appears
	// AFTER a non-empty one, so no short-circuit until it is reached; result empty regardless.
	d.eqSorted("SINTER set then absent", bs("SINTER"), s1, absent)

	// Contrast: SUNION does NOT short-circuit; a wrong-type operand anywhere is WRONGTYPE
	// (byte-for-byte error compare via d.eq).
	d.eq("SUNION with wrongtype operand => WRONGTYPE", bs("SUNION"), s1, wrong)
	d.eq("SUNION absent then wrongtype => WRONGTYPE", bs("SUNION"), absent, wrong)
	// SDIFF likewise type-checks all operands (no short-circuit on empty first).
	d.eq("SDIFF absent then wrongtype => WRONGTYPE", bs("SDIFF"), absent, wrong)
	d.eq("SDIFF set then wrongtype => WRONGTYPE", bs("SDIFF"), s1, wrong)

	// And SINTER WITH a wrong-type operand while the first operand is NON-empty does report
	// WRONGTYPE (the short-circuit only applies when an earlier operand is empty).
	d.eq("SINTER nonempty then wrongtype => WRONGTYPE", bs("SINTER"), s1, wrong)
}

// TestDiffSRandMemberCounts covers GAP 2: SRANDMEMBER count edge cases. Positive counts
// return DISTINCT members in unspecified order (so eqSorted), capped at cardinality; count=0
// returns an empty array; negative count returns exactly -count members WITH repeats, so the
// multiset is nondeterministic and only its LENGTH is comparable (checked structurally here,
// not by value).
func TestDiffSRandMemberCounts(t *testing.T) {
	d := newDiffer(t)

	sr := d.k("sr")
	d.eq("SADD sr", bs("SADD"), sr, bs("a"), bs("b"), bs("c"))

	// count = 0 -> empty array on both sides (order-free but trivially equal).
	d.eqSorted("SRANDMEMBER count 0", bs("SRANDMEMBER"), sr, bs("0"))

	// count == cardinality -> all distinct members, shuffled. eqSorted normalizes order.
	d.eqSorted("SRANDMEMBER count == card", bs("SRANDMEMBER"), sr, bs("3"))

	// count > cardinality -> still all (distinct) members, never padded/duplicated.
	d.eqSorted("SRANDMEMBER count > card", bs("SRANDMEMBER"), sr, bs("5"))

	// count > cardinality on a singleton set -> exactly that one member.
	one := d.k("sr_one")
	d.eq("SADD one", bs("SADD"), one, bs("solo"))
	d.eqSorted("SRANDMEMBER singleton over-count", bs("SRANDMEMBER"), one, bs("9"))

	// Absent key: positive count -> empty array; count 0 -> empty array; negative -> empty.
	absent := d.k("sr_absent")
	d.eqSorted("SRANDMEMBER absent positive", bs("SRANDMEMBER"), absent, bs("4"))
	d.eqSorted("SRANDMEMBER absent zero", bs("SRANDMEMBER"), absent, bs("0"))
	d.eqSorted("SRANDMEMBER absent negative", bs("SRANDMEMBER"), absent, bs("-4"))

	// Negative count on a present set: returns exactly -count members WITH possible repeats.
	// Values are random so we cannot value-compare; assert the reply is an array of the exact
	// requested length on both endpoints (structural parity).
	d.eqRandLen("SRANDMEMBER negative count length", 4, bs("SRANDMEMBER"), sr, bs("-4"))
	d.eqRandLen("SRANDMEMBER negative count exceeds card length", 7, bs("SRANDMEMBER"), sr, bs("-7"))
	// Negative count on a singleton set repeats the sole member -count times.
	d.eqRandLen("SRANDMEMBER negative singleton length", 5, bs("SRANDMEMBER"), one, bs("-5"))

	// No-count form (arity boundary): single random member as a bulk string, or nil on absent.
	// Value is random, but on an absent key the reply is deterministically the nil bulk.
	d.eq("SRANDMEMBER absent no-count => nil", bs("SRANDMEMBER"), absent)

	// WRONGTYPE: SRANDMEMBER on a string key, both with and without a count arg.
	wrong := d.k("sr_str")
	d.eq("SET wrong", bs("SET"), wrong, bs("v"))
	d.eq("SRANDMEMBER wrongtype no-count", bs("SRANDMEMBER"), wrong)
	d.eq("SRANDMEMBER wrongtype with count", bs("SRANDMEMBER"), wrong, bs("2"))
}

// TestDiffSetAlgebraStoreEmptyDeletesDest covers GAP 3 + GAP 5: when a *STORE result is
// empty the dest key must be DELETED (reply :0, and afterwards TYPE/EXISTS reflect no key),
// and the dest is overwritten regardless of its prior type. Operand order for SDIFFSTORE
// matters.
func TestDiffSetAlgebraStoreEmptyDeletesDest(t *testing.T) {
	d := newDiffer(t)

	s1 := d.k("st1")
	s2 := d.k("st2")
	d.eq("SADD s1", bs("SADD"), s1, bs("a"), bs("b"))
	d.eq("SADD s2", bs("SADD"), s2, bs("a"), bs("c"))

	// --- SINTERSTORE empty result deletes dest ---
	dst := d.k("st_dst")
	// Pre-seed dest with a DIFFERENT type to prove overwrite-then-delete semantics.
	d.eq("preseed dst as string", bs("SET"), dst, bs("stale"))
	absent := d.k("st_absent")
	// {a,b} ^ (absent) = empty -> :0, dest deleted (even though it held a string).
	d.eq("SINTERSTORE empty => :0", bs("SINTERSTORE"), dst, s1, absent)
	d.eq("dst deleted after empty SINTERSTORE (EXISTS 0)", bs("EXISTS"), dst)
	d.eq("dst TYPE none after empty SINTERSTORE", bs("TYPE"), dst)

	// Non-empty SINTERSTORE then an emptying one, to prove the delete happens on the second.
	d.eq("SINTERSTORE nonempty => :1", bs("SINTERSTORE"), dst, s1, s2) // {a}
	d.eqSorted("dst members after nonempty", bs("SMEMBERS"), dst)
	d.eq("EXISTS dst after nonempty", bs("EXISTS"), dst)
	d.eq("SINTERSTORE emptying overwrites+deletes => :0", bs("SINTERSTORE"), dst, s1, absent)
	d.eq("dst gone after emptying overwrite", bs("EXISTS"), dst)

	// --- SUNIONSTORE with all-empty operands deletes dest ---
	udst := d.k("st_udst")
	d.eq("preseed udst as set", bs("SADD"), udst, bs("old1"), bs("old2"))
	a1 := d.k("st_ea1")
	a2 := d.k("st_ea2")
	d.eq("SUNIONSTORE all-empty => :0", bs("SUNIONSTORE"), udst, a1, a2)
	d.eq("udst deleted after empty SUNIONSTORE", bs("EXISTS"), udst)

	// --- SDIFFSTORE: operand order matters + empty result deletes dest ---
	ddst := d.k("st_ddst")
	// s1 - s2 = {a,b} - {a,c} = {b}
	d.eq("SDIFFSTORE s1-s2 => :1", bs("SDIFFSTORE"), ddst, s1, s2)
	d.eqSorted("ddst == {b}", bs("SMEMBERS"), ddst)
	// s2 - s1 = {a,c} - {a,b} = {c}  (order-sensitive: different result)
	d.eq("SDIFFSTORE s2-s1 => :1", bs("SDIFFSTORE"), ddst, s2, s1)
	d.eqSorted("ddst == {c}", bs("SMEMBERS"), ddst)
	// s1 - s1 = empty -> :0, dest deleted.
	d.eq("SDIFFSTORE s1-s1 empty => :0", bs("SDIFFSTORE"), ddst, s1, s1)
	d.eq("ddst deleted after self-diff", bs("EXISTS"), ddst)
	d.eq("ddst TYPE none after self-diff", bs("TYPE"), ddst)

	// SDIFFSTORE with an absent first operand: empty - anything = empty -> :0, dest deleted.
	edst := d.k("st_edst")
	d.eq("preseed edst as string", bs("SET"), edst, bs("x"))
	d.eq("SDIFFSTORE absent-first empty => :0", bs("SDIFFSTORE"), edst, absent, s1)
	d.eq("edst deleted after absent-first diff", bs("EXISTS"), edst)

	// SDIFFSTORE where the subtracted operand is absent leaves the first set intact.
	fdst := d.k("st_fdst")
	d.eq("SDIFFSTORE s1-absent => card of s1", bs("SDIFFSTORE"), fdst, s1, absent)
	d.eqSorted("fdst == s1 members", bs("SMEMBERS"), fdst)
}

// TestDiffSMoveTypeOrder covers GAP 4: SMOVE checks source type then dest type BEFORE
// membership, but when the source is absent it returns :0 without ever type-checking dest.
func TestDiffSMoveTypeOrder(t *testing.T) {
	d := newDiffer(t)

	src := d.k("sm_src")
	dst := d.k("sm_dst")
	d.eq("SADD src", bs("SADD"), src, bs("m1"), bs("m2"))
	d.eq("SADD dst", bs("SADD"), dst, bs("d1"))

	wsrc := d.k("sm_wsrc")
	wdst := d.k("sm_wdst")
	d.eq("SET wrong src", bs("SET"), wsrc, bs("str"))
	d.eq("SET wrong dst", bs("SET"), wdst, bs("str"))
	absent := d.k("sm_absent")

	// Source is wrong-type => WRONGTYPE immediately (dest type irrelevant).
	d.eq("SMOVE wrongsrc -> wrongdst => WRONGTYPE (source first)", bs("SMOVE"), wsrc, wdst, bs("m"))
	d.eq("SMOVE wrongsrc -> goodset => WRONGTYPE", bs("SMOVE"), wsrc, dst, bs("m"))

	// Source is a valid set but dest is wrong-type => WRONGTYPE.
	d.eq("SMOVE goodset -> wrongdst => WRONGTYPE (dest checked)", bs("SMOVE"), src, wdst, bs("m1"))

	// Source is ABSENT and dest is wrong-type: member cannot be in an absent set, so Redis
	// returns :0 WITHOUT type-checking dest. Load-bearing: a naive impl that type-checks dest
	// up front would wrongly return WRONGTYPE.
	d.eq("SMOVE absentsrc -> wrongdst => :0 (dest not checked)", bs("SMOVE"), absent, wdst, bs("m"))

	// Successful move: member present in source, dest is a valid set.
	d.eq("SMOVE valid => :1", bs("SMOVE"), src, dst, bs("m1"))
	d.eqSorted("src after move", bs("SMEMBERS"), src)
	d.eqSorted("dst after move", bs("SMEMBERS"), dst)

	// Move a member NOT in source (source present, dest present) => :0, nothing changes.
	d.eq("SMOVE absent-member => :0", bs("SMOVE"), src, dst, bs("nope"))
	d.eqSorted("src unchanged", bs("SMEMBERS"), src)
	d.eqSorted("dst unchanged", bs("SMEMBERS"), dst)

	// Move the LAST member out of source: source becomes empty and is deleted.
	solo := d.k("sm_solo")
	solodst := d.k("sm_solodst")
	d.eq("SADD solo", bs("SADD"), solo, bs("only"))
	d.eq("SMOVE last member => :1", bs("SMOVE"), solo, solodst, bs("only"))
	d.eq("empty source deleted after last SMOVE", bs("EXISTS"), solo)
	d.eqSorted("solodst has the member", bs("SMEMBERS"), solodst)

	// Move a member that already exists in dest: source loses it, dest unchanged in size.
	src2 := d.k("sm_src2")
	dst2 := d.k("sm_dst2")
	d.eq("SADD src2", bs("SADD"), src2, bs("x"), bs("y"))
	d.eq("SADD dst2", bs("SADD"), dst2, bs("x"))
	d.eq("SMOVE dup into dst => :1", bs("SMOVE"), src2, dst2, bs("x"))
	d.eqSorted("src2 lost x", bs("SMEMBERS"), src2)
	d.eqSorted("dst2 still {x}", bs("SMEMBERS"), dst2)

	// SMOVE to the SAME key when the member exists: :1, set unchanged (self-move).
	self := d.k("sm_self")
	d.eq("SADD self", bs("SADD"), self, bs("p"), bs("q"))
	d.eq("SMOVE self->self present => :1", bs("SMOVE"), self, self, bs("p"))
	d.eqSorted("self unchanged after self-move", bs("SMEMBERS"), self)

	// Binary member bytes through SMOVE (NUL + high byte) to catch binary-unsafe paths.
	bsrc := d.k("sm_bsrc")
	bdst := d.k("sm_bdst")
	binMember := string([]byte{0x00, 0xff, 0x41})
	d.eq("SADD binary member", bs("SADD"), bsrc, bs(binMember))
	d.eq("SMOVE binary member => :1", bs("SMOVE"), bsrc, bdst, bs(binMember))
	d.eqSorted("bdst has binary member", bs("SMEMBERS"), bdst)
	d.eq("bsrc deleted after binary move", bs("EXISTS"), bsrc)
}

// TestDiffSetAlgebraBinaryAndSingle covers extra J-coverage corners of the set-algebra
// family: single-operand forms, binary members flowing through union/inter/diff, and the
// self-operand identities — all order-unspecified so compared with eqSorted.
func TestDiffSetAlgebraBinaryAndSingle(t *testing.T) {
	d := newDiffer(t)

	s1 := d.k("sa1")
	s2 := d.k("sa2")
	// Include NUL and 0xff bytes as members to exercise binary-safe set algebra.
	binA := string([]byte{0x00, 0x01})
	binB := string([]byte{0xff, 0xfe})
	d.eq("SADD s1", bs("SADD"), s1, bs("a"), bs(binA), bs(binB))
	d.eq("SADD s2", bs("SADD"), s2, bs(binA), bs("z"))

	// Single-operand set algebra: SUNION/SINTER/SDIFF of one set each equals the set itself.
	d.eqSorted("SUNION single == members", bs("SUNION"), s1)
	d.eqSorted("SINTER single == members", bs("SINTER"), s1)
	d.eqSorted("SDIFF single == members", bs("SDIFF"), s1)

	// Binary members survive union/intersection/difference.
	d.eqSorted("SUNION binary", bs("SUNION"), s1, s2)
	d.eqSorted("SINTER binary (shared binA)", bs("SINTER"), s1, s2)
	d.eqSorted("SDIFF binary (s1-s2)", bs("SDIFF"), s1, s2)
	d.eqSorted("SDIFF binary reversed (s2-s1)", bs("SDIFF"), s2, s1)

	// Self identities: A ^ A == A, A - A == empty, A | A == A.
	d.eqSorted("SINTER self == self", bs("SINTER"), s1, s1)
	d.eqSorted("SDIFF self == empty", bs("SDIFF"), s1, s1)
	d.eqSorted("SUNION self == self", bs("SUNION"), s1, s1)

	// STORE the single-operand union into a dest (effectively a set copy), binary-safe.
	cp := d.k("sa_copy")
	d.eq("SUNIONSTORE single-copy count", bs("SUNIONSTORE"), cp, s1)
	d.eqSorted("copy == s1", bs("SMEMBERS"), cp)

	// SINTERSTORE binary shared member.
	idst := d.k("sa_idst")
	d.eq("SINTERSTORE binary count", bs("SINTERSTORE"), idst, s1, s2)
	d.eqSorted("idst == {binA}", bs("SMEMBERS"), idst)
}

// eqRandLen asserts a SRANDMEMBER-with-negative-count reply is a RESP array of exactly
// wantLen elements on BOTH endpoints. The elements themselves are random (and may repeat),
// so only the length is deterministic; this catches a proxy that returns the wrong count,
// a non-array, or an error where Redis returns an array.
func (d *differ) eqRandLen(desc string, wantLen int, args ...[]byte) {
	d.n++
	rp := d.p.do(args...)
	ro := d.o.do(args...)
	ep, okp := respArrayElements(rp)
	eo, oko := respArrayElements(ro)
	if !okp || !oko {
		d.t.Errorf("%s: non-array reply\n  proxy =%q\n  oracle=%q", desc, rp, ro)
		return
	}
	if len(ep) != wantLen || len(eo) != wantLen {
		d.t.Errorf("%s: length mismatch (want %d)\n  proxy =%d %v\n  oracle=%d %v",
			desc, wantLen, len(ep), ep, len(eo), eo)
	}
}
