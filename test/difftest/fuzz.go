package difftest

import (
	"fmt"
	"math/rand"
	"reflect"
)

// RandomSequence is a testing/quick generator that produces random RESP2
// command sequences constrained to the supported command space. It implements
// quick.Generator so it can drive the fuzz entry point via quick.Check:
//
//	quick.Check(func(seq RandomSequence) bool { ... }, cfg)
//
// The generator is "smart": it draws from a small fixed key pool and emits only
// well-formed commands for the String / Hash / List / Set / SortedSet / TTL
// families. This keeps sequences inside the input space both endpoints accept,
// so mismatches point at genuine behavioral divergence rather than at garbage
// input. Every generated sequence also starts by deleting the key pool so runs
// are independent even against a persistent oracle.
type RandomSequence struct {
	Sequence Sequence
}

// fuzzKeys is the small key pool the fuzzer draws from. A small pool maximizes
// the chance of exercising type conflicts, overwrites, and expiry interactions.
var fuzzKeys = []string{"difftest:fz:a", "difftest:fz:b", "difftest:fz:c"}

// commandGenerators are the per-family builders the fuzzer samples from. Each
// returns a well-formed command using the provided rng and key.
var commandGenerators = []func(r *rand.Rand, key string) Command{
	// Strings
	func(r *rand.Rand, key string) Command { return Cmd("SET", key, randValue(r)) },
	func(r *rand.Rand, key string) Command { return Cmd("GET", key) },
	func(r *rand.Rand, key string) Command { return Cmd("APPEND", key, randValue(r)) },
	func(r *rand.Rand, key string) Command { return Cmd("STRLEN", key) },
	func(r *rand.Rand, key string) Command { return Cmd("INCR", key) },
	func(r *rand.Rand, key string) Command { return Cmd("DECRBY", key, randIntStr(r)) },
	// Hashes
	func(r *rand.Rand, key string) Command { return Cmd("HSET", key, randField(r), randValue(r)) },
	func(r *rand.Rand, key string) Command { return Cmd("HGET", key, randField(r)) },
	func(r *rand.Rand, key string) Command { return Cmd("HDEL", key, randField(r)) },
	func(r *rand.Rand, key string) Command { return Cmd("HLEN", key) },
	// Lists
	func(r *rand.Rand, key string) Command { return Cmd("LPUSH", key, randValue(r)) },
	func(r *rand.Rand, key string) Command { return Cmd("RPUSH", key, randValue(r)) },
	func(r *rand.Rand, key string) Command { return Cmd("LPOP", key) },
	func(r *rand.Rand, key string) Command { return Cmd("LLEN", key) },
	func(r *rand.Rand, key string) Command { return Cmd("LRANGE", key, "0", "-1") },
	// Sets
	func(r *rand.Rand, key string) Command { return Cmd("SADD", key, randValue(r)) },
	func(r *rand.Rand, key string) Command { return Cmd("SREM", key, randValue(r)) },
	func(r *rand.Rand, key string) Command { return Cmd("SCARD", key) },
	func(r *rand.Rand, key string) Command { return Cmd("SISMEMBER", key, randValue(r)) },
	// Sorted sets
	func(r *rand.Rand, key string) Command { return Cmd("ZADD", key, randIntStr(r), randValue(r)) },
	func(r *rand.Rand, key string) Command { return Cmd("ZSCORE", key, randValue(r)) },
	func(r *rand.Rand, key string) Command { return Cmd("ZCARD", key) },
	// Keys / TTL
	func(r *rand.Rand, key string) Command { return Cmd("TYPE", key) },
	func(r *rand.Rand, key string) Command { return Cmd("EXISTS", key) },
	func(r *rand.Rand, key string) Command { return Cmd("TTL", key) },
	func(r *rand.Rand, key string) Command { return Cmd("EXPIRE", key, randIntStr(r)) },
	func(r *rand.Rand, key string) Command { return Cmd("PERSIST", key) },
	func(r *rand.Rand, key string) Command { return Cmd("DEL", key) },
}

// GenerateSequence builds a random, well-formed command sequence of the given
// length using r. It is the reusable core behind the quick.Generator; call it
// directly when you need a sequence without the reflect wrapper.
func GenerateSequence(r *rand.Rand, length int) Sequence {
	if length < 1 {
		length = 1
	}
	cmds := make([]Command, 0, length+1)

	// Start clean: delete the whole key pool so the sequence is independent.
	cleanup := make([]string, 0, len(fuzzKeys)+1)
	cleanup = append(cleanup, "DEL")
	cleanup = append(cleanup, fuzzKeys...)
	cmds = append(cmds, Cmd(cleanup...))

	for i := 0; i < length; i++ {
		key := fuzzKeys[r.Intn(len(fuzzKeys))]
		gen := commandGenerators[r.Intn(len(commandGenerators))]
		cmds = append(cmds, gen(r, key))
	}

	return Sequence{
		Name:     fmt.Sprintf("fuzz-%d", length),
		Commands: cmds,
	}
}

// Generate implements quick.Generator. It produces a RandomSequence whose
// length scales with quick's size parameter (bounded so sequences stay a
// reasonable length for live differential runs).
func (RandomSequence) Generate(r *rand.Rand, size int) reflect.Value {
	length := size%16 + 1
	return reflect.ValueOf(RandomSequence{Sequence: GenerateSequence(r, length)})
}

func randValue(r *rand.Rand) string {
	// A small alphabet and short length keep values printable and encourage
	// collisions (so SADD/SREM/HSET overwrite the same members).
	const alphabet = "abcdefg"
	n := r.Intn(4) + 1
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[r.Intn(len(alphabet))]
	}
	return string(b)
}

func randField(r *rand.Rand) string {
	fields := []string{"f1", "f2", "f3"}
	return fields[r.Intn(len(fields))]
}

func randIntStr(r *rand.Rand) string {
	// Range spans negatives and zero to exercise sign handling.
	return fmt.Sprintf("%d", r.Intn(2001)-1000)
}
