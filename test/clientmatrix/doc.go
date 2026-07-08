// Package clientmatrix holds the client-matrix smoke tests for the redimos
// handshake / connection-management surface (task 6.2).
//
// The goal of these tests is to verify that real Redis client libraries can
// complete their connection handshake against redimos and exercise the
// requirement 2.1 / 2.5 / 2.6 flows:
//
//   - Requirement 2.1: HELLO replies "-ERR unknown command 'HELLO'" so that
//     protocol-3 capable clients (go-redis v9, redis-py 5+) fall back to RESP2
//     and still connect successfully.
//   - Requirement 2.5: a correct AUTH password authenticates the connection.
//   - Requirement 2.6: an unauthenticated connection is rejected with
//     "-NOAUTH Authentication required." for business commands, and a wrong
//     password does not authenticate.
//
// # What runs in Go CI
//
// The Go smoke tests (see goredis_v9_test.go and goredis_v8_test.go) boot an
// in-process redimos server (command.NewRouter + server.New on an ephemeral
// port) and drive it with the go-redis client library. They run as part of the
// normal `go test ./...` invocation with no external runtime required, because
// go-redis is a Go module dependency and the server is embedded in the test
// process.
//
// # What does NOT run in Go CI
//
// A full multi-language client matrix (jedis on the JVM, redis-py on CPython)
// cannot run inside Go CI without those runtimes installed. Those clients are
// covered by the documented, operator-run smoke scripts under this directory:
//
//   - README.md          — how to run the manual client-matrix smoke tests.
//   - redis_py_smoke.py  — redis-py 5+ handshake / HELLO-fallback / AUTH script.
//   - JedisSmoke.java     — jedis handshake / AUTH snippet.
//
// Those scripts target a *live* redimos proxy (started separately) and are
// explicitly out of scope for `go test`. This split keeps CI hermetic while
// still documenting the cross-language handshake contract that requirement 2.1
// exists to satisfy.
package clientmatrix
