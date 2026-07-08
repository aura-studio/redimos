# Client-matrix handshake smoke tests (task 6.2)

This directory verifies that real Redis client libraries can complete their
connection handshake against `redimos` and exercise the connection-management
flows from Requirement 2:

- **Requirement 2.1** — `HELLO` replies `-ERR unknown command 'HELLO'`, so
  protocol-3 capable clients (go-redis v9, redis-py 5+) **fall back to RESP2**
  and still connect.
- **Requirement 2.5** — a correct `AUTH` password authenticates the connection.
- **Requirement 2.6** — an unauthenticated connection is rejected with
  `-NOAUTH Authentication required.`; a wrong password does not authenticate.

## What runs in Go CI (automated)

The **go-redis** smoke tests run as part of the normal `go test ./...` and need
no external services — they boot an in-process `redimos` server
(`command.NewRouter` + `server.New` on an ephemeral port) and drive it with the
real client library:

| File | Client | Protocol | What it proves |
| --- | --- | --- | --- |
| `goredis_v9_test.go` | `github.com/redis/go-redis/v9` | RESP3-capable → falls back to RESP2 | HELLO fallback, PING, ECHO, AUTH success/failure, pre-auth NOAUTH |
| `goredis_v8_test.go` | `github.com/go-redis/redis/v8` | RESP2-only (no HELLO) | Baseline RESP2 connect, PING, ECHO, AUTH success/failure, pre-auth NOAUTH |

```bash
# From the redimos module root
go test -v ./test/clientmatrix/
```

`go-redis` v9 issues `HELLO` on the first command; redimos rejects it with the
unknown-command error and v9 transparently continues over RESP2. The v9 tests
assert the whole flow recovers (PONG / echo / auth all succeed). `go-redis` v8
never sends `HELLO`, so it validates the plain RESP2 path.

## What does NOT run in Go CI (manual / cross-language)

A full multi-language matrix — **jedis** (JVM) and **redis-py** (CPython) —
cannot run inside Go CI without those runtimes. The scripts below are for
operators to run **against a live redimos proxy**, and are intentionally out of
scope for `go test`.

### Start a live proxy first

```bash
# Build and run redimos locally (no auth)
go run ./cmd/redimos --listen 127.0.0.1:6380

# ...or with a requirepass to exercise the AUTH flow
go run ./cmd/redimos --listen 127.0.0.1:6380 --requirepass s3cret
```

> Adjust the flag names to match `cmd/redimos/main.go` in your checkout; the
> point is a redimos instance listening on `127.0.0.1:6380`.

### redis-py 5+ (`redis_py_smoke.py`)

```bash
python3 -m pip install "redis>=5"

# No-auth handshake / HELLO-fallback / PING / ECHO
REDIMOS_ADDR=127.0.0.1:6380 python3 redis_py_smoke.py

# AUTH flow (correct + wrong password + pre-auth rejection)
REDIMOS_ADDR=127.0.0.1:6380 REDIMOS_PASS=s3cret python3 redis_py_smoke.py
```

redis-py 5+ negotiates RESP3 with `HELLO` by default. redimos's unknown-command
reply makes the client fall back to RESP2, which is exactly the compatibility
behavior Requirement 2.1 guarantees.

### jedis (`JedisSmoke.java`)

`JedisSmoke.java` is a self-contained `main` you can compile against a jedis jar
(4.x or 5.x). It performs `PING`, `ECHO`, and — when `REDIMOS_PASS` is set — the
`AUTH` flow.

```bash
# With jedis + its deps on the classpath (example paths)
javac -cp "jedis-5.1.0.jar" JedisSmoke.java
REDIMOS_ADDR=127.0.0.1:6380 java -cp ".:jedis-5.1.0.jar:slf4j-api.jar:commons-pool2.jar" JedisSmoke
```

jedis speaks RESP2 by default (it does not force `HELLO`), so it connects
directly; the snippet documents the same PING / ECHO / AUTH contract for the JVM
ecosystem.

## Summary

| Client | Runtime | In Go CI? | Covered by |
| --- | --- | --- | --- |
| go-redis v9 | Go | ✅ yes | `goredis_v9_test.go` |
| go-redis v8 | Go | ✅ yes | `goredis_v8_test.go` |
| redis-py 5+ | CPython | ❌ manual | `redis_py_smoke.py` |
| jedis 4.x/5.x | JVM | ❌ manual | `JedisSmoke.java` |
