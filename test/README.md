# Differential-testing oracle (Pika v3.2.2)

This directory hosts the behavior **oracle** used to verify that `redimos` is
byte-for-byte compatible with Pika v3.2.2 (Requirement 1.6). The same RESP2
command sequence is sent to both Pika and `redimos`, and the raw replies are
compared byte-for-byte.

## What `docker-compose.yml` starts

- **Service `pika-oracle`** running the official `pikadb/pika:v3.2.2` image.
- Pika listens on its default port **9221** inside the container. We publish it
  on the host as the familiar Redis port **6379**, so local runs and CI can
  point a standard Redis client at `localhost:6379`.
- RocksDB data and logs are kept in named volumes (`pika-data`, `pika-log`) so
  restarts preserve state. Use `down -v` for a clean-room oracle between runs.

## Bring it up

From the `redimos` module root:

```bash
# Start the oracle in the background
docker compose -f test/docker-compose.yml up -d

# Check status / health
docker compose -f test/docker-compose.yml ps

# Tail Pika logs
docker compose -f test/docker-compose.yml logs -f pika-oracle

# Smoke test with any Redis client
redis-cli -p 6379 ping        # -> PONG

# Stop and remove containers + volumes (clean slate)
docker compose -f test/docker-compose.yml down -v
```

> On older Docker installs use the hyphenated `docker-compose` binary instead of
> the `docker compose` plugin subcommand.

## CI usage

CI starts the oracle before running the differential test suite and tears it
down afterwards. The suite points its Redis client at `localhost:6379` (the
published host port) and its `redimos` client at the proxy under test, then
asserts the raw RESP replies match.

## Differential assertion engine (`test/difftest`)

The `difftest` Go package (task 2.2) is the harness that sends an identical
RESP2 command sequence to both endpoints and compares the raw replies
**byte-for-byte**. It has three building blocks:

- **Raw RESP reader** (`resp.go`, `ReadReply`) — captures the exact reply bytes
  of one full frame, including type prefixes and CRLFs, recursing into arrays.
  This is what lets the engine catch differences in error text, null encodings
  (`$-1` vs `*0` vs `*-1`), and integer boundaries at the byte level.
- **Command-matrix entry point** (`matrix.go` + `TestDiffMatrix`) — a curated
  set of sequences targeting error text, null encodings, WRONGTYPE, integer
  boundaries, and TTL return values.
- **Random-sequence fuzz entry point** (`fuzz.go` + `TestDiffFuzz`) — a
  `testing/quick` generator that emits well-formed random command sequences
  across all data-structure families and asserts the two endpoints agree.

### Guarded execution

The live differential tests **skip cleanly** unless both endpoints are
configured, so `go test ./...` passes with no infrastructure:

```bash
# Skips TestDiffMatrix / TestDiffFuzz (no env vars set)
go test ./test/difftest

# Run against live endpoints (Pika oracle + redimos proxy)
PIKA_ADDR=localhost:6379 REDIMOS_ADDR=localhost:6380 \
  go test -v ./test/difftest -run 'TestDiff'
```

The RESP reader, command encoder, and fuzz generator are covered by pure
in-memory unit and property tests that always run (no endpoints required).
