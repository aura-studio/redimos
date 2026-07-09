# In-process embedding (`redimos.NewInProcessClient`)

redimos can be embedded **in-process** and driven with the standard
[go-redis](https://github.com/redis/go-redis) client over an **in-memory
connection** — no TCP, no kernel networking. This is an additive API alongside the
`cmd/redimos` TCP proxy binary; the TCP path is unchanged.

```go
client, closer, err := redimos.NewInProcessClient(ddb, redimos.Options{
    Table:   "redis-data",
    MultiDB: true,
})
if err != nil {
    return err
}
defer closer.Close()

client.Set(ctx, "k", "v", 0) // standard *redis.Client, but in-process
```

## What it is

`NewInProcessClient` returns a `*redis.Client` (the ordinary go-redis client) wired
to an in-process redimos proxy through a **buffered in-memory `net.Conn`**. Every
go-redis command the proxy implements works exactly as it does over TCP — same RESP2
wire behaviour, same command semantics, same per-connection serial pipelining — but
nothing touches the network.

It differs from the TCP proxy in exactly two ways, both intentional:

- **Synchronous delete.** A `DEL` (and any expiry-driven reclamation) removes the
  key's meta and then reclaims its member items **inline, before the command
  returns**, under the same `IsLive` recreate-guard the async path uses (a
  DEL-then-recreate race never wipes the new incarnation). There is no lazy-delete
  queue and no window in which members linger.
- **Zero background goroutines.** The embedding starts **none** of the TCP binary's
  background workers: no async lazy-delete worker, no orphan sweeper, no backend
  health probe, and no metrics HTTP server. The only goroutines are the
  per-connection redcon serving goroutines (one per go-redis pooled connection), and
  `closer.Close()` ends those too.

Everything else — command dispatch, the storage seam over redimo/DynamoDB, the meta
type/expiry/counter logic, SCAN cursor handling — is the **same code** the TCP binary
runs; the embedding just re-wires it without the network transport and background
workers.

## When to use which

| Use | When |
| --- | --- |
| **`NewInProcessClient`** (this API) | Your Go program wants Redis-3.2 semantics on DynamoDB, in-process, driven with the familiar go-redis API, with synchronous deletes and no background goroutines. Best for embedding in a service or a test without standing up a separate proxy process. |
| **`cmd/redimos` TCP proxy** | Multiple / non-Go clients, or you want the operational surface: `/metrics`, `/healthz`, `/readyz`, the async lazy-delete queue, the orphan sweeper, circuit breaker, request logging. Run it as a sidecar/standalone and connect any Redis client over TCP. |
| **redimo library directly** | You want the DynamoDB-native API (not RESP/Redis semantics) and are willing to give up the Redis command surface, meta layer and proxy behaviour. |

## API

```go
func NewInProcessClient(ddb *dynamodb.Client, opts Options) (*redis.Client, io.Closer, error)
```

- `ddb` — a configured AWS SDK v2 `*dynamodb.Client` (the same client you would pass
  to the TCP binary via its endpoint/credential flags).
- Returns the go-redis `*redis.Client`, an `io.Closer` that tears the proxy down
  (closes every in-memory connection, ending its serving goroutine), and any
  construction error.
- Call `closer.Close()` when done. Closing the returned `*redis.Client` too is fine
  (and conventional), but closing the `io.Closer` already severs the server side.

### `Options`

The embedding-relevant subset of the `cmd/redimos` flags — the knobs that shape the
Store and the command Router. Background-worker and observability flags do not apply
(the embedding starts none of those). The zero value builds a single-DB,
strongly-consistent proxy over the redimo default table.

| Field | Meaning | Default |
| --- | --- | --- |
| `Table` | DynamoDB single-table name. | redimo default table |
| `Consistency` | `"strong"` (reads its own writes, like Redis) or `"eventual"`. | `"strong"` |
| `MultiDB` | Permit `SELECT` of a non-zero DB index (keys map to a `d{n}:` prefix). | `false` |
| `Databases` | Logical DB count `SELECT` accepts when `MultiDB` is set: valid index `[0, Databases)`. | `16` |
| `MaxCollectionResult` | Cap members a whole-collection reply (HGETALL/SMEMBERS/LRANGE/…) may materialize; `0` disables. | `0` |
| `MaxCommandBytes` | Reject a single command whose raw wire size exceeds N bytes; `0` disables. | `0` |
| `AutoCreateTable` | Create the table with redimo's schema if missing, else verify an existing table is redimo-compatible, before returning (mirrors the CLI `-auto-create-table`; needs `dynamodb:DescribeTable`+`CreateTable`). | `false` |

## Example

Complete, copy-pasteable examples live in `example_test.go`:
`Example_inProcessClient`, and `Example_inProcessClient_autoCreateTable` (which sets
`AutoCreateTable: true` so the embedding provisions its own table on first use). In short:

```go
client, closer, err := redimos.NewInProcessClient(ddb, redimos.Options{
    Table:   "redis-data",
    MultiDB: true,
})
if err != nil {
    log.Fatal(err)
}
defer closer.Close()

ctx := context.Background()
client.Set(ctx, "greeting", "hello", 0)
val, _ := client.Get(ctx, "greeting").Result() // "hello"

client.HSet(ctx, "user:1", "name", "ada") // Redis 3.2 HSET: one field/value per call
name, _ := client.HGet(ctx, "user:1", "name").Result() // "ada"

client.Del(ctx, "greeting", "user:1") // synchronous: members reclaimed before Del returns
```

## Notes / limitations

- The client speaks **RESP2** (`Protocol: 2`) with `DisableIdentity: true` so it does
  not depend on `HELLO` / `CLIENT SETINFO` (which redimos declines like Redis 3.2).
- The transport is a buffered in-memory conn (not `net.Pipe`): go-redis pipelines its
  connection setup, and `net.Pipe`'s synchronous/unbuffered writes would deadlock. The
  buffered conn's writes never block, so pipelining is safe.
- Because deletes are synchronous, a `DEL` of a very large collection does its member
  reclamation on the calling goroutine — sized like the TCP binary's per-batch
  `DeleteMembers`, but not rate-limited or backgrounded. For workloads that delete
  huge keys and need the reclamation off the request path, prefer the TCP proxy's
  async lazy-delete.
