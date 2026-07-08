# 命令覆盖（Command Coverage）

本文件记录 redimos 通过**进程内 go-redis 客户端**（`redimos.NewInProcessClient`）实测得到的
命令**可用 / 不可用**清单，v1 与 v2 两条线分别列出。

- 数据来源：根包 `coverage_test.go` 的 `TestCommandCoverage`，经 `client.Do(ctx, args...)`
  逐条发送、按回包错误分类；测试受环境变量门控（未设 `REDIMOS_DDB_ENDPOINT` /
  `REDIMOS_DDB_TABLE` 时跳过），因此离线 `go test ./...` 保持全绿。
- 分类规则：回包错误文本（不区分大小写）含 `unknown command`（未注册）、`not supported`、
  `is disabled`、`invalid DB index`（后三者为设计内拒绝）→ **不可用**；其余（成功，或仅因
  本次入参触发 WRONGTYPE / 语法等**逐值**错误）→ **可用**（命令确实被路由与校验，只是不满意
  这组具体参数）。
- **v1 门控多于 v2**：v1（redimo v1.7.2）把位操作（SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/
  BITFIELD）、HyperLogLog（PFADD/PFCOUNT/PFMERGE）、SETRANGE/GETRANGE/STRLEN/SUBSTR/APPEND/
  SETEX/PSETEX、HSTRLEN、LINSERT，以及 EXPIRE/PERSIST/RENAME/KEYS 等**未注册**（回
  `unknown command` 或设计内拒绝），这些在 v2（redimo v2）上均已可用。
- **v1.7.2 新增只读内省（供 Redis 桌面管理器浏览键树）**：SCAN / TYPE / TTL / PTTL / HSCAN /
  SSCAN / ZSCAN 已由 redimo v1.7.2 的**只读**原语接通（`Client.ScanKeys` 原始表扫描枚举 pk、
  `Client.TypeOf` 按 item 形状判类型），**不改写入格式、不碰任何原有命令**，全兼容线上旧数据。
  注意：① TTL/PTTL 对存在键恒返回 `-1`（v1 无过期机制），缺失键 `-2`；② TYPE 对
  string/list/hash **精确**，对 **set 与 zset** 用 `skN` 量级启发式（v1.6.1/1.7.0 无类型标记、
  两者共享存储无法逐项区分：set 成员是随机 int63、zset 是分数，~99% 准；极端情形——如分数全为
  ≥2⁵² 大整数的 zset——可能判成 set，仅影响 GUI 查看器选择、不影响数据本身）。

> 注：两条线共同的“不可用”几乎都是**真正的 Redis 3.2 之外或设计内拒绝**：Pub/Sub、事务
> （MULTI/EXEC/WATCH/DISCARD）、Lua（EVAL/SCRIPT）、阻塞命令（BLPOP/BRPOP）、DUMP/RESTORE、
> RANDOMKEY、MOVE、OBJECT、FLUSHDB/FLUSHALL、HELLO，以及 Redis 6/7 才有的
> GETEX/GETDEL/COPY/UNLINK/LMOVE/SMISMEMBER/ZMSCORE/ZPOPMIN/ZPOPMAX/ZRANDMEMBER/HRANDFIELD/
> GEOSEARCH。

> **流水线（pipelining）vs 事务（transactions）**：进程内客户端的**普通流水线可用**——
> 多条命令批量下发、一次 flush，经缓冲内存连接不会死锁，按序返回正确回包（见
> `TestInProcessClient_Pipelining`，实测 SET/GET/LPUSH/LRANGE 批处理正确）。但
> **事务型流水线 `TxPipeline()`（MULTI/EXEC）不可用**：redimos 按设计门控 MULTI/EXEC/WATCH，
> 故 `TxPipelined` 返回 `ERR transactions (MULTI/EXEC/WATCH) are not supported on this proxy`
> （go-redis 会因此丢弃该连接并打印一行 `Conn has unread data ... removing it` 日志，属预期）。

---

## v1（redimo v1.6.1）

实测表 `redis-data-v1`（String 键）。**可用 97，不可用 56。**（v1.7.2 起 SCAN/TYPE/TTL/PTTL/HSCAN/SSCAN/ZSCAN 由不可用转为可用。）

### 可用（USABLE, 90）

| 家族 | 数量 | 命令 |
| --- | --- | --- |
| strings | 12 | DECR, DECRBY, GET, GETSET, INCR, INCRBY, INCRBYFLOAT, MGET, MSET, MSETNX, SET, SETNX |
| hashes | 14 | HDEL, HEXISTS, HGET, HGETALL, HINCRBY, HINCRBYFLOAT, HKEYS, HLEN, HMGET, HMSET, HSCAN, HSET, HSETNX, HVALS |
| lists | 13 | LINDEX, LLEN, LPOP, LPUSH, LPUSHX, LRANGE, LREM, LSET, LTRIM, RPOP, RPOPLPUSH, RPUSH, RPUSHX |
| sets | 15 | SADD, SCARD, SDIFF, SDIFFSTORE, SINTER, SINTERSTORE, SISMEMBER, SMEMBERS, SMOVE, SPOP, SRANDMEMBER, SREM, SSCAN, SUNION, SUNIONSTORE |
| zsets | 21 | ZADD, ZCARD, ZCOUNT, ZINCRBY, ZINTERSTORE, ZLEXCOUNT, ZRANGE, ZRANGEBYLEX, ZRANGEBYSCORE, ZRANK, ZREM, ZREMRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYLEX, ZREVRANGEBYSCORE, ZREVRANK, ZSCAN, ZSCORE, ZUNIONSTORE |
| geo | 6 | GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEORADIUSBYMEMBER |
| generic | 7 | DEL, EXISTS, PTTL, SCAN, TOUCH, TTL, TYPE |
| connection | 9 | CLIENT, COMMAND, CONFIG, DBSIZE, ECHO, INFO, PING, SELECT, TIME |

### 不可用（NOT-USABLE, 63）

| 家族 | 数量 | 命令（原因） |
| --- | --- | --- |
| strings | 15 | APPEND, BITCOUNT, BITFIELD, BITOP, BITPOS, GETBIT, GETDEL, GETEX, GETRANGE, PSETEX, SETBIT, SETEX, SETRANGE, STRLEN, SUBSTR — 全部 `unknown command`（未注册；GETDEL/GETEX 为 Redis 6+，其余为 v1 线整批门控） |
| hashes | 2 | HRANDFIELD（Redis 6.2）, HSTRLEN（v1 门控，无字段长度原语）— `unknown command` |
| lists | 4 | BLPOP, BRPOP — 设计内拒绝（阻塞命令不支持）；LINSERT, LMOVE — `unknown command`（LINSERT v1 门控；LMOVE 为 Redis 6.2） |
| sets | 1 | SMISMEMBER（Redis 6.2）— `unknown command` |
| zsets | 4 | ZMSCORE（Redis 6.2）, ZPOPMAX/ZPOPMIN（Redis 5.0）, ZRANDMEMBER（Redis 6.2）— `unknown command` |
| geo | 1 | GEOSEARCH（Redis 6.2）— `unknown command` |
| generic | 15 | EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT, PERSIST, KEYS, RENAME, RENAMENX, COPY, UNLINK — `unknown command`（EXPIRE 家族/PERSIST：v1 无 TTL 存储，注册会撒谎故**仍门控**；KEYS/RENAME：v1 门控；COPY/UNLINK 为 Redis 6+）；DUMP, RESTORE, OBJECT, MOVE, RANDOMKEY — 设计内拒绝（`not supported`）。**注：TTL/PTTL/TYPE/SCAN 已在 v1.7.2 转为可用（见上表）** |
| hll | 3 | PFADD, PFCOUNT, PFMERGE — `unknown command`（v1 门控） |
| connection | 3 | FLUSHDB, FLUSHALL — 设计内拒绝（`is disabled`，会清空整表）；HELLO — `unknown command`（回退 RESP2） |
| pubsub/txn/script | 8 | SUBSCRIBE, PUBLISH（Pub/Sub 不支持）；MULTI, EXEC, DISCARD, WATCH（事务不支持）；EVAL, SCRIPT（Lua 不支持）— 全部设计内拒绝（`not supported`） |

---

## v2（redimo v2）

实测表 `redis-data`（Binary 键）。**可用 120，不可用 33。**

### 可用（USABLE, 120）

| 家族 | 数量 | 命令 |
| --- | --- | --- |
| strings | 25 | APPEND, BITCOUNT, BITFIELD, BITOP, BITPOS, DECR, DECRBY, GET, GETBIT, GETRANGE, GETSET, INCR, INCRBY, INCRBYFLOAT, MGET, MSET, MSETNX, PSETEX, SET, SETBIT, SETEX, SETNX, SETRANGE, STRLEN, SUBSTR |
| hashes | 15 | HDEL, HEXISTS, HGET, HGETALL, HINCRBY, HINCRBYFLOAT, HKEYS, HLEN, HMGET, HMSET, HSCAN, HSET, HSETNX, HSTRLEN, HVALS |
| lists | 14 | LINDEX, LINSERT, LLEN, LPOP, LPUSH, LPUSHX, LRANGE, LREM, LSET, LTRIM, RPOP, RPOPLPUSH, RPUSH, RPUSHX |
| sets | 15 | SADD, SCARD, SDIFF, SDIFFSTORE, SINTER, SINTERSTORE, SISMEMBER, SMEMBERS, SMOVE, SPOP, SRANDMEMBER, SREM, SSCAN, SUNION, SUNIONSTORE |
| zsets | 21 | ZADD, ZCARD, ZCOUNT, ZINCRBY, ZINTERSTORE, ZLEXCOUNT, ZRANGE, ZRANGEBYLEX, ZRANGEBYSCORE, ZRANK, ZREM, ZREMRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYLEX, ZREVRANGEBYSCORE, ZREVRANK, ZSCAN, ZSCORE, ZUNIONSTORE |
| geo | 6 | GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEORADIUSBYMEMBER |
| generic | 12 | DEL, EXISTS, EXPIRE, EXPIREAT, PERSIST, PEXPIRE, PEXPIREAT, PTTL, SCAN, TOUCH, TTL, TYPE |
| hll | 3 | PFADD, PFCOUNT, PFMERGE |
| connection | 9 | CLIENT, COMMAND, CONFIG, DBSIZE, ECHO, INFO, PING, SELECT, TIME |

### 不可用（NOT-USABLE, 33）

| 家族 | 数量 | 命令（原因） |
| --- | --- | --- |
| strings | 2 | GETDEL, GETEX（Redis 6+）— `unknown command` |
| hashes | 1 | HRANDFIELD（Redis 6.2）— `unknown command` |
| lists | 3 | BLPOP, BRPOP — 设计内拒绝（阻塞命令不支持）；LMOVE（Redis 6.2）— `unknown command` |
| sets | 1 | SMISMEMBER（Redis 6.2）— `unknown command` |
| zsets | 4 | ZMSCORE（Redis 6.2）, ZPOPMAX/ZPOPMIN（Redis 5.0）, ZRANDMEMBER（Redis 6.2）— `unknown command` |
| geo | 1 | GEOSEARCH（Redis 6.2）— `unknown command` |
| generic | 10 | COPY, UNLINK（Redis 6+）— `unknown command`；DUMP, RESTORE, OBJECT, MOVE, RANDOMKEY, RENAME, RENAMENX — 设计内拒绝（`not supported`）；KEYS — 设计内拒绝（`is disabled`，改用 SCAN） |
| connection | 3 | FLUSHDB, FLUSHALL — 设计内拒绝（`is disabled`，会清空整表）；HELLO — `unknown command`（回退 RESP2） |
| pubsub/txn/script | 8 | SUBSCRIBE, PUBLISH（Pub/Sub 不支持）；MULTI, EXEC, DISCARD, WATCH（事务不支持）；EVAL, SCRIPT（Lua 不支持）— 全部设计内拒绝（`not supported`） |

---

## v1 → v2 关键差异

v2 相对 v1 新增可用的命令（v1 上均为 `unknown command`，v2 上已可用）：

- **位操作**：SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD
- **过期 / 类型 / 迭代**：EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT, TTL, PTTL, PERSIST, TYPE, SCAN
- **字符串区间 / 追加 / 定长**：APPEND, STRLEN, SETRANGE, GETRANGE, SUBSTR, SETEX, PSETEX
- **HyperLogLog**：PFADD, PFCOUNT, PFMERGE
- **子迭代 / 定长**：HSCAN, HSTRLEN, SSCAN, ZSCAN
- **列表插入**：LINSERT

两条线的“不可用”交集就是**设计内拒绝**（Pub/Sub、事务、Lua、阻塞、DUMP/RESTORE、RANDOMKEY、
MOVE、OBJECT、FLUSHDB/FLUSHALL、HELLO）加上 **Redis 3.2 之后才引入的命令**
（GETEX/GETDEL/COPY/UNLINK/LMOVE/SMISMEMBER/ZMSCORE/ZPOPMIN/ZPOPMAX/ZRANDMEMBER/HRANDFIELD/
GEOSEARCH）。其中 KEYS 与 RENAME/RENAMENX 在 v1 表现为 `unknown command`（未注册），在 v2 表现为
设计内拒绝（注册了 handler 但明确拒绝）——两者都不可用，仅拒绝机制不同。

## 复现

```sh
# v1
MSYS_NO_PATHCONV=1 docker run --rm --add-host=host.docker.internal:host-gateway \
  -v W:/github.com/aura-studio/redimos:/src -v C:/Users/User/go/pkg/mod:/go/pkg/mod \
  -w /src -e GOTOOLCHAIN=local -e GOFLAGS=-mod=mod -e GOPROXY=off \
  -e REDIMOS_DDB_ENDPOINT=http://host.docker.internal:8000 -e REDIMOS_DDB_TABLE=redis-data-v1 \
  golang:1.25 sh -c 'go test . -count=1 -run TestCommandCoverage -v'

# v2（在 v2 工作树，表名 redis-data）
#   -v W:/github.com/aura-studio/redimos-v2-wt:/src ... -e REDIMOS_DDB_TABLE=redis-data
```
