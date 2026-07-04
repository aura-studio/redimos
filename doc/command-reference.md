# Redis 3.2 命令 × redimos 存储边界对照

> 数据来源：`redis/redis` @ branch `3.2` · `src/server.c` 的 `redisCommandTable`（174 条真实命令，`QUIT` 在查表前被拦截故不在表内）。
> 判据：命令带 `w`（写）/`r`（读键空间）标志即触及键空间；只有触及键空间**且被 redimos 支持**的命令才真正经 redimo（DynamoDB 存储层）。
> 本表由 `doc/gen/` 的脚本从命令表自动生成，随 redimos 版本更新。

## 汇总

| 处置 | 数量 | 说明 |
|---|---:|---|
| **经 redimo** | 113 | 数据/键状态读写，真正打 DynamoDB |
| 桩 | 7 | 固定/内存态回答（如 DBSIZE→`:0`），不碰键空间 |
| 连接 | 4 | 仅连接状态（AUTH/SELECT/PING/ECHO） |
| 代理拒绝 | 24 | 定制拒绝（KEYS/RENAME） |
| 不支持 | 26 | 未知命令路径（是数据命令但 redimos 未支持/超范围） |
| **合计** | 174 | 其中 113 需要 redimo |

近期在 v1.4.0 新增并经 redimo 的命令（此前为「不支持」）：**MSETNX · SUBSTR · TOUCH · ZLEXCOUNT · ZREMRANGEBYLEX**。

## 需要 redimo（数据面，经存储层） — 113 条

数据与键的元信息（类型/TTL/计数）都存在 DynamoDB，这些命令必须读/写它。

| 命令 | sflags | firstkey | 键空间 | 家族 | 走 redimo | 原因 |
|---|---|---:|:---:|---|:---:|---|
| `append` | `wm` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `decr` | `wmF` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `decrby` | `wmF` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `get` | `rF` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `getrange` | `r` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `getset` | `wm` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `incr` | `wmF` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `incrby` | `wmF` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `incrbyfloat` | `wmF` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `mget` | `r` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `mset` | `wm` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `msetnx` | `wm` | 1 | 是 | string | 是 | **✓** 新增 v1.4.0 → 经 redimo |
| `psetex` | `wm` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `set` | `wm` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `setex` | `wm` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `setnx` | `wmF` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `setrange` | `wm` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `strlen` | `rF` | 1 | 是 | string | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `del` | `w` | 1 | 是 | key/expiry | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `exists` | `rF` | 1 | 是 | key/expiry | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `expire` | `wF` | 1 | 是 | key/expiry | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `expireat` | `wF` | 1 | 是 | key/expiry | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `persist` | `wF` | 1 | 是 | key/expiry | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `pexpire` | `wF` | 1 | 是 | key/expiry | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `pexpireat` | `wF` | 1 | 是 | key/expiry | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `pttl` | `rF` | 1 | 是 | key/expiry | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `touch` | `rF` | 1 | 是 | key/expiry | 是 | **✓** 新增 v1.4.0 → 经 redimo |
| `ttl` | `rF` | 1 | 是 | key/expiry | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `type` | `rF` | 1 | 是 | key/expiry | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hdel` | `wF` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hexists` | `rF` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hget` | `rF` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hgetall` | `r` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hincrby` | `wmF` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hincrbyfloat` | `wmF` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hkeys` | `rS` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hlen` | `rF` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hmget` | `r` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hmset` | `wm` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hscan` | `rR` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hset` | `wmF` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hsetnx` | `wmF` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hstrlen` | `rF` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `hvals` | `rS` | 1 | 是 | hash | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `lindex` | `r` | 1 | 是 | list | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `linsert` | `wm` | 1 | 是 | list | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `llen` | `rF` | 1 | 是 | list | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `lpop` | `wF` | 1 | 是 | list | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `lpush` | `wmF` | 1 | 是 | list | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `lpushx` | `wmF` | 1 | 是 | list | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `lrange` | `r` | 1 | 是 | list | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `lrem` | `w` | 1 | 是 | list | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `lset` | `wm` | 1 | 是 | list | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `ltrim` | `w` | 1 | 是 | list | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `rpop` | `wF` | 1 | 是 | list | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `rpoplpush` | `wm` | 1 | 是 | list | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `rpush` | `wmF` | 1 | 是 | list | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `rpushx` | `wmF` | 1 | 是 | list | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `sadd` | `wmF` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `scard` | `rF` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `sdiff` | `rS` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `sdiffstore` | `wm` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `sinter` | `rS` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `sinterstore` | `wm` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `sismember` | `rF` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `smembers` | `rS` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `smove` | `wF` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `spop` | `wRF` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `srandmember` | `rR` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `srem` | `wF` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `sscan` | `rR` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `substr` | `r` | 1 | 是 | set | 是 | **✓** 新增 v1.4.0 → 经 redimo |
| `sunion` | `rS` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `sunionstore` | `wm` | 1 | 是 | set | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zadd` | `wmF` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zcard` | `rF` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zcount` | `rF` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zincrby` | `wmF` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zinterstore` | `wm` | 0 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zlexcount` | `rF` | 1 | 是 | zset | 是 | **✓** 新增 v1.4.0 → 经 redimo |
| `zrange` | `r` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zrangebylex` | `r` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zrangebyscore` | `r` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zrank` | `rF` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zrem` | `wF` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zremrangebylex` | `w` | 1 | 是 | zset | 是 | **✓** 新增 v1.4.0 → 经 redimo |
| `zremrangebyrank` | `w` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zremrangebyscore` | `w` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zrevrange` | `r` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zrevrangebylex` | `r` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zrevrangebyscore` | `r` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zrevrank` | `rF` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zscan` | `rR` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zscore` | `rF` | 1 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `zunionstore` | `wm` | 0 | 是 | zset | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `scan` | `rR` | 0 | 是 | scan | 是 | 数据/键状态读写 → 经 redimo 映射到 DynamoDB |
| `bitcount` | `r` | 1 | 是 | bit | 是 | **✓** 新增 v1.6.0 → 经 redimo（BIT，单键字节兼容；BITOP 多键非原子） |
| `bitfield` | `wm` | 1 | 是 | bit | 是 | **✓** 新增 v1.6.0 → 经 redimo（BIT，单键字节兼容；BITOP 多键非原子） |
| `bitop` | `wm` | 2 | 是 | bit | 是 | **✓** 新增 v1.6.0 → 经 redimo（BIT，单键字节兼容；BITOP 多键非原子） |
| `bitpos` | `r` | 1 | 是 | bit | 是 | **✓** 新增 v1.6.0 → 经 redimo（BIT，单键字节兼容；BITOP 多键非原子） |
| `geoadd` | `wm` | 1 | 是 | geo | 是 | **✓** 新增 v1.5.0；v1.8.0 改字节兼容版（zset + 52-bit geohash，非 S2）→ 经 redimo 存储 |
| `geodist` | `r` | 1 | 是 | geo | 是 | **✓** 新增 v1.5.0；v1.8.0 改字节兼容版（zset + 52-bit geohash，非 S2）→ 经 redimo 存储 |
| `geohash` | `r` | 1 | 是 | geo | 是 | **✓** 新增 v1.5.0；v1.8.0 改字节兼容版（zset + 52-bit geohash，非 S2）→ 经 redimo 存储 |
| `geopos` | `r` | 1 | 是 | geo | 是 | **✓** 新增 v1.5.0；v1.8.0 改字节兼容版（zset + 52-bit geohash，非 S2）→ 经 redimo 存储 |
| `georadius` | `w` | 1 | 是 | geo | 是 | **✓** 新增 v1.5.0；v1.8.0 改字节兼容版（zset + 52-bit geohash，非 S2）→ 经 redimo 存储 |
| `georadius_ro` | `r` | 1 | 是 | geo | 是 | **✓** 新增 v1.10.0 → GEORADIUS_RO/GEORADIUSBYMEMBER_RO 只读变体，别名到已实现的 GEO 命令（禁 STORE/STOREDIST） |
| `georadiusbymember` | `w` | 1 | 是 | geo | 是 | **✓** 新增 v1.5.0；v1.8.0 改字节兼容版（zset + 52-bit geohash，非 S2）→ 经 redimo 存储 |
| `georadiusbymember_ro` | `r` | 1 | 是 | geo | 是 | **✓** 新增 v1.10.0 → GEORADIUS_RO/GEORADIUSBYMEMBER_RO 只读变体，别名到已实现的 GEO 命令（禁 STORE/STOREDIST） |
| `getbit` | `rF` | 1 | 是 | bit | 是 | **✓** 新增 v1.6.0 → 经 redimo（BIT，单键字节兼容；BITOP 多键非原子） |
| `pfadd` | `wmF` | 1 | 是 | hll | 是 | **✓** 新增 v1.7.0 → 经 redimo（HLL；PFCOUNT 低基数字节一致、高基数在误差内） |
| `pfcount` | `r` | 1 | 是 | hll | 是 | **✓** 新增 v1.7.0 → 经 redimo（HLL；PFCOUNT 低基数字节一致、高基数在误差内） |
| `pfmerge` | `wm` | 1 | 是 | hll | 是 | **✓** 新增 v1.7.0 → 经 redimo（HLL；PFCOUNT 低基数字节一致、高基数在误差内） |
| `setbit` | `wm` | 1 | 是 | bit | 是 | **✓** 新增 v1.6.0 → 经 redimo（BIT，单键字节兼容；BITOP 多键非原子） |

## 不需要 redimo — 桩 — 7 条

redimos 用固定或内存态回答，不访问 DynamoDB。

| 命令 | sflags | firstkey | 键空间 | 家族 | 走 redimo | 原因 |
|---|---|---:|:---:|---|:---:|---|
| `client` | `as` | 0 | 否 | — | 否 | 服务器自省，固定/内存态回答 |
| `command` | `lt` | 0 | 否 | — | 否 | 服务器自省，固定/内存态回答 |
| `config` | `lat` | 0 | 否 | — | 否 | 服务器自省，固定/内存态回答 |
| `dbsize` | `rF` | 0 | 是 | — | 否 | 服务器自省，固定/内存态回答；键计数用 :0 桩不扫表 |
| `info` | `lt` | 0 | 否 | — | 否 | 服务器自省，固定/内存态回答 |
| `slowlog` | `a` | 0 | 否 | — | 否 | 服务器自省，固定/内存态回答 |
| `time` | `RF` | 0 | 否 | — | 否 | 服务器自省，固定/内存态回答 |

## 不需要 redimo — 连接层 — 4 条

只操作连接状态。

| 命令 | sflags | firstkey | 键空间 | 家族 | 走 redimo | 原因 |
|---|---|---:|:---:|---|:---:|---|
| `auth` | `sltF` | 0 | 否 | — | 否 | 仅操作连接状态，不碰键空间 |
| `echo` | `F` | 0 | 否 | — | 否 | 仅操作连接状态，不碰键空间 |
| `ping` | `tF` | 0 | 否 | — | 否 | 仅操作连接状态，不碰键空间 |
| `select` | `lF` | 0 | 否 | — | 否 | 仅操作连接状态，不碰键空间 |

## 不需要 redimo — 代理拒绝 — 24 条

定制拒绝：DynamoDB 表达代价过高。

| 命令 | sflags | firstkey | 键空间 | 家族 | 走 redimo | 原因 |
|---|---|---:|:---:|---|:---:|---|
| `asking` | `F` | 0 | 否 | — | 否 | 代理拒绝：Redis Cluster 槽迁移的一次性标志，非 cluster 单一 keyspace 代理无意义（v1.11.0 起专属拒绝） |
| `blpop` | `ws` | 1 | 是 | — | 否 | 代理拒绝：阻塞命令需长连接阻塞语义（改用非阻塞 LPOP/RPOP/RPOPLPUSH；v1.10.0 起专属拒绝） |
| `brpop` | `ws` | 1 | 是 | — | 否 | 代理拒绝：阻塞命令需长连接阻塞语义（改用非阻塞 LPOP/RPOP/RPOPLPUSH；v1.10.0 起专属拒绝） |
| `brpoplpush` | `wms` | 1 | 是 | — | 否 | 代理拒绝：阻塞命令需长连接阻塞语义（改用非阻塞 LPOP/RPOP/RPOPLPUSH；v1.10.0 起专属拒绝） |
| `discard` | `sF` | 0 | 否 | — | 否 | 代理拒绝：事务需排队+原子应用多命令（v1.10.0 起专属拒绝） |
| `eval` | `s` | 0 | 否 | — | 否 | 代理拒绝：Lua 脚本需内嵌解释器（v1.10.0 起专属拒绝） |
| `evalsha` | `s` | 0 | 否 | — | 否 | 代理拒绝：Lua 脚本需内嵌解释器（v1.10.0 起专属拒绝） |
| `exec` | `sM` | 0 | 否 | — | 否 | 代理拒绝：事务需排队+原子应用多命令（v1.10.0 起专属拒绝） |
| `flushall` | `w` | 0 | 是 | — | 否 | 代理拒绝：会清空整个 DynamoDB 表（v1.5.1 起专属拒绝） |
| `flushdb` | `w` | 0 | 是 | — | 否 | 代理拒绝：会清空整个 DynamoDB 表（v1.5.1 起专属拒绝） |
| `keys` | `rS` | 0 | 是 | — | 否 | 代理拒绝：KEYS 无界全扫在 DynamoDB 上危险 |
| `multi` | `sF` | 0 | 否 | — | 否 | 代理拒绝：事务需排队+原子应用多命令（v1.10.0 起专属拒绝） |
| `psubscribe` | `pslt` | 0 | 否 | — | 否 | 代理拒绝：发布订阅需连接级订阅+跨连接 fan-out，无状态代理不适合（v1.10.0 起专属拒绝） |
| `publish` | `pltF` | 0 | 否 | — | 否 | 代理拒绝：发布订阅需连接级订阅+跨连接 fan-out，无状态代理不适合（v1.10.0 起专属拒绝） |
| `pubsub` | `pltR` | 0 | 否 | — | 否 | 代理拒绝：发布订阅需连接级订阅+跨连接 fan-out，无状态代理不适合（v1.10.0 起专属拒绝） |
| `punsubscribe` | `pslt` | 0 | 否 | — | 否 | 代理拒绝：发布订阅需连接级订阅+跨连接 fan-out，无状态代理不适合（v1.10.0 起专属拒绝） |
| `rename` | `w` | 1 | 是 | — | 否 | 代理拒绝：RENAME 需整集合搬迁，代价过高 |
| `renamenx` | `wF` | 1 | 是 | — | 否 | 代理拒绝：RENAME 需整集合搬迁，代价过高 |
| `script` | `s` | 0 | 否 | — | 否 | 代理拒绝：Lua 脚本需内嵌解释器（v1.10.0 起专属拒绝） |
| `shutdown` | `alt` | 0 | 否 | — | 否 | 代理拒绝：会终止所有租户共享的进程，且无 RDB 可先落盘（v1.11.0 起专属拒绝） |
| `subscribe` | `pslt` | 0 | 否 | — | 否 | 代理拒绝：发布订阅需连接级订阅+跨连接 fan-out，无状态代理不适合（v1.10.0 起专属拒绝） |
| `unsubscribe` | `pslt` | 0 | 否 | — | 否 | 代理拒绝：发布订阅需连接级订阅+跨连接 fan-out，无状态代理不适合（v1.10.0 起专属拒绝） |
| `unwatch` | `sF` | 0 | 否 | — | 否 | 代理拒绝：事务需排队+原子应用多命令（v1.10.0 起专属拒绝） |
| `watch` | `sF` | 1 | 否 | — | 否 | 代理拒绝：事务需排队+原子应用多命令（v1.10.0 起专属拒绝） |

## 不经 redimo — 未支持（未知命令） — 26 条

是 Redis 数据命令，但 redimos 在命令层就短路，不发起存储调用。

| 命令 | sflags | firstkey | 键空间 | 家族 | 走 redimo | 原因 |
|---|---|---:|:---:|---|:---:|---|
| `bgrewriteaof` | `a` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `bgsave` | `a` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `cluster` | `a` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `debug` | `as` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `dump` | `r` | 1 | 是 | — | 否 | 键管理：DynamoDB 表达不了/代价过高 |
| `lastsave` | `RF` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `latency` | `aslt` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `migrate` | `w` | 0 | 是 | — | 否 | 键管理：DynamoDB 表达不了/代价过高 |
| `monitor` | `as` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `move` | `wF` | 1 | 是 | — | 否 | 键管理：DynamoDB 表达不了/代价过高 |
| `object` | `r` | 2 | 是 | — | 否 | 键管理：DynamoDB 表达不了/代价过高 |
| `pfdebug` | `w` | 0 | 是 | — | 否 | HyperLogLog：可经命令层实现，尚未做（同 BIT） |
| `pfselftest` | `a` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `psync` | `ars` | 0 | 是 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `randomkey` | `rR` | 0 | 是 | — | 否 | 键管理：DynamoDB 表达不了/代价过高 |
| `readonly` | `F` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `readwrite` | `F` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `replconf` | `aslt` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `restore` | `wm` | 1 | 是 | — | 否 | 键管理：DynamoDB 表达不了/代价过高 |
| `restore-asking` | `wmk` | 1 | 是 | — | 否 | 键管理：DynamoDB 表达不了/代价过高 |
| `role` | `lst` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `save` | `as` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `slaveof` | `ast` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `sort` | `wm` | 1 | 是 | — | 否 | 键管理：DynamoDB 表达不了/代价过高 |
| `sync` | `ars` | 0 | 是 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
| `wait` | `s` | 0 | 否 | — | 否 | 服务器/复制/管理：不适用于无状态代理 |
