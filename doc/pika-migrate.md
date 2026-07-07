# redimos 作为 pika-migrate 迁移目标（实测验证）

**结论：redimos 是经真机端到端实测验证的 pika-migrate 迁移目标。** 用真实 Pika v3.2.2 作源、真实 pika-migrate 工具（Pika `v3_2_7_migrate` 分支）在线迁移 523 个键到 redimos → DynamoDB，全量同步 100% 逐字节一致、增量同步对所有确定性命令一致、代理零命令拒绝。唯一分歧是非确定性命令（`SPOP`）——属命令级复制的固有限制，非 redimos 缺陷。

这条链路正是 Pika 存量数据迁上 AWS（DynamoDB）的可行路径（如 slots-nano pika→AWS）。

---

## 1. 拓扑与机制

```
真实 Pika v3.2.2  ──(Pika 私有主从复制: dbsync 全量 + binlog 增量)──►  pika-migrate 工具
   (源, :9221)                                                        (伪装成 Pika 从库, :9222)
                                                                              │
                                                        高层 Redis 写命令 (RESP)│  target-redis-host/port
                                                                              ▼
                                                                        redimos (:6380) ──► DynamoDB
                                                                          (迁移目标)
```

- **pika-migrate 本质是一个改过的 Pika 3.2 二进制**（`v3_2_7_migrate` 分支，`Qihoo360/pika`）。它用 `slaveof <源IP> <源端口> force` **伪装成源 Pika 的从库**，走 Pika 自己的私有复制协议（**不是** Redis 的 `SYNC`/`PSYNC`）。
- **全量阶段**：向源请求 dbsync 全量快照（经 rsync 落地本地 blackwidow/RocksDB 快照），然后 `RetransmitData()` 打开快照、每种数据类型起一个 `MigratorThread` 扫描并重建**标准 Redis 写命令**发往目标：
  - string → `SET key value [EX ttl]`
  - hash → `HMSET key f v ...`（按 `sync-batch-num` 分批）+ `EXPIRE`
  - set → `SADD key m ...`（分批）+ `EXPIRE`
  - zset → `ZADD key score m ...`（分批）+ `EXPIRE`
  - list → `RPUSH key e ...`（分批）+ `EXPIRE`
- **增量阶段**：全量后继续当从库收 binlog，把每条写命令**原样转发**给目标（`SET/SETNX/SETEX/APPEND/INCR/HSET/HMSET/HDEL/SADD/SREM/ZADD/ZINCRBY/LPUSH/RPUSH/LSET/LINSERT/SETRANGE/DEL/EXPIRE/...`），仅把 Pika 内部的 `pksetexat key ts value` 改写成 `SETEX key <ts-now> value`。
- **对目标的握手极简**：只发 `PING`（无密码时，仅用来探测 `NOAUTH`）或 `AUTH <pwd>`（配了密码时）。**从不发** `SELECT`/`SYNC`/`PSYNC`/`SLAVEOF`/`REPLCONF`/`RESTORE`/`DUMP`/`MULTI`/`SCRIPT`。全部写默认 DB0。回包被批量丢弃（约每 200 条读一次）。

**为什么 redimos 天然兼容**：redimos 拒绝的全是复制/RDB 序列化类命令（`SYNC`/`PSYNC`/`SLAVEOF`/`REPLCONF`/`RESTORE`/`DUMP`/`MIGRATE`），而这些只在**源侧**用 Pika 私有方言说，**从不发给目标**。目标只收 redimos 完全支持的高层写命令。

---

## 2. 操作步骤（含实测踩坑）

> **开箱即用**：本仓库 [`deploy/pika-migrate/`](../deploy/pika-migrate/) 已把下面这套构建 + 修复 + 配置固化成一个 `Dockerfile` + `entrypoint.sh`（用环境变量配目标),实测容器化跑通同一套 523 键迁移(0 不一致)。想手工理解或定制看下面步骤；想直接用看该目录 README。

前置：源 Pika v3.2.2（镜像 `pikadb/pika:v3.2.2`，注意该镜像无 ENTRYPOINT，须显式 `pika -c /pika/conf/pika.conf`，端口 9221）、redimos 目标已起（默认 DB0 即可，无需 `-multi-db`）。

1. **构建 pika-migrate**（C++，gcc 4.8 系；无预编译包）：
   ```
   git clone --depth 1 -b v3_2_7_migrate https://github.com/Qihoo360/pika.git
   cd pika && git submodule update --init --recursive --depth 1 && make -j4
   ```
   踩坑：老 3.2 镜像里 (a) 缺 `libstdc++.so` 开发符号链接（`ln -sf /usr/lib64/libstdc++.so.6.0.19 /usr/lib64/libstdc++.so`），或 (b) Makefile 的 `-static-libstdc++` 找不到 64 位 `libstdc++.a` → 去掉 `-static-libstdc++` 改动态链接即可（在同镜像里跑，运行时有 `.so.6`）。产物 `output/bin/pika`。
2. **配置目标**（`conf/pika.conf`，非命令行）：`target-redis-host`/`target-redis-port`/`target-redis-pwd`、`sync-batch-num`（分批大小）、`redis-sender-num`（并发发送线程）、`instance-mode : classic` + `databases : 1`（**必须单 DB，否则 FATAL 退出**）。建议在**源**上 `config set expire-logs-nums 10000` 让 binlog 撑过全量窗口。
3. **启动工具**：`pika -c pika.conf`（监听 9222，其 rsync 服务占 `端口+1000`=10222）。
4. **触发迁移**：`redis-cli -p 9222 slaveof <源IP> 9221 force`。

**关键踩坑（不然会失败/崩溃）：**
- ⚠️ **`slaveof` 必须用源的 IP，不能用主机名**。工具收到 dbsync 快照后会校验快照 info 里的 `master_ip`（真实 IP）是否等于 `slaveof` 时存的 master ip（`pika_partition.cc` 的 sanity check）；用主机名会 `Error master node ip port` 失败。
- ⚠️ **每个工具进程只允许一次 DBSync**（`pika_rm.cc` 的 `already_dbsync` 静态量，第二次直接 `LOG(FATAL)` 崩溃，避免向目标重复灌数据）。要重迁必须**重启工具进程**（并 `pkill` 掉残留的 `rsync` 守护进程，否则 10222 端口被占，启动即 FATAL）。
- 重启工具/重连时，源侧从库注册有 ~30s recv-timeout，太快重连会 `Meta Sync Failed: Slave AlreadyExist`；重启源（数据落盘不丢）可立即清干净。
- dbsync 全量走 **rsync**，源和从（工具）两侧都要装 `rsync` 二进制。

---

## 3. 实测覆盖与结果

真机（Docker）：源 `pikadb/pika:v3.2.2` → pika-migrate（自建 `v3_2_7_migrate`）→ redimos → DynamoDB Local。差分校验用 go-redis 逐键比对（类型 + 值 + TTL 存在性），另单独校验 TTL 数值。

| 维度 | 结果 |
|---|---|
| 全量同步，523 键，5 种类型 | ✅ **0 处不一致** |
| 大集合（set 250 / hash 200 field / zset 150 / list 300，按 100 分批） | ✅ 计数与内容全对 |
| 二进制 / Unicode / 空串 值 | ✅ 逐字节一致 |
| TTL 数值（string/hash/setex） | ✅ 精确一致（2579/98692/4579 逐一相等）|
| 增量：20+ 命令类型（SET/APPEND/INCR/INCRBY/HSET/HINCRBY/SADD/SREM/ZADD/ZINCRBY/ZREM/RPUSH/LPUSH/LSET/LINSERT/SETRANGE/DEL/SETEX/EXPIRE …）| ✅ 全部正确复制 |
| 非确定性 `SPOP` | ⚠️ 分歧（见下）|
| redimos 拒绝/报错的命令数 | ✅ **0**（工具侧 0 WRONGTYPE/unknown、redimos 侧 0 unmapped error）|

---

## 4. 已知限制与注意事项

- **非确定性命令（`SPOP`）在增量阶段会分歧**：pika-migrate 把 `SPOP key` **原样转发**给目标，目标独立地随机弹出一个成员 → 与源弹出的成员大概率不同 → 该键分歧。这是**命令级复制的固有问题**（真 Redis 会把 `SPOP` 改写成确定性的 `SREM <成员>` 再复制；pika-migrate 不改写），**不是 redimos 缺陷**——redimos 的 `SPOP` 行为正确。规避：迁移期间避免对源发 `SPOP`（及其它非确定性写）；或对受影响键**重做一次确定性写**（实测 `DEL`+`SADD` 重建后增量即收敛，523 键归零分歧）。
- **TTL 亚秒精度**：redimos 内部秒粒度（`PEXPIRE`/`PSETEX`/`PTTL` 截断到秒）。pika-migrate 全量用 `EXPIRE <秒>`，无影响；若源用毫秒级 TTL，目标会截断到秒（与 redimos 既有的 Pika 对齐行为一致）。
- **`INFO`/`DBSIZE` 是桩**：redimos `INFO` 无 `# Keyspace`、`DBSIZE` 恒 0。pika-migrate 目标握手只用 `PING`/`AUTH`，不受影响；但依赖目标 `# Keyspace`/`DBSIZE` 做进度校验的外部工具会读到空/0。
- **单值大小**：redimos 值落 DynamoDB 单项（~400KB 上限，减去开销约 390KB 可用）。超大单值的 `SET` 会被后端拒绝——同 redimos 既有约束。
- **多 DB**：pika-migrate 仅迁 DB0 且从不发 `SELECT`，故目标用不用 `-multi-db` 都行。若换用会发 `SELECT n` 的迁移工具，则目标必须带 `-multi-db`。

### 4.1 迁移完整性边界（round-10 兼容维度审计补充）

「523 键 0 不一致」是针对一份具体测试负载的实测结果。系统审计 Pika 能写入 binlog 的全部写命令后，以下边界会让**特定操作静默丢失**（回包被 pika-migrate 批量丢弃，工具侧无告警）——多为**源侧使用了 redimos 有意不支持/受平台限制的写**，非 redimos 缺陷；列此供迁移前评估与规避。

- **redimos 拒绝的写命令**（`SORT … STORE` / `MOVE` / `RESTORE`）：若源在**增量期**执行了这些写，pika-migrate 原样转发 → redimos 拒 → 该操作丢失。`SORT STORE`（Redis 3.2 是写；Pika blackwidow 对 `SORT` 支持有限）、`MOVE`（跨 DB，单 DB 迁移一般不触发）、客户端 `RESTORE`（罕见）。规避：迁移期避免对源发这些命令，或迁移后对受影响键重做一次确定性写。**注**：`RENAME`/`RENAMENX`/`FLUSHDB`/`FLUSHALL`/`PERSIST` 已实现，不在此列。
- **超大值 / 成员名 / 键名被 guard 拒**：Pika（RocksDB）单值可达几十 MB，redimos 在 ~390KB（值）/ 1023B（集合成员名/哈希字段名）/ ~2046B（键名）处 `guard` 拒 → 该键/操作丢失。规避：迁移前确认源无超限键（同 §4「单值大小」，扩展到成员/键名）。
- **批量大小 flag 必须为默认 0**：全量按 `sync-batch-num`（常 100+）发大 `HMSET`/`SADD`/`ZADD`/`RPUSH`。目标 redimos 若设了 `-max-collection-result` 或 `-max-command-bytes` 非 0，超阈批量会被拒 → 丢失。**迁移目标保持这两个 cap 为默认 0（禁用）**。
- **`pksetexat` → `SETEX <ts-now>` 的 TTL 漂移**：pika-migrate 把 Pika 内部的绝对过期时刻改写成相对 `SETEX`，差值按工具端解析时刻算；若迁移慢或时钟不同步，目标 TTL 会偏移，极端情形 `ts ≤ now` 时算出 `SETEX ttl ≤ 0` 被拒（该近过期键丢失）。属 pika-migrate 改写 + 时钟问题，redimos `SETEX` 本身正确。
- **多步 `INCRBYFLOAT`/`HINCRBYFLOAT` 累加漂移**：binlog 转发的是增量 delta、目标独立累加。源 Pika 用 C long double（80-bit），redimos 用 Go float64 → 多步累加后第 ~17 位有效数字分叉（`redis-3.2-compatibility.md` §4.1 已接受地板）。单步值与整数计数不受影响。
- **空串浮点值（v1.51.0 已修，零丢失）**：此前源 `SET k ""` 后 binlog 转发的 `INCRBYFLOAT k <delta>`（Redis 把空串读作 0.0）在 redimos 被拒 → 静默丢。**v1.51.0 起 `""`→`0.0`（对齐 strtod）**，`INCRBYFLOAT`/`HINCRBYFLOAT`/`GEOADD`/`GEORADIUS` 的空串路径现全部对齐 3.2，该路径零丢失。

---

## 5. 目标侧命令覆盖

pika-migrate 发给目标的全部命令均被 redimos 支持：

| 阶段 | 命令 | redimos |
|---|---|---|
| 握手 | `PING` / `AUTH` | ✅ |
| 全量 string | `SET [EX]` | ✅ |
| 全量 hash | `HMSET` | ✅ |
| 全量 set | `SADD` | ✅ |
| 全量 zset | `ZADD` | ✅ |
| 全量 list | `RPUSH` | ✅ |
| 全量过期 | `EXPIRE <秒>` | ✅ |
| 增量 | 源 binlog 原样转发的写命令（`SET/SETNX/SETEX/APPEND/INCR(BY)/DECR(BY)/GETSET/SETRANGE/SETBIT/HSET/HMSET/HSETNX/HDEL/HINCRBY(FLOAT)/SADD/SREM/SPOP*/ZADD/ZREM/ZINCRBY/LPUSH/RPUSH/LPOP/RPOP/LSET/LTRIM/LREM/LINSERT/RPOPLPUSH/DEL/EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT` 等） | ✅（`SPOP*` 见 §4 非确定性）|
| 内部改写 | `pksetexat` → `SETEX` | ✅ |

> redimos 拒绝的 `SYNC`/`PSYNC`/`SLAVEOF`/`REPLCONF`/`RESTORE`/`DUMP`/`MIGRATE`/`MULTI`/`SCRIPT` 全部只在**源侧**用 Pika 私有协议交互，**从不发给目标**，故不影响本迁移链路。
