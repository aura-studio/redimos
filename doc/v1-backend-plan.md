# redimos v1 线：对接 redimo v1.6.1 实现计划

> 分支 `v1`（module `github.com/aura-studio/redimos`，**不带 /v2**）。v2 线（module `…/redimos/v2`）对接 redimo v2、多 db、全功能，二者互不影响。
> 本线目标：**纯对接 redimo v1.6.1**，仅 db0，redimo v1 没有的能力一律返回 `ERR unknown command`。不改 redimo 代码。

## 核心约束（redimo v1.6.1 事实）
- rv1 Client 是**值接收者**，`NewClient(*dynamodb.Client).Table().StronglyConsistent()/.EventuallyConsistent()`；**无 `WithContext`**（内部 `context.TODO()`）。
- rv1 **无任何 Meta/类型/cnt/TTL 机制**，**任何方法都不返回 WRONGTYPE**。string 存 `sk=""`，集合存 `sk=member`。
- rv1 有高层命令方法（GET/SET/GETSET/MGET/MSET/INCR 族/H*/L*/S*/Z*/GEO*/X*），二进制安全（BytesValue）。
- rv1 **缺**：SETCAS/HSETCAS、BatchGET、EnsureType(Expiring)/CreateTypeIfAbsent/LoadMeta/SetExpire/Persist/DeleteMeta(IfEmpty)/DeleteMembers/SweepOrphans、HScanPage/ZScanPage/ScanMetaKeys/ZMembersOrdered、`redimo.KeyType`/`MetaSK`/`ErrWrongType`/`MaxBatchWriteItems`。

## 架构决定
1. **丢弃 redimos 的 meta 层**（type/cnt/TTL）。回复计数改从 rv1 直接方法取（LLEN/SCARD/ZCARD/HLEN、push/add 的返回值）。
2. **被迫接受的语义差异**（均源于所选 v1.6.1）：无 WRONGTYPE、无 TTL、INCR 尽力而为（无 SETCAS）。
3. **db0 only**：`encodePK` 恒用 db0（去掉 `{db}:` 前缀逻辑仍可留但恒 0），`SELECT n>0` → `ERR invalid DB index`。
4. **GATE = 不注册命令** → dispatch 落到 `resp.ErrUnknownCommand` → `ERR unknown command '<name>'`。

## 命令去向（PROXY 55 / PARTIAL 20 / GATE 42，源自 v1.6.1 映射工作流）
- **PROXY/PARTIAL（保留，接 rv1）**：GET SET(NX/XX) SETNX GETSET MGET MSET MSETNX INCR DECR INCRBY DECRBY INCRBYFLOAT；DEL EXISTS TOUCH；H 全套（除 HSCAN/HSTRLEN）；L：LPUSH RPUSH LPUSHX RPUSHX LPOP RPOP LLEN LINDEX LRANGE LSET LTRIM LREM；S：SADD SREM SISMEMBER SMEMBERS SCARD SPOP SRANDMEMBER SMOVE SINTER/STORE SUNION/STORE SDIFF/STORE；Z：ZADD ZREM ZCARD ZSCORE ZINCRBY ZCOUNT ZRANK ZREVRANK ZRANGE ZREVRANGE ZRANGEBYSCORE ZREVRANGEBYSCORE ZRANGEBYLEX ZREVRANGEBYLEX ZLEXCOUNT ZREMRANGEBY{RANK,SCORE,LEX} ZINTERSTORE ZUNIONSTORE；GEO 全 6。
- **GATE（不注册 → unknown command）**：SET EX/PX、SETEX PSETEX、APPEND STRLEN SETRANGE GETRANGE SUBSTR GETDEL、SETBIT GETBIT BITCOUNT BITPOS BITFIELD BITOP、PFADD PFCOUNT PFMERGE；LINSERT BLPOP BRPOP；SSCAN；ZPOPMIN ZPOPMAX ZSCAN ZMSCORE；TYPE EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST RENAME RENAMENX MOVE KEYS SCAN RANDOMKEY DUMP RESTORE OBJECT UNLINK；HSCAN HSTRLEN。

## 计数契约变更（关键）
现状：handler 走 `r.Storage.Meta.Load(pk).Count`（如 LPUSH 回 `m.Count`）。
改为：计数从 rv1 直接取——LPUSH/RPUSH 用 rv1 `LPUSH/RPUSH` 返回的 newLength；SADD 用 rv1 `SADD` 返回 addedMembers + `SCARD`；LLEN/SCARD/ZCARD/HLEN 用 rv1 对应方法。移除 `adjustCount`/`ensureTypeExpiring`/`Meta.Load` 依赖。

## 文件级工作分解
1. `internal/storage/store.go` + `store_core.go`：删/改 Meta 方法（EnsureType 族、Load/SetExpire/Persist/DeleteMeta*/DeleteMembers/SweepOrphans）；`s.client` 去 `WithContext`；`MaxBatchWriteItems`→本地常量；去 `redimo.KeyType/ErrWrongType`。
2. `store_strings.go`：INCR 族改 rv1 `INCR/INCRBY/INCRBYFLOAT`（或 GET+SET RMW）；MGET 用 rv1 `MGET`（去 BatchGET）；去 SETCAS。
3. `store_scan.go` + `store_hashes.go`(HScan)/`store_zsets.go`(ZScan)：SCAN 族整体 GATE，删相关 Store 方法。
4. `internal/meta/*`：本线不再需要独立 meta 层——评估删除或改成 rv1 计数直通。
5. `internal/command/*`：注销 42 个 GATE 命令的 `r.reg`；计数回复改走 rv1；`SELECT` db0-only。
6. `internal/command/datacmd.go`：`encodePK` db0-only。

## 测试
- Docker 离线构建（`GOPROXY=off`，rv1.6.1 已在缓存）。
- 差分测试仅覆盖**保留子集**对 redis:3.2 oracle；GATE 命令断言回 `ERR unknown command`。
- 复用 `test/integration/differ_test.go` harness。

## 进度（v1.0.0 完成）
- [x] v1 分支全新根；redimo 降级到 v1.6.1。
- [x] 存储层去 meta + 接 rv1 高层方法（不写 #meta；计数走 rv1 分区计数）。
- [x] 命令层 GATE 42 + 计数改道 + db0-only。
- [x] `go build`/`go vet`/`go test ./...` 全绿（135 个 v2-语义单测按 v1 语义 skip/adapt）。
- [x] 真机 DynamoDB Local 验证：strings/hashes/lists/sets/zsets/geo 全对，GATE 全 unknown，db0-only。

### 真机踩坑（fake-store 看不到）
- rv1.6.1 用 **String(S) 主键**（v2 用 B）；建表须 pk/sk=S。`cmd/redimos/health.go` 探针也须用 S 键。
- rv1.6.1 **list 不二进制容错**（对 BytesValue 无检查断言 → RPUSH panic）；store_lists 改传 `StringValue`。
- v1 `LoadMeta.Type` 恒空 → GET/MGET/INCRBYFLOAT 读路径的类型 gate 须去掉（否则误报 WRONGTYPE）。
- rv1 `MGET` 用 TransactGetItems 拒重复 item → MGetStrings 先去重 pk。

### 已知硬限制（rv1.6.1 地板，不改 redimo 无解）
- 集合成员/列表元素/哈希字段名/zset 成员**仅 UTF-8 安全**（字符串**值**、哈希字段**值**走 B，仍完全二进制安全）。
- 无类型标签 → 同 pk 混类型 key 的长度回分区总数（单类型 key 正常）。
