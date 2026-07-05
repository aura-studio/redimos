# redimos × Redis 3.2 兼容性

本文档记录 redimos（RESP2 代理，后端 DynamoDB，存储层为 redimo/v2）对 **Redis 3.2** 的兼容状态：已对齐的部分、已知差异、并发原子性、字符/字节安全，以及待办项。随代码提交更新。

配套文档：

- [`command-reference.md`](command-reference.md) — Redis 3.2 全部 174 条命令 × redimos 处置（是否经 redimo）的逐条对照表。
- [`gen/`](gen/) — 上表的生成脚本与数据（`redisCommandTable` 解析 + redimos 处置交叉映射）。
- [`pika-migrate.md`](pika-migrate.md) — redimos 作为 **pika-migrate** 迁移目标的真机实测验证（真实 Pika v3.2.2 → redimos，全量 523 键逐字节一致、增量确定性命令一致、零拒绝；含操作步骤与非确定性命令 `SPOP` 限制）。

对照口径以 **`redis/redis` @ branch `3.2` 的 `src/server.c`**（`redisCommandTable`）为权威来源，而非仅看 redimos 代码。

---

## 1. 结论速览

| 维度 | 状态 |
|---|---|
| 命令覆盖 | 174 条中 **114 条经 redimo 存储支持**（含 GEO+`_ro`、BIT、HLL+`PFDEBUG`）；**42 条代理拒绝**（专属错误消息）；**14 条连接/桩**（含 SAVE/BGSAVE/…/WAIT/ROLE/PFSELFTEST 固定回复桩）；**4 条连接层**。**未知命令路径清零**：174 条真实 3.2 命令**全部显式处理**（v1.18.0 起，连 `dump`/`restore`/`migrate`/`sync`/`psync`/`slaveof` 等实现不了的也注册为代理拒绝而非未知命令） |
| **字符/字节安全** | ✅ **完全对齐**：key、string 值、hash 字段名/值、set/zset 成员、list 元素对 0x00–0xff 全部 256 个字节值与 Redis 一致，无碰撞（v2.0.1 起） |
| **并发原子性** | ⚠️ **部分等同**：单项读改写（INCR/HINCRBY/APPEND…）、分值自增、**SET NX/SETNX（v1.9.0 起）**已原子；其余多项/多步写（S\*STORE、Z\*STORE、SMOVE、RPOPLPUSH、部分写可见性）**非原子**——受 DynamoDB 单事务 100 项上限所限，大结果集无法完全等同 Redis 单线程模型 |
| 差分（单连接） | 大量命令字节一致；残余差异见 §4 |

一句话：**单连接 / 无争用下 redimos 与 Redis 3.2 高度兼容；并发原子性与若干平台相关项（浮点精度、DynamoDB 数值域）是结构性差异。**

---

## 2. 版本与修复历程

| 版本 | 内容 |
|---|---|
| redimo **v2.0.0** | 模块升 v2 大版本（`github.com/aura-studio/redimo/v2`），与 v1 不兼容；含 meta/TTL/count 层、scan/sweep、list 修复 |
| redimo **v2.0.1** | **二进制安全**：pk/sk 由 DynamoDB String 改 Binary；`encodeSK/decodeSK` 保留前缀方案修掉空排序键哨兵碰撞；list 元素值改 Binary 存储 |
| redimos **v1.0.x** | 首发 / CI / Dockerfile 依赖收敛 |
| redimos **v1.1.0** | 迁移到 redimo/v2 |
| redimos **v1.2.0** | HINCRBY/HINCRBYFLOAT 原子化（CAS+HSETCAS）、GET 非 String 键回 WRONGTYPE、HMGET 去重；SET/SETEX/PSETEX 类型无关覆盖；LINDEX/LSET 先查存在性；错误文本对齐（部分） |
| redimos **v1.3.0** | 依赖升 redimo/v2 v2.0.1（二进制安全） |
| redimos **v1.4.0** | 新增 MSETNX/SUBSTR/TOUCH/ZLEXCOUNT/ZREMRANGEBYLEX；zset 等分值字典序；ZRANGEBYLEX `LIMIT`；MSET 类型无关覆盖；`invalid expire time in <cmd>` / `invalid cursor` 对齐 3.2 |
| redimos **v1.5.0** | 新增 **GEO 家族**（GEOADD/GEODIST/GEOPOS/GEOHASH/GEORADIUS/GEORADIUSBYMEMBER，功能版）接到 redimo 的 GEO 原语 |
| redimos **v1.5.1** | FLUSHALL/FLUSHDB 从「不支持」改为「代理拒绝」（专属拒绝消息） |
| redimos **v1.6.0** | 新增 **BIT 家族**（SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD），纯命令层，单键字节兼容 |
| redimos **v1.7.0** | 新增 **HLL 家族**（PFADD/PFCOUNT/PFMERGE），纯命令层,Redis 3.2 hll.c 忠实移植 |
| redimos **v1.8.0** | **GEO 改为字节兼容版**：在 zset 之上重写（member.score = 52-bit geohash，与 Redis 同构），弃用 redimo 的 S2 seam；GEOPOS/GEOHASH/GEODIST/WITHHASH 与 Redis 3.2 **逐字节一致**（实测 265/265 差分通过） |
| redimo **v2.0.2** | 新增 `CreateTypeIfAbsent`：SETNX/SET NX 的原子占位原语（条件 meta 写） |
| redimos **v1.9.0** | **SET NX / SETNX 原子化**：改用 redimo v2.0.2 `CreateTypeIfAbsent` 单条条件 meta 写占位，消除 check-then-set 竞态（实测 30×50 并发每轮恰好一个 `:1`） |
| redimos **v1.10.0** | **处置重分类**：发布订阅 / Lua / 事务 / 阻塞四家族由「不支持（未知命令）」改为 **代理拒绝**（专属消息，因这些命令在 Redis 3.2 里真实存在）；新增 **GEORADIUS_RO / GEORADIUSBYMEMBER_RO**（只读变体，别名到已实现 GEO，实测 9/9 差分一致）。经 redimo 113 / 代理拒绝 22 / 不支持 28 |
| redimos **v1.11.0** | 按 §11 评估逐步扩充代理拒绝：**SHUTDOWN**（会终止多租户共享进程）、**ASKING**（Cluster 槽迁移一次性标志，非 cluster 代理无意义）由「不支持」改为**代理拒绝**（实测拒绝且代理存活）。代理拒绝 24 / 不支持 26 |
| redimos **v1.12.0** | **READONLY** → 代理拒绝（Cluster replica 只读服务开关，非 cluster 代理无 replica/slot 可切换；standalone Redis 3.2 原版回 `ERR This instance has cluster support disabled`，redimos 用统一代理风格消息）。代理拒绝 25 / 不支持 25 |
| redimos **v1.13.0** | **READWRITE**（READONLY 的反向，清除只读标志）、**REPLCONF**（master↔replica 复制子协议）→ 代理拒绝。注：REPLCONF 属 §11「架构上不可能实现」档，但仍**注册专属拒绝**而非落未知命令——处置(拒绝) 与 可实现性(不可能) 是两码事。代理拒绝 27 / 不支持 23 |
| redimos **v1.14.0** | **RANDOMKEY / MOVE / SORT / OBJECT / MONITOR / CLUSTER / LATENCY / DEBUG** → 代理拒绝（各带专属消息）。至此 §11「宜代理拒绝」桶 12 条**全部落地**。代理拒绝 35 / 不支持 15 |
| redimos **v1.15.0** | **7 个固定回复桩**（`SAVE`→+OK / `BGSAVE` / `BGREWRITEAOF` / `LASTSAVE`→当前秒 / `ROLE`→master 形态 / `WAIT`→:0 / `PFSELFTEST`→+OK，实测与 3.2 稳态一致）+ **实现 `PFDEBUG`**（GETREG/ENCODING/TODENSE/DECODE；**GETREG 实测字节兼容**：N=20 稀疏、N=5000 稠密两端 md5 一致）。经 redimo 114 / 桩 14 / 不支持仅剩 7（全是架构不可能项） |
| redimos **v1.16.0** | **内部 pk 前缀统一为 `{n}:`**（db0=`0:`、db1=`1:`、…，去掉旧的 db0=`0:`/dbN=`d{n}:` 不对称）；纯内部、Redis 协议不可见、无碰撞、对迁移零影响 |
| redimo **v2.0.3** | **修复 SCAN**：`ScanMetaKeys` 的过滤把排序键 `#sk = :meta` 当 String 比，但 sk 自 v2.0.1 起是 Binary，导致真实 DynamoDB 上 **SCAN 恒空**（GET 等不受影响）。改用 Binary `encodeSK(MetaSK)` 匹配 |
| redimos **v1.17.0** | 升级 redimo v2.0.3，**SCAN 现在能用了**（实测真 DynamoDB：db0 与多 db 均正确按库返回 + MATCH 过滤）。KEYS 代理拒绝时提示的「用 SCAN」至此名副其实 |
| redimos **v1.18.0** | 最后 7 条「架构不可能」命令 **DUMP/RESTORE/RESTORE-ASKING/MIGRATE/SYNC/PSYNC/SLAVEOF** → 代理拒绝（各带专属消息）。**未知命令路径清零**：174 条真实 Redis 3.2 命令全部显式处理。代理拒绝 42 / 不支持 0 |
| redimos **v1.19.0** | **移除内置 Pika 迁移子系统**（`internal/migrate`：dual-write / shadow-read / fallback / importer + `--dual-write`/`--pika-addr`/`--shadow-read`/`--fallback`/`--migrate-prefixes` 等 flag）。该子系统此前仅在 main.go 构造并打日志、**从未接入读写请求路径**（router 拿的是裸 store），属死脚手架；迁移改用外部 pika-migrate 工具，与本项目无关，故清除 |
| redimo **v2.1.0** | **结构一致性清理（未上线，破坏性）**：删 streams.go/geo.go(S2)/composite.go（~1750 行）+ 裁 `golang/geo`/`geohash`/`oklog/ulid` 依赖；去掉 set 成员的随机 skN（纯死写、污染 LSI、使 SADD 属性层非幂等）；list 元素改二进制容忍（`valueBytes`,去掉会 panic 的 `e.(StringValue)` 断言）+ index skN 由 float 格式改 int 格式（与 ParseInt 读路径一致）；修 `CreateProvisionedTable` 成功误报 nil-`%w` bug。redimo 全测试绿 |
| redimos **v1.20.0** | 升 redimo v2.1.0（list 元素改传 `BytesValue`,与 string/hash 统一）；**新增内置集成测试 `test/integration/`**：差分一致性(74 命令 vs 3.2 逐字节)/命令原子性(SETNX 恰好一个赢家 + INCR 计数==已确认)/全字符集(256 字节全类型往返),见 §10.1 |
| redimo **v2.2.0** | **List 结构收敛**:把头/尾 index 计数器从独立 `_redimo/<key>` 分区折进 list 自己的 `#meta` 项(保留属性 `il`/`ir`,原子 `ADD`+`RETURN UPDATED_NEW`)。**至此每种类型都是"单分区(data+#meta)"**,删 list 不再留孤儿分区。顺带修 `lLen` 既有 off-by-one(改走 skN LSI 计数,结构性排除 `#meta`)。redimo 全测试绿(含并发 list) |
| redimos **v1.21.0** | 升 redimo v2.2.0（纯依赖升级,list index 机制全在 redimo 内部,redimos 侧零改动）；集成测试 + list 头尾顺序/删后重建 差分复验全绿 |
| redimo **v2.3.0** | **`#meta` 专属排序键前缀（未上线,破坏性）**：保留项从"把字符串 `#meta` 走成员前缀 `0x01` 编码"改为专属前缀字节 `skPrefixMeta=0x02`。此前一个名叫 `#meta` 的用户成员/字段/键会编码成与 meta 项相同的字节,**静默覆盖** `t/exp/cnt/il/ir`（数据损坏）；现改按前缀字节判定（`isMetaItem`）而非解码后比字符串,真正的 `#meta` 成员得以正确存取。顺带把 `conditionFailureError` 从子串匹配错误文本改为 `errors.As` 命中 SDK 类型化异常并逐项检查 `CancellationReasons`——被限流/冲突的事务不再被误判为丢 CAS（原会耗尽 RMW 重试）。全测试绿 |
| redimo **v2.4.0** | **正确性修复 + 去重 + 请求级 context（未改存储格式）**。修 6 个库级 bug（均新增回归测试并对 v2.3.0 反证失败）：LREM 头/尾选取按十进制字符串比 skN（`"10"<"2"`）导致删错项→改按解析 int64 序；SRANDMEMBER 不过滤 `#meta`（可泄漏为随机成员）；`zGeneralRange` 词法路径/`zGeneralCount` 词法路径在无界 `- +` 时含 `#meta`；HLEN/SCARD/ZCARD 用 `Select=Count` 把 `#meta` 多算 1；LSET 再插失败时吞错误还报 `ok=true`。清理：删 `SweepOrphans` 恒假分支；抽 `doIncr`（INCR*/HINCR* 共用）；三个近同的 list 分页循环合并为 `pagedListItems`。新增 `Client.WithContext(ctx)` 把请求级 context 贯穿所有 DynamoDB 调用（替换硬编码 `context.TODO()`,默认 `Background()`）。全测试绿(含 `-race`) |
| redimos **v1.22.0** | 升 redimo **v2.4.0**（跨越 v2.3.0 的 `#meta` 前缀破坏性变更,未上线故无迁移成本）。代理端零改动：容量(SCARD/HLEN/ZCARD/LLEN)读 `meta.cnt`、词法/秩范围与 LSET/LTRIM/LREM/LINSERT 由代理自实现,故多数 v2.4.0 库级修复对代理为潜伏改善（redimo 库测试已覆盖）。**差分套件扩到 83 命令**（新增 LREM 高位下标数值序、ZRANGEBYLEX/ZREVRANGEBYLEX `- +`、ZLEXCOUNT `- +`/有界）——逐字节 vs Redis 3.2 全绿,复验代理路径经 v2.3.0 前缀变更后仍 `#meta` 安全。单元 + 集成(原子性/字符集/差分) 全绿 |
| redimo **v2.5.0** | **`go` 指令 1.14→1.25 + 现代化**（无存储/API 语义变更）：`interface{}`→`any`(全包)、`HScanPage`/`ZScanPage` 抽泛型 `collectNonMetaItems[T]`(1.14 时因无泛型而搁置的去重现补上)、ZUNIONSTORE/ZINTERSTORE 的 MIN/MAX 累加器改用内置 `min`/`max`。`go mod tidy` 拆分 direct/indirect。全测试绿 |
| redimos **v1.23.0** | **`go` 指令 1.24→1.25** + 升 redimo **v2.5.0**；`interface{}`→`any`(store.go)。纯现代化/依赖升级,零行为变更；单元 + 集成(原子性/字符集/差分 83 命令) 全绿 |
| redimo **v2.6.0** | 多角度改进:**原子性**——`EnsureType` 用 `ALL_NEW` 回传加后 cnt(+ delta=0 时省 `ADD #cnt`)、新增条件删 `DeleteMetaIfEmpty`(`#cnt<=0` 才删),让代理关掉清空竞态;**成本**——LPUSH/RPUSH 一次范围 `ADD` 分配 N 个下标 + `BatchWriteItem`(替 2N 次 UpdateItem),SADD/SREM 一次 `BatchGetItem` 快照 + `BatchWriteItem`,LREM 批量删;**正确性**——LSET 不再 panic(解析失败回错)+ 收 `any`,`ReturnValue.IntE` 溢出报错。全测试绿(含 -race) |
| redimos **v1.24.0** | 升 redimo **v2.6.0** + 多角度改进:**#1 原子性**——`adjustCount` 改用 `EnsureType` 同一次原子写回传的 cnt + `DeleteMetaIfEmpty` 条件删,**关闭 load-then-delete TOCTOU**(并发 add 撑起 cnt 时条件删失败,不再把新成员遗留在被删 meta 下);**#2 安全**——`writeStoreError` 默认分支不再回显裸后端/DynamoDB 错误(改通用可重试 `ERR` + 服务端日志),`ErrRMWMaxRetries` 显式保留原义;**#3 安全**——新增 `--max-collection-result` 上限,HGETALL/HKEYS/HVALS/SMEMBERS 读后端前按 `meta.cnt` 预判超限即拒,挡单命令 OOM;**#6 可观测**——惰性删除 丢弃/失败/队列深度、孤儿清扫 运行/回收/失败、RMW 重试耗尽 全部接入 Prometheus;**#7 运维**——`validateConfig` 启动期校验 flag 边界(delete-batch∈[1,25]/consistency 等)快速失败;**#8**——两仓 CI 加 `-race`。单元 + 集成(原子性/字符集/差分 83 命令) 全绿 |
| redimos **v1.25.0** | P2 收尾 5 项:**#19 属性测试**(list 顺序/set 唯一/zset 分值序,随机序列 vs 参照模型,走真代理→DDB)——**抓到并修复一个真实 bug**:清空集合后 `DeleteMetaIfEmpty` 曾**遗留了 pk 异步删成员的入队**,若在惰性删除跑之前重建键,会把新成员一并抹掉;改为**不入队**(空集合无成员可回收,遗留仅由周清扫兜底)。**#20** `/readyz` 就绪探针(输出后台回收/争用快照)+ 关停分阶段计时;**#21** deleter/sweeper 结构化日志 `Logger` 接缝(`StdLogger`,OnError 回退);**#18** BIT/HLL 命令族 wire 测试(SETBIT/GETBIT/BITCOUNT/BITOP、PFADD/PFCOUNT/PFMERGE);**#14** `--scan-timeout`(经 `WithContext` 真下传取消到 DynamoDB Scan)+ `--max-command-bytes` 超大命令拒收。单元 + 集成(含 3 项属性测试)全绿 |

**收官说明**:22 项方案共落地 17 项;主动跳过 5 项并说明理由——#12(库 LLEN 读 cnt 对其真实调用方反更慢)、#13(游标循环容量远小于 2⁶⁴,实际恒 O(1))、#15/#16(类型化 Args / Register 返错各 ~100/200 处机械大改,价值最低风险最高)、#22(IsExpired 边界是有意的 Pika 对齐)。

| 版本 | 变更 |
|---|---|
| redimos **v1.26.0** | **对齐检测从 3 维扩到 13 维**(见 §10.2):新增 A 错误/arity/WRONGTYPE、B 数值/浮点/溢出、C 回复形状、D 边界/索引、E TTL/过期、F 无序集合等价、G SCAN 不变量、H 多 DB 隔离、I 单键寄存器安全、J 命令覆盖扫射——纯新增测试(无生产码改动),全部对真 Redis 3.2 实测通过。**实测刻画并记录两处平台/设计所限的分歧**:浮点累加(long double vs float64)、TTL 亚秒精度(秒级/Pika 对齐)。集成套件共 26 个测试函数全绿 |
| redimos **v1.28.0** | **真 AWS DynamoDB 端到端实测 + 修一个真后端才现的 bug**(见 §10.3):在 EC2 上把代理指向 us-east-1 真 DynamoDB 跑 13 维集成——Porcupine 线性化在真后端逮到 **DEL→重建惰性删除竞态**(`DEL k;SET k v;GET k` 可返回 nil),已由 **redimo v2.6.1**(`DeleteMembers` 先查 `#meta`、键被重建则跳过)修复,真 DynamoDB 上 3/3 通过。另记录第三处平台分歧:DynamoDB Number **负零归一化**(`-0`→`0`,Redis 保留 `-0`)。升 redimo v2.6.1 |
| redimos **v1.27.0** | **补齐先前推迟的 A 组 5 项**:**#16** `Table.Register` 由 panic 改**返错**(可单测 + 聚合报告所有坏注册,`finishRegistration` 汇总失败)；**#15** 给 `args.go` 补 `ParseFloat`/`ParseFloatReply`/`WriteNotFloat` 浮点三件套(对齐已有 `ParseInt` 三件套)+ 新增索引式类型访问器 `Args`(委托规范解析器、零逻辑重复),并把散落的 `parseFloatArg`/`parseScore` 收敛到 `ParseFloat`；**#20** BITFIELD wire 测试(SET/GET/INCRBY×WRAP/SAT/FAIL×有/无符号+错误路径)；**#21** 用 **Porcupine** 做**完整线性化检查**(单键 SET/GET/DEL 480 并发操作历史,实测可线性化);redimo 侧 **#19** 补 `DeleteMembers`/`ScanMetaKeys`/`SweepOrphans` 库级测试。全单元 + 全集成(含 Porcupine)全绿 |
| redimos **v1.29.0** | **修正 v1.28.0 的 DEL-重建修复放错层导致的回归**(见 §10.3):v1.28.0 把重建守卫塞进 **redimo `DeleteMembers`** 本身——但该原语被**同步的** `LReplaceAll`(LSET/LTRIM/LREM/LINSERT 重写活列表时先清空)复用,守卫让「清空活列表」变成 no-op,列表改写后新旧元素**拼接**(真 DynamoDB 上属性测试逮到 `LRANGE=[旧… 新…]`)。根因:重建守卫不该住在被同步/异步两条路复用的原语里。**redimo v2.6.2 把 `DeleteMembers` revert 回无条件删**;重建守卫改到 **redimos 惰性删除器**(`DeleterConfig.IsLive` 回调,仅异步惰性删除路走它,`LReplaceAll` 不经删除器故不受影响),`cmd/redimos` 用 `store.LoadMeta` 接线。新增删除器单测覆盖守卫(活键跳过/孤儿回收)。真 DynamoDB 上全 13 维 + Porcupine + 列表属性测试全绿。升 redimo v2.6.2 |
| redimos **v1.31.0** | **事务 fence 实测被否、revert 回非事务 `IsLive`;寄存器线性化诚实化**(见 §10.3):v1.29.0 的 `IsLive` 守卫非原子仍偶发丢写,遂在 v1.30.0 试过 `redimo v2.6.4 DeleteMembersIfDead`(把死活检查+删成员放进同一 `TransactWriteItems`)。逻辑上消除丢写,但**真 DynamoDB 上引入更糟回归**:事务锁令并发/顺序 `SET` 的 `UpdateItem` 撞 `TransactionConflictException` 而失败(压测 151 次),`SET` 报错对 Redis 代理不可接受。故 **revert**:redimo 回无条件 `DeleteMembers`,异步删除器保留非事务 `IsLive` 尽力守卫(不让 `SET` 失败、比无条件删少抹,但不能消除 DEL-then-SET 丢写——彻底修需 per-incarnation epoch,超范围)。深挖确认**架构性分歧**:string 分体存储 + SET 非原子写 + 读路径非快照双读 ⇒ **并发/顺序 DEL+SET 下单键寄存器既不可线性化也不保证不丢写**,与既有「redimos 并发原子性 ≠ Redis 3.2」一致。诚实化:`TestRegisterLinearizable` 只跑 SET/GET(硬断言线性化,实测通过);DEL+SET 丢写不作断言、仅记录;`IsLive` 跳活由单测覆盖。回到 redimo v2.6.2 |
| redimos **v1.32.0** | **一致性检测再扩 6 维(K–P)+ 修 ZADD 标志缺口**(见 §10.2):新增 K 键生命周期/空即删、L 变更返回值/幂等、M SCAN MATCH 通配语义、N 位操作对拍、O 编码阈值不变性(兼分页压测)、P 类型覆盖/建键语义,全部对真 Redis 3.2 逐字节实测通过。维度 L **逮到真兼容缺口**:`ZADD` 不支持 `NX/XX/CH/INCR`(回 `-ERR syntax error`);已在 `handleZAdd` 实现四标志(NX/XX 门控、CH 计变更、INCR 返新分值/门控 nil、错误串对齐、重复成员按 Redis 顺序逐对计数),无标志走原快路径,`zadd_flags_test.go` 锁定。全集成 + 全单元全绿,无生产码回归 |
| redimo **v2.7.0** | **16-agent 审查 P1/P2 修复**:`pagedListItems`/`zGeneralRange` 负 `Query` Limit 下溢(>1MB list 的 LRANGE 全废)、ZINTERSTORE 首源 WEIGHT 未乘、`ZRANGE(start>0,stop<0)` 等分超返 + ZREMRANGEBYRANK 超删、ZRANK 返等分**最大**名次(改按分值计「之前」+ 等分组内逐字典序定位)、`MGET` 缺失键并到空串键碰撞(改 `TransactGetItems` 按请求序 keyed)。全套测试绿(含 -race) |
| redimo **v2.8.0** | **审查成本/正确性收尾**:新增 `BatchGET`(`BatchGetItem`、100/批分块 + UnprocessedKeys 重试;代理 MGET 的真批量,替逐 `GET` 扇出,非事务 `MGET` 更省)、`ZPOPMIN/ZPOPMAX count<=0` 空弹(原走 `zGeneralRange` 无界会清空整集)、`SISMEMBER` 只投影分区键(存在性检查不再拉整成员项)。`TestBatchGET` 真 DynamoDB 覆盖 present/missing/dup/>100 分块 |
| redimos **v1.33.0** | **升 redimo v2.8.0 + 52 条审查项收尾**(全量 16-agent 对抗审查,见 [[redimo-redimos-audit-2026-07]]):**P1 崩溃/DoS**——BITFIELD `#idx` 溢出 panic + 巨偏移 OOM、SETBIT 537MB 才拒、SRANDMEMBER 负 count 40GB 分配/panic(全加封顶 + 写前 `CheckValueSize`);per-command metrics + slowlog 接了线却从不记录(`ObservedDispatcher` 计时/计错/慢查询)。**P2 正确性**——SETRANGE 空值跳 WRONGTYPE、AUTH 常量时间比较、SET/SETNX 覆盖异类型**孤儿泄漏**(异步删除器 `IsLive` 守卫 + `SweepOrphans` 一见新 meta 即当活,故同步回收旧成员)、LRANGE/ZRANGE 集合 cap 按有效区间。**P2 成本**——LRANGE 走 redimo 有界 `LRANGE`、ZRANK 走有界 `ZRANK`、MGetStrings 走 `BatchGET`(zset 区间/计数因排他/±inf/等分序保留整集读,已注释权衡)。**P2 生命周期**——优雅关停 `server.Drain` 等在途命令(不丢在途 lazy-delete 入队)、sweeper 加抖动 `InitialDelay`(频繁重启也每生命周期扫一次)、deleter `IsLive` 错误计数 + metric、metrics 端口绑定失败改启动期致命。**P3**——EXPIRE/PEXPIRE 溢出不再删键、LINSERT 类型检查先于尺寸、INCRBYFLOAT/HINCRBYFLOAT 拒无穷增量、HMSET 奇参用 Redis 专属串、SINTER 按键序空短路。CI Go 钉 1.25.5 + 加真 Redis 3.2 集成 job;terraform 表 schema 修为 B 键 + skN LSI。全单元 + 差分套(LRANGE/MGET/ZRANK/list/encoding/zset 边界)对真 Redis 3.2 实测通过 |

---

## 3. 字符 / 字节安全（已对齐）

Redis 的 key 与 value 是任意字节串（0x00–0xff）。redimos 各"位置"的对齐（逐字节实测 vs 真 Redis 3.2）：

| 位置 | 存储 | 字节对齐 |
|---|---|---|
| string 值 / hash 字段值 | DynamoDB Binary（`val`） | ✅ 256/256 |
| **key（pk）** | DynamoDB Binary | ✅ 256/256（v2.0.1 前仅 128/256 且碰撞） |
| **hash 字段名 / set·zset 成员（sk）** | DynamoDB Binary + 保留前缀编码 | ✅ 256/256 |
| **list 元素值** | DynamoDB Binary | ✅ 256/256 |

要点：

- Binary 排序键按**无符号字节序**排序，等于 Redis 的字典序（顺带保证 zset 等分值 tie-break、ZRANGEBYLEX 顺序正确）。
- 空排序键（仅 String 值项用 `sk=""`）编码为保留标记 `0x00`，成员编码为 `0x01‖原始字节`；真实成员永不与该标记冲突，`""` 与 `/` 不再碰撞。

---

## 4. 差分兼容：已知残余差异

多数命令与 Redis 3.2 字节一致。以下差异**仍存在**：

### 4.1 平台固有（不可在不重构的前提下消除）

- **浮点精度**：INCRBYFLOAT/HINCRBYFLOAT/ZINCRBY 用 Go `float64`，Redis 用 80-bit long double + `%.17Lg`（如 `0.1×3` = `0.30000000000000004` vs `0.3`）。
- **数值域**：ZADD 极值/±inf 分值超出 DynamoDB Number 域（~1E±126），泄漏 `ValidationException`。
- **score 解析**：Go `strconv.ParseFloat` ≠ glibc `strtod`（空串、`0x10` hex、`1_0` 下划线、`1e-400`、裸 `(` 边界的接受/拒绝不同）。

### 4.2 错误文本风格（3.2 用裸大写、redimos 用现代带引号）

- arity 错误：`wrong number of arguments for MSET`（3.2）vs `... for 'mset' command`（redimos）——系统性风格差，未统一改。

### 4.3 未实现（可实现，见 §6）

- MSETNX/SUBSTR/TOUCH/ZLEXCOUNT/ZREMRANGEBYLEX → **已在 v1.4.0 实现**。
- 见 `command-reference.md` 的「不支持」段。

---

## 5. 并发原子性

Redis 单线程串行执行，每条命令原子。redimos 把命令映射为多次 DynamoDB 调用；能用单项/原生原子操作表达的已原子，需"多 item 一致"的则非原子。

| 类别 | 命令 | 是否等同 Redis 3.2 |
|---|---|---|
| 单项读改写 | INCR/DECR/INCRBY/INCRBYFLOAT/APPEND/SETRANGE | ✅ 原子（CAS） |
| hash 自增 | HINCRBY/HINCRBYFLOAT | ✅ 原子（HSETCAS，v1.2.0 起） |
| zset 分值自增 | ZINCRBY | ✅ 原子（DynamoDB 原生 ADD） |
| 不同成员计数 | SADD/ZADD/HSET distinct | ✅ 稳态计数正确 |
| **NX 条件写** | SET NX / SETNX | ✅ **原子（v1.9.0 起）**：单条条件 meta 写占位，并发只有一个"赢" |
| **多步复合写** | S\*STORE / Z\*STORE / SMOVE / RPOPLPUSH 中间态 | ❌ 非原子（dest 清空-重填非事务） |
| **部分写可见性** | SCARD/ZCARD/LLEN vs 实际成员 | ❌ 并发读可见半写中间态 |
| 删除竞态 | delete-on-empty、DEL+重建 | ❌ 非原子（Load→DeleteMeta + 异步回收） |

**SET NX / SETNX（v1.9.0 已收口）**：改用 redimo v2.0.2 的 `CreateTypeIfAbsent` —— 一条条件 `UpdateItem`（`attribute_not_exists(#t) OR #exp <= :now`）原子占住 meta 项，把"存在性判断 + 建类型"并成一步，消除了原先 `keyLive` 读 → 写之间的 TOCTOU 窗口。**实测**：30 轮 × 50 并发 SETNX 打同一新鲜键 → 每轮恰好一个 `:1`（与真 Redis 3.2 一致）；新鲜/同类型/异类型/过期键 21 项差分逐字节一致。残留仅一个崩溃窗口（占位成功后、写 value 前进程死掉 → 一个"活着的空串键"），非并发正确性问题,且与所有 redimos 写共用惰性回收兜底。

**其余多步写的根因与天花板**：`meta + data + cnt` 跨多个 DynamoDB item 的写未用 `TransactWriteItems` 包裹。**关键约束**：DynamoDB 事务上限为 **单事务 100 项 / 4MB**，而 Redis 的 set/zset/list 可含百万成员。因此**大结果集的 `S*STORE`/`Z*STORE`、大 key 的 `DEL`、`SADD` 超过 100 成员**等本质上无法在单个事务里完成（必须分片 → 跨片非原子），**"完全等同 Redis 单线程原子性"在 DynamoDB 上不可达**——这不是实现取舍，而是平台天花板（redimo 自身的 `S*STORE` 也是"读 + 写两步"）。可原子化的是**成员数有界**的复合写（`SMOVE` ≤4 项、`RPOPLPUSH` 有界、`MSETNX` ≤50 键），设计上可用 `TransactWriteItems` 收口,SETNX 是这条路线上第一个落地的（且因为只动一个 meta 项,连事务都不需要）。

---

## 6. 待办 / 可继续项

| 项 | 类型 | 说明 |
|---|---|---|
| ~~SET NX / SETNX 原子性~~ | 原子性 | ✅ **v1.9.0 已完成**（redimo v2.0.2 `CreateTypeIfAbsent` 条件占位，见 §5） |
| 多步写事务化（有界部分） | 架构 | 用 `TransactWriteItems` 收口成员数**有界**的复合写（SMOVE/RPOPLPUSH/MSETNX≤50）；`S*STORE`/`Z*STORE` 等无界结果集受 100 项/4MB 事务上限所限**无法完全原子**，仅能分片（跨片非原子）|
| score 解析对齐 | 差分 | 移植 glibc `strtod` 语义 |
| arity 错误文本 | 差分 | 系统性改成 3.2 裸大写风格（测试改动大） |
| GEO STORE/STOREDIST | 功能 | GEO 家族已在 **v1.8.0 字节兼容（见 §7）**；仅余 `STORE`/`STOREDIST` 两个可选项未做（GEORADIUS 结果写入另一 zset），其余全部逐字节一致 |
| HINCRBYFLOAT 精度 / 数值域 | 平台固有 | 需 long double 模拟 / 换数值编码，通常不做 |

---

## 7. GEO（v1.8.0 字节兼容版）

Redis GEO 本质就是 zset（member 的 score 编码位置，`TYPE` 返回 `zset`）。**v1.8.0 按 Redis 自身的做法重写**：一个 GEO 键就是一个 zset，member 的 score = 该位置的 **52-bit geohash**（lat 限 Web-Mercator 区间 [-85.05, 85.05]，26 位 lat + 26 位 lon 交错，恰好可被 float64 精确表示）。6 个命令因此变成纯命令层逻辑，直接建在 zset store 之上——不再需要 redimo 的 GEO 原语，旧的 `storage.GeoStore`（S2）seam 已删除。

- **已实现**：GEOADD / GEODIST / GEOPOS / GEOHASH / GEORADIUS / GEORADIUSBYMEMBER；GEORADIUS[BYMEMBER] 支持 `WITHCOORD` / `WITHDIST` / `WITHHASH` / `COUNT` / `ASC` / `DESC`；单位 m/km/mi/ft；非 zset 键回 `WRONGTYPE`；GEOADD 维护 zset 的 meta/type + cnt，非法单位/越界坐标的错误文本对齐 Redis。
- **实现要点**：`internal/command/geohash.go` 忠实移植 Redis geohash 的 encode/decode/deinterleave 与 11 字符标准 geohash 串；`geo.go` 用 `ZAdd`/`ZScore`/`ZRangeByRank` 落地。GEOADD 存 `geohashEncode52(lat,lon)` 为 score；GEOPOS/GEOHASH/GEODIST 从 score 解码回坐标；GEORADIUS 读全部成员、用 haversine 精确距离过滤（与 Redis 用 geohash 邻格剪枝得到的**结果集完全相同**）。坐标格式复刻 Redis `%.17Lf` + 去尾零（`ld2string` humanfriendly）；距离用地球半径常量 `6372797.560856`。
- **与 Redis 3.2 的实测对拍**：官方 Palermo/Catania 样例 + 120 个随机点的模糊差分，覆盖 GEOADD/GEOPOS/GEOHASH/GEODIST（m/km/mi/ft）/GEORADIUS[BYMEMBER]（全 `WITH*` 组合/`COUNT`/`ASC`/`DESC`）以及全部错误路径（非法单位/越界/参数个数/WRONGTYPE/空键）→ **265/265 项逐字节一致**。

**未做**：仅 `STORE` / `STOREDIST` 两个可选项（把 GEORADIUS 结果写入另一个 zset）尚未实现；其余全部字节兼容。

> 备注：redimo 库自身仍保留基于 Google S2 的 GEO 原语（供其他使用方），redimos 只是不再走它——GEO 的字节兼容完全在 redimos 命令层达成，redimo 依赖版本不变（v2.0.1）。

---

## 8. BIT（v1.6.0 已支持）

位命令全部作用于 String 值的字节;redimos 的 String 本就按二进制存储,所以 BIT **纯在命令层实现,不动 redimo**:

- **已实现**:SETBIT / GETBIT / BITCOUNT / BITPOS / BITOP(AND/OR/XOR/NOT) / BITFIELD(GET/SET/INCRBY + OVERFLOW WRAP/SAT/FAIL + `#index` 偏移)。
- **实现要点**:命令层在 `internal/command/bit.go` 与 `bitfield.go`;SETBIT/BITFIELD 写复用 String 的 CAS 读改写循环 `rmwString`;GETBIT/BITCOUNT/BITPOS 用 `readCurrentString`;BITOP 读 N 个源写 dest;BITFIELD 的定宽整数用 `math/big` 保证各位宽(i1..i64 / u1..u63)的溢出边界精确。
- **实测对拍(真 Redis 3.2.12)**:SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD 在 set/read、字节范围、清位越界、缺键、AND/OR/XOR/NOT、OVERFLOW SAT/FAIL/#索引 上 **逐字节一致**。
- **约束**:值上限 390KB(DynamoDB item 限制)→ 最大 bit offset ≈ **319 万**(Redis 是 2^32),超大 offset 的增长写会被值大小错误拒绝。**BITOP 是多键写**,与 MSET/*STORE 一样**非原子**(单连接下字节正确)。

## 9. HLL / HyperLogLog（v1.7.0 已支持）

HyperLogLog 是存在 Redis String 里的 "HYLL" blob;和 BIT 一样 **纯命令层实现,不动 redimo**。

- **已实现**:PFADD / PFCOUNT / PFMERGE(`internal/command/hll.go`)。非 HLL 串回 `WRONGTYPE Key is not a valid HyperLogLog string value.`;TYPE 为 string。
- **实现要点**:Redis 3.2 `hll.c` 忠实移植——MurmurHash64A(seed `0xadc83b19`)、p=14 → 16384 个 6-bit 寄存器(稠密编码)、Ertl 2017 估计器(hllSigma/hllTau)。PFADD/PFMERGE 复用 String CAS 读改写。
- **实测对拍(真 Redis 3.2.12)**:PFADD(0/1)、PFMERGE、多键 PFCOUNT、TYPE、非 HLL WRONGTYPE **逐字节一致**;PFCOUNT 估计值在 **≤3000 基数完全一致**,更高基数在 HyperLogLog 固有误差(~0.81%)内(如 3 万时差 ~0.35%)——源于 Go float64 与 C long double 的估计器精度差,寄存器运算本身精确。
- **差异**:redimos 只写**稠密**编码,故存储 blob 与 Redis(从稀疏起)不逐字节相同(GET 一个 PF 键会不同);但 PFCOUNT/PFADD/PFMERGE 的语义与取值如上。PFDEBUG/PFSELFTEST(调试)未做。

## 10. 测试环境（Docker）

全部测试在 Docker 内跑（不用宿主）：真 Redis 3.2 作 oracle 与 redimos 代理同网,对拍字节级差分 + 并发原子性 + 逐字节字符对齐。详见仓库 `test/` 与差分框架 `test/difftest/`。

### 10.1 内置集成测试 `test/integration/`（真 redimo→DynamoDB 路径）

三条核心性质写成常规 Go 测试,走**真实 redimo→DynamoDB 路径**(非内存 fake),用环境变量 gate —— 裸 `go test ./...` 自动 skip,Docker harness 里跑:

- 环境:`REDIMOS_PROXY_ADDR`(必填,如 `rdms-proxy:6380`)、`REDIMOS_REDIS_ORACLE`(差分测试用,如 `redimos-redis32:6379`)。
- **`charset_test.go`（全字符集）**:string 值/key 名、hash 字段+值、set/zset 成员、list 元素,全部对 **256 个单字节 + 0..255 全序列 + 内嵌 CRLF/NUL/RESP 注入样本** 做逐字节往返;实测全绿。
- **`atomicity_test.go`（命令原子性）**:SETNX 20 轮×40 并发**恰好一个 `:1`**;INCR 16 路并发下**计数器 == 已确认 INCR 数**(CAS 从不丢/重已确认的更新;高争用下少量 INCR 会耗尽有界重试返回可重试错误,与 Redis 单线程不同,已在日志中标注)。
- **`differential_test.go`（差分一致性）**:**83 条** redimo 后端命令(string/key/hash/list/set/zset/BIT/HLL,只取顺序确定的)对真 Redis 3.2 **逐字节一致**。v1.22.0 起新增覆盖 LREM 高位下标数值序(重复项落在 `"10"<"2"` 字符串序边界)、ZRANGEBYLEX/ZREVRANGEBYLEX `- +`、ZLEXCOUNT `- +`/有界——复验代理的词法/秩范围路径经 redimo v2.3.0 `#meta` 前缀变更后仍与 Redis 逐字节一致且不泄漏/多算 `#meta`。

实测(vs redis:3.2.12)四项全绿。

### 10.2 对齐检测的十个维度(v1.26.0）

原三维(差分/原子/字符集)是必要地基但偏 happy-path/单命令/单线程。v1.26.0 补齐 **10 个对齐检测维度**(`test/integration/`,共享 `differ` 双端对拍框架:`eq` 逐字节 / `eqSorted` 无序集合 / `eqFloatClose` 数值容差 / `eqIntClose` 整数容差),全部对真 Redis 3.2 实测:

| 维度 | 文件 | 检测内容 | 结果 |
|---|---|---|---|
| **A 错误/arity/WRONGTYPE** | `errors_test.go` | 参数个数错/打错类型键/非法参数 的错误串,57 例 | ✅ 逐字节一致 |
| **B 数值/浮点/溢出** | `numeric_test.go` | ZADD/ZSCORE 分值格式化(含科学计数/负零)、INCR 溢出错误 | ✅ 直接格式化逐字节一致;**INCRBYFLOAT/ZINCRBY 累加**见下「已知分歧」 |
| **C 回复形状** | `shape_test.go` | 缺失/空/存在键的 `$-1`/`*0`/`$0`/`:N`/TYPE/TTL 形状,48 例 | ✅ 逐字节一致 |
| **D 边界/索引** | `boundary_test.go` | LRANGE/GETRANGE/ZRANGE 负索引/越界/`start>stop`、LSET 越界、`(`/`inf` 分值边界、词法边界 | ✅ 逐字节一致 |
| **E TTL/过期** | `ttl_test.go` | TTL/PTTL/PERSIST 哨兵、EXPIRE 后取值(±1s)、到期真消失 | ✅ 秒级一致;**亚秒精度**见下 |
| **F 无序回复集合等价** | `unordered_test.go` | SMEMBERS/HKEYS/HVALS/HGETALL/SUNION/SINTER/SDIFF 排序后比集合(补差分刻意跳过的洞) | ✅ 一致 |
| **G SCAN 家族不变量** | `scan_invariant_test.go` | SCAN/SSCAN/HSCAN/ZSCAN 从 0 迭代回 0 **全覆盖、不重复**,累积集 == 种子集 == oracle 累积集 | ✅ 一致 |
| **H 多 DB 隔离** | `multidb_test.go` | SELECT n 后键跨库不可见(pk 前缀隔离)对拍 | ✅ 逐字节一致(需 `-multi-db`) |
| **I 单键寄存器安全** | `linearizability_test.go` | 16×40 并发 SET/GET,每次 GET 必为某次真实写入值(无撕裂/幻读);可线性化子集 | ✅ 无违例(多项写非原子仍属已知、不在此检) |
| **J 命令覆盖扫射** | `coverage_sweep_test.go` | 二线命令(SETEX/GETSET/MSET/MSETNX/HMSET/HSETNX/HINCRBY/LINSERT/L{PUSH,POP}X/LTRIM/SMOVE/S*STORE/ZINCRBY/ZRANK/ZREVRANGE/ZREMRANGEBY*/EXISTS 多键/DEL 多键…)57 例广度对拍 | ✅ 一致 |

**再扩 6 个新维度(K–P,`test/integration/`,同一 `differ` 双端框架,全部对真 Redis 3.2 实测)**:

| 维度 | 文件 | 检测内容 | 结果 |
|---|---|---|---|
| **K 键生命周期/空即删** | `lifecycle_test.go` | 经 SREM/HDEL/ZREM/LPOP/RPOP/LREM/SPOP/LTRIM/ZREMRANGEBY{RANK,SCORE,LEX} 删到空后,`EXISTS=0`/`TYPE=none`/`TTL=-2`,且重建不带旧 TTL | ✅ 逐字节一致 |
| **L 变更返回值/幂等语义** | `mutation_return_test.go` | SADD 返「新增数非总数」、ZADD 新增 vs 更新、HSET 1新/0更、SMOVE/SETNX/PERSIST/EXPIRE 1/0、EXISTS 多键计数、L{INSERT,PUSHX} 缺键 | ✅ 一致(**逮到 ZADD 缺 NX/XX/CH/INCR,已修**) |
| **M SCAN MATCH 通配语义** | `scan_match_test.go` | `*`/`?`/`[abc]`/`[a-c]`/`[^a]` 否定/字面/`?????` 定长 在 SSCAN/HSCAN/ZSCAN/SCAN(nonce 前缀)上的匹配集 | ✅ 一致 |
| **N 位操作对拍** | `bitops_test.go` | SETBIT 零扩展、GETBIT 越界=0、BITCOUNT 字节范围、BITPOS 找 0/1 及全 1 串「越界返回下一位、带 end 返 -1」边角、BITOP AND/OR/XOR/NOT 不等长补零/缺键当零串 | ✅ 逐字节一致 |
| **O 编码阈值不变性** | `encoding_invariance_test.go` | 集合跨 Redis ziplist/intset/skiplist 阈值(150 元素)行为一致;兼作 redimos **DynamoDB 分页正确性**压测 | ✅ 一致 |
| **P 类型覆盖/建键语义** | `type_overwrite_test.go` | SET 无视旧类型覆盖为 string(旧集合被清)、各命令建键类型(HSET→hash…INCR/APPEND/SETRANGE/SETBIT→string)、GETSET 及错类型 WRONGTYPE | ✅ 逐字节一致 |

> **维度 L 逮到并修复的真兼容缺口**:`ZADD` 此前把 key 之后全部实参当 score/member 对,不解析 **`NX`/`XX`/`CH`/`INCR`** 标志(Redis 3.2 特性)——`ZADD k CH 1 a` 直接回 `-ERR syntax error`。已在 `handleZAdd` 实现四标志(含 NX/XX 门控、CH 计变更数、INCR 返新分值或门控 nil、`NX XX` 冲突与 `INCR` 多对错误串对齐 3.2、重复成员按 Redis 顺序语义逐对计数),无标志走原快路径;`zadd_flags_test.go` 全维锁定,真 Redis 3.2 全绿。

**三处已知、经实测刻画的分歧(非 bug,平台/设计所限,均已记录)**:
1. **浮点累加精度**:Redis 用 C `long double`(80 位)做 INCRBYFLOAT/ZINCRBY/HINCRBYFLOAT 累加,Go 只有 `float64`(64 位),结果在第 ~17 位有效数字分叉(如 `5003.1` vs `5003.10000000000000009`)。**直接分值存取/格式化逐字节一致**,仅累加分叉;测试改用 `eqFloatClose` 数值容差断言并记录。
2. **TTL 亚秒精度**:redimos TTL 为**秒精度**(对齐 Pika v3.2.2),`PEXPIRE 200ms` 会向下取整、瞬时过期,与 Redis 毫秒精度不同。秒级过期两端一致。
3. **负零归一化**(真 DynamoDB 实测才现):DynamoDB 的 Number 类型把 `-0` 归一化为 `0`,而 Redis 保留 `-0`——`ZADD -0` 后 `ZSCORE` 代理回 `0`、Redis 回 `-0`。**本地模拟器不归一化,把这点藏了**;真后端暴露。极端边角,`numeric_test.go` 已排除 `-0` 并注明。

### 10.3 真 AWS DynamoDB 端到端实测

除本地 DynamoDB Local 外,把代理指向 **us-east-1 真 DynamoDB**(EC2 上,`consistency=strong` 走 LSI 强一致读),对真 Redis 3.2 跑同一套 13 维集成——**真后端抓到一个本地模拟器藏住的真实 bug**:

- **DEL→重建的惰性删除竞态 →「永久丢写」**(Porcupine 线性化检查在真 DynamoDB 上逮到):`DEL k` 入队异步 `DeleteMembers(pk)`;紧接着 `SET k v` 重建 value 项;惰性删除器随后把它一并抹掉(删 pk 下所有非 `#meta` 项,含新 string value 项)→ 之后 `GET k` 对**已确认的写永久**返回 nil。**真 DynamoDB 的网络延迟给了删除器 mid-test 运行的窗口,本地模拟器太快藏住了**。修复(**三轮**):
  - **redimo v2.6.1**:在 `DeleteMembers` 里加「`#meta` 复现则跳过」守卫——**放错了层**,`DeleteMembers` 也被**同步的** `LReplaceAll`(LSET/LTRIM/LREM/LINSERT 重写活列表)复用,守卫让「清空活列表」变 no-op,列表改写后新旧拼接(真 DynamoDB 列表属性测试逮到 `LRANGE=[旧… 新…]`)。
  - **redimos v1.29.0 / redimo v2.6.2**:`DeleteMembers` revert 回无条件删,重建守卫下移到只在异步路的 `DeleterConfig.IsLive` 回调(`LoadMeta`)——但 `LoadMeta` 查活与删除**非原子**,`SET` 落在两者之间仍被抹(全量跑 Porcupine 偶发)。
  - **redimos v1.30.0 / redimo v2.6.4:尝试事务 fence,实测被否**。新增 `DeleteMembersIfDead`——把「查 pk 死活」与「删成员」放进**同一个 `TransactWriteItems`**(首动作 `ConditionCheck(attribute_not_exists(#t) OR #exp <= :now)`)。逻辑上消除了丢写窗口,但**真 DynamoDB 上引入更糟的回归**:`TransactWriteItems` 对涉及项加短锁,**并发的非事务写**(`SET` 的 `UpdateItem`:EnsureType 改 `#meta`、SetString 改 value 项)撞上即 `TransactionConflictException` 失败——**连单连接顺序 `DEL;SET;GET` 都能让 `SET` 失败**(压测日志实测 151 次 `Transaction is ongoing for the item`,`GET` 读到上一轮的旧值)。事务锁换来的「不丢写」代价是「`SET` 报错」,对 Redis 代理不可接受。
  - **redimos v1.31.0(收敛):revert 回非事务 `IsLive` 尽力守卫**。redimo 回到无条件 `DeleteMembers`;异步删除器保留 `DeleterConfig.IsLive`(删前 `LoadMeta`,活键跳过)——非事务=**不会让 `SET` 失败**,且比无条件删**少抹**(删除器在 `SET` 之后跑到时会跳过)。但它**不能消除** DEL-then-SET 丢写:删除器可能在 `DEL` 与 `SET` 之间抢到入队项(`LoadMeta` 见 `#meta` 缺)、`DeleteMembers` 又在 `SET` 之后落地,抹掉刚写的 value 项。**彻底修需 per-incarnation epoch**(每项打代次戳、删除器按代次条件删)——一次大改,超出本轮范围。

- **结论:redimos 惰性删除架构下,单键寄存器在(并发,乃至紧凑顺序的)DEL+SET 下既不可线性化、也不保证不丢写**。根因是 string 的**分体存储**+ **非原子写**(`SET`=EnsureType 后 SetString)+ **非快照读**(`#meta` 与 value 两次独立读)+ **惰性回收滞留 value**。这与既有结论一致并强化之:**redimos 并发原子性 ≠ Redis 3.2**(见既往差分/并发原子性实测)。诚实化后测试**只断言 redimos 确实提供的保证**:`TestRegisterLinearizable` 跑 **SET/GET**(单项强一致=可线性化,实测通过);并发/顺序 `DEL+SET` 的丢写**不作断言**,仅在此**记录为架构性分歧**;`IsLive` 的跳活行为由单测 `TestDeleter_SkipsRecreatedKey` 覆盖。

> 这正是"在真测试环境更深入测试"的价值:`-0` 归一化、DEL 重建丢写、并发 DEL 寄存器不可线性化、以及**事务 fence 的 `TransactionConflictException` 回归**——全部**只在真 DynamoDB / 加压历史检查下现形**,本地模拟器把它们藏住了。`-0` 已修;丢写/不可线性化刻画为架构性分歧;事务方案实测被否、已 revert。

`I`(全量线性化)现拆为两断言:SET/GET 子集**必须**线性化(硬断言),并发 DEL 子集刻画为已记录的架构分歧;`DeleteMembersIfDead` 事务 fence 保证并发 DEL 下**不丢已确认写**。

---

## 11. 命令处置模型与剩余「不支持」可行性评估

### 11.1 三种处置的判据

| 处置 | 含义 | 判据 |
|---|---|---|
| **经 redimo** | 真正读写 DynamoDB | 数据/键状态命令，redimos 已实现 |
| **代理拒绝** | 注册 + 专属错误消息（一等公民拒绝） | 命令**存在于 Redis 3.2**，但代理明确不做——理由是无界成本 / 非原子多项写 / 语义在无状态代理上无意义。给专属消息而非「未知命令」，让客户端知道命令被识别但故意不可用 |
| **不支持（未知命令）** | 未注册 → `-ERR unknown command` | 只用于 **Redis 3.2 里本就不存在**的命令（如 Streams `X*`，5.0 才有）。此时「未知命令」正是与真 Redis 3.2 逐字节一致的回复 |

> v1.10.0 把发布订阅 / Lua / 事务 / 阻塞从「不支持」上移到「代理拒绝」，正是因为它们在 3.2 里真实存在，回「未知命令」会误导（暗示命令不存在）。

### 11.2 剩余 28 条「不支持」评估（多智能体对抗式评审结论）

对当前仍落在「未知命令」路径的 28 条命令逐条评估可实现性，判定分四档：

| 档 | 数量 | 命令 | 建议 |
|---|---|---|---|
| **真能实现** | 1 | ✅ `pfdebug`（v1.15.0） | 命令层解包 HYLL 的 16384×6-bit 寄存器。**GETREG 实测字节兼容**（Redis 稀疏/稠密两端一致）；ENCODING/TODENSE/DECODE 因 redimos 恒 DENSE 对 Redis 稀疏态 approx |
| **固定回复即正确（stub）** | 7（✅ **全部落地**，v1.15.0） | ✅ `save`→`+OK` · ✅ `bgsave`→`+Background saving started` · ✅ `bgrewriteaof`→`+Background append only file rewriting started` · ✅ `lastsave`→`:<当前秒>` · ✅ `role`→`[master,0,[]]` · ✅ `wait`→`:0` · ✅ `pfselftest`→`+OK`（均实测与 3.2 稳态一致） | 零 DynamoDB 交互、零并发隐患，让标准客户端/框架（写后 `WAIT`、连接自检等）不再撞未知命令 |
| **代理拒绝（3.2 里有但故意拒）** | 12（✅ **全部落地**） | ✅ `shutdown`（多租户 DoS，v1.11.0）· ✅ `asking`（Cluster 语义，v1.11.0）· ✅ `readonly`（v1.12.0）· ✅ `readwrite`（v1.13.0）· ✅ `randomkey`（无界全扫，同 KEYS）· ✅ `move`（整集合迁移非原子，同 RENAME）· ✅ `sort`（`BY/GET` 无界扇出 + `STORE` 非原子）· ✅ `object`（暴露 Redis 内部编码）· ✅ `monitor`（需跨连接命令总线）· ✅ `cluster`（纯 Cluster 语义）· ✅ `latency`（有状态进程内监控）· ✅ `debug`（多子命令，含 SEGFAULT 真崩）—— 均 v1.14.0 | 已全部注册专属拒绝错误（同 KEYS/RENAME/FLUSH），比未知命令更利于客户端识别 |
| **架构上不可能** | 8（✅ **全部代理拒绝**） | ✅ `replconf`（v1.13.0）· ✅ `dump`/`restore`/`restore-asking`（需 Redis 内部 RDB 序列化+CRC64）· ✅ `migrate`（需另一个真 Redis）· ✅ `sync`/`psync`（需数据集 dump 成 RDB blob 流式复制）· ✅ `slaveof`（需复制 backlog/主从链路）—— 后 7 条均 v1.18.0 | **可实现性 ≠ 处置**：这些实现不了，但已全部注册「代理拒绝」（专属消息）而非落未知命令 |

**一句话结论（v1.18.0 后）**：评估里的四类**已全部落地** —— pfdebug 实现、7 个 stub、12 条宜代理拒绝、8 条架构不可能（后者实现不了但也全部注册为代理拒绝）。**至此 174 条真实 Redis 3.2 命令没有一条落「未知命令」路径**：要么经 redimo 存储、要么固定回复桩、要么连接层、要么带专属消息的代理拒绝。那 8 条架构不可能项仍是 **DynamoDB 无状态代理的能力天花板**（要 RDB 内部格式 / 另一个真 Redis / 复制 backlog），只是现在拒得更友好、可识别。

> 评审方法：4 个评审 agent 分组给判定 + 4 个对抗 agent 逐条反驳「能实现/可 stub」的主张 + 1 个综合。`sort` 即被对抗评审从「能实现」下调为「代理拒绝」（裸 SORT 机械可拼，但 `BY/GET` 无界、`STORE` 非原子、byteCompat 仅 approx）。

> 本文档随后续提交更新；改了命令支持面后请重新生成 `command-reference.md`（见 `gen/README.md`）。
