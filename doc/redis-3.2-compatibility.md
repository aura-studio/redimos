# redimos × Redis 3.2 兼容性

本文档记录 redimos（RESP2 代理，后端 DynamoDB，存储层为 redimo/v2）对 **Redis 3.2** 的兼容状态：已对齐的部分、已知差异、并发原子性、字符/字节安全，以及待办项。随代码提交更新。

配套文档：

- [`command-reference.md`](command-reference.md) — Redis 3.2 全部 174 条命令 × redimos 处置（是否经 redimo）的逐条对照表。
- [`gen/`](gen/) — 上表的生成脚本与数据（`redisCommandTable` 解析 + redimos 处置交叉映射）。

对照口径以 **`redis/redis` @ branch `3.2` 的 `src/server.c`**（`redisCommandTable`）为权威来源，而非仅看 redimos 代码。

---

## 1. 结论速览

| 维度 | 状态 |
|---|---|
| 命令覆盖 | 174 条中 108 条经 redimo 支持（含 v1.5.0 GEO、v1.6.0 BIT 家族）；其余为控制面（连接/桩，不需存储）或未支持家族（HLL/阻塞/脚本/事务/复制…） |
| **字符/字节安全** | ✅ **完全对齐**：key、string 值、hash 字段名/值、set/zset 成员、list 元素对 0x00–0xff 全部 256 个字节值与 Redis 一致，无碰撞（v2.0.1 起） |
| **并发原子性** | ⚠️ **部分等同**：单项读改写（INCR/HINCRBY/APPEND…）与分值自增已原子；多项/多步写（S\*STORE、Z\*STORE、SMOVE、RPOPLPUSH、SETNX、部分写可见性）**非原子**，与 Redis 单线程模型不等同 |
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
| **NX 条件写** | SET NX / SETNX | ❌ 非原子（check-then-set，并发可多个"赢"） |
| **多步复合写** | S\*STORE / Z\*STORE / SMOVE / RPOPLPUSH 中间态 | ❌ 非原子（dest 清空-重填非事务） |
| **部分写可见性** | SCARD/ZCARD/LLEN vs 实际成员 | ❌ 并发读可见半写中间态 |
| 删除竞态 | delete-on-empty、DEL+重建 | ❌ 非原子（Load→DeleteMeta + 异步回收） |

**根因**：`meta + data + cnt` 多步写未用 `TransactWriteItems` 包裹。要整体对齐需为每条多 item 命令加事务（受 DynamoDB 25/100 item、成本、无部分成功限制）。

---

## 6. 待办 / 可继续项

| 项 | 类型 | 说明 |
|---|---|---|
| SET NX / SETNX 原子性 | 原子性 | 需 meta 层"条件创建"原语；属多步写非原子的一员，宜与下一项一并做 |
| 多步写事务化 | 架构 | 用 `TransactWriteItems` 收口 S\*STORE/Z\*STORE/SMOVE/RPOPLPUSH/SETNX + 部分写可见性 |
| score 解析对齐 | 差分 | 移植 glibc `strtod` 语义 |
| arity 错误文本 | 差分 | 系统性改成 3.2 裸大写风格（测试改动大） |
| GEO STORE/STOREDIST + 字节兼容 | 功能/差分 | GEO 家族已在 **v1.5.0 实现（功能版，见 §7）**；余下 STORE/STOREDIST 未做，且 GEOPOS/GEOHASH/GEODIST 因 S2 编码与 Redis 52-bit geohash 不同而低位有差 |
| HINCRBYFLOAT 精度 / 数值域 | 平台固有 | 需 long double 模拟 / 换数值编码，通常不做 |

---

## 7. GEO（v1.5.0 已支持，功能版）

Redis GEO 本质是 zset（member 的 score 编码位置，`TYPE` 返回 `zset`）。redimos v1.5.0 把 6 个 GEO 命令接到 redimo 的 GEO 原语：

- **已实现**：GEOADD / GEODIST / GEOPOS / GEOHASH / GEORADIUS / GEORADIUSBYMEMBER；GEORADIUS 支持 `WITHCOORD` / `WITHDIST` / `WITHHASH` / `COUNT` / `ASC` / `DESC`；单位 m/km/mi/ft；非 zset 键回 `WRONGTYPE`；GEOADD 维护 zset 的 meta/type + cnt。
- **实现要点**：命令层在 `internal/command/geo.go`，经独立 `storage.GeoStore` seam 调 redimo；距离用 haversine + Redis 地球半径常量 `6372797.560856`；`WITHHASH` 的 52-bit geohash 编码与 Redis **完全一致**。
- **与 Redis 3.2 的实测对拍**（Palermo/Catania）：GEOADD、GEORADIUS[BYMEMBER] 的成员/顺序/COUNT/WITHDIST/WITHHASH、`TYPE`、WRONGTYPE、缺成员 → **逐字节一致**；GEODIST → 4 位小数一致（米单位差约 0.1m）；GEOPOS → 亚米级低位差；GEOHASH → 前 10 字符一致，尾部精度/补零不同。

**未做 / 差异根因**：`STORE` / `STOREDIST` 选项尚未实现。GEOPOS/GEOHASH/GEODIST 的低位差源于 redimo 内部用 **Google S2 cell ID** 而非 Redis 的 52-bit 交错 geohash 存储位置；要达到这几项的字节兼容，需把 `redimo` 的 `GLocation` 编码换成 Redis geohash（lat 限 [-85.05, 85.05]）。GEORADIUS 的**成员正确性**不受影响（S2 CellUnionBound 是正确的球面覆盖）。

---

## 8. BIT（v1.6.0 已支持）

位命令全部作用于 String 值的字节;redimos 的 String 本就按二进制存储,所以 BIT **纯在命令层实现,不动 redimo**:

- **已实现**:SETBIT / GETBIT / BITCOUNT / BITPOS / BITOP(AND/OR/XOR/NOT) / BITFIELD(GET/SET/INCRBY + OVERFLOW WRAP/SAT/FAIL + `#index` 偏移)。
- **实现要点**:命令层在 `internal/command/bit.go` 与 `bitfield.go`;SETBIT/BITFIELD 写复用 String 的 CAS 读改写循环 `rmwString`;GETBIT/BITCOUNT/BITPOS 用 `readCurrentString`;BITOP 读 N 个源写 dest;BITFIELD 的定宽整数用 `math/big` 保证各位宽(i1..i64 / u1..u63)的溢出边界精确。
- **实测对拍(真 Redis 3.2.12)**:SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD 在 set/read、字节范围、清位越界、缺键、AND/OR/XOR/NOT、OVERFLOW SAT/FAIL/#索引 上 **逐字节一致**。
- **约束**:值上限 390KB(DynamoDB item 限制)→ 最大 bit offset ≈ **319 万**(Redis 是 2^32),超大 offset 的增长写会被值大小错误拒绝。**BITOP 是多键写**,与 MSET/*STORE 一样**非原子**(单连接下字节正确)。

## 9. 测试环境（Docker）

全部测试在 Docker 内跑（不用宿主）：真 Redis 3.2 作 oracle 与 redimos 代理同网,对拍字节级差分 + 并发原子性 + 逐字节字符对齐。详见仓库 `test/` 与差分框架 `test/difftest/`。

> 本文档随后续提交更新；改了命令支持面后请重新生成 `command-reference.md`（见 `gen/README.md`）。
