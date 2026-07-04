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
| 命令覆盖 | 174 条中 111 条经 redimo 存储支持（含 v1.5/v1.8 GEO、v1.6 BIT、v1.7 HLL 家族）；其余为控制面（连接/桩，不需存储）或未支持家族（阻塞/脚本/事务/复制…） |
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
| redimos **v1.7.0** | 新增 **HLL 家族**（PFADD/PFCOUNT/PFMERGE），纯命令层,Redis 3.2 hll.c 忠实移植 |
| redimos **v1.8.0** | **GEO 改为字节兼容版**：在 zset 之上重写（member.score = 52-bit geohash，与 Redis 同构），弃用 redimo 的 S2 seam；GEOPOS/GEOHASH/GEODIST/WITHHASH 与 Redis 3.2 **逐字节一致**（实测 265/265 差分通过） |

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

> 本文档随后续提交更新；改了命令支持面后请重新生成 `command-reference.md`（见 `gen/README.md`）。
