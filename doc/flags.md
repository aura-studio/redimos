# redimos 命令行参数详解

redimos 是一个把 Redis(RESP2 / Redis 3.2 线协议)映射到 DynamoDB 的代理。所有配置**只通过命令行 flag**(不读环境变量配置文件;凭据的环境变量由 AWS SDK 默认链读取,见下)。

```
redimos [flags]
```

> **两条线通用**:本文件的参数在 `redimos`(v1 线,后端 redimo v1.6.1)和 `redimos/v2`(v2 线,后端 redimo v2)**完全一致**。行为差异仅在命令覆盖面(v1 gate 掉 SCAN/TTL/位运算/PF 等)与单/多 db 语义,和 flag 无关,见文末「v1 / v2 差异」。

---

## 目录
- [1. 监听 / 认证](#1-监听--认证)
- [2. DynamoDB 连接](#2-dynamodb-连接) ← 重点,含**三种连接模式**
- [3. 多 DB](#3-多-db)
- [4. 大小 / 超时防护](#4-大小--超时防护)
- [5. 限流 / 熔断 / 后台回收](#5-限流--熔断--后台回收)
- [6. SCAN 游标](#6-scan-游标)
- [7. 可观测](#7-可观测) ← 含 **metrics 端口自动避让**
- [附:三种 DynamoDB 连接模式(带完整示例)](#附三种-dynamodb-连接模式)
- [附:单 db / 多 db 键编码](#附单-db--多-db-键编码)
- [v1 / v2 差异](#v1--v2-差异)

---

## 1. 监听 / 认证

### `-addr`
- **默认**:`:6379`
- **类型**:string(监听地址)
- **作用**:RESP2 端点的监听地址,客户端(redis-cli / GUI / SDK)连这里。
- **示例**:`-addr :6379`、`-addr 127.0.0.1:6380`
- **备注**:多实例同机器时每个要给不同端口;绑不上(端口被占等)**启动即失败**。

### `-requirepass`
- **默认**:空(不鉴权)
- **类型**:string
- **作用**:单密码 `AUTH`。设了之后客户端必须先 `AUTH <password>`。
- **示例**:`-requirepass s3cr3t`
- **备注**:空 = 关闭鉴权。密码比较为常量时间。

---

## 2. DynamoDB 连接

> 这一组决定「连哪个 DynamoDB、用什么凭据」。全部**可选**;组合方式对应下面的[三种连接模式](#附三种-dynamodb-连接模式)。判定逻辑:**任一 endpoint flag 非空 → 安装 endpoint resolver;任一 credential flag 非空 → 安装静态凭据;两者都不给 → 走 AWS SDK 默认链。**

### `-table`
- **默认**:`redis-data`
- **作用**:DynamoDB 单表名(所有 key 都存这一张表)。
- **示例**:`-table redis-data`
- **备注**:表必须**预先存在**且 schema 正确(见文末「表 schema」),启动时会做一次 `GetItem` 探活,不存在直接失败。

### `-region`
- **默认**:空(走默认链的 region:env `AWS_REGION` / profile)
- **作用**:AWS region。**同时兼作 `-endpoint-url` 自定义 endpoint 的签名 region**(所以不需要单独的 signing-region 参数)。
- **示例**:`-region us-east-1`
- **备注**:模式②③(连真实 AWS、不设 endpoint)必须有 region 才能解析真实 endpoint——由本 flag 或 env `AWS_REGION` 提供。

### `-endpoint-url`
- **默认**:空
- **作用**:DynamoDB endpoint URL 覆盖。设了就**安装 endpoint resolver**,把 DynamoDB 服务请求指向这个 URL(签名用 `-region`)。
- **示例**:`-endpoint-url http://localhost:8000`(dynamodb-local)、`-endpoint-url https://dynamodb.us-east-1.amazonaws.com`
- **备注**:**设了它、且没给任何凭据 flag 时,会自动注入 dummy 静态凭据**——所以本地 dynamodb-local 只写这一个 flag 就能跑(否则 SDK 会因「无凭据」报错)。

### `-endpoint-partition-id`
- **默认**:空
- **作用**:endpoint resolver 的 AWS partition id(`aws` / `aws-cn` / `aws-us-gov`)。很少用。
- **示例**:`-endpoint-partition-id aws`
- **备注**:仅在设了自定义 endpoint 时才有意义。

### `-access-key-id` / `-secret-access-key` / `-session-token`
- **默认**:空(空则走默认凭据链)
- **作用**:**静态 AWS 凭据**。任一非空就安装一个静态 `CredentialsProvider`(AK / SK / 临时凭据的 Token)。
- **示例**:`-access-key-id AKIA... -secret-access-key xxxx -session-token yyyy`
- **备注**:`-session-token` 用于 STS 临时凭据(可留空用长期凭据)。静态凭据的 `Source` 固定标为 `redimos`。**不设**这三个 → 凭据走 SDK 默认链(env / profile / IAM role)。

---

## 3. 多 DB

### `-multi-db`
- **默认**:关(false)
- **作用**:开启后支持 `SELECT` 非 0 的 db,按 db **分区**(pk = `{db}:key`)。
- **示例**:`-multi-db`
- **备注**:**不开(默认单 db)时**:所有 `SELECT n` 都被接受但**别名到同一个 keyspace**(pk = 裸 key,无前缀)。见[单/多 db 键编码](#附单-db--多-db-键编码)。

### `-databases`
- **默认**:16(Redis 默认)
- **作用**:多 db 模式下 `SELECT` 的上界:合法 db 为 `[0, databases)`,越界回 `ERR invalid DB index`。
- **示例**:`-databases 16`
- **备注**:仅在 `-multi-db` 开时生效。

---

## 4. 大小 / 超时防护

### `-max-collection-result`
- **默认**:0(关)
- **作用**:整集合回复 / 操作数(`HGETALL`/`SMEMBERS`/`LRANGE`/`ZRANGE`/`*STORE` 操作数…)成员数超过 N 就拒绝,避免代理内存被撑爆。
- **示例**:`-max-collection-result 100000`

### `-max-command-bytes`
- **默认**:0(关)
- **作用**:单条命令原始 wire 字节超过 N 就拒绝。
- **示例**:`-max-command-bytes 10485760`(10MB)

### `-command-timeout`
- **默认**:0(关)
- **类型**:duration
- **作用**:单条命令的后端调用截止时间,超时则取消并回错误。
- **示例**:`-command-timeout 3s`

---

## 5. 限流 / 熔断 / 后台回收

### `-delete-batch-size`
- **默认**:25(DynamoDB `BatchWriteItem` 上限)
- **作用**:惰性删除器每次 `BatchWriteItem` 删多少成员项。
- **示例**:`-delete-batch-size 25`
- **备注**:范围 1–25。**v1 线基本用不到**(无 meta 层,删除走 rv1 同步 DEL)。

### `-delete-rate`
- **默认**:50(每秒 pk 数)
- **类型**:float
- **作用**:惰性删除器每秒处理的 pk 数(限速),`<=0` 关闭限速。
- **示例**:`-delete-rate 50`

### `-circuit-breaker-threshold`
- **默认**:0(关)
- **作用**:累计 N 次 DynamoDB 节流后打开「减载熔断」,命令快速失败,保护后端。
- **示例**:`-circuit-breaker-threshold 20`

### `-circuit-breaker-cooldown`
- **默认**:5s
- **类型**:duration
- **作用**:熔断打开后持续减载多久。

### `-sweep-interval`
- **默认**:168h(1 周)
- **类型**:duration
- **作用**:**孤儿清扫器**的运行周期。孤儿 = 数据成员项还在、但归属 key 的 `#meta` 项已没了的残留 item(惰性删除的兜底)。定期全表扫并清理。
- **备注**:⚠️ **v1 线上是 no-op**(v1 无 meta 层、没有孤儿概念),留着只为接口一致。**v2 线**上才真的每周清孤儿。

---

## 6. SCAN 游标

> 这组配置 `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN` 的游标注册表。**v1 线 gate 掉了所有 SCAN 族命令**,这组在 v1 上无实际作用;仅 v2 线有效。

### `-inst-id`
- **默认**:空(自动生成)
- **作用**:代理实例 id,用于 SCAN 游标的归属校验(防止游标被别的实例误用)。
- **示例**:`-inst-id proxy-a`

### `-scan-capacity`
- **默认**:10000
- **作用**:最大存活 SCAN 游标数。

### `-scan-ttl`
- **默认**:10m
- **类型**:duration
- **作用**:单个 SCAN 游标的寿命。

### `-scan-timeout`
- **默认**:5s
- **类型**:duration
- **作用**:单页 SCAN 打后端的最长耗时,`0` 关闭。

---

## 7. 可观测

### `-metrics-addr`
- **默认**:`:9121`
- **作用**:HTTP 监听地址,提供 `/metrics`(Prometheus)、`/healthz`、`/readyz`。
- **示例**:`-metrics-addr :9121`、`-metrics-addr :0`
- **⭐ 自动端口避让**:如果配置的地址**被占用**(`address already in use`,例如同机器多实例),会**自动退到 `:0` 让 OS 分配一个空闲端口**,并把**实际绑定的端口打进启动日志**(`redimos serving: ... metrics=[::]:38955`)。其它 bind 错误(权限、非法地址)仍然启动失败。也可以直接写 `-metrics-addr :0` 显式让 OS 选端口。

### `-slowlog-threshold`
- **默认**:10ms
- **类型**:duration
- **作用**:命令耗时超过该阈值就记进 slowlog 环形缓冲。

### `-slowlog-capacity`
- **默认**:128
- **作用**:slowlog 环形缓冲大小。

### `-request-log`
- **默认**:`none`
- **作用**:PII-safe 的结构化(JSON)请求日志级别:`none` | `error` | `slow` | `all`。

### `-consistency`
- **默认**:`strong`
- **作用**:默认读一致性:`strong`(强一致,读己写)| `eventual`(最终一致,更省更快)。
- **备注**:读改写命令(INCR 等)不依赖读一致性,两种设置下都正确。

### `-retry-max-attempts`
- **默认**:5
- **作用**:AWS SDK 对节流(ProvisionedThroughputExceeded)的最大重试次数(指数退避)。

---

## 附:三种 DynamoDB 连接模式

判定逻辑再强调一次:**endpoint flag 有值 → endpoint resolver;credential flag 有值 → 静态凭据;都不给 → SDK 默认链。**

### 模式① 本地 DynamoDB Local
只写 endpoint,凭据自动注入 dummy:
```
redimos -addr :6379 -table redis-data \
  -endpoint-url http://localhost:8000 -region us-east-1
```
> ⚠️ DynamoDB Local **按 access-key 分表命名空间**:dummy 凭据看到的表和用别的 AK 建的表是隔离的。用 aws-cli 建表时也要用同样的(dummy)凭据。

### 模式② 本地用静态 AK/SK/Token 连线上 DynamoDB
给静态凭据,不设 endpoint(走真实 AWS):
```
redimos -addr :6379 -table my-table -region us-east-1 \
  -access-key-id AKIA... -secret-access-key xxxx -session-token yyyy
```

### 模式③ 线上(EC2/ECS/EKS)走默认链
什么连接 flag 都不给,凭据/region 由 SDK 默认链解析(env `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`/`AWS_REGION` 或 IAM role):
```
redimos -addr :6379 -table my-table
```
> 本地跑模式③(无 env 凭据、无 IAM)会因找不到凭据而超时——这是正常的,它就是给有 IAM role 的云上环境用的。

---

## 附:单 db / 多 db 键编码

| 模式 | flag | pk 编码 | SELECT n(n>0) | 表里 key 长啥样 |
|---|---|---|---|---|
| **单 db(默认)** | 不加 `-multi-db` | **裸 key**(无前缀) | 接受,别名到同一 keyspace | `foo` |
| **多 db** | `-multi-db` | `{db}:key` | `[0,databases)` 内 OK,越界报 `invalid DB index` | `0:foo` / `1:foo` |

> ⚠️ **两种模式的存储布局不兼容**(裸 key vs 带前缀):单 db 模式去读多 db 写的表,前缀会显性出现在 key 名里;多 db 去读单 db 写的表,前缀匹配不上就看不到。**同一部署不要在两种模式间来回切**。

---

## v1 / v2 差异

flag 层面 **v1 与 v2 完全一致**;语义差异来自后端:

| | `redimos`(v1 线) | `redimos/v2`(v2 线) |
|---|---|---|
| 后端 | redimo **v1.6.1** | redimo **v2** |
| 命令覆盖 | 子集,不支持的(SCAN 族 / TTL / TYPE / 位运算 / PF / SETRANGE 等)返回 `unknown command` | 全量 |
| WRONGTYPE / TTL | 无(rv1.6.1 无类型/TTL 机制) | 有 |
| `-sweep-interval` / SCAN 游标组 | 实际不生效(no-op / 命令被 gate) | 生效 |
| 集合成员二进制安全 | 成员/字段名仅 UTF-8 安全(值仍二进制安全) | 全二进制安全 |

> 所有连接 / 多db / metrics 参数在两条线上**行为一致**。
