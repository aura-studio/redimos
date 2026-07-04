import re, json
src = open('server.c', encoding='utf-8', errors='replace').read()
m = re.search(r'redisCommandTable\[\] = \{(.*?)\n\};', src, re.S)
rows = re.findall(r'\{"([^"]+)",\s*\w+,\s*(-?\d+),\s*"([a-zA-Z]*)",\s*\d+,\s*\w+,\s*(-?\d+),\s*(-?\d+),\s*(-?\d+)', m.group(1))
rows = [(n.lower(), int(ar), s, int(fk)) for n, ar, s, fk, lk, st in rows if n not in ('host:', 'post')]

via = set('''get set setnx setex psetex append strlen setrange getrange getset mget mset incr incrby incrbyfloat decr decrby
del exists expire expireat pexpire pexpireat ttl pttl persist type
hset hsetnx hget hmset hmget hdel hlen hexists hkeys hvals hgetall hincrby hincrbyfloat hstrlen hscan
lpush rpush lpushx rpushx lpop rpop llen lrange lindex lset linsert lrem ltrim rpoplpush
sadd srem scard sismember smembers smove spop srandmember sinter sunion sdiff sinterstore sunionstore sdiffstore sscan
zadd zscore zincrby zcard zcount zrange zrevrange zrangebyscore zrevrangebyscore zrank zrevrank zrem zremrangebyrank zremrangebyscore zrangebylex zrevrangebylex zunionstore zinterstore zscan
scan'''.split())
conn = set('auth ping echo quit select'.split())
stub = set('command info dbsize config client slowlog time'.split())
# Server persistence/replication no-op stubs (v1.15.0): fixed benign reply, no
# storage interaction.
stub_extra = {
    'save':         '桩：DynamoDB 即持久层，无 RDB 可存 → 固定 +OK（v1.15.0）',
    'bgsave':       '桩：无 RDB/fork → 固定 +Background saving started（v1.15.0）',
    'bgrewriteaof': '桩：无 AOF → 固定 +Background append only file rewriting started（v1.15.0）',
    'lastsave':     '桩：无 RDB，回 router 时钟当前秒（v1.15.0）',
    'role':         '桩：standalone 诚实回 master 形态 [master,0,[]]（v1.15.0）',
    'wait':         '桩：无 Redis 副本(DynamoDB 已持久) → 固定 :0（v1.15.0）',
    'pfselftest':   '桩：HLL 自检可观测契约即 +OK → 固定 +OK（v1.15.0）',
}
reject = set('keys rename renamenx flushall flushdb'.split())
pubsub = set('subscribe unsubscribe psubscribe punsubscribe publish pubsub'.split())
script = set('eval evalsha script'.split())
txn = set('multi exec discard watch unwatch'.split())
admin = set()  # all former admin/replication commands are now proxy-reject (reject_extra)
# Individual real-Redis-3.2 commands promoted from unsupported to proxy-reject as the
# maintainer converts them one by one (each with its own dedicated message).
reject_extra = {
    'shutdown':  '代理拒绝：会终止所有租户共享的进程，且无 RDB 可先落盘（v1.11.0 起专属拒绝）',
    'asking':    '代理拒绝：Redis Cluster 槽迁移的一次性标志，非 cluster 单一 keyspace 代理无意义（v1.11.0 起专属拒绝）',
    'readonly':  '代理拒绝：Redis Cluster replica 只读服务开关，非 cluster 代理无 replica/slot 可切换（v1.12.0 起专属拒绝）',
    'readwrite': '代理拒绝：清除 Cluster replica 只读标志（READONLY 的反向），无 cluster/replica 状态可复位（v1.13.0 起专属拒绝）',
    'replconf':  '代理拒绝：master↔replica 复制子协议（端口/capa 协商 + ACK offset 心跳），无复制链路/offset（v1.13.0 起专属拒绝）',
    'dump':           '代理拒绝：需 Redis 内部 RDB 序列化，代理不产出（v1.18.0；实现不了但显式拒绝而非未知命令）',
    'restore':        '代理拒绝：需反序列化 Redis RDB payload，代理无 RDB 解析器（v1.18.0）',
    'restore-asking': '代理拒绝：Cluster 槽迁移变体 + 继承 RESTORE 的 RDB 反序列化，均不适用（v1.18.0）',
    'migrate':        '代理拒绝：需对另一个真 Redis 做 DUMP/RESTORE + 删本地，代理无此对端（v1.18.0）',
    'sync':           '代理拒绝：需把数据集 dump 成 RDB blob 流式复制，代理无内存数据集表示（v1.18.0）',
    'psync':          '代理拒绝：需复制 ID + 逐字节 offset backlog，stateless 代理从无（v1.18.0）',
    'slaveof':        '代理拒绝：代理无本地数据集，当不了 replica 也当不了 master（v1.18.0）',
    'randomkey': '代理拒绝：分区表上取随机键需无界全表扫（同 KEYS）；用 SCAN（v1.14.0 起专属拒绝）',
    'move':      '代理拒绝：跨 DB 搬键=按新 pk 前缀重写整集合，非原子（同 RENAME）（v1.14.0 起专属拒绝）',
    'sort':      '代理拒绝：BY/GET 每元素每模式一次外部读（无界扇出）+ STORE 非原子整集合替换（v1.14.0 起专属拒绝）',
    'object':    '代理拒绝：ENCODING/REFCOUNT/IDLETIME 暴露 Redis 内部表示，DynamoDB item 没有（v1.14.0 起专属拒绝）',
    'monitor':   '代理拒绝：需跨连接全局命令流+长驻流,无状态代理无法提供（同发布订阅）（v1.14.0 起专属拒绝）',
    'cluster':   '代理拒绝：单一逻辑 keyspace 无 slot/成员,整族语义不适用（v1.14.0 起专属拒绝）',
    'latency':   '代理拒绝：进程内有状态延迟监控,无状态代理不积累、多实例不自洽（v1.14.0 起专属拒绝）',
    'debug':     '代理拒绝：多子命令(OBJECT/RELOAD/SET-ACTIVE-EXPIRE/SEGFAULT),无统一回复且部分危险（v1.14.0 起专属拒绝）',
}
# BIT family implemented (v1.6.0), byte-compatible for single-key ops (BITOP is
# multi-key non-atomic).
bit_new = set('setbit getbit bitcount bitop bitpos bitfield'.split())
bit = set()
# PFADD/PFCOUNT/PFMERGE implemented (v1.7.0); pfdebug stays unsupported (debug).
hll_new = set('pfadd pfcount pfmerge'.split())
hll = set()
# PFDEBUG implemented (v1.15.0): command-layer unpack of the HYLL register blob.
newly_pfdebug = set('pfdebug'.split())
# The 6 base GEO commands are implemented (v1.5.0; rewritten byte-compatible in
# v1.8.0 as a zset with a 52-bit geohash score); the read-only _ro variants
# (Redis 3.2.10+) are not registered.
geonew = set('geoadd geodist geopos geohash georadius georadiusbymember'.split())
geo = set('georadius_ro georadiusbymember_ro'.split())
block = set('blpop brpop brpoplpush'.split())
flush = set('flushall flushdb'.split())
keymgmt = set()  # migrate/dump/restore/restore-asking are now proxy-reject (reject_extra)
# Newly implemented — now served via redimo. Kept as named sets so the table can
# badge them as recently added.
newly = set('msetnx substr touch zlexcount zremrangebylex'.split())  # v1.4.0
newly_geo = geonew  # v1.5.0
newly_bit = bit_new  # v1.6.0
newly_hll = hll_new  # v1.7.0
newly_geo_ro = geo  # v1.10.0: GEORADIUS_RO / GEORADIUSBYMEMBER_RO (aliases of the base GEO commands)
via = via | newly | newly_geo | newly_bit | newly_hll | newly_geo_ro | newly_pfdebug
notimpl = set()

listcmds = set('lpush rpush lpushx rpushx lpop rpop llen lrange lindex lset linsert lrem ltrim rpoplpush'.split())
keyexp = set('del exists expire expireat pexpire pexpireat ttl pttl persist type touch'.split())

def fam(n):
    if n in via or n in notimpl:
        if n in geonew or n in geo:
            return 'geo'
        if n in ('setbit','getbit','bitcount','bitpos','bitop','bitfield'):
            return 'bit'
        if n in ('pfadd','pfcount','pfmerge','pfdebug'):
            return 'hll'
        if n[0] == 'h':
            return 'hash'
        if n in listcmds:
            return 'list'
        if n[0] == 'z':
            return 'zset'
        if n in keyexp:
            return 'key/expiry'
        if n == 'scan':
            return 'scan'
        if n[0] == 's' and n not in ('scan', 'set', 'setnx', 'setex', 'setrange', 'strlen'):
            return 'set'
        return 'string'
    return ''

out = []
for n, ar, s, fk in rows:
    keyspace = ('w' in s) or ('r' in s)
    if n in via:
        disp, need = 'via-redimo', '是'
        if n in newly_hll:
            reason = '✓ 新增 v1.7.0 → 经 redimo（HLL；PFCOUNT 低基数字节一致、高基数在误差内）'
        elif n in newly_bit:
            reason = '✓ 新增 v1.6.0 → 经 redimo（BIT，单键字节兼容；BITOP 多键非原子）'
        elif n in newly_geo:
            reason = '✓ 新增 v1.5.0；v1.8.0 改字节兼容版（zset + 52-bit geohash，非 S2）→ 经 redimo 存储'
        elif n in newly:
            reason = '✓ 新增 v1.4.0 → 经 redimo'
        elif n in newly_geo_ro:
            reason = '✓ 新增 v1.10.0 → GEORADIUS_RO/GEORADIUSBYMEMBER_RO 只读变体，别名到已实现的 GEO 命令（禁 STORE/STOREDIST）'
        elif n in newly_pfdebug:
            reason = '✓ 新增 v1.15.0 → PFDEBUG GETREG/ENCODING/TODENSE/DECODE，命令层解包 HYLL 寄存器（GETREG 字节兼容；redimos 恒 DENSE，其余对 Redis 稀疏态 approx）'
        else:
            reason = '数据/键状态读写 → 经 redimo 映射到 DynamoDB'
    elif n in conn:
        disp, need, reason = 'connection', '否', '仅操作连接状态，不碰键空间'
    elif n in stub:
        disp, need = 'stub', '否'
        reason = '服务器自省，固定/内存态回答' + ('；键计数用 :0 桩不扫表' if n == 'dbsize' else '')
    elif n in stub_extra:
        disp, need, reason = 'stub', '否', stub_extra[n]
    elif n in reject:
        disp, need = 'proxy-reject', '否'
        if n == 'keys':
            reason = '代理拒绝：KEYS 无界全扫在 DynamoDB 上危险'
        elif n in ('flushall', 'flushdb'):
            reason = '代理拒绝：会清空整个 DynamoDB 表（v1.5.1 起专属拒绝）'
        else:
            reason = '代理拒绝：RENAME 需整集合搬迁，代价过高'
    elif n in pubsub:
        disp, need, reason = 'proxy-reject', '否', '代理拒绝：发布订阅需连接级订阅+跨连接 fan-out，无状态代理不适合（v1.10.0 起专属拒绝）'
    elif n in script:
        disp, need, reason = 'proxy-reject', '否', '代理拒绝：Lua 脚本需内嵌解释器（v1.10.0 起专属拒绝）'
    elif n in txn:
        disp, need, reason = 'proxy-reject', '否', '代理拒绝：事务需排队+原子应用多命令（v1.10.0 起专属拒绝）'
    elif n in bit:
        disp, need, reason = 'unsupported', '否', '位运算：超范围'
    elif n in hll:
        disp, need, reason = 'unsupported', '否', 'HyperLogLog：可经命令层实现，尚未做（同 BIT）'
    elif n in block:
        disp, need, reason = 'proxy-reject', '否', '代理拒绝：阻塞命令需长连接阻塞语义（改用非阻塞 LPOP/RPOP/RPOPLPUSH；v1.10.0 起专属拒绝）'
    elif n in reject_extra:
        disp, need, reason = 'proxy-reject', '否', reject_extra[n]
    elif n in flush:
        disp, need, reason = 'unsupported', '否', '全表清空：未支持'
    elif n in keymgmt:
        disp, need, reason = 'unsupported', '否', '键管理：DynamoDB 表达不了/代价过高'
    elif n in notimpl:
        disp, need, reason = 'unsupported', '否', '★可经 redimo 实现，尚未注册'
    elif n in admin:
        disp, need, reason = 'unsupported', '否', '服务器/复制/管理：不适用于无状态代理'
    else:
        disp, need, reason = 'unsupported', '否', '其它'
    out.append(dict(cmd=n, arity=ar, sflags=s, firstkey=fk, keyspace='是' if keyspace else '否', fam=fam(n), disp=disp, need=need, reason=reason))

json.dump(out, open('cmds.json', 'w', encoding='utf-8'), ensure_ascii=False)
from collections import Counter
print('rows:', len(out))
print('need=yes:', sum(1 for r in out if r['need'] == '是'))
print('disp:', dict(Counter(r['disp'] for r in out)))
print('star:', [r['cmd'] for r in out if '★' in r['reason']])
