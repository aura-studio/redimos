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
reject = set('keys rename renamenx flushall flushdb'.split())
pubsub = set('subscribe unsubscribe psubscribe punsubscribe publish pubsub'.split())
script = set('eval evalsha script'.split())
txn = set('multi exec discard watch unwatch'.split())
admin = set('bgrewriteaof bgsave save lastsave shutdown slaveof replconf asking readonly readwrite wait pfselftest debug monitor cluster latency role sync psync'.split())
# BIT family implemented (v1.6.0), byte-compatible for single-key ops (BITOP is
# multi-key non-atomic).
bit_new = set('setbit getbit bitcount bitop bitpos bitfield'.split())
bit = set()
hll = set('pfadd pfcount pfmerge pfdebug'.split())
# The 6 base GEO commands are implemented (v1.5.0); the read-only _ro variants
# (Redis 3.2.10+) are not registered.
geonew = set('geoadd geodist geopos geohash georadius georadiusbymember'.split())
geo = set('georadius_ro georadiusbymember_ro'.split())
block = set('blpop brpop brpoplpush'.split())
flush = set('flushall flushdb'.split())
keymgmt = set('move migrate dump restore restore-asking randomkey object sort'.split())
# Newly implemented — now served via redimo. Kept as named sets so the table can
# badge them as recently added.
newly = set('msetnx substr touch zlexcount zremrangebylex'.split())  # v1.4.0
newly_geo = geonew  # v1.5.0
newly_bit = bit_new  # v1.6.0
via = via | newly | newly_geo | newly_bit
notimpl = set()

listcmds = set('lpush rpush lpushx rpushx lpop rpop llen lrange lindex lset linsert lrem ltrim rpoplpush'.split())
keyexp = set('del exists expire expireat pexpire pexpireat ttl pttl persist type touch'.split())

def fam(n):
    if n in via or n in notimpl:
        if n in ('geoadd','geodist','geopos','geohash','georadius','georadiusbymember'):
            return 'geo'
        if n in ('setbit','getbit','bitcount','bitpos','bitop','bitfield'):
            return 'bit'
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
        if n in newly_bit:
            reason = '✓ 新增 v1.6.0 → 经 redimo（BIT，单键字节兼容；BITOP 多键非原子）'
        elif n in newly_geo:
            reason = '✓ 新增 v1.5.0 → 经 redimo（GEO，功能版）'
        elif n in newly:
            reason = '✓ 新增 v1.4.0 → 经 redimo'
        else:
            reason = '数据/键状态读写 → 经 redimo 映射到 DynamoDB'
    elif n in conn:
        disp, need, reason = 'connection', '否', '仅操作连接状态，不碰键空间'
    elif n in stub:
        disp, need = 'stub', '否'
        reason = '服务器自省，固定/内存态回答' + ('；键计数用 :0 桩不扫表' if n == 'dbsize' else '')
    elif n in reject:
        disp, need = 'proxy-reject', '否'
        if n == 'keys':
            reason = '代理拒绝：KEYS 无界全扫在 DynamoDB 上危险'
        elif n in ('flushall', 'flushdb'):
            reason = '代理拒绝：会清空整个 DynamoDB 表（v1.5.1 起专属拒绝）'
        else:
            reason = '代理拒绝：RENAME 需整集合搬迁，代价过高'
    elif n in pubsub:
        disp, need, reason = 'unsupported', '否', '发布订阅：架构受限（需连接级订阅+跨连接 fan-out，无状态代理不适合）'
    elif n in script:
        disp, need, reason = 'unsupported', '否', 'Lua 脚本：架构受限（需内嵌解释器）'
    elif n in txn:
        disp, need, reason = 'unsupported', '否', '事务：架构受限（需原子多命令）'
    elif n in bit:
        disp, need, reason = 'unsupported', '否', '位运算：超范围'
    elif n in hll:
        disp, need, reason = 'unsupported', '否', 'HyperLogLog：可经命令层实现，尚未做（同 BIT）'
    elif n in geo:
        disp, need, reason = 'unsupported', '否', 'GEO：超范围'
    elif n in block:
        disp, need, reason = 'unsupported', '否', '阻塞命令：架构受限（需长连接阻塞语义）'
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
