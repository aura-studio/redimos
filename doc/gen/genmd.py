# -*- coding: utf-8 -*-
# Generates doc/command-reference.md from cmds.json (parsed Redis 3.2 command table
# cross-referenced with redimos disposition). Re-run when the disposition changes.
import json
data = json.load(open('cmds.json', encoding='utf-8'))

disp_order = {'via-redimo': 0, 'stub': 1, 'connection': 2, 'proxy-reject': 3, 'unsupported': 4}
fam_order = {'string': 0, 'key/expiry': 1, 'hash': 2, 'list': 3, 'set': 4, 'zset': 5, 'scan': 6, '': 9}
data.sort(key=lambda r: (disp_order[r['disp']], fam_order.get(r['fam'], 9), r['cmd']))

n = len(data)
c = lambda d: sum(1 for r in data if r['disp'] == d)
n_via, n_stub, n_conn, n_rej, n_uns = c('via-redimo'), c('stub'), c('connection'), c('proxy-reject'), c('unsupported')
n_new = sum(1 for r in data if '新增' in r['reason'])

disp_label = {'via-redimo': '经 redimo', 'stub': '桩', 'connection': '连接', 'proxy-reject': '代理拒绝', 'unsupported': '不支持'}

out = []
W = out.append
W('# Redis 3.2 命令 × redimos 存储边界对照')
W('')
W('> 数据来源：`redis/redis` @ branch `3.2` · `src/server.c` 的 `redisCommandTable`（%d 条真实命令，`QUIT` 在查表前被拦截故不在表内）。' % n)
W('> 判据：命令带 `w`（写）/`r`（读键空间）标志即触及键空间；只有触及键空间**且被 redimos 支持**的命令才真正经 redimo（DynamoDB 存储层）。')
W('> 本表由 `doc/gen/` 的脚本从命令表自动生成，随 redimos 版本更新。')
W('')
W('## 汇总')
W('')
W('| 处置 | 数量 | 说明 |')
W('|---|---:|---|')
W('| **经 redimo** | %d | 数据/键状态读写，真正打 DynamoDB |' % n_via)
W('| 桩 | %d | 固定/内存态回答（如 DBSIZE→`:0`），不碰键空间 |' % n_stub)
W('| 连接 | %d | 仅连接状态（AUTH/SELECT/PING/ECHO） |' % n_conn)
W('| 代理拒绝 | %d | 定制拒绝（KEYS/RENAME） |' % n_rej)
W('| 不支持 | %d | 未知命令路径（是数据命令但 redimos 未支持/超范围） |' % n_uns)
W('| **合计** | %d | 其中 %d 需要 redimo |' % (n, n_via))
W('')
W('近期在 v1.4.0 新增并经 redimo 的命令（此前为「不支持」）：**MSETNX · SUBSTR · TOUCH · ZLEXCOUNT · ZREMRANGEBYLEX**。')
W('')

groups = [
    ('via-redimo', '需要 redimo（数据面，经存储层）', '数据与键的元信息（类型/TTL/计数）都存在 DynamoDB，这些命令必须读/写它。'),
    ('stub', '不需要 redimo — 桩', 'redimos 用固定或内存态回答，不访问 DynamoDB。'),
    ('connection', '不需要 redimo — 连接层', '只操作连接状态。'),
    ('proxy-reject', '不需要 redimo — 代理拒绝', '定制拒绝：DynamoDB 表达代价过高。'),
    ('unsupported', '不经 redimo — 未支持（未知命令）', '是 Redis 数据命令，但 redimos 在命令层就短路，不发起存储调用。'),
]

for disp, title, note in groups:
    rows = [r for r in data if r['disp'] == disp]
    W('## %s — %d 条' % (title, len(rows)))
    W('')
    W(note)
    W('')
    W('| 命令 | sflags | firstkey | 键空间 | 家族 | 走 redimo | 原因 |')
    W('|---|---|---:|:---:|---|:---:|---|')
    for r in rows:
        reason = r['reason'].replace('✓', '**✓**').replace('★', '★')
        W('| `%s` | `%s` | %d | %s | %s | %s | %s |' % (
            r['cmd'], r['sflags'] or '—', r['firstkey'], r['keyspace'],
            r['fam'] or '—', r['need'], reason))
    W('')

open('command-reference.md', 'w', encoding='utf-8').write('\n'.join(out))
print('wrote command-reference.md', len('\n'.join(out)), 'chars;', n, 'commands')
