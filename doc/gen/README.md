# 命令对照表生成器

`../command-reference.md` 由 Redis 3.2 官方命令表（`redis/redis` @ branch `3.2` ·
`src/server.c` 的 `redisCommandTable`）解析后，与 redimos 的命令注册与处置交叉映射生成。

## 重新生成

```bash
# 1) 拉取 Redis 3.2 命令表源文件（仅解析，不编译 Redis）
curl -s https://raw.githubusercontent.com/redis/redis/3.2/src/server.c -o server.c

# 2) 解析命令表 + 交叉映射 redimos 处置 -> cmds.json
python gen.py

# 3) cmds.json -> ../command-reference.md
python genmd.py && mv command-reference.md ../command-reference.md
```

## 改了命令支持面后

在 `gen.py` 里更新对应集合即可：

- `via`     — 经 redimo 支持的数据命令
- `newly`   — 本轮新增（表中标绿）
- `stub`/`conn`/`reject` — 桩 / 连接 / 代理拒绝
- 其余落入 unsupported（未知命令）

`cmds.json` 是上次生成的快照；若无网络可直接 `python genmd.py` 复用它。
