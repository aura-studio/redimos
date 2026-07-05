# pika-migrate (build image)

A reproducible build of **pika-migrate** — the online tool that migrates a real
**Pika** into **redimos**. pika-migrate is a modified Pika 3.2 binary
(`Qihoo360/pika` branch `v3_2_7_migrate`) that disguises itself as a *slave* of
the source Pika, then forwards the data to a *target* (redimos) as plain
high-level Redis write commands.

This directory just packages the tool. The mechanism, the verified end-to-end
test results (real Pika v3.2.2 → redimos: 523 keys, 0 mismatches; incremental
correct for all deterministic commands; zero redimos rejections), and the full
list of gotchas live in **[`../../doc/pika-migrate.md`](../../doc/pika-migrate.md)**.

## Why a Dockerfile here

pika-migrate has no prebuilt binary/image and is an old (2019-era) C++ codebase.
The `Dockerfile` builds it against the matching `pikadb/pika:v3.2.2` toolchain
(gcc 4.8) and bakes in the fixes needed to compile and package it:

- drop `-static-libstdc++` from the Makefile (the base ships no 64-bit
  `libstdc++.a`; link it dynamically — the runtime `.so.6` is in the same base);
- shallow clone + submodules (glog/slash/pink/blackwidow/rocksdb);
- reuse the same base for the runtime stage so the binary's shared-lib ABI
  (libstdc++/libprotobuf/libsnappy/libgflags/libz + `rsync`) matches.

## Build

```sh
# GitHub may need a proxy on your network; omit --build-arg in open networks.
docker build -t pika-migrate:local \
  --build-arg HTTPS_PROXY=http://host.docker.internal:7897 \
  --build-arg HTTP_PROXY=http://host.docker.internal:7897 \
  deploy/pika-migrate
```

## Run a migration

1. **Target**: a running redimos (default DB0 is fine — pika-migrate never sends
   `SELECT`; no `-multi-db` needed).
2. **Start the tool**, pointing it at the target:

   ```sh
   docker run -d --name pika-migrate --network <shared-net> \
     -e TARGET_REDIS_HOST=redimos -e TARGET_REDIS_PORT=6379 \
     pika-migrate:local
   ```

   Env vars (all optional): `TARGET_REDIS_HOST` (default `127.0.0.1`),
   `TARGET_REDIS_PORT` (`6379`), `TARGET_REDIS_PWD` (empty), `SYNC_BATCH_NUM`
   (`100`), `REDIS_SENDER_NUM` (`10`), `EXPIRE_LOGS_NUMS` (`10000`).

3. **Trigger** the migration — use the source Pika's **IP address**, not a
   hostname (the tool validates the snapshot's `master_ip` against this):

   ```sh
   redis-cli -h <pika-migrate-host> -p 9222 slaveof <SOURCE_PIKA_IP> 9221 force
   ```

   It runs a full DBSync (via `rsync`, port 10222) then continues as a slave
   streaming the incremental binlog. Watch progress with
   `redis-cli -p 9222 info replication` (`master_link_status:up`) and the
   container logs (`Retransmit Finish`).

## Gotchas (see the doc for detail)

- `slaveof` **by IP, not hostname**, or the snapshot fails the master-ip check.
- **One DBSync per process** — re-triggering a full sync means re-running the
  container (`docker rm -f pika-migrate && docker run ...`).
- Set `config set expire-logs-nums 10000` on the **source** too, so the binlog
  survives the full-sync window and incremental sync can attach.
- **Non-deterministic commands** (e.g. `SPOP`) are forwarded verbatim and can
  diverge on the target during incremental sync — this is a general
  command-replication limitation, not a redimos issue. Avoid them during
  migration, or re-do the affected key with a deterministic write to converge.
