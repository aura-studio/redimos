# syntax=docker/dockerfile:1

# =============================================================================
# pika-migrate build image
#
# pika-migrate is the online Pika -> Redis(-compatible) migration tool: a
# modified Pika 3.2 binary (Qihoo360/pika branch `v3_2_7_migrate`) that disguises
# itself as a *slave* of a source Pika (Pika's private replication, NOT Redis
# SYNC/PSYNC) and forwards the data to a *target* as plain high-level Redis write
# commands (SET/HMSET/SADD/ZADD/RPUSH/EXPIRE + PING/AUTH handshake — never SELECT/
# RESTORE/SYNC). This image builds it so it can migrate a real Pika into redimos.
#
# See ../../doc/pika-migrate.md for the mechanism, the verified end-to-end test
# results (real Pika v3.2.2 -> redimos, 523 keys, 0 mismatches), and operational
# gotchas.
#
# The Pika 3.2 base image supplies the exact 2019-era toolchain (gcc 4.8, git,
# make, rsync) AND the runtime shared libraries the binary links against, so no
# package installation is needed — only GitHub access to clone the source.
#
# Build (GitHub may need a proxy on your network; omit the args in open ones):
#   docker build -t pika-migrate:local \
#     --build-arg HTTPS_PROXY=http://host.docker.internal:7897 \
#     --build-arg HTTP_PROXY=http://host.docker.internal:7897 \
#     deploy/pika-migrate
#
# Run (point it at your redimos target, then trigger the migration BY IP):
#   docker run -d --name pika-migrate --network <net> \
#     -e TARGET_REDIS_HOST=redimos -e TARGET_REDIS_PORT=6379 pika-migrate:local
#   redis-cli -p 9222 slaveof <SOURCE_PIKA_IP> 9221 force   # IP, not hostname!
# =============================================================================

ARG PIKA_IMAGE=pikadb/pika:v3.2.2

# ----------------------------- build stage ----------------------------------
FROM ${PIKA_IMAGE} AS build

ARG PIKA_MIGRATE_REF=v3_2_7_migrate
ARG PIKA_MIGRATE_REPO=https://github.com/Qihoo360/pika.git
# Proxy passthrough for the GitHub clone + submodule fetch (empty in open networks).
ARG HTTP_PROXY=
ARG HTTPS_PROXY=
ENV http_proxy=${HTTP_PROXY} https_proxy=${HTTPS_PROXY}

WORKDIR /build
# NOTE: the base image's 2019-era git does NOT support `--depth` on
# `git submodule update` (only on clone), so the submodules are fetched full.
# The Makefile also links with -static-libstdc++, but this base ships no 64-bit
# libstdc++.a (only the runtime .so.6) — link libstdc++ dynamically instead (the
# final stage uses the same base, so the .so.6 is present at run time; without
# this the final link fails "cannot find -lstdc++").
RUN set -eux; \
    git clone --depth 1 -b "${PIKA_MIGRATE_REF}" "${PIKA_MIGRATE_REPO}" pika; \
    cd pika; \
    git submodule update --init --recursive; \
    sed -i 's/-static-libstdc++//g' Makefile; \
    make -j"$(nproc)"; \
    test -f output/bin/pika

# ----------------------------- final stage ----------------------------------
# Same base as the build stage on purpose: it guarantees the runtime shared
# libraries (libstdc++, libprotobuf, libsnappy, libgflags, libz, ...) and the
# rsync binary match the compiled tool's ABI, so it runs without extra packages.
FROM ${PIKA_IMAGE} AS final

LABEL org.opencontainers.image.title="pika-migrate" \
      org.opencontainers.image.description="Pika 3.2 online migration tool (v3_2_7_migrate) — migrates a source Pika into redimos" \
      org.opencontainers.image.source="https://github.com/Qihoo360/pika/tree/v3_2_7_migrate"

COPY --from=build /build/pika/output/bin/pika /usr/local/bin/pika-migrate
COPY --from=build /build/pika/conf/pika.conf  /opt/pika-migrate/pika.conf.template
COPY entrypoint.sh                            /usr/local/bin/pika-migrate-entrypoint
RUN chmod +x /usr/local/bin/pika-migrate-entrypoint

WORKDIR /mig
# 9222: the tool's own RESP port — send `slaveof <src-ip> <src-port> force` here.
# 10222 (= port + 1000): its rsync service that receives the source full-sync snapshot.
EXPOSE 9222 10222

ENTRYPOINT ["/usr/local/bin/pika-migrate-entrypoint"]
