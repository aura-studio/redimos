#!/bin/sh
# Render the pika-migrate config from environment variables, then start the tool.
#
# The tool listens on :9222 (RESP). After it is up, trigger the migration from a
# client, using the SOURCE Pika's IP ADDRESS (not a hostname — the tool validates
# the dbsync snapshot's master_ip against what you pass here):
#
#     redis-cli -p 9222 slaveof <SOURCE_PIKA_IP> 9221 force
#
# Only ONE DBSync is allowed per process, so re-triggering a full sync means
# re-running the container (a fresh `docker run` gives a clean process). See
# ../../doc/pika-migrate.md.
set -eu

CONF=/mig/pika.conf
cp /opt/pika-migrate/pika.conf.template "$CONF"

# --- target (the redimos / Redis-compatible destination data is written to) ---
sed -i "s#^target-redis-host.*#target-redis-host : ${TARGET_REDIS_HOST:-127.0.0.1}#" "$CONF"
sed -i "s#^target-redis-port.*#target-redis-port : ${TARGET_REDIS_PORT:-6379}#" "$CONF"
sed -i "s#^target-redis-pwd.*#target-redis-pwd : ${TARGET_REDIS_PWD:-}#" "$CONF"

# --- migration tuning ---
# sync-batch-num: elements packed per HMSET/SADD/ZADD/RPUSH (and scan batch size).
# redis-sender-num: concurrent sender threads (commands sharded by hash(key)).
sed -i "s#^sync-batch-num.*#sync-batch-num : ${SYNC_BATCH_NUM:-100}#" "$CONF"
sed -i "s#^redis-sender-num.*#redis-sender-num : ${REDIS_SENDER_NUM:-10}#" "$CONF"

# Keep enough binlog on the SOURCE so incremental sync can attach after full sync.
# (Set this on the SOURCE too: `redis-cli -h <src> -p 9221 config set expire-logs-nums 10000`.)
sed -i "s#^expire-logs-nums.*#expire-logs-nums : ${EXPIRE_LOGS_NUMS:-10000}#" "$CONF"

echo "pika-migrate: target=${TARGET_REDIS_HOST:-127.0.0.1}:${TARGET_REDIS_PORT:-6379}" \
     "sync-batch-num=${SYNC_BATCH_NUM:-100} redis-sender-num=${REDIS_SENDER_NUM:-10}"
echo "pika-migrate: listening on :9222 — trigger with:  redis-cli -p 9222 slaveof <SOURCE_PIKA_IP> 9221 force"

exec pika-migrate -c "$CONF"
