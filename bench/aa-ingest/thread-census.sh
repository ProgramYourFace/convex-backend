#!/bin/bash
# =============================================================================
# bench/aa-ingest/thread-census.sh <N>
#
# Boot N instances IDLE and take a thread census. No ingest: this measures what
# merely HOSTING a tenant costs in threads, descriptors and memory — which is a
# different question from what SERVING one costs, and the one that decides
# whether a cell can hold hundreds of mostly-idle systems.
#
# Reports run states too. Threads asleep in S cost a stack and a scheduler
# table entry; they are not contention. The distinction is the whole point.
#
#   ./thread-census.sh 300
#
# Requires: target/release/convex-multitenant-backend.
# =============================================================================
set -u
N=$1
WORK=/tmp/census-$N
rm -rf "$WORK"; mkdir -p "$WORK"
NAMES=(); for i in $(seq 1 "$N"); do NAMES+=("t-c$(printf '%03d' "$i")"); done
ROSTER=$(IFS=,; echo "${NAMES[*]}")
MULTITENANT_GROUP=cell-local MULTITENANT_BASE_DOMAIN=localtest \
MULTITENANT_ORIGIN_SCHEME=http MULTITENANT_DB=rocksdb \
MULTITENANT_DATA_DIR="$WORK/data" MULTITENANT_INSTANCES="$ROSTER" \
MULTITENANT_ROOT_SECRET=00000000000000000000000000000000000000000000000000000000000000ab \
MULTITENANT_MAX_INSTANCES=512 DISABLE_BEACON=true DO_NOT_REQUIRE_SSL=true \
RUST_LOG=error INDEX_CACHE_VERIFY_PERCENT=0 \
  "${CONVEX_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}/target/release/convex-multitenant-backend" > "$WORK/log" 2>&1 &
PID=$!
for _ in $(seq 1 300); do
  [ "$(grep -c 'opened RocksDB at' "$WORK/log" 2>/dev/null)" -ge "$N" ] && break
  kill -0 $PID 2>/dev/null || { echo "N=$N EXITED"; tail -5 "$WORK/log"; exit 1; }
  sleep 1
done
sleep 5   # let it settle
TH=$(ls /proc/$PID/task | wc -l)
RSS=$(ps -o rss= -p $PID | tr -d ' ')
FDS=$(ls /proc/$PID/fd 2>/dev/null | wc -l)
# Thread run states: R=running, S=sleeping(parked), D=uninterruptible
STATES=$(for t in /proc/$PID/task/*/stat; do awk '{print $3}' "$t" 2>/dev/null; done | sort | uniq -c | tr '\n' ' ')
echo "N=$N threads=$TH rss=$((RSS/1024))MiB fds=$FDS states: $STATES"
if [ "$N" = "8" ] || [ "$N" = "1" ]; then
  echo "  --- thread names (N=$N) ---"
  for t in /proc/$PID/task/*/comm; do cat "$t" 2>/dev/null; done | sed 's/[0-9]\+$//' | sort | uniq -c | sort -rn | head -14
fi
kill $PID 2>/dev/null; wait $PID 2>/dev/null
