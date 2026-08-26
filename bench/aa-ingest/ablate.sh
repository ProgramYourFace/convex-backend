#!/bin/bash
# Runs the ingest load under one env configuration and reports throughput
# alongside what RocksDB says it was doing — compactions, stalls, flushes —
# so a slow run can be attributed rather than guessed at.
set -u
LABEL="$1"; shift
ROOT=${CONVEX_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}
BIN=$ROOT/target/release/convex-local-backend
WORK=/tmp/ablate/$LABEL
INSTANCE=carnitas
SECRET=4361726e697461732c206c69746572616c6c79206d65616e696e6720226c6974

rm -rf "$WORK" && mkdir -p "$WORK"
ADMIN_KEY=$($ROOT/target/release/generate_key "$INSTANCE" "$SECRET" 2>/dev/null | tail -1)

INSTANCE_NAME=$INSTANCE INSTANCE_SECRET=$SECRET DISABLE_BEACON=true RUST_LOG=error \
  INDEX_CACHE_VERIFY_PERCENT=0 env "$@" \
  "$BIN" --db rocksdb "$WORK/db" \
    --instance-name "$INSTANCE" --instance-secret "$SECRET" \
    --port 3210 --site-proxy-port 3211 --local-storage "$WORK/storage" \
    > "$WORK/backend.log" 2>&1 &
PID=$!
for i in $(seq 1 120); do curl -sf http://127.0.0.1:3210/version >/dev/null 2>&1 && break; sleep 1; done
if ! curl -sf http://127.0.0.1:3210/version >/dev/null 2>&1; then
  echo "$LABEL: did not come up"; tail -5 "$WORK/backend.log"; kill $PID 2>/dev/null; exit 1
fi

cd "$(dirname "${BASH_SOURCE[0]}")"
CONVEX_URL=http://127.0.0.1:3210 npx convex deploy --admin-key "$ADMIN_KEY" \
  --url http://127.0.0.1:3210 -y >"$WORK/deploy.log" 2>&1 || {
    echo "$LABEL: deploy failed"; tail -5 "$WORK/deploy.log"; kill $PID 2>/dev/null; exit 1; }

RESULT=$(CONVEX_URL=http://127.0.0.1:3210 CONVEX_ADMIN_KEY="$ADMIN_KEY" \
  EVENTS=${EVENTS:-40000} BATCH=${BATCH:-64} LANES=${LANES:-8} \
  DEVICES=${DEVICES:-512} MERGE_PERCENT=${MERGE_PERCENT:-30} READS=${READS:-1000} \
  node ./drive.mjs)

# RocksDB's own account of the run, before teardown.
LOG="$WORK/db/LOG"
count() { grep -c "$1" "$LOG" 2>/dev/null | head -1 | tr -dc '0-9'; }
# Markers from RocksDB's plain-text LOG. An earlier version counted
# "level0_slowdown", which only ever matched the *configuration* line printed at
# startup — six column families, six matches, zero information.
COMPACTIONS=$(count "Compacting .*@"); COMPACTIONS=${COMPACTIONS:-0}
FLUSHES=$(count "Flushing memtable"); FLUSHES=${FLUSHES:-0}
STALLS=$(count "Stalling writes"); STALLS=${STALLS:-0}
STOPS=$(count "Stopping writes"); STOPS=${STOPS:-0}
L0SLOW=$(count "Level-0 flush table"); L0SLOW=${L0SLOW:-0}
SIZE=$(du -sm "$WORK/db" 2>/dev/null | cut -f1)

kill $PID 2>/dev/null; wait $PID 2>/dev/null
# The database is 222 MiB at 200k events and the stats above are everything we
# want from it. Leaving them behind filled the disk mid-sweep and took the
# harness's own output with it.
KEEP=${ABLATE_KEEP:-0}
if [ "$KEEP" = "0" ]; then rm -rf "$WORK"; fi
echo "$LABEL $RESULT rocksdb={\"compactions\":$COMPACTIONS,\"flushes\":$FLUSHES,\"stalls\":$STALLS,\"stops\":$STOPS,\"l0_tables\":$L0SLOW,\"db_mb\":$SIZE}"
