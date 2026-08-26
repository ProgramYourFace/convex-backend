#!/bin/bash
# Boots a Convex backend on the given persistence backend, deploys the
# aa-app-shaped functions, drives the ingest load, and tears down.
set -u
BACKEND="$1"; shift
ROOT=${CONVEX_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}
BIN=$ROOT/target/release/convex-local-backend
WORK=${BENCH_WORK:-/tmp/aa-run}/$BACKEND
INSTANCE=carnitas
SECRET=4361726e697461732c206c69746572616c6c79206d65616e696e6720226c6974

rm -rf "$WORK" && mkdir -p "$WORK"

case "$BACKEND" in
  sqlite)   DB_ARGS=(--db sqlite   "$WORK/convex.sqlite3") ;;
  rocksdb)  DB_ARGS=(--db rocksdb  "$WORK/rocksdb") ;;
  postgres)
    psql "postgresql://convex@localhost:5433/postgres" -qc "DROP DATABASE IF EXISTS $INSTANCE" >/dev/null 2>&1
    psql "postgresql://convex@localhost:5433/postgres" -qc "CREATE DATABASE $INSTANCE" >/dev/null 2>&1
    DB_ARGS=(--db postgres-v5 "postgresql://convex@localhost:5433") ;;
  *) echo "unknown backend $BACKEND"; exit 1 ;;
esac

EXTRA_FLAGS=""
[ "$BACKEND" = "postgres" ] && EXTRA_FLAGS="--do-not-require-ssl"
ADMIN_KEY=$($ROOT/target/release/generate_key "$INSTANCE" "$SECRET" 2>/dev/null | tail -1)
export CONVEX_ADMIN_KEY="$ADMIN_KEY"

INSTANCE_NAME=$INSTANCE INSTANCE_SECRET=$SECRET DO_NOT_REQUIRE_SSL=true \
  DISABLE_BEACON=true RUST_LOG=${RUST_LOG:-error} \
  INDEX_CACHE_VERIFY_PERCENT=0 \
  "$BIN" "${DB_ARGS[@]}" \
    --instance-name "$INSTANCE" --instance-secret "$SECRET" $EXTRA_FLAGS \
    --port 3210 --site-proxy-port 3211 \
    --local-storage "$WORK/storage" \
    > "$WORK/backend.log" 2>&1 &
PID=$!

for i in $(seq 1 120); do
  curl -sf http://127.0.0.1:3210/version >/dev/null 2>&1 && break
  sleep 1
done
if ! curl -sf http://127.0.0.1:3210/version >/dev/null 2>&1; then
  echo "$BACKEND: backend did not come up"; tail -20 "$WORK/backend.log"; kill $PID 2>/dev/null; exit 1
fi
echo "$BACKEND: up, version $(curl -s http://127.0.0.1:3210/version)"

cd "$(dirname "${BASH_SOURCE[0]}")"
CONVEX_URL=http://127.0.0.1:3210 npx convex deploy --admin-key "$ADMIN_KEY" --url http://127.0.0.1:3210 -y >"$WORK/deploy.log" 2>&1
if [ $? -ne 0 ]; then echo "$BACKEND: deploy failed"; tail -20 "$WORK/deploy.log"; kill $PID 2>/dev/null; exit 1; fi
echo "$BACKEND: functions deployed"

CONVEX_URL=http://127.0.0.1:3210 CONVEX_ADMIN_KEY="$ADMIN_KEY" \
  EVENTS=${EVENTS:-4000} BATCH=${BATCH:-64} LANES=${LANES:-8} \
  DEVICES=${DEVICES:-512} MERGE_PERCENT=${MERGE_PERCENT:-30} READS=${READS:-1000} \
  node "$(dirname "${BASH_SOURCE[0]}")/drive.mjs"
RC=$?

kill $PID 2>/dev/null; wait $PID 2>/dev/null
exit $RC
