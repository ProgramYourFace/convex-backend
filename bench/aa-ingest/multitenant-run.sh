#!/bin/bash
# =============================================================================
# bench/aa-ingest/multitenant-run.sh — N tenant systems in ONE process, each on
# its own RocksDB store, each ingesting aa-app-shaped location data.
#
# This is the end-to-end test the crate's unit tests cannot be: it boots the
# real `convex-multitenant-backend`, routes by HOSTNAME through the real host
# resolver, deploys the aa-app-shaped functions to every instance separately,
# drives a disjoint device space into each, and then asks each tenant what it
# can see. Isolation here is a query that returns null, not an assertion about
# a data structure.
#
# Routing is by Host header, not by the x-aa-instance escape hatch, precisely
# because `convex deploy` cannot send a custom header — which makes the CLI an
# honest test of the path a browser actually takes.
#
#   TENANTS=3 EVENTS=2000 ./multitenant-run.sh
#
# Requires: target/release/{convex-multitenant-backend,generate_key}, node,
# npm install in this directory, and write access to /etc/hosts.
# =============================================================================
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT=${CONVEX_ROOT:-$(cd "$HERE/../.." && pwd)}
BIN=$ROOT/target/release/convex-multitenant-backend
KEYGEN=$ROOT/target/release/generate_key
DERIVE=${DERIVE_SECRET:-/tmp/claude-0/-home-user/e16f4260-48a1-50b9-9b12-ac752d94e83d/scratchpad/derive_secret.py}

GROUP=${GROUP:-cell-local}
BASE=${BASE:-localtest}
# Not configurable: API_PORT/SITE_PORT are compile-time constants in
# crates/multitenant_backend/src/instance.rs and the binary takes no argv at
# all — every setting arrives through the environment. In a pod that is fine
# (the containerPorts map them); here it just means we cannot move them.
PORT=3210
SITE_PORT=3211
WORK=${WORK:-/tmp/mt-run}
TENANTS=${TENANTS:-3}
EVENTS=${EVENTS:-2000}
DEVICES=${DEVICES:-64}
LANES=${LANES:-4}
BATCH=${BATCH:-64}

# 32 bytes. Every instance's secret is HKDF'd from this in-process; nothing is
# stored and nothing has to be written before an instance can serve.
ROOT_SECRET=${ROOT_SECRET:-00000000000000000000000000000000000000000000000000000000000000ab}
PREFIX=${PREFIX:-convex-multitenant/instance-secret/v1/}
# The default is x-convex-instance; aa-app's cell manifest sets x-aa-instance,
# so use that here and exercise the override at the same time.
INSTANCE_HEADER=${INSTANCE_HEADER:-x-aa-instance}

NAMES=(); for i in $(seq 1 "$TENANTS"); do NAMES+=("t-sys$(printf '%02d' "$i")"); done
ROSTER=$(IFS=,; echo "${NAMES[*]}")

say() { echo "==> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

[ -x "$BIN" ]    || fail "no $BIN — cargo build --release -p multitenant_backend --bin convex-multitenant-backend"
[ -x "$KEYGEN" ] || fail "no $KEYGEN — cargo build --release -p keybroker --bin generate_key"
[ -f "$DERIVE" ] || fail "no $DERIVE (HKDF helper)"
[ -d "$HERE/node_modules" ] || fail "run npm install in $HERE first"

rm -rf "$WORK" && mkdir -p "$WORK"

# --- hostnames -------------------------------------------------------------
# <instance>.<group>.api.<base> is what the resolver parses; the bare
# <group>.api.<base> resolves to the legacy instance, which we do not set.
say "mapping ${TENANTS} tenant hostnames to 127.0.0.1"
HOSTS_LINE="127.0.0.1 $GROUP.api.$BASE $GROUP.site.$BASE"
for n in "${NAMES[@]}"; do
  HOSTS_LINE="$HOSTS_LINE $n.$GROUP.api.$BASE $n.$GROUP.site.$BASE"
done
sed -i "/# multitenant-run/d" /etc/hosts 2>/dev/null
echo "$HOSTS_LINE # multitenant-run" >> /etc/hosts || fail "cannot write /etc/hosts"

# --- boot ------------------------------------------------------------------
say "starting the host: $TENANTS instances, rocksdb, one store each"
MULTITENANT_GROUP="$GROUP" \
MULTITENANT_BASE_DOMAIN="$BASE" \
MULTITENANT_ORIGIN_SCHEME=http \
MULTITENANT_DB=rocksdb \
MULTITENANT_DATA_DIR="$WORK/data" \
MULTITENANT_INSTANCES="$ROSTER" \
MULTITENANT_ROOT_SECRET="$ROOT_SECRET" \
MULTITENANT_SECRET_INFO_PREFIX="$PREFIX" \
MULTITENANT_INSTANCE_HEADER="$INSTANCE_HEADER" \
MULTITENANT_MAX_INSTANCES=12 \
DISABLE_BEACON=true \
DO_NOT_REQUIRE_SSL=true \
RUST_LOG=${RUST_LOG:-info} \
INDEX_CACHE_VERIFY_PERCENT=0 \
  "$BIN" > "$WORK/backend.log" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null' EXIT

for _ in $(seq 1 180); do
  curl -sf "http://127.0.0.1:$PORT/version" >/dev/null 2>&1 && break
  kill -0 $PID 2>/dev/null || { tail -30 "$WORK/backend.log"; fail "backend exited during boot"; }
  sleep 1
done
curl -sf "http://127.0.0.1:$PORT/version" >/dev/null 2>&1 \
  || { tail -30 "$WORK/backend.log"; fail "backend never became ready"; }
say "up, version $(curl -s "http://127.0.0.1:$PORT/version")"

# --- fail-closed routing, before any data exists ---------------------------
# NOT /version: that is a META route on ConvexHttpService, mounted ahead of the
# resolving middleware precisely so a readiness probe passes before any instance
# exists. It answers 200 for any Host, by design. /instance_name extracts
# MtState, so it is resolved per request and is what the routing rules govern.
say "checking the router fails closed"
probe() { curl -s -o /dev/null -w '%{http_code}' "$@" "http://127.0.0.1:$PORT/instance_name"; }
UNKNOWN=$(probe -H "Host: nosuch.$GROUP.api.$BASE")
[ "$UNKNOWN" = "404" ] || fail "an unknown instance returned $UNKNOWN, expected 404"
NOHOST=$(probe -H "Host: 127.0.0.1:$PORT")
[ "$NOHOST" = "404" ] || fail "an unresolvable Host returned $NOHOST, expected 404 (never a default tenant)"
# Deliberately a name that is NOT hosted: resolve() checks the conflict before
# the hosted-set lookup, so this exercises the rule at any tenant count —
# ${NAMES[1]} does not exist when TENANTS=1.
CONFLICT=$(probe -H "Host: ${NAMES[0]}.$GROUP.api.$BASE" -H "$INSTANCE_HEADER: nosuch")
[ "$CONFLICT" = "400" ] || fail "a Host/header conflict returned $CONFLICT, expected 400"
OK=$(probe -H "Host: ${NAMES[0]}.$GROUP.api.$BASE")
[ "$OK" = "200" ] || fail "a hosted instance returned $OK, expected 200"
RESOLVED=$(curl -s -H "Host: ${NAMES[0]}.$GROUP.api.$BASE" "http://127.0.0.1:$PORT/instance_name")
[ "$RESOLVED" = "${NAMES[0]}" ] || fail "Host for ${NAMES[0]} resolved to '$RESOLVED'"
say "  unknown -> 404, unresolvable Host -> 404, conflict -> 400, hosted -> 200 ($RESOLVED)"

# --- deploy + ingest, per tenant -------------------------------------------
declare -A KEYS
for n in "${NAMES[@]}"; do
  SEC=$(python3 "$DERIVE" "$ROOT_SECRET" "$n" "$PREFIX") || fail "derive failed for $n"
  KEY=$("$KEYGEN" "$n" "$SEC" 2>/dev/null | tail -1) || fail "generate_key failed for $n"
  KEYS[$n]=$KEY
  URL="http://$n.$GROUP.api.$BASE:$PORT"

  say "[$n] deploying the aa-app-shaped functions"
  ( cd "$HERE" && CONVEX_URL="$URL" npx convex deploy --admin-key "$KEY" --url "$URL" -y ) \
    > "$WORK/deploy-$n.log" 2>&1 || { tail -25 "$WORK/deploy-$n.log"; fail "[$n] deploy failed"; }

  say "[$n] ingesting $EVENTS location events over $DEVICES devices"
  ( cd "$HERE" && CONVEX_URL="$URL" CONVEX_ADMIN_KEY="$KEY" \
      EVENTS=$EVENTS BATCH=$BATCH LANES=$LANES DEVICES=$DEVICES \
      DEVICE_PREFIX="$n-dev-" MERGE_PERCENT=30 READS=200 \
      node drive.mjs ) 2>&1 | sed "s/^/    [$n] /" \
    || fail "[$n] ingest failed"
done

# --- what each tenant can see ----------------------------------------------
say "asking each tenant what it holds"
summary() {
  local n=$1
  curl -s -X POST "http://$n.$GROUP.api.$BASE:$PORT/api/query" \
    -H 'content-type: application/json' \
    -H "Authorization: Convex ${KEYS[$n]}" \
    -d '{"path":"read:tenantSummary","args":{},"format":"json"}'
}
FAILED=0
for n in "${NAMES[@]}"; do
  OUT=$(summary "$n")
  echo "    [$n] $OUT"
  echo "$OUT" | grep -q "\"$n-dev-\"" \
    || { echo "    [$n] MISSING its own device prefix"; FAILED=1; }
  for other in "${NAMES[@]}"; do
    [ "$other" = "$n" ] && continue
    if echo "$OUT" | grep -q "$other-dev-"; then
      echo "    [$n] LEAK: can see $other's devices"; FAILED=1
    fi
  done
done

# The sharper form of the same question: ask tenant A directly for a device
# that only tenant B ingested. A working split answers null.
say "cross-tenant lookup must return null"
for n in "${NAMES[@]}"; do
  for other in "${NAMES[@]}"; do
    [ "$other" = "$n" ] && continue
    RES=$(curl -s -X POST "http://$n.$GROUP.api.$BASE:$PORT/api/query" \
      -H 'content-type: application/json' -H "Authorization: Convex ${KEYS[$n]}" \
      -d "{\"path\":\"read:latestForDevice\",\"args\":{\"deviceId\":\"$other-dev-1\"},\"format\":\"json\"}")
    if echo "$RES" | grep -q "$other-dev-1"; then
      echo "    LEAK: $n resolved $other's device: $RES"; FAILED=1
    fi
  done
done
[ $FAILED -eq 0 ] && say "  no tenant can see another's devices"

# --- on-disk shape ----------------------------------------------------------
say "on-disk layout"
for n in "${NAMES[@]}"; do
  D="$WORK/data/instances/$n"
  [ -d "$D/db" ] || { echo "    [$n] MISSING $D/db"; FAILED=1; }
  echo "    [$n] $(du -sh "$D/db" 2>/dev/null | cut -f1) db, $(ls "$D/db" | wc -l) files"
done

# --- one process, one block cache ------------------------------------------
say "process shape"
echo "    RSS $(ps -o rss= -p $PID | awk '{printf "%.0f MiB", $1/1024}'), threads $(ls /proc/$PID/task | wc -l), fds $(ls /proc/$PID/fd 2>/dev/null | wc -l)"
echo "    stores opened: $(grep -c 'instance .* opened RocksDB at' "$WORK/backend.log") (rocksdb_persistence logs its own line too, so match the instance one)"

[ $FAILED -eq 0 ] || fail "isolation checks failed"
say "PASS — $TENANTS tenants, one process, one store each, no cross-tenant read"
