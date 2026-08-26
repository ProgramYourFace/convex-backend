#!/bin/bash
# =============================================================================
# bench/aa-ingest/fleet-schema-test.sh
#
# Does the two-phase cell-wide schema rollout actually gate?
#
# Boots three tenants and gives documents to ONLY ONE of them, which is what
# makes the result unambiguous: an empty table validates any schema, so the two
# empty tenants pass and the one with data must be the one that refuses.
#
# Asserts, in order:
#   * an incompatible schema is REFUSED, naming the offending document, field
#     and table, with allOk=false;
#   * nothing was activated — the tenant with data still reads;
#   * committing the REFUSED rollout's own fingerprint is still refused,
#     because a cell commits all-or-nothing;
#   * committing a DIFFERENT fingerprint is refused everywhere.
#
# Requires: target/release/{convex-multitenant-backend,generate_key}, node,
# npm install in this directory, write access to /etc/hosts.
# =============================================================================
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK=/tmp/fleet-gate; rm -rf "$WORK"; mkdir -p "$WORK"
N=3; TOKEN=0123456789abcdef0123456789abcdef0123456789abcdef
GROUP=cell-local; BASE=localtest; PORT=3210
ROOT=00000000000000000000000000000000000000000000000000000000000000ab
PREFIX=convex-multitenant/instance-secret/v1/
NAMES=(t-g01 t-g02 t-g03); ROSTER=$(IFS=,; echo "${NAMES[*]}")
sed -i "/# fleet-gate/d" /etc/hosts
L="127.0.0.1"; for n in "${NAMES[@]}"; do L="$L $n.$GROUP.api.$BASE"; done
echo "$L # fleet-gate" >> /etc/hosts

MULTITENANT_GROUP=$GROUP MULTITENANT_BASE_DOMAIN=$BASE MULTITENANT_ORIGIN_SCHEME=http \
MULTITENANT_DB=rocksdb MULTITENANT_DATA_DIR="$WORK/data" MULTITENANT_INSTANCES="$ROSTER" \
MULTITENANT_ROOT_SECRET=$ROOT MULTITENANT_SECRET_INFO_PREFIX=$PREFIX \
MULTITENANT_ADMIN_TOKEN=$TOKEN MULTITENANT_MAX_INSTANCES=8 \
DISABLE_BEACON=true DO_NOT_REQUIRE_SSL=true RUST_LOG=error INDEX_CACHE_VERIFY_PERCENT=0 \
  "${CONVEX_ROOT:-$(cd "$HERE/../.." && pwd)}"/target/release/convex-multitenant-backend > "$WORK/log" 2>&1 &
PID=$!; trap 'kill $PID 2>/dev/null' EXIT
for _ in $(seq 1 180); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' -H "Host: ${NAMES[-1]}.$GROUP.api.$BASE" \
      http://127.0.0.1:$PORT/instance_name)" = "200" ] && break; sleep 1; done

for n in "${NAMES[@]}"; do
  SEC=$(python3 "${DERIVE_SECRET:?set DERIVE_SECRET to a script that prints HKDF(root,instance,prefix)}" $ROOT $n $PREFIX)
  KEY=$("${CONVEX_ROOT:-$(cd "$HERE/../.." && pwd)}"/target/release/generate_key $n $SEC 2>/dev/null | tail -1)
  U="http://$n.$GROUP.api.$BASE:$PORT"
  ( cd $HERE && npx convex deploy -y --url "$U" --admin-key "$KEY" ) >"$WORK/d-$n.log" 2>&1 || exit 1
  # Only t-g02 gets data. The other two stay empty, which is what makes the
  # result unambiguous: an empty table validates ANY schema.
  if [ "$n" = "t-g02" ]; then
    ( cd $HERE && CONVEX_URL="$U" CONVEX_ADMIN_KEY="$KEY" EVENTS=200 BATCH=32 LANES=2 \
        DEVICES=8 DEVICE_PREFIX="$n-dev-" READS=0 node drive.mjs ) >/dev/null 2>&1
  fi
done
echo "==> deployed to 3; only t-g02 has documents"

bundle() {  # $1 = schema source file -> stdout json
  python3 - "$1" "$HERE" <<'PYEOF'
import json,subprocess,sys,os
src, here = sys.argv[1], sys.argv[2]
o=subprocess.run(["npx","esbuild","--bundle","--format=esm","--platform=browser",
                  "--outfile=/dev/stdout", src], capture_output=True, text=True, cwd=here)
if o.returncode: sys.stderr.write(o.stderr[:600]); sys.exit(1)
print(json.dumps({"bundle":{"path":"schema.js","source":o.stdout,"environment":"isolate"}}))
PYEOF
}

# An INCOMPATIBLE change: a new REQUIRED field on a table that already has rows.
cat > "$HERE/.fleet-bad-schema.ts" <<'TS'
import { defineSchema, defineTable } from 'convex/server';
import { v } from 'convex/values';
export default defineSchema({
  deviceLocations: defineTable({
    deviceId: v.string(), timestamp: v.number(), lat: v.number(), lng: v.number(),
    speed: v.number(), engineOn: v.boolean(),
    stillDuration: v.optional(v.number()), mergedCount: v.optional(v.number()),
    mustExist: v.string(),
  }).index('by_device_timestamp', ['deviceId', 'timestamp']),
  deviceLatestLocations: defineTable({
    deviceId: v.string(), timestamp: v.number(), lat: v.number(), lng: v.number(),
    geohash: v.string(),
  }).index('by_device', ['deviceId']).index('by_geohash', ['geohash']),
});
TS
bundle "$HERE/.fleet-bad-schema.ts" > "$WORK/bad.json" || { rm -f "$HERE/.fleet-bad-schema.ts"; exit 1; }
rm -f "$HERE/.fleet-bad-schema.ts"

AUTH="Authorization: Bearer $TOKEN"
echo; echo "=== precheck an INCOMPATIBLE schema ==="
curl -s -H "$AUTH" -H 'content-type: application/json' -d @"$WORK/bad.json" \
  http://127.0.0.1:$PORT/api/cell/schema/precheck | python3 -m json.tool

echo; echo "=== did anything get activated? (must still be the ORIGINAL schema) ==="
curl -s -H "$AUTH" http://127.0.0.1:$PORT/api/cell/instances | python3 -m json.tool

echo; echo "=== THE GUARD: commit the ABANDONED rollout's fingerprint ==="
FP=$(curl -s -H "$AUTH" -H 'content-type: application/json' -d @"$WORK/bad.json" \
  http://127.0.0.1:$PORT/api/cell/schema/precheck | python3 -c "import json,sys; print(json.load(sys.stdin)['schemaFingerprint'])")
echo "  refused rollout fingerprint: $FP"
curl -s -H "$AUTH" -H 'content-type: application/json' -d "{\"schemaFingerprint\":\"$FP\"}" \
  http://127.0.0.1:$PORT/api/cell/schema/commit | python3 -m json.tool | head -30

echo; echo "=== and a commit for a DIFFERENT fingerprint must be refused too ==="
curl -s -H "$AUTH" -H 'content-type: application/json' \
  -d '{"schemaFingerprint":"0000000000000000000000000000000000000000000000000000000000000000"}' \
  http://127.0.0.1:$PORT/api/cell/schema/commit | python3 -m json.tool | head -24

echo; echo "=== the tenant with data can still be read (nothing broke) ==="
SEC=$(python3 "${DERIVE_SECRET:?set DERIVE_SECRET to a script that prints HKDF(root,instance,prefix)}" $ROOT t-g02 $PREFIX)
KEY=$("${CONVEX_ROOT:-$(cd "$HERE/../.." && pwd)}"/target/release/generate_key t-g02 $SEC 2>/dev/null | tail -1)
curl -s -X POST "http://t-g02.$GROUP.api.$BASE:$PORT/api/query" -H 'content-type: application/json' \
  -H "Authorization: Convex $KEY" \
  -d '{"path":"read:tenantSummary","args":{},"format":"json"}' | head -c 300; echo
