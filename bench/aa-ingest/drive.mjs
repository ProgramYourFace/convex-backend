// Drives the aa-app-shaped ingest mutation over HTTP against a running Convex
// backend, the way the relay drives a cell: N partitions applying batches
// concurrently, each batch a single mutation containing many events.
//
// This measures the whole stack — HTTP, the V8 isolate, the committer, the
// index cache, retention and persistence — which is what the storage-only
// benchmarks in this branch did not.
//
// Partitioning: each lane owns a DISJOINT slice of the device space, which is
// how a device-keyed RedPanda partition actually delivers — every event for a
// device lands on the same partition, always. An earlier version of this
// driver had lanes pull from a shared batch queue, which let two lanes touch
// the same device's `deviceLatestLocations` row and fail the mutation with an
// OCC conflict. That was an artifact of the driver, not of the backends.

const url = process.env.CONVEX_URL ?? 'http://127.0.0.1:3210';
const adminKey = process.env.CONVEX_ADMIN_KEY;
const events = Number(process.env.EVENTS ?? 4000);
const batch = Number(process.env.BATCH ?? 64);
const lanes = Number(process.env.LANES ?? 8);
const devices = Number(process.env.DEVICES ?? 512);
const mergePercent = Number(process.env.MERGE_PERCENT ?? 30);
const reads = Number(process.env.READS ?? 1000);

async function post(endpoint, path, args) {
  const res = await fetch(`${url}/api/${endpoint}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Convex ${adminKey}`,
    },
    body: JSON.stringify({ path, args, format: 'json' }),
  });
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
  const body = await res.json();
  if (body.status !== 'success') {
    throw new Error(body.errorMessage ?? JSON.stringify(body));
  }
  return body.value;
}

// Event `k` within a lane that owns devices [dStart, dStart+dPer).
function makeEvent(lane, dStart, dPer, k) {
  const device = dStart + (k % dPer);
  return {
    deviceId: `dev-${device}`,
    // Successive events for one device step forward in time, so the
    // run-length-encoding neighbour reads see a real time series.
    timestamp: 1_700_000_000_000 + Math.floor(k / dPer) * 1000,
    lat: 37 + (device % 100) / 1000,
    lng: -122 + (device % 100) / 1000,
    speed: k % 7,
    engineOn: k % 3 !== 0,
    // Decided by the caller so the merge ratio is a property of the load, not
    // of floating-point drift maths that would differ between runs.
    merge: k % 100 < mergePercent,
  };
}

const percentile = (sorted, p) =>
  sorted.length ? sorted[Math.min(sorted.length - 1, Math.round((sorted.length - 1) * p))] : 0;

async function main() {
  const dPer = Math.floor(devices / lanes);
  if (dPer < 1) throw new Error('DEVICES must be >= LANES');
  const perLane = Math.floor(events / lanes);

  const latencies = [];
  let inserted = 0;
  let merged = 0;
  const started = Date.now();

  await Promise.all(
    Array.from({ length: lanes }, async (_, lane) => {
      const dStart = lane * dPer;
      for (let k = 0; k < perLane; k += batch) {
        const count = Math.min(batch, perLane - k);
        const evs = Array.from({ length: count }, (_, j) =>
          makeEvent(lane, dStart, dPer, k + j)
        );
        const t0 = performance.now();
        const r = await post('mutation', 'ingest:ingestBatch', { events: evs });
        latencies.push(performance.now() - t0);
        inserted += r.inserted;
        merged += r.merged;
      }
    })
  );
  const writeSeconds = (Date.now() - started) / 1000;
  const applied = inserted + merged;

  const readStart = Date.now();
  let readCount = 0;
  await Promise.all(
    Array.from({ length: lanes }, async (_, lane) => {
      for (let i = lane; i < reads; i += lanes) {
        await post('query', 'read:latestForDevice', { deviceId: `dev-${i % devices}` });
        readCount++;
      }
    })
  );
  const readSeconds = (Date.now() - readStart) / 1000;

  latencies.sort((a, b) => a - b);
  console.log(
    JSON.stringify({
      applied,
      batch,
      lanes,
      inserted,
      merged,
      eventsPerSecond: +(applied / writeSeconds).toFixed(0),
      mutationsPerSecond: +(latencies.length / writeSeconds).toFixed(1),
      p50Ms: +percentile(latencies, 0.5).toFixed(2),
      p99Ms: +percentile(latencies, 0.99).toFixed(2),
      readsPerSecond: +(readCount / readSeconds).toFixed(0),
    })
  );
}

main().catch((e) => {
  console.error(String(e));
  process.exit(1);
});
