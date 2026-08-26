# aa-app-shaped ingest benchmark

Drives a real `convex-local-backend` over HTTP with a batched, device-keyed
IoT ingest load, so the whole stack is measured — HTTP, the V8 isolate, the
committer, the index cache, retention and persistence. The storage-only
benchmarks elsewhere in this branch (`persistence-bench`) measure the
`Persistence` trait in isolation; this one does not, which is why its ratios
are smaller and more representative.

## Shape of the load

`convex/ingest.ts` mirrors aa-app's `insertNewLocationPayload`: per event, two
run-length-encoding neighbour reads on `by_device_timestamp`, then either
extending the previous cluster in place or inserting a new row, then upserting
the device's latest-state row. Three indexed reads and one to two writes per
event, applied sequentially within a batch because events for one device are
order-dependent.

`drive.mjs` runs `LANES` concurrent appliers. **Each lane owns a disjoint slice
of the device space**, which is how a device-keyed partitioned bus actually
delivers: every event for a device always lands on the same partition. This is
load-bearing. An earlier version had lanes pull from a shared batch queue,
which let two lanes touch the same device's `deviceLatestLocations` row and
fail the mutation with `OptimisticConcurrencyControlFailure` after exhausting
its retries. That killed 4 of 9 runs and, in the runs that survived, silently
charged the slower backends more retry cost — inflating the apparent spread
between engines. If you change the partitioning, re-check that
`applied == EVENTS` and that no run reports an OCC error.

## Running

Requires `target/release/convex-local-backend` and `target/release/generate_key`:

    CONVEX_PREBUILT_JS=1 cargo build --release -p local_backend

(`CONVEX_PREBUILT_JS=1` skips the build script's own `pnpm install`; build the
JS first with `pnpm install` + `turbo run build` under `npm-packages/`.)

Then, per backend:

    ./run-backend.sh sqlite
    ./run-backend.sh rocksdb
    ./run-backend.sh postgres      # needs a server on :5433

Knobs: `EVENTS`, `BATCH`, `LANES`, `DEVICES`, `MERGE_PERCENT`, `READS`,
`RUST_LOG`, `CONVEX_ROOT`, `BENCH_WORK`.

Note that `--do-not-require-ssl` is a bare clap flag with no environment
binding; setting `DO_NOT_REQUIRE_SSL` in the environment does nothing and
Postgres will fail its TLS handshake at boot.

## Results

4 cores, Postgres colocated, `BATCH=64 LANES=8 DEVICES=512 MERGE_PERCENT=30`.
Every run applied identical work; medians over n=10 at 4k events.

| Backend  | 4k ev/s | 40k ev/s | 40k p50 | 40k p99 |
|----------|---------|----------|---------|---------|
| RocksDB  | 1976    | 1528     | 293 ms  | 590 ms  |
| Postgres | 1374    | 923      | 576 ms  | 757 ms  |
| SQLite   | 1308    | 753      | 691 ms  | 1078 ms |

RocksDB leads Postgres by 1.44x at 4k events and 1.66x at 40k: the advantage
grows as the tables fill, which is the regime a production cell actually runs
in. RocksDB's throughput spread is the widest of the three (stdev 174 vs
Postgres's 44 over n=10), so Postgres remains the more predictable engine.

Reads are a wash across all three (881-1034/s) — they are served from Convex's
index cache, so storage barely participates. The case for this work rests on
the write path.

## Multi-tenant run

`multitenant-run.sh` boots `convex-multitenant-backend` with N tenant systems in
ONE process, each on its own RocksDB store, deploys these same functions to
every instance separately, and drives a disjoint device space into each. It is
the end-to-end counterpart to the crate's unit tests: routing goes through the
real host resolver by `Host` header, because `convex deploy` cannot send a
custom one — which makes the CLI an honest test of the path a browser takes.

```
TENANTS=3 EVENTS=1500 DEVICES=48 ./multitenant-run.sh
```

It asserts, in order: an unknown instance is 404, an unresolvable `Host` is 404
(never a default tenant), a `Host`/header conflict is 400, a hosted instance is
200 and resolves to its own name; then per tenant, that it holds only its own
devices and that asking it for a neighbour's device id returns null.

`/version` is deliberately NOT used for the routing checks — it is a meta route
on `ConvexHttpService`, mounted ahead of the resolving middleware so a readiness
probe passes before any instance exists, and it answers 200 for any `Host`.

### After a light ingest, sequential

Sequential runs at `EVENTS=400 DEVICES=16 LANES=2` — the tenants have written
and then gone quiet, so this is hosting cost plus a resident working set.

| tenants | RSS | threads | fds |
|---|---|---|---|
| 1 | 131 MiB | 37 | 22 |
| 4 | 221 MiB | 58 | 55 |
| 8 | 351 MiB | 79 | 99 |

~100 MiB fixed plus **~31 MiB per tenant**. Compare the three conditions, which
differ by an order of magnitude and are all real:

| condition | per tenant |
|---|---|
| idle, never written (`thread-census.sh`) | ~1.4 MiB |
| quiet after a light ingest (above) | ~31 MiB |
| all tenants writing at once (`CONCURRENT=1`) | ~70 MiB |

The spread is the working set — memtables, in-flight batches and cached blocks
that exist only around a write. Size a cell against the busy number.

None of the three is the block cache: a cache per open would add a quarter of
the container limit again for every tenant, and RSS would be in GiB by tenant
three. That it is not is the process-wide `SHARED_MEMORY` singleton doing its
job, observed rather than argued.

### Concurrent: every tenant busy at once

`CONCURRENT=1` drives all tenants simultaneously instead of one after another.
Sequential runs say nothing about contention, and contention is the question a
cell has to answer.

```
CONCURRENT=1 TENANTS=8 EVENTS=3000 DEVICES=64 LANES=4 ./multitenant-run.sh
```

4 cores, 3,000 events per tenant:

| tenants | cell ev/s | per tenant | p50 | p99 | peak RSS | bytes/event |
|---|---|---|---|---|---|---|
| 1 | 1,531 | 1,531 | 138 ms | 248 ms | 161 MiB | 811 |
| 2 | 1,934 | 967 | 255 ms | 368 ms | 270 MiB | 807 |
| 4 | 1,920 | 480 | 498 ms | 953 ms | 407 MiB | 808 |
| 8 | 2,104 | 263 | 919 ms | 1,274 ms | 657 MiB | 810 |

Three results, and they point in different directions.

**The cell saturates at ~1,900–2,100 ev/s and stays there.** Eight times the
tenants buys 1.37x the total work; each tenant's share falls as 1/N. That is a
CPU ceiling — four cores, one shared isolate pool — not a storage one. Adding
tenants divides a cell's throughput rather than multiplying it.

**Commit latency grows ~1.9x per doubling** (138 → 255 → 498 → 919 ms). This is
NOT committer contention: each instance has its own committer and its own OCC
domain. It is queuing for the shared pool that feeds them.

**Bytes per event never moves** — 807 to 811 across the whole range, half a
percent. Each store is an independent LSM tree with no shared pages, no shared
free-space map and no shared vacuum queue, so nothing amplifies across tenants.
A cell's disk is the sum of its tenants and nothing else.

Both compute numbers are worst-case: all tenants at full tilt simultaneously.
A real cell's tenants are mostly idle, which is the premise of packing them.

## Hosting cost — `thread-census.sh`

What N idle instances cost before any request arrives.

```
./thread-census.sh 300
```

| tenants | threads | RSS | fds | run states |
|---|---|---|---|---|
| 1 | 23 | 77 MiB | 22 | 19 S + 4 V8 |
| 8 | 51 | 93 MiB | 99 | 47 S + 4 |
| 24 | 85 | 114 MiB | 275 | 81 S + 4 |
| 64 | 174 | 176 MiB | 715 | 170 S + 4 |
| **300** | **655** | **505 MiB** | **3,342** | **651 S + 4** |

Steady from 8 upward: **~2.0 threads, ~1.4 MiB, ~11 fds per tenant.**

**No thread is per-tenant.** The census at N=1 is entirely process-wide
machinery — 6 tokio workers, 6 bounded-pool, 4 V8, 4 RocksDB compaction, plus
singletons — and every one of those pools lives in `SharedResources`, cloned
into each instance. Per-instance work is a tokio *task*. The ~2/tenant growth
is bounded pools filling lazily toward their caps, not a thread per tenant, and
at 300 tenants 651 of 655 threads are asleep. Parked threads cost a stack and a
scheduler entry; they are not contention.

**What binds at that scale is the memtable floor, not threads.** The per-family
target is `WRITE_BUFFER_BYTES / (max_instances x 5)` clamped to 4–64 MiB, and
the derived block cache clamps at 4 GiB regardless of host memory — so at 300
instances the floor demands 300 x 5 x 4 MiB = 6 GiB against a 1 GiB budget,
5.9x oversubscribed, and the write-buffer manager force-flushes continuously.
Setting `ROCKSDB_BLOCK_CACHE_BYTES` explicitly bypasses the derive clamp; 24 GiB
brings 300 instances to exactly 1.00x. That number is arithmetic from the clamp
constants — no run here has pushed the cache to its cap.
