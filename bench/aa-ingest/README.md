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

### Measured, one process, identical load per tenant

`EVENTS=400 DEVICES=16 LANES=2`, so the working set is small and the block cache
is nowhere near its cap. Ingest held ~1900–2000 events/s per tenant at
`EVENTS=1500 DEVICES=48`.

| tenants | RSS | threads | fds |
|---|---|---|---|
| 1 | 131 MiB | 37 | 22 |
| 4 | 221 MiB | 58 | 55 |
| 8 | 351 MiB | 79 | 99 |

**~100 MiB fixed plus ~31 MiB per tenant, and ~11 fds per tenant.** The
per-tenant term is the `Application` and its workers — committer, subscriptions,
retention, index-cache handle — not the block cache: a cache per open would add
its whole share again for every tenant (a quarter of the container limit each),
and RSS would be measured in GiB by tenant three. That it is not is the
process-wide `SHARED_MEMORY` singleton doing its job, observed rather than
argued.

Two caveats before this becomes a capacity model: the working set here is small
enough that the shared cache never approaches its cap, so this measures
per-instance fixed cost and not steady-state under load; and descriptors grow
linearly, which is why `MULTITENANT_MAX_INSTANCES` divides that budget rather
than trusting the default.
