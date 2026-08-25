# kvbench

Compares candidate storage engines on the **exact read and write shapes** that
`convex-backend`'s `Persistence` / `PersistenceReader` traits demand.

Published benchmarks for embedded stores measure `fillrandom` — one key per write, no
secondary structures, no batching, no MVCC. That is not what Convex asks a store to do, so
those numbers do not transfer. This harness models the real thing, so a storage-engine
decision can be made against a measurement taken on the hardware that will run it.

Standalone crate: it lives outside the workspace (`members = ["crates/*"]`), has its own
lockfile, and never affects a `convex-backend` build.

See [`docs/proposals/002-storage-engine.md`](../../docs/proposals/002-storage-engine.md)
for the design this measures.

## What it models

**Write shape.** From `IndexRegistry::index_updates`
(`crates/indexing/src/index_registry.rs:189-222`) plus the Postgres DDL
(`crates/postgres/src/sql.rs:56-160`), one Convex document write produces one `documents`
row and one `indexes` row per index on the table — `by_id` and `by_creation_time` always,
plus every user index. Postgres additionally maintains two secondary indexes on
`documents`. With three indexes that is **six physical keys per document write**, which is
what the harness writes.

Writes are grouped into atomic batches of `--batch` documents: one batch is one
`Persistence::write` call, matching `COMMITTER_MAX_WRITE_BATCH_DOCUMENTS`.

**Key encodings.** Convex does MVCC in its data model, not in the engine — every
`PersistenceReader` method takes an explicit `read_timestamp`. So the store needs only
ordered keys, range scans, point gets and atomic batches. Encoding the timestamp
descending (`!ts == u64::MAX - ts`) inside the key turns "newest version at or before
`read_ts`" into a single forward seek:

```
doc  : table_id[16] ‖ id[16]        ‖ !ts[8]   -> document bytes
dlog : ts[8]        ‖ table_id[16]  ‖ id[16]   -> ()
dtab : table_id[16] ‖ ts[8]         ‖ id[16]   -> ()
idx  : index_id[16] ‖ index_key[..] ‖ !ts[8]   -> tag[1] ‖ table_id[16] ‖ id[16]
```

**Read shapes.** Two, both taken from real query text:

- `index_scan` — for each distinct key in a range, the newest version at or before
  `read_ts`, skipping tombstones (`crates/sqlite/src/lib.rs:157-177`). The harness scans
  one device's slice of a `(device, ts)` user index, the shape a
  `withIndex(q => q.eq('deviceId', d))` query produces.
- `previous_revisions` — given `(table_id, id, ts)`, the newest revision strictly before
  `ts` (`crates/postgres/src/sql.rs:930+`). This one is on the commit path:
  `check_generated_ids` (`crates/database/src/committer.rs:1566-1600`) runs it for every
  newly generated document ID on every commit.

**SQLite is the control.** It runs Convex's own DDL from `crates/sqlite/src/lib.rs:624+`
— the same primary keys and secondary indexes Convex asks a relational engine for — in the
same process, on the same disk, with no network. The gap between it and the KV engines is
therefore the *storage model alone*, with the client/server boundary factored out. It is
not a stand-in for Postgres, which additionally pays six SQL statements and a socket per
write.

## Running

```sh
# pure-Rust engines only — no C++ toolchain needed
cargo run --release -- --docs 500000

# add RocksDB (needs a C++ toolchain and libclang)
LIBCLANG_PATH=/usr/lib/llvm-18/lib cargo run --release --features rocksdb -- --docs 500000
```

If `clang-sys` cannot find libclang, it wants a file literally named `libclang.so`:

```sh
mkdir -p /tmp/libclang && ln -sf /usr/lib/llvm-18/lib/libclang.so.1 /tmp/libclang/libclang.so
export LIBCLANG_PATH=/tmp/libclang
```

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--docs N` | 200000 | documents to write |
| `--batch N` | 64 | documents per atomic write (`COMMITTER_MAX_WRITE_BATCH_DOCUMENTS`) |
| `--indexes N` | 3 | indexes on the table (`by_id` + `by_creation_time` + one user index) |
| `--value-bytes N` | 400 | document size |
| `--scans N` | 2000 | `index_scan` iterations |
| `--point-gets N` | 20000 | `previous_revisions` iterations |
| `--durable` | on | fsync every batch |
| `--relaxed` | | no fsync — what Postgres with `synchronous_commit=off` is doing |
| `--engines a,b,c` | all built | subset of `fjall,rocksdb,redb,sqlite` |
| `--dir PATH` | `/tmp/kvbench` | scratch directory |

**Run it on the storage class the cell actually uses**, not on a laptop SSD. On
network-attached block storage fsync latency dominates, and that is precisely the axis
these engines differ on. Run `--relaxed` and `--durable` both: the first is the
like-for-like comparison against production today, the second shows what durability would
cost if you wanted it back.

## Engine configuration

Recorded so the numbers can be judged, and so nobody has to read the source to find out
what was tuned:

| Engine | Configuration |
|---|---|
| RocksDB | LZ4 on data blocks at every level, `increase_parallelism(4)`, one column family per keyspace |
| fjall | Defaults — which already means worker threads scaled to available cores, and LZ4 on data blocks from L2 down |
| redb | Defaults |
| SQLite | `journal_mode=WAL`; `synchronous=FULL` under `--durable`, `OFF` under `--relaxed` |

Nobody has spent real effort tuning any of them. Treat a gap between the two LSMs as
un-tuned rather than as a verdict.

## Known gaps, deliberately

- **Forward index scans only.** Descending scans are more expensive under this key
  encoding — a reverse iterator meets each key's versions oldest-first and must read the
  whole retained run. See §5.2 of the proposal. If `.order('desc')` dominates your query
  mix, extend the harness before trusting the scan column.
- **Layout A.** Document bytes live in `doc` (keyed by id). The proposal recommends layout
  B — bytes in `dlog` (keyed by ts), mirroring the Postgres heap — which changes the read
  numbers but not the write numbers.
- **Index keys are unique, so version shadowing is barely exercised.** The user index is
  keyed `(device, ts, id)` — an append-only telemetry index, where every event is its own
  key with exactly one version. A real app also has upsert-shaped indexes (a per-device
  "latest location" row) where one key accumulates many versions inside the retention
  window, and that is where the MVCC walk actually costs something. Add an index keyed on
  `device` alone to measure it.
- **No `ConflictStrategy::Error` cost.** A B-tree gets uniqueness free from its primary
  key; an LSM needs a point get per key to detect a collision. See §5.1. The harness does
  not charge the KV engines for it.
- **Single-threaded.** Convex issues up to `COMMITTER_MAX_CONCURRENT_WRITE_BATCHES` (16)
  concurrent `Persistence::write` calls, and RocksDB's write-group mechanism coalesces
  concurrent writers into one WAL fsync. Single-threaded numbers therefore *understate*
  RocksDB under real concurrency.

Each is a small extension. Add the ones that matter for your decision rather than trusting
the defaults.
