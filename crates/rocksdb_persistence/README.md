# rocksdb_persistence

An embedded RocksDB implementation of `Persistence` / `PersistenceReader`.

Selected with `--db rocksdb`, where the positional database spec is a filesystem
path rather than a connection URL:

```sh
convex-local-backend --db rocksdb /convex/data/db
```

Nothing above the trait changes. The committer, index cache, retention,
subscriptions, streaming export and the search and vector indexes are untouched,
and the Convex developer API is identical — this is a storage engine swap, not a
feature.

## Why

Convex's storage schema is already an append-only, multi-version log: a document
update writes a new `(id, ts)` revision plus a new `(index_id, key, ts)` index
entry, and never modifies a row in place. The relational backends store that log
on top of B-tree pages that *are* updated in place — paying leaf lookups, page
splits, checkpoint full-page writes and vacuum for a workload that never
overwrites anything, in a separate process, across a socket. An LSM tree's
on-disk model is that log.

Convex also asks very little of a storage engine. Every read through
`PersistenceReader` is explicitly timestamp-scoped — `index_scan` takes a
`read_timestamp`, `load_documents` a `TimestampRange`, `previous_revisions` a set
of `(id, ts)` pairs — so Convex implements MVCC in its own data model and needs
none from the engine. What is left is ordered keys, range scans, point gets and
atomic durable batches.

See [`docs/proposals/002-storage-engine.md`](../../docs/proposals/002-storage-engine.md)
for the analysis, and [`tools/kvbench`](../../tools/kvbench) for a harness that
measures candidate engines on these exact shapes.

## Layout

Five column families. `!ts` is `u64::MAX - ts` big-endian, so a key's versions
sort newest-first.

```
dlog    ts[8] ‖ tablet[16] ‖ id[16]            -> encoded document
docs    tablet[16] ‖ id[16] ‖ !ts[8]           -> ()
dtab    tablet[16] ‖ ts[8] ‖ id[16]            -> ()
idx     index_id[16] ‖ esc(index_key) ‖ !ts[8] -> deleted flag + document pointer
globals key                                    -> JSON
```

This mirrors the Postgres schema: `dlog` is the `documents` heap under its
primary key `(ts, table_id, id)`, and `docs` / `dtab` are its two secondary
indexes. Document bodies live in `dlog` so that `index_scan`'s join — which
already knows `(ts, tablet, id)` from the index entry — is a single point get,
and so that the timestamp-ordered scans behind `load_documents` and streaming
export read their values sequentially.

Two consequences of the descending timestamp:

- **`index_scan` is a seek, not a sort.** "The newest version at or before
  `read_ts`" is `seek(key ‖ !read_ts)`; the relational backends need
  `DISTINCT ON` plus a sort for the same answer.
- **Retention is a contiguous range.** "Delete every version at or before `ts`"
  is a suffix of the key's run.

Document bodies are stored as the same JSON the Postgres and SQLite backends
write, so a database can move between backends without a re-encoding step.

### Why index keys are escaped

`idx` concatenates a variable-length index key with a fixed-length timestamp, and
naive concatenation does not preserve order: with raw bytes, `[1,2] ‖ !ts` sorts
*after* `[1,2,3] ‖ !ts`, because `0xFF… > 0x03`, while `[1,2] < [1,2,3]`. Convex
index keys are not guaranteed prefix-free, so the variable-length component is
escaped into a self-terminating, order-preserving form first (`0x00` → `0x00
0xFF`, terminated by `0x00 0x00`). `keys.rs` proves the property with a proptest.

## Knobs

Ordinary environment variables, read through `cmd_util::env::env_config`.

| Knob | Default | Meaning |
|---|---|---|
| `ROCKSDB_SYNC_WRITES` | `true` | fsync the WAL before `write` returns |
| `ROCKSDB_CHECK_CONFLICTS` | `true` | enforce `ConflictStrategy::Error` |
| `ROCKSDB_BLOCK_CACHE_BYTES` | 512 MiB | the whole memory budget: cached data, index and filter blocks *and* memtable charge all come out of it |
| `ROCKSDB_WRITE_BUFFER_BYTES` | ¼ of the cache | ceiling on memtable memory across all column families — a share of the cache, not memory on top of it |
| `ROCKSDB_MEMTABLE_BYTES` | 64 MiB | per-column-family memtable |
| `ROCKSDB_BACKGROUND_JOBS` | cores | flush and compaction threads |
| `ROCKSDB_BLOB_THRESHOLD_BYTES` | 4096 | document size above which bodies move to blob files; `0` disables |
| `ROCKSDB_SCAN_PAGE_ROWS` | 1024 | rows per page in the streaming read paths |
| `ROCKSDB_SHUTDOWN_TIMEOUT_SECONDS` | 30 | how long `shutdown` waits for compactions |

`ROCKSDB_SYNC_WRITES=false` is the analogue of Postgres's
`synchronous_commit=off`: it trades a bounded window of recent writes on host
loss for throughput, and is only safe where an upstream log can replay into
idempotent appliers. "An upstream log" has to mean one whose checkpoint cannot
survive a crash that this store did not: if the checkpoint lives in a *different*
durability domain it can roll back less than this one does, and the difference is
lost writes rather than replayed ones. See
[`docs/proposals/004-rocksdb-in-kubernetes.md`](../../docs/proposals/004-rocksdb-in-kubernetes.md) §2.

RocksDB does not read cgroup limits, so in a container `ROCKSDB_BLOCK_CACHE_BYTES`
is the setting that keeps the process inside its memory limit. Compaction buffers,
iterators and WAL buffers live outside the cache, so size it at roughly half of
what the container can spare rather than all of it.

## Configuring a write-heavy deployment

Swapping the engine removes the storage-side cost. Two Convex knobs above the trait
are worth revisiting at the same time, because they are sized for interactive OLTP
rather than sustained ingest. Neither default is changed here — they belong to every
Convex deployment, not just this backend — but on an ingest-shaped workload both are
usually wrong:

| Knob | Default | Why it matters here |
|---|--:|---|
| `INDEX_CACHE_VERIFY_PERCENT` | 100 | At the default, every index-cache **hit** also performs the persistence read it was meant to avoid and compares the two pages. The cache cannot save a read until this is lowered. Worth 1.8–1.9× on read throughput; `1` keeps a sampled check running. |
| `TABLES_TO_LOAD_IN_MEMORY` | *empty* | Comma-separated tables to pin in memory at startup, alongside the system tables Convex always pins. Reads against a pinned table's indexes never reach storage. Only the live row set is held, so memory tracks table size, not write volume — pin small, hot, bounded tables and nothing that grows with time. |
| `UDF_EXECUTOR_OCC_INITIAL_BACKOFF_MS` | 100 | On an OCC conflict the whole mutation re-runs after this delay. For a mutation that takes single-digit milliseconds, the first backoff alone is an order of magnitude more than the work being retried, and two conflicts put one mutation past 300 ms before it has accomplished anything. |
| `COMMITTER_MAX_WRITE_BATCH_DOCUMENTS` / `_BYTES` | 64 / 64 KiB | The committer combines independent commits into one `Persistence::write`. Larger batches amortise per-write overhead — which matters far more against a network database than against this one, but still helps. |

Measure before changing any of them: if OCC retries are not what your function log shows,
lowering the backoff buys nothing. The first two are measured on an ingest-shaped
workload in [`docs/proposals/003-beyond-the-storage-layer.md`](../../docs/proposals/003-beyond-the-storage-layer.md),
which also surveys what the ingest path still pays for above this trait.

## Semantics that differ from the relational backends

Both are deliberate, and both are the reason this file exists rather than a
one-line "it's just a KV store".

**Uniqueness is not free.** `ConflictStrategy::Error` costs nothing in a B-tree,
which gets it from a primary key; an LSM silently shadows an existing key
instead. It is enforced here with one batched, bloom-filtered point get per row
written. On the commit path that check is redundant — commit timestamps strictly
increase, so `(id, ts)` cannot collide, and `check_generated_ids` rejects reused
document ids a layer up — but `Database::initialize` writes bootstrap rows
outside a transaction and relies on it. So it is on by default;
`ROCKSDB_CHECK_CONFLICTS=false` buys throughput by giving up collision detection.

**There is no lease.** The relational backends fence a stolen leadership with two
extra statements per write. An embedded store has no such concept: the process
holding the directory lock is the writer. For a single-node deployment that is
simpler and stronger than an advisory row, but it has consequences:

- Exactly one process may open the directory for writing. A `StatefulSet` must
  guarantee one pod at a time; `replicas: 1` with `OnDelete` does.
- There is no failover through a shared database and no read replica. Recovery is
  restore-from-backup plus the WAL, not promote-a-follower.
- A standalone reader (`connect_persistence_reader`) opens a RocksDB *secondary*
  instance beside the primary, which reads its files without taking the write
  lock and catches up on demand.

## Durability and recovery

A `Persistence::write` is one RocksDB `WriteBatch` spanning every column family.
Atomic flush is enabled, so recovery restores a consistent cut across all of
them — an index entry can never survive a crash that lost its document.

With `ROCKSDB_SYNC_WRITES` on, the batch is in the WAL and fsynced before `write`
returns. RocksDB coalesces concurrent writers into one write group and syncs the
shared WAL once for all of them, which suits the committer's up-to-16 concurrent
writes.

WAL recovery uses `PointInTime` mode: a torn tail from an unclean shutdown is
truncated at the last consistent record rather than refusing to open.

## Threading

RocksDB is synchronous. Every call into it runs on a blocking pool thread, so no
operation occupies an async worker. Documents are serialized on the caller's
thread, as the Postgres backend also does.

Reads are paged: a page is fetched on a blocking thread, the retention validator
is consulted, and only then are its rows yielded — the same order Postgres uses,
so a snapshot that falls out of retention mid-scan is never handed to the caller.
Paging without an engine snapshot is safe because a `(ts, id)` row is written
once and only ever removed by retention, which cannot touch anything the
validator has approved.

## Not implemented

**Backup and point-in-time restore.** The durability story is the volume and
nothing else. RocksDB's `Checkpoint::create_checkpoint` hard-links a consistent
snapshot into a new directory on the same filesystem, near-instantly and at
near-zero extra space, which is the right primitive to build on — but it is not
exposed here, and neither is a restore path. A deployment adopting this backend
needs an answer for data its upstream log cannot replay. See
[`docs/proposals/004-rocksdb-in-kubernetes.md`](../../docs/proposals/004-rocksdb-in-kubernetes.md) §6.


**Retention via compaction filter.** The largest remaining win, and deliberately
not attempted here. Convex's retention worker deletes *superseded* versions; it
is not a blanket time cut. A filter that dropped everything older than a
watermark would delete the only surviving version of a key that has not been
written in a while. A version-aware filter is correct — during compaction a key's
versions are adjacent and in descending order, so the filter keeps the first at
or before the watermark and drops the rest, which is how CockroachDB and TiKV do
MVCC GC — but it needs its own correctness tests around never-updated keys before
it can be trusted with the only copy of a row. Until then `delete` and
`delete_index_entries` are ordinary batch deletes and the existing retention
worker drives them unchanged.

**Descending index scans read every retained version.** With `!ts` in the key, a
forward scan meets each key's newest version first: one entry per key. A reverse
scan resolves each key with a forward seek too, so it costs the same per key —
but a key updated many times inside the retention window has more versions to
skip past in either direction. Index retention is four minutes by default, so
this is typically one to three versions.

## Tests

`cargo test -p rocksdb_persistence` covers the encodings (unit plus proptest for
the order-preservation property the `idx` layout rests on) and the trait
behaviour: multi-version reads at explicit timestamps, tombstones, interval
bounds and ordering, paging across page boundaries in both directions, the
`previous_revisions` family, conflict detection, all three retention deletes,
persistence globals, `max_ts`, and durability across a reopen with and without a
clean shutdown.
