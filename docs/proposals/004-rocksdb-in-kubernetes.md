# Running the RocksDB backend in a self-hosted Kubernetes cell

**Status:** operational analysis; one code change (memtable memory ceiling), no migration tooling yet
**Date:** 2026-08-25
**Follows:** [002-storage-engine.md](./002-storage-engine.md) (the backend), [003-beyond-the-storage-layer.md](./003-beyond-the-storage-layer.md) (the measurements)
**Followed by:** [005-backup-and-restore.md](./005-backup-and-restore.md), the design for §6's gap
**Reference topology:** a per-cell `StatefulSet` with `replicas: 1`, a native Postgres
sidecar on pod-localhost, a Convex backend and a relay, with `pg-data` and `convex-data`
PVCs.

---

## 0. Summary

The switch is smaller operationally than it looks, because the reference topology already
satisfies the one invariant an embedded engine imposes. But it is **not** free, and two
items below are correctness issues rather than tuning:

| | Item | Kind | Status |
|---|---|---|---|
| §1 | Single-writer: already guaranteed by `replicas: 1` | invariant | satisfied by the existing design |
| §2 | The relay checkpoint stops sharing a durability domain with Convex | **correctness** | rule below; keep `ROCKSDB_SYNC_WRITES=true` |
| §3 | Memory is one budget, and RocksDB reads no cgroup limit | **bug** | fixed: the cache is derived from the container limit |
| §4 | Crash recovery stops being unbounded | win | no action |
| §5 | Disk: ~2.9× less, and one fewer PVC | win | resize |
| §6 | No backup or point-in-time restore | **gap** | backup/restore/rehearsal implemented per [005](./005-backup-and-restore.md); no PITR, and none possible at this layer |
| §7 | No in-place migration from Postgres | gap | export/import, or new cells only |

---

## 1. Single-writer: the constraint is already the design

An embedded store has no lease. The process holding the directory lock *is* the writer;
there is no advisory row to fence a stolen leadership with, and no way for two pods to
coordinate through the database because there is no database to coordinate through.

The reference manifest already states this as an invariant, for its own reasons:

> `replicas: 1` is load-bearing: a Convex backend never scales out; Kubernetes
> guarantees at-most-one pod per ordinal, which is what dissolved the EH
> zombie-stealing machinery.

That is exactly the guarantee RocksDB needs, and `updateStrategy: OnDelete` reinforces
it — the pod is replaced only when deliberately deleted, never rolled underneath itself.
So the hardest operational constraint of an embedded engine costs nothing here.

Two consequences remain, and they are real losses:

- **No failover through a shared database.** With Postgres, a backend could in principle
  be pointed at a surviving database. With RocksDB the data is in the pod's volume; the
  recovery story is the PVC, and if the PVC is gone, restore-from-backup (§6).
- **No read replica.** `connect_persistence_reader` opens a RocksDB *secondary*
  instance, which reads the primary's files without taking the write lock and catches up
  on demand — but only from the same filesystem, so it is a same-pod or same-node tool,
  not a cross-pod one.

Neither is a regression for a topology whose backend already never scales out.

---

## 2. The relay checkpoint stops sharing a durability domain

**This is the one item that can lose data, and it needs a decision before adoption.**

The reference cell runs one Postgres holding two databases: `cell_kind_01` (Convex) and
`relay` (the relay's ingest checkpoints). The manifest's justification for
`synchronous_commit=off` rests on exactly that:

> synchronous_commit=off is SAFE here by design: the relay checkpoint lives in this same
> PG, so a crash rolls state and checkpoint back together and the bus replays into
> idempotent appliers.

That argument is sound, and it is **entirely a property of the two living in one
instance**. Move Convex to RocksDB and they become two independent durability domains
that can lose different amounts of recent history on the same host failure. Which
direction the skew runs decides whether you replay or lose data:

| Convex | Relay checkpoint | On host loss | Verdict |
|---|---|---|---|
| RocksDB, `SYNC_WRITES=true` | Postgres, `synchronous_commit=off` | checkpoint rolls back further than Convex state → the bus replays events Convex already applied | **safe** — appliers are idempotent |
| RocksDB, `SYNC_WRITES=false` | Postgres, `synchronous_commit=off` | either may roll back further; Convex can lose applied writes the relay believes are checkpointed | **silent data loss** |

So the rule is: **once Convex is not in the relay's Postgres, `ROCKSDB_SYNC_WRITES` must
stay at its default of `true`.** The unsafe setting is only ever available again if the
relay's checkpoint moves into the same RocksDB — which it cannot, because that store is
Convex's, not the relay's.

The good news is that you are unlikely to want it. `synchronous_commit=off` exists in
this manifest because a Postgres commit fsyncs a WAL full of full-page writes on a
StandardSSD PVC, and the manifest records all apply lanes serializing behind `WALWrite`.
A RocksDB commit is one WAL append, with concurrent writers coalesced into a single
write group and one fsync shared between them, and no full-page writes at all — which is
why the measured commit p50 in [003](./003-beyond-the-storage-layer.md) is 40 ms
*with* fsync on, against 115 ms for Postgres *with fsync effectively off*. You are
trading an unsafe fast path for a safe one that is still faster.

Note that the Postgres sidecar does **not** disappear: the relay still needs its
tracking store. What changes is that the sidecar stops carrying the cell's data —
`shared_buffers` can drop from 2 GB to something like 128 MB, its memory limit with it,
and `pg-data` shrinks from 32 Gi to the size of 32 partition offsets plus slack.

---

## 3. Memory is one budget, and the default split was wrong

RocksDB does not read cgroup limits. Everything it holds has to be bounded by
configuration or the *backend* container gets OOMKilled — which, unlike a Postgres
sidecar being killed, takes the cell down.

The backend is configured so that memory really is bounded: the block cache is an LRU of
a fixed size, `cache_index_and_filter_blocks` puts index and bloom-filter blocks inside
it rather than beside it, and the `WriteBufferManager` is constructed *with the cache*,
so unflushed memtable memory is charged against the same budget. One number to size.

The defaults were wrong for that design: `ROCKSDB_BLOCK_CACHE_BYTES` and
`ROCKSDB_WRITE_BUFFER_BYTES` were both 512 MiB, so memtables could consume the entire
cache and evict every data, index and filter block — including the bloom filters the
per-write uniqueness check depends on — precisely when writes were heaviest.
`ROCKSDB_WRITE_BUFFER_BYTES` now defaults to a quarter of the cache.

**This is now derived rather than configured.** Left unset, the cache is sized from the
container's own memory limit — cgroup v2 `memory.max` or v1 `memory.limit_in_bytes`,
walking up the hierarchy so a limit on any ancestor binds, capped at physical memory and
clamped to [64 MiB, 4 GiB]. `ROCKSDB_BLOCK_CACHE_PERCENT` is the share, defaulting to 25.
For the reference backend container at `limits: { memory: 4Gi }` that is 1 GiB of cache,
chosen without anyone setting anything, and logged at startup with the limit it came
from.

A quarter rather than a half because the backend also runs V8 isolates, whose heaps are
the other large consumer in the process, and compaction buffers, iterators and WAL
buffers sit outside the cache. `ROCKSDB_BLOCK_CACHE_BYTES` still overrides the
derivation outright.

Against that, the sidecar gives back most of its `limits: { memory: 1536Mi }`.

**Unrelated but worth fixing either way:** the sidecar currently sets
`shared_buffers=2GB` inside a container limited to `1536Mi`. Postgres reserves the
buffer pool as shared memory and the cgroup charges those pages as they are touched, so
that container is sized 512 Mi below its own buffer pool before any backend or
`work_mem` allocation. That is an OOMKill waiting for the pool to fill. If §2's
shrink happens, it resolves itself.

---

## 4. Crash recovery stops being unbounded

The manifest carries a 20-minute startup probe and the reason for it:

> 20 min: WAL replay after an unclean shutdown at fleet write load exceeds 2 min — a
> shorter window kills PG mid-recovery and restarts replay from zero, a death spiral.

That failure mode is a property of Postgres's recovery model: replay starts at the last
checkpoint, and with `max_wal_size=4GB` and `checkpoint_completion_target=0.9` the
distance between checkpoints under write load is large.

RocksDB replays only the WAL segments in front of the memtables that had not been
flushed — bounded by `ROCKSDB_WRITE_BUFFER_BYTES`, which is now 256 MiB at the default
cache size, not by a checkpoint interval. Recovery is seconds, and bounded by
configuration rather than by how long the process had been running. `PointInTime`
recovery mode also means a torn tail from an unclean kill truncates at the last
consistent record instead of refusing to open, so there is no "won't start after a hard
kill" state to be in.

The 20-minute probe window can come down substantially once the cell's data is no longer
in Postgres. Keep it generous — the sidecar still recovers its own small database — but
the death spiral it was defending against is gone.

---

## 5. Disk

Measured on the device-location workload: **13.3 MB against 38.9 MB** for the same data,
a 2.9× reduction, from three effects — no full-page writes in the WAL, no per-row tuple
header and no page free-space overhead, and block compression.

Two things offset it, neither large:

- **Space amplification during compaction.** A leveled LSM transiently holds the inputs
  and outputs of a running compaction. Budget ~1.1–1.5× live size, not the ~2× a naive
  reading of "LSM space amplification" suggests, because most compactions are local.
- **Blob files.** Documents at or above `ROCKSDB_BLOB_THRESHOLD_BYTES` (4 KiB) live
  outside the LSM and are garbage-collected on their own schedule.

Volume changes for the reference cell:

- `convex-data` (16 Gi) gains the RocksDB directory alongside file storage and search
  indexes. It needs to grow — but by less than `pg-data` shrinks.
- `pg-data` (32 Gi) collapses to whatever the relay's checkpoints need. Note that
  **PVCs cannot be shrunk in place**; this is a new-cell change, or a
  create-new/migrate/delete-old operation.

One thing to watch that Postgres did not have: compaction is background I/O that
competes with foreground writes on the same PVC. `ROCKSDB_BACKGROUND_JOBS` defaults to
the core count; on a 2-CPU backend container that is 2, which is the right order. Raising
it on a cell with a slow PVC will make things worse, not better.

---

## 6. Backup

**Superseded by [005](./005-backup-and-restore.md), which is implemented.** The gap this
section described is closed: `BackupEngine` generations on a timer, retention, an
advisory directory lock, verification before pruning, a rehearsal that decodes rather
than counts, and a `rocksdb-backup` binary for restore. What follows is the reasoning
that led there, kept because the constraints have not changed.

The one part that remains true as written is the limitation at the end: there is no
point-in-time recovery and there cannot be at this layer.

Postgres gives you `pg_dump`, `pg_basebackup`, WAL archiving and point-in-time recovery,
all of them well understood and all of them things an operator already knows how to
automate. The RocksDB backend as it stands gives you **none of that**. Its durability
story is the PVC and nothing else. Lose the volume and the cell's data is gone.

RocksDB itself has two mechanisms for this, and **both are already exposed by the
`rocksdb` 0.22 crate this backend depends on** — the gap is wiring, not capability:

| API | What it does | Shape |
|---|---|---|
| `Checkpoint::create_checkpoint` | Consistent snapshot of the whole database, hard-linking SST files into a new directory | Same filesystem only. Near-instant, near-zero extra space at creation. Ideal for "snapshot then copy off-node". |
| `BackupEngine` | `create_new_backup_flush`, `restore_from_latest_backup`, `restore_from_backup`, `purge_old_backups`, `verify_backup`, `get_backup_info` | **Incremental** — unchanged SST files are shared between backups, so backup *n+1* only writes what changed. Opens against a RocksDB `Env`, so the destination can be another filesystem. Carries its own restore path and checksum verification. |

`BackupEngine` is the closer match to what an operator expects: numbered backups, a
retention policy, verification, and a restore that does not require the database to be
running. `create_new_backup_flush(db, true)` flushes memtables first, so a backup does
not depend on the WAL to be complete.

What is still undecided is not the mechanism but the policy: where backups go (a sidecar
that syncs to object storage, or a `BackupEngine` opened against a mounted volume), how
often, how many are kept, and how a restore is driven in a `StatefulSet` whose pod owns
the directory. None of that is written.

Until it is, the mitigations available are the ones that live above the trait, and both
already exist in this deployment:

- **The bus is the log.** Ingest events are in RedPanda with a per-cell topic and
  checkpoints in the relay. A rebuilt cell can replay from the bus into idempotent
  appliers, which is the same mechanism §2 relies on. This covers ingested state, not
  derived or user-authored state.
- **Streaming export.** `CONVEX_ENABLE_STREAMING_EXPORT` is already on and the relay
  already tails `document_deltas` into ClickHouse. That is a warehouse copy, not a
  restorable backup, but it means the row history exists somewhere else.

Neither is a substitute for a snapshot you can restore. Treat "implement backup export"
as a prerequisite for anything holding data that the bus cannot replay.

### What SurrealDB does, and why it is not the model to copy

SurrealDB is the closest comparable — a Rust database that ships RocksDB as its default
single-node engine — so it is worth knowing that **it uses neither of the above.** A
`grep` for `Checkpoint`, `create_checkpoint`, `BackupEngine` and `backup_engine` across
its tree returns nothing; its RocksDB layer (`core/src/kvs/rocksdb/`) touches durability
options and compaction and stops there.

Its answer is instead:

- **Logical export.** `surreal export` writes SurrealQL — schema and data as statements —
  and `surreal import` replays it. Portable and engine-agnostic, which is the stated
  reason for the choice.
- **Volume snapshots**, described in their own docs as something to combine with
  exports "where available", with the caveat that they "depend on filesystem layout and
  binary compatibility".
- **No point-in-time recovery.** Their documentation says so directly: "point-in-time
  recovery is not implicit in a single export: each file reflects one moment", and
  points users at more frequent exports, replicas, or a journaled log upstream.

The scaling problem with that choice is documented in their own tracker: issue #7189
reports **~200 K records across 69 tables taking 7+ hours to restore** from an 850 MB
`.surql` file, because restore is sequential SQL parsing and per-statement execution, so
it grows linearly with database size. The top-ranked proposal in that issue is
storage-engine-level backup and restore. It is open, with no maintainer response.

Convex is already in the same position as SurrealDB's export path — snapshot
export/import exists above the trait and would replay through the same machinery — so
adopting the logical-export answer would inherit the same restore-time problem. The
engine-level path is strictly better here and is a few hundred lines, not a research
project.

### Their durability model, which is worth borrowing from

SurrealDB's RocksDB layer exposes three sync modes on the connection string, defaulting
to the safe one:

| Mode | Mechanism | Loss window |
|---|---|---|
| `sync=every` *(default)* | A `CommitCoordinator` batches waiters and performs a single `flush_wal(true)` for the group | none |
| `sync=<interval>` | `manual_wal_flush(true)` plus a background thread flushing on a timer | bounded by the interval |
| `sync=never` | OS buffers only | unbounded |

This backend now has all three. The safe end is equivalent work by a different route:
rather than an explicit coordinator, `WriteOptions::set_sync(true)` lets RocksDB's own
write groups coalesce concurrent writers and fsync the shared WAL once, which suits a
committer that already batches. SurrealDB needs the explicit coordinator because each of
its optimistic transactions commits on its own thread.

`ROCKSDB_SYNC_INTERVAL_MS` adds the middle: `manual_wal_flush` plus a background thread
calling `flush_wal(true)` on the interval, which turns an unbounded loss window into a
number.

**What it is worth, measured.** The answer is "it depends on the commit rate, and for
this deployment's shape, not much" — see the crate README for the table. At one commit
per event it is +25 %; at 64 events per commit it is within noise, because 25 fsyncs a
second against a 0.17 ms fsync is not a cost. The reference cell batches ingest events
into large mutations, so it sits at the far end of that curve. The one consistent effect
at that end is tail latency — commit p99 came down 15–20 % across three runs — but
throughput did not move.

**It does not rescue §2, and the tempting argument that it might is worth refuting.** One
could reason: if Convex's interval (say 100 ms) is shorter than the relay's Postgres
`wal_writer_delay` (200 ms by default under `synchronous_commit=off`), the checkpoint
rolls back *further* than the data, so the skew runs in the safe direction and the bus
replays the difference. That is true on average and worthless as a guarantee — there is
no ordering relationship between two independent timers fsyncing two independent files,
and a host loss does not respect either. Narrowing a window is not the same as closing
it. `ROCKSDB_SYNC_WRITES=true` stays the rule while the checkpoint lives elsewhere.

---

## 7. Migration

There is no in-place Postgres → RocksDB conversion. Document bodies are stored as the
same JSON on both backends, so no re-encoding would be needed, but nothing walks one
backend's tables and writes the other's — and the index entries, table metadata and
persistence globals all have to come across consistently.

The realistic paths, in order of how much they ask of you:

1. **New cells only.** `--db rocksdb /convex/data/db` on cells created after the switch.
   Cells are the unit of scale here, so a fleet can migrate by attrition.
2. **Convex snapshot export/import.** Export from the Postgres cell, import into a fresh
   RocksDB cell. Requires a write freeze for the duration.
3. **Shadow write.** Phase 3 of [002](./002-storage-engine.md) — run both backends under
   production load and compare. Not implemented, and the right thing to do before
   trusting the switch at fleet scale.

---

## 8. What does not change

Worth stating, because it bounds the blast radius. The Convex developer API is
identical — this is a storage engine swap, not a feature. Above `Arc<dyn Persistence>`
nothing learns that anything moved: the committer, index cache, retention, subscriptions,
streaming export, and the search and vector indexes are untouched. `CONVEX_CLOUD_ORIGIN`,
`INSTANCE_NAME`, `INSTANCE_SECRET`, the readiness probe on `/version`, the ingress, the
relay's `CELL_SITE_URL` and `CONVEX_URL` and every application-level knob in the manifest
carry over unchanged. The change is `POSTGRES_URL` becoming `--db rocksdb <path>`, plus
the resource and volume consequences above.
