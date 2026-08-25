# Running the RocksDB backend in a self-hosted Kubernetes cell

**Status:** operational analysis; one code change (memtable memory ceiling), no migration tooling yet
**Date:** 2026-08-25
**Follows:** [002-storage-engine.md](./002-storage-engine.md) (the backend), [003-beyond-the-storage-layer.md](./003-beyond-the-storage-layer.md) (the measurements)
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
| §3 | Memory is one budget and the default split was wrong | **bug** | fixed here |
| §4 | Crash recovery stops being unbounded | win | no action |
| §5 | Disk: ~2.9× less, and one fewer PVC | win | resize |
| §6 | No backup or point-in-time restore | **gap** | not implemented — read this before adopting |
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

Sizing rule for a container:

```
ROCKSDB_BLOCK_CACHE_BYTES  ≈  (container memory limit - what the backend itself needs) / 2
```

The halving is deliberate: compaction input and output buffers, open iterators and WAL
buffers all live outside the cache, and in practice total RSS runs somewhat above the
cache size. For the reference backend container — `limits: { memory: 4Gi }`, already
using ~1.3 Gi — that lands around 1 Gi of cache, up from the 512 MiB default:

```yaml
- { name: ROCKSDB_BLOCK_CACHE_BYTES, value: "1073741824" }   # 1 GiB; memtables get 256 MiB of it
```

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

## 6. There is no backup story yet — read this before adopting

This is the honest gap, and the largest operational risk of the switch.

Postgres gives you `pg_dump`, `pg_basebackup`, WAL archiving and point-in-time recovery,
all of them well understood and all of them things an operator already knows how to
automate. The RocksDB backend as it stands gives you **none of that**. Its durability
story is the PVC and nothing else. Lose the volume and the cell's data is gone.

RocksDB's own primitive for this is `Checkpoint::create_checkpoint`, which hard-links
the SST files into a new directory on the same filesystem — near-instant, near-zero extra
space at creation, and consistent. That directory can then be copied off-node at leisure.
It is not exposed by this crate, and wiring it up means deciding where checkpoints go,
how they are retained, and how a restore is driven. None of that is written.

Until it is, the mitigations available are the ones that live above the trait, and both
already exist in this deployment:

- **The bus is the log.** Ingest events are in RedPanda with a per-cell topic and
  checkpoints in the relay. A rebuilt cell can replay from the bus into idempotent
  appliers, which is the same mechanism §2 relies on. This covers ingested state, not
  derived or user-authored state.
- **Streaming export.** `CONVEX_ENABLE_STREAMING_EXPORT` is already on and the relay
  already tails `document_deltas` into ClickHouse. That is a warehouse copy, not a
  restorable backup, but it means the row history exists somewhere else.

Neither is a substitute for a snapshot you can restore. Treat "implement checkpoint
export" as a prerequisite for anything holding data that the bus cannot replay.

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
