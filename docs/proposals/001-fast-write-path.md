# A fast write path for Convex, informed by SpacetimeDB

**Status:** proposal
**Date:** 2026-08-25
**Scope:** the Convex self-hosted backend write path, for high-rate IoT ingestion
**Constraint:** keep disk-based table scale — the working set may exceed RAM

---

## 0. The question

SpacetimeDB commits a transaction in a fraction of a millisecond. A Convex mutation
carrying comparable work takes hundreds. This document (a) identifies the specific
mechanisms behind SpacetimeDB's number, (b) traces where Convex's milliseconds
actually go, with line references, and (c) proposes a layered set of changes that
recover most of the gap **without** adopting SpacetimeDB's defining constraint
(all data resident in RAM), and **without** forking the parts of `convex-backend`
that the upstream team actively develops.

The design principle throughout: everything lands behind the `Persistence` /
`PersistenceReader` trait pair or behind env knobs. That seam is the one upstream
itself maintains three implementations against (Postgres, MySQL, SQLite), so it is
the cheapest place in the tree to stand.

---

## 1. Why SpacetimeDB is fast

Nine mechanisms. Eight of them are portable; one is not, and it happens to be
load-bearing. Being clear about which is which is the whole point of this section.

### M1 — The database *is* the application process

A reducer is a WASM function running inside the database process, and a reducer
call *is* a transaction. There is no compute↔storage network hop, no SQL wire
protocol, no per-operation boundary crossing between a JS runtime and the store.
Reading a row is a function call.

### M2 — All state is memory-resident, in a custom row format ⚠️ *not portable*

Rows live in 64 KiB pages (`crates/table/src/indexes.rs:28`), addressed by a
`RowPointer` that packs page index and offset into a single `u64`
(`crates/table/src/indexes.rs:284-311`). Rows are stored in BFLATN with a
`static_layout` fast path for fixed-size types. There is no buffer pool, no
eviction, no page fault: reads are pointer chases.

The docs are explicit about the cost of this:

> SpacetimeDB holds all data in memory, so the practical limit is the available
> RAM on the host.
> — `docs/docs/00100-intro/00100-getting-started/00500-faq.md:274`

**This is the mechanism we cannot copy**, and it is not separable from the
headline latency number. Everything below, however, is independent of memory
residency — which is what makes the rest of this document possible.

### M3 — Pessimistic single-writer: no OCC, no aborts, no retries

`begin_mut_tx` takes a **write lock on the entire committed state** for the
lifetime of the transaction (`crates/datastore/src/locking_tx_datastore/datastore.rs:976-997`).
Writers are serialized. There is no read-set tracking for conflict detection, no
validation phase, no abort, no backoff, no re-execution.

The trade is zero write parallelism — which is affordable precisely because each
transaction is microseconds long. A design that serializes 10 µs transactions
still does 100k/s; a design that serializes 10 ms transactions does 100/s.

### M4 — Commit merges into memory; durability is write-behind

`commit_tx` merges the transaction's delta into committed state and calls
`request_durability`, which is **non-blocking** (`crates/engine/src/relational_db.rs:875-895`).
The trait contract says so in as many words:

> This method must never block, and accept new transactions even if they cannot
> be made durable immediately.
> — `crates/durability/src/lib.rs:133-147`

And the engine acknowledges the consequence in a comment:

> our durability is an asynchronous write-behind log
> — `crates/engine/src/relational_db.rs:935`

Commit latency is therefore the cost of a memory merge. Disk is not on the path.

### M5 — Serialization is deferred off the commit path

`append_tx` accepts a `PreparedTx = Box<dyn IntoTransaction<T>>`
(`crates/durability/src/lib.rs:115`) — in practice a *thunk*
(`crates/engine/src/durability.rs:30`). The encoding of row data into the log
format happens on the durability actor's thread, not on the committing thread.
Even the CPU cost of serialization is moved off the critical path.

### M6 — Group commit that needs no tuning

The durability actor drains the queue with `recv_many(..., usize::MAX)`, writes
every drained transaction to the commitlog, then performs exactly **one**
`flush_and_sync` (`crates/durability/src/imp/local.rs:269-315`).

The self-tuning property matters: at idle the batch is one transaction and
latency is minimal; under load the batch grows and one fsync amortizes across
thousands of transactions. There is no delay knob to get wrong.

### M7 — Append-only log, no read-modify-write

The commitlog is a sequence of ~1 GiB segments with CRC32C per commit and an
offset index. Nothing is updated in place: no B-tree maintenance, no MVCC row
versions to vacuum, no page splits, no full-page writes at checkpoint. Recovery
is *latest snapshot + replay the suffix* (`crates/snapshot/src/lib.rs:1-22`).

### M8 — Durability appears on the *read* path, and only on request

`confirmed_reads` is an opt-in per-connection flag
(`crates/core/src/client/client_connection.rs:82`, `92`). With it off — the
default — subscribers receive updates as soon as the commit is in memory. With it
on, the sender awaits `DurableOffset::wait_for(tx_offset)` before releasing the
update (`client_connection.rs:240-250`).

This is the sharpest idea in the system: durability latency is charged to the
readers who ask for it, rather than to every writer unconditionally.

### M9 — Ephemeral / event tables

Rows inserted into an event table are broadcast to subscribers and then
discarded — never merged into committed state, therefore never logged, never
indexed, never retained. Committed state filters them out of the persisted
`TxData` (`crates/datastore/src/locking_tx_datastore/committed_state.rs:1001`).
For pure fan-out — telemetry to a dashboard, where the warehouse is the system of
record — the write cost is zero.

### Licensing (read before borrowing code)

`spacetimedb-commitlog`, `spacetimedb-durability`, `spacetimedb-datastore` and
`spacetimedb-table` are **BSL 1.1** — source-available, not open source, and not
vendorable into a product. `spacetimedb-sats`, `spacetimedb-lib` and
`spacetimedb-bindings` are Apache-2.0.

Everything in this document is a **design** borrowing, not a code borrowing. The
log implementation proposed in Layer 1 (§3) is ~600–900 LOC written from scratch, or a
dependency on an Apache/MIT embedded store.

---

## 2. Where Convex's milliseconds go

Traced against `convex-backend` at `7cce8fb`, and against the aa-app self-hosted
cell manifest (`infra/k8s/base/cells/cell-kind-01.yaml`) for deployment numbers.

### 2.1 The commit acknowledges *after* the persistence write

This is the structural difference from M4. In the committer loop, the caller's
oneshot is fired only once the persistence-write future has resolved:

```
crates/database/src/committer.rs:460-461
    self.publish_commit(pending_write, write_bytes, index_key_writes);
    let _ = result.send(Ok(commit_ts));
```

and that future is `track_and_write_to_persistence`, which **awaits**
`WriteBatcher::write` → `Persistence::write`
(`crates/database/src/committer.rs:1080-1083`). Every millisecond Postgres spends
is a millisecond of mutation latency.

To be fair to the current design: Convex already has group commit. `WriteBatcher`
(`crates/database/src/write_batcher.rs`) combines independent commits into batched
`Persistence::write` calls once ≥3 writes are in flight, holding a partial batch
open for `COMMITTER_MAX_COMMIT_DELAY_MS`. The mechanism is right; §3.1 argues the
defaults are sized for small OLTP commits rather than ingest batches.

### 2.2 Each persistence write is a six-statement Postgres transaction

`PostgresPersistence::write` runs inside `Lease::transact`
(`crates/postgres/src/lib.rs:484`, `1815`), which issues:

1. `BEGIN`
2. `advisory_lease_check` — a `SELECT` (`postgres/src/lib.rs:1844-1852`)
3. `INSERT INTO documents …` (batched, ≤1024 rows/statement)
4. `INSERT INTO indexes …` (batched, concurrent with 3)
5. `lease_precond` — `SELECT … FOR UPDATE` (`postgres/src/lib.rs:1867-1876`)
6. `COMMIT`

Two of those six exist only for multi-node leader fencing — dead weight for a
single-writer cell. Loopback RTT makes each cheap individually, but the count is
paid per batch, and steps 3–4 are where row-level cost lands.

### 2.3 Write amplification: one document write → one row per index

`IndexRegistry::index_updates` (`crates/indexing/src/index_registry.rs:189-222`)
emits an index update for every index on the table, on every write. Keyed by
`(index_id, index_key)`, an insertion overwrites the matching deletion, so an
update produces **one `indexes` row per index** (plus a tombstone for each index
whose key actually changed).

Every Convex table carries `by_id` and `by_creation_time` implicitly. So:

| Table shape | Postgres rows per document write |
|---|---|
| no user indexes | 3 (`documents` + 2 system indexes) |
| 1 user index | 4 |
| 3 user indexes | 6 |

One logical telemetry event touching three tables with a few indexes each is
comfortably 20–40 Postgres row inserts.

### 2.4 Writes are unbatched at the isolate boundary

In `run_async_syscall_batch` (`crates/isolate/src/environment/udf/async_syscall.rs:658-720`)
reads batch — `AsyncSyscallBatch::Reads` (line 670), up to
`MAX_SYSCALL_BATCH_SIZE = 16`. Writes do not: `1.0/insert`, `1.0/replace`,
`1.0/shallowMerge`, `1.0/remove` are all `AsyncSyscallBatch::Unbatched` (line
674). Each is one V8↔Rust crossing with a JSON serialize on the way in and out
(`async_syscall.rs:1211-1248`).

`1.0/runUdf` — the subtransaction primitive behind `ctx.runMutation` — is also
unbatched, and each call is a full nested UDF execution: argument validation,
component resolution, subtransaction setup and teardown.

A 1000-event batch that calls `ctx.runMutation` per event and writes 2 documents
each pays ~3000 unbatched boundary crossings before any of it reaches the
committer.

### 2.5 OCC retry backoff is sized for human-scale mutations

On conflict, the *entire mutation* re-runs against a fresh transaction
(`crates/application/src/application_function_runner/mod.rs:927-1030`), with:

```
crates/common/src/knobs.rs:293-298
UDF_EXECUTOR_OCC_INITIAL_BACKOFF_MS = 100
UDF_EXECUTOR_OCC_MAX_BACKOFF_MS     = 2000
UDF_EXECUTOR_OCC_MAX_RETRIES        = 4
```

For a 5 ms mutation, the first backoff alone is 20× the work being retried. Two
conflicts put a single mutation over 300 ms before it has done anything. This
alone can account for a "hundreds of milliseconds" observation, independent of
storage.

### 2.6 Retention runs against the same database

Index entries are retained 4 minutes, documents 14 days
(`crates/common/src/knobs.rs:642`, `652`). The deleters compete with the ingest
path for the same Postgres.

### 2.7 In the aa-app cell specifically

From `infra/k8s/base/cells/cell-kind-01.yaml`:

- The Postgres sidecar is capped at **`cpu: "1"`**. §2.3's row amplification lands
  on one core doing heap inserts plus B-tree maintenance plus WAL plus
  `wal_compression=on`.
- `synchronous_commit=off` is already set — SpacetimeDB's write-behind idea,
  applied one layer down, with a correct safety argument recorded in the manifest
  (relay checkpoint and applied state roll back together; the bus replays into
  idempotent appliers).
- Measured ~84 env/s per cell at 16 apply lanes.

That `synchronous_commit=off` is already in place is the most useful single fact
here: **the fsync is not the remaining bottleneck.** What is left is round-trip
count (§2.2), row amplification (§2.3) on one core, boundary crossings (§2.4), and
retry backoff (§2.5). A proposal that only removed the fsync would deliver
nothing. Each layer below attacks a different one of those four.

---

## 3. Proposal

Four layers, ordered by payoff-to-risk. Each is independently deployable and
independently revertable. Stop wherever the measurements say you have enough.

### The seam

```
                    ┌──────────────────────────────────────┐
   committer.rs ───▶│  Arc<dyn Persistence>                │  ← we implement this
   (untouched)      │  Arc<dyn PersistenceReader>          │
                    └──────────────────────────────────────┘
                                     │ delegates to
                                     ▼
                        PostgresPersistence (untouched)
```

- `Persistence` (`crates/common/src/persistence.rs:223`) — 7 required methods.
- `PersistenceReader` (`crates/common/src/persistence.rs:410`) — 7 required, the
  rest defaulted.
- Wiring: `connect_persistence` (`crates/db_connection/src/lib.rs:137`) →
  `Database::load(persistence, …)` (`crates/local_backend/src/lib.rs:167`).

Every read in the system flows through `persistence.reader()` — verified: the only
call sites are `committer.rs`, `database.rs`, `retention.rs`, `table_summary.rs`,
`database_index_workers/`, `application/src/lib.rs`, `local_backend/src/lib.rs`.
A decorating `Persistence` that returns a decorating `PersistenceReader` is a
complete and airtight interception point.

---

### Layer 0 — Knobs only. No code. Hours.

| Knob | Now | Suggested | Rationale |
|---|---|---|---|
| `UDF_EXECUTOR_OCC_INITIAL_BACKOFF_MS` | 100 | 5 | §2.5 — sized for 5 ms mutations |
| `UDF_EXECUTOR_OCC_MAX_BACKOFF_MS` | 2000 | 100 | as above |
| `COMMITTER_MAX_WRITE_BATCH_DOCUMENTS` | 64 | 4096 | §2.2 — amortize the 6 statements |
| `COMMITTER_MAX_WRITE_BATCH_BYTES` | 64 KiB | 8 MiB | as above |
| `COMMITTER_MAX_COMMIT_DELAY_MS` | 1 | 2 | wider window to fill a batch |
| `COMMITTER_BATCH_WRITE_THRESHOLD` | 3 | 1 | always batch on an ingest cell |
| `INDEX_CACHE_SIZE` | 512 MiB | as RAM allows | keeps reads off disk |
| pg sidecar `cpu` limit | 1 | 4+ | §2.7 — this is a hard wall |

Nothing here is a fork. Expect a meaningful multiple on the ingest path purely
from OCC backoff and Postgres CPU, before any code is written.

---

### Layer 1 — `WalPersistence`: a write-ahead decorator. *The core proposal.*

This is M4 + M5 + M6 + M7, ported to Convex as a `Persistence` decorator. New
crate, `crates/wal_persistence/`.

```rust
pub struct WalPersistence {
    inner: Arc<dyn Persistence>,        // PostgresPersistence, untouched
    wal: Arc<SegmentedLog>,             // append-only, CRC'd, group-committed
    overlay: Arc<RwLock<Overlay>>,      // acked-but-unmaterialized rows
    materializer: AbortOnDropHandle<()>,// drains WAL → inner.write()
}
```

**Write path.** `write(documents, indexes, _)` encodes the rows, appends one frame
to the WAL, inserts them into the overlay, group-fsyncs with the other writers
queued behind it, and returns. Postgres is not on this path.

**Materializer.** A background task drains the WAL into `inner.write()` in large
batches — thousands of rows per Postgres transaction, so the two lease statements
of §2.2 are amortized across all of them instead of paid per commit — then trims
the overlay and the WAL.

**Read path — the correctness-critical part.** `reader()` returns an
`OverlayReader` that merges the overlay into `index_scan`,
`load_documents`, `load_documents_from_table`, `previous_revisions`,
`previous_revisions_of_documents` and `max_ts`.

This merge is *not* optional. `IndexCache::apply_writes` — called from
`publish_commit` (`crates/database/src/committer.rs:1111-1128`) — refreshes only
intervals that are *already cached*. A read of a cold interval after an
acked-but-unmaterialized write would go to Postgres and miss it. The overlay
closes exactly that hole.

**Ordering of persistence globals.** `write_persistence_global` — especially
`MaxRepeatableTimestamp`, written by `bump_max_repeatable_ts`
(`crates/database/src/committer.rs:808-871`) — must be sequenced *behind* the
document writes in the same log. Otherwise a crash can leave a repeatable
timestamp pointing past durable data.

**Boot.** Replay the WAL into `inner` to completion before the backend reads
anything, then hand over. `is_fresh()` delegates.

**Two durability tiers.**

- **fsync-then-ack (default).** Identical durability to today. The difference is
  that the fsync is one sequential append to local NVMe instead of a six-statement
  Postgres transaction. Group-committed, so it self-tunes exactly as M6 does.
- **ack-then-fsync (`WAL_WRITE_BEHIND=true`, opt-in).** SpacetimeDB semantics: a
  bounded loss window on host loss. For aa-app this is defensible on precisely the
  argument already written into the cell manifest for `synchronous_commit=off` —
  the relay checkpoint and applied state recover together, and RedPanda replays
  into idempotent appliers. Do not enable it for a deployment without that
  property.

**Overlay sizing and backpressure.** At 50k document-writes/s × 6 rows × ~200 B
with a 1 s materialization lag: ~60 MB. Cap it in bytes; when the cap is hit,
`write()` blocks rather than growing. The degraded mode is today's latency, never
data loss.

**What this buys.** It removes Postgres from mutation latency entirely and turns
the ingest path's relationship with Postgres from *synchronous, small, frequent*
into *asynchronous, large, rare* — which is the shape Postgres is good at. Disk
scale is preserved exactly: Postgres remains the system of record for everything
older than the ~1 s overlay window, and nothing about the working set has to fit
in RAM.

**What it does not buy.** It does not reduce the total row count Postgres must
eventually absorb (§2.3) — it only removes it from the latency path. If the
materializer, not the WAL, becomes the ceiling, that is Layer 2b/4 territory.

---

### Layer 2 — Cut the work at the source

Three independent items.

**(a) Batched write syscalls.** Add `AsyncSyscallBatch::Writes` alongside `Reads`
in `crates/isolate/src/environment/udf/async_syscall.rs:658-720`, plus a
`db.insertMany()` in the JS client. A 1000-insert batch collapses from 1000
boundary crossings to ~63.

This is the most upstream-able change in this document — it is a pure win for
every Convex workload and a plausible PR. Keep it as its own commit so it can be
dropped the day upstream ships an equivalent.

**(b) Fewer indexes on hot ingest tables.** Each index is one `indexes` row per
document write, forever, on that one Postgres core. An append-only telemetry table
read only by `(deviceId, ts)` range needs exactly one user index. `by_id` and
`by_creation_time` are the floor. This is an application change and costs nothing.

**(c) Fewer subtransactions.** Every `ctx.runMutation` is a full nested UDF
execution (§2.4). Where per-event rollback isolation is not required, call the
implementation inline.

---

### Layer 3 — Ephemeral tables, as a persistence-level filter

M9, achievable without touching the committer: `WalPersistence::write` drops
document and index rows whose tablet is in a declared ephemeral set, instead of
forwarding them.

Such rows still flow through the committer, the write log and the subscription
machinery — so live queries still fire — but never reach disk, never accrue index
rows, and never need retention. For telemetry whose real system of record is
ClickHouse via the egress path, this removes the write entirely rather than making
it faster.

This is a genuine semantic change: a table you cannot read back. Scope it to
explicitly declared tables and make reads return empty, exactly as SpacetimeDB
does for event tables.

---

### Layer 4 — Optional: replace Postgres with an embedded LSM

Only if Layer 1's measurements show the materializer is the ceiling.

`documents` and `indexes` are both plain ordered key-value maps —
`(id, ts) → doc` and `(index_id, key, ts) → value`. That is precisely an LSM's
shape, and LSM write amplification on append-heavy workloads is far below a
B-tree's. Implementing `Persistence` directly over redb / fjall / RocksDB on local
NVMe deletes the sidecar, its CPU limit, its lease round trips and its checkpoint
full-page writes — while remaining disk-based and unbounded by RAM.

Cost: you own MVCC range-scan semantics and retention deletes. ~2000–3000 LOC.
`crates/sqlite/src/lib.rs` (767 lines) is the reference for the shape of a minimal
implementation.

---

## 4. Measure before building

Every number needed to choose between the layers already exists.

| Signal | Where | Points to |
|---|---|---|
| `commit_persistence_write_timer` | `database/src/metrics.rs` | Layer 1 |
| `lease_check_timer`, `lease_precond_timer`, `insert_timer` | `postgres/src/metrics.rs` | Layer 1 vs 2b |
| `log_write_batch(acks, docs, bytes)` | `database/src/write_batcher.rs:196` | is batching happening at all? |
| `concurrent_commits_gauge`, commit admission pause | `database/src/committer.rs:408-424` | committer saturation |
| `mutation_retry_count` | function log | Layer 0 |
| pg `wait_event` sampling, `pg_stat_statements` | sidecar | Layer 2b / 4 |

**Decision rule.** Persistence-write time dominates → Layer 1. OCC retries
dominate → Layer 0, then narrow read sets. Isolate time dominates → Layer 2a.
Postgres CPU pinned with persistence-write time already small → Layer 2b, then 4.

---

## 5. Upstream-compatibility ledger

| Change | Files touched upstream | Rebase exposure |
|---|---|---|
| Layer 0 | none (env vars) | none |
| Layer 1 | `clusters/src/lib.rs` (+1 `DbDriverTag` variant), `db_connection/src/lib.rs` (+1 match arm in each of two fns), `common/src/knobs.rs` (+N, append-only) | negligible |
| Layer 1 body | `crates/wal_persistence/` — 100% new | none |
| Layer 2a | `isolate/src/environment/udf/async_syscall.rs`, JS client | **real** — isolate this commit |
| Layer 2b, 2c | aa-app only | none |
| Layer 3 | inside `crates/wal_persistence/` | none |
| Layer 4 | `crates/lsm_persistence/` — new, + 1 match arm | negligible |

Untouched by every layer: `committer.rs`, `database.rs`, `transaction.rs`,
`write_log.rs`, `crates/postgres/`, `crates/indexing/`, retention, subscriptions,
streaming export.

**Rebase discipline.** One layer per commit, always rebased onto upstream `main`,
never interleaved. Layer 2a is the only commit with real conflict risk; keeping it
last and separate means a bad rebase costs one commit, not the branch.

---

## 6. Honest summary

SpacetimeDB's headline latency comes from a bundle in which memory residency is
load-bearing (M2), and that part is not available to a system that must scale past
RAM. What *is* available — and is where most of the practical gap lives — is the
rest of the bundle: keep durable I/O off the commit path (M4), defer serialization
(M5), group-commit without a tuning knob (M6), append instead of read-modify-write
(M7), and charge durability latency to the readers who ask for it (M8).

Layer 1 ports M4–M7 behind a trait Convex already maintains three implementations
of. Layer 0 removes a retry policy sized for a different workload. Layer 2 stops
paying for work that need not happen. Layer 3 ports M9 for the traffic that is
really a stream.

The measurement in §4 should happen first, because §2.7 shows this deployment has
already eliminated the fsync — which means the layer that *sounds* most like
SpacetimeDB is not automatically the one that pays best here.
