# What else the batched ingest path pays for

**Status:** two items implemented (§1, §2 config); the rest surveyed with evidence
**Date:** 2026-08-25
**Follows:** [002-storage-engine.md](./002-storage-engine.md), which replaced the engine under `Persistence`
**Constraint:** unchanged — zero change to the Convex developer API.

---

## 0. Summary

[002](./002-storage-engine.md) removed the storage-side cost of a write. This document
is the answer to "what is left?" for a **batched ingest pipeline**: a mutation that
applies many independent events in one transaction, each doing a small
read-modify-write against a hot per-entity key.

Everything below was found by reading this tree, and the first two are measured on
`crates/persistence_bench` (device-location workload, 8 000 events, batch 64, 512
devices, 30 % RLE merges, RocksDB backend, 4-core VM):

| | Area | Where | Effect | Status |
|---|---|---|--:|---|
| §1 | Hot bounded tables are read from disk when they could be pinned in memory | `model/src/lib.rs:654` | **+32 % events/s, 10× reads/s** | **implemented** |
| §2 | Every index-cache hit is *also* read from persistence and compared | `common/src/knobs.rs:1995` | **+11 % events/s, 1.8–1.9× reads/s** | config change, measured |
| §3 | Retention reads the whole document log a second time | `database/src/retention.rs:666` | ~2× steady-state storage work | not attempted |
| §4 | Index keys are computed twice for every row written | `write_log.rs:138` vs `committer.rs:962` | duplicate CPU per row | not attempted |
| §5 | The module graph is re-instantiated on every function call | `analyze.rs:449` | fixed cost per call and per `runMutation` | app-side, one line |
| §6 | Writes never batch across the isolate boundary; sequential reads don't either | `async_syscall.rs:244` | one round trip per `db.*` call | app-side restructure |
| §7 | OCC backoff is sized for interactive OLTP | `UDF_EXECUTOR_OCC_INITIAL_BACKOFF_MS` | 100 ms per conflict | config change |
| §8 | Subscription invalidation is proportional to writes × subscribers | `subscription.rs:492` | depends on live queries | measure first |

Each row adds one change to the row above it:

```
                            events/s  commits/s   p50 ms   p99 ms   reads/s
sqlite   stock                   917       14.3    68.68   102.43      6720
sqlite   verify=0                1028       16.1    62.10    90.02      9220
sqlite   verify=0 + pinned       1216       19.0    54.10    73.46    108228
rocksdb  stock                   1244       19.4    49.97    69.13      6078
rocksdb  verify=0                1208       18.9    50.37    82.02     10606
rocksdb  verify=0 + pinned       1590       24.8    39.55    58.68    111058
```

Commit p50 on RocksDB goes 50.0 ms → 39.6 ms and p99 69.1 ms → 58.7 ms, on top of the
101.97 ms → 57.28 ms that the engine swap already bought. Reads against the pinned
table stop touching storage altogether.

Read the write columns with the run-to-run spread in mind: on this 4-core VM the
`events/s` figure moves by roughly ±5 % between identical runs, which is why §2 shows a
*small* write regression here (1244 → 1208) and a gain in the paired runs in §2 below
(1046 → 1157, 1140 → 1272). §2's effect on writes is real but within a couple of
standard deviations of the noise; its effect on reads is not, and neither is §1's on
either axis.

---

## 1. Pinning hot tables in memory

**The mechanism already exists and is hardcoded to system tables.**

`BackendInMemoryIndexes` holds a table's enabled database indexes as an in-memory
ordered map, maintained by every commit. `DatabaseIndexSnapshot::start_range_fetch`
consults it *before* the transaction cache and before the reader
(`crates/indexing/src/database_index_snapshot.rs:387`), and a hit returns the whole
interval with `next_cursor: End` — no paging, no persistence call, no index-cache
lookup. This is exactly SpacetimeDB's memory residency, applied per table.

Which tables get this was a fixed list — `APP_TABLES_TO_LOAD_IN_MEMORY` in
`crates/model/src/lib.rs:654`, thirteen system tables — passed once at startup to
`Database::load_indexes_into_memory`, which accepts an arbitrary `BTreeSet<TableName>`.

### What changed

A new environment knob, `TABLES_TO_LOAD_IN_MEMORY`, is unioned into that set:

```sh
TABLES_TO_LOAD_IN_MEMORY=deviceLatestLocations,fleetConfig
```

`tables_to_load_in_memory()` in `crates/model/src/lib.rs` returns the union; the single
call site now uses it. That is the whole change on the Convex side — about thirty lines,
additive, in a file whose surrounding list rarely moves. `crates/indexing` gains one
line: the per-table load is logged at `info` with its name, key count and byte count,
because a pinned table is held for the life of the process and an operator needs to see
how big it is.

### Why this is safe for the disk-scale constraint

The in-memory map holds **only the live row set**: `DatabaseIndexMap::insert` replaces by
key and `remove` deletes (`crates/indexing/src/in_memory_indexes.rs:451`). It is not a
version log. Memory tracks the table's *size*, not its write volume — a table rewritten
a million times a day costs the same as one written once. Historical reads still work
because each `Snapshot` carries its own persistent (structurally shared) clone of the
map, so a transaction reading at an older timestamp sees that snapshot's version.

Writes are unaffected: the table is still written to persistence exactly as before. This
is a read-side residency choice, not a durability one.

### What to pin, and what not to

Pin a table when it is **small, bounded, and read on the hot path** — a
latest-state-per-entity row, a config or lookup table consulted by every request. In the
benchmark, `deviceLatestLocations` is one row per device: the ingest path reads it and
rewrites it on every event, and the dashboard reads it constantly.

Do **not** pin anything that grows with time. `deviceLocations` — the event history — is
the counterexample: pinning it would hold the whole time series in RAM, which is the
in-memory-transaction limit this fork exists to avoid. There is no eviction and no size
cap; the knob will do what you tell it to.

### Measured

```
rocksdb, verify=0            events/s  commits/s   p50 ms   p99 ms   reads/s
  no pinned tables               1208       18.9    50.37    82.02     10606
  deviceLatestLocations          1590       24.8    39.55    58.68    111058
```

Reads go up 10×, because they stop reaching storage. Writes go up 32 %, because the
`deviceLatestLocations` upsert reads before it replaces, and that read is now free.

### Limits worth knowing

- **Startup only.** `Database::load_indexes_into_memory` loads at the latest snapshot
  and then panics if a commit landed while it was loading
  (`crates/database/src/committer.rs:798`), so it is only safe while nothing else is
  committing — which in practice means during startup. A table created after the process
  started is picked up on the next restart, and a name that does not resolve to an
  existing table is silently skipped. Deploy the schema, then restart.
- **It moves work onto the committer thread.** Every commit applies index updates to the
  pinned map, in `Snapshot::update`, on the single committer task. The measurement above
  is net of that, but pinning many large tables would eventually invert the trade.
- **Text and vector indexes are not pinned** — `load_enabled_for_tables` skips them
  deliberately.

Try `--pin` in `persistence_bench` before setting it in a deployment:

```sh
persistence-bench --backends rocksdb --pin deviceLatestLocations
```

---

## 2. The index cache verifies every hit against persistence

`INDEX_CACHE_VERIFY_PERCENT` defaults to **100** (`crates/common/src/knobs.rs:1995`).
At that setting, `IndexCacheReader::index_page` does this on a cache **hit**
(`crates/indexing/src/database_index_snapshot.rs:227`):

```rust
let verify_cache_results = cfg!(any(test, feature = "testing"))
    || rand::random_range(0..100) < *INDEX_CACHE_VERIFY_PERCENT;
if verify_cache_results {
    let index_page = self.reader.index_page(...).await?;   // the read the cache just avoided
    if index_page != cached_page { /* … */ panic!(…) }
}
```

It performs the persistence read anyway, compares the two pages, and panics on a
mismatch. So on a stock self-hosted deployment the index cache **cannot save a read** —
a hit costs a cache lookup *plus* the full read *plus* a page comparison, and only a
miss is as cheap as having no cache at all.

This is a shadow-mode correctness check, and a sensible one to run while a cache is being
rolled out. It is not something an ingest deployment should be paying for. Setting

```sh
INDEX_CACHE_VERIFY_PERCENT=0
```

gives the cache its intended behaviour. The default is left alone here: it belongs to
upstream, and lowering it is a deployment decision, not a fork decision. Note that
`cfg!(any(test, feature = "testing"))` forces verification regardless, so the test suite
keeps checking the invariant no matter what the environment says.

**Measured**, same workload, RocksDB, two runs each:

```
verify=100    events/s 1046 / 1140     reads/s  5561 /  5770
verify=0      events/s 1157 / 1272     reads/s  9996 / 11043
```

Reads 1.8–1.9×; writes about 11 %, because the read-modify-write path benefits too.

A middle setting — `INDEX_CACHE_VERIFY_PERCENT=1` — keeps a sampled check running in
production at roughly 1 % of the cost. That is the setting to prefer if you want to
keep the safety net.

---

## 3. Retention reads the whole document log a second time

`LeaderRetentionManager::expired_index_entries`
(`crates/database/src/retention.rs:666`) streams **every revision pair** written in the
retention window — `load_revision_pairs` over a `TimestampRange`, which resolves each
row's previous revision — and then, for each superseded revision and each index on its
table:

```rust
let index_key = prev_rev.index_key(index_fields).to_bytes();
let key_sha256 = Sha256::hash(&index_key);
```

recomputes the index key, hashes it, and yields one delete, plus a second for the
tombstone when the key changed. So the steady-state cost of a document write is not just
the write: it is also a log read, a previous-revision resolve, an index-key
recomputation and hash per index, and up to two row deletes per index — all of it
landing on the same core as the ingest itself, four minutes later
(`INDEX_RETENTION_DELAY`, default 240 s).

For an RLE ingest path this is the dominant hidden multiplier, because *every merge is a
replace*: it always produces a superseded revision.

The structural fix is the one `crates/rocksdb_persistence/README.md` already flags as
not implemented — a **version-aware compaction filter**. During compaction a key's
versions are adjacent and, with the `!ts` encoding, in descending order; a filter can
keep the first version at or before the retention watermark and drop the rest, which is
how CockroachDB and TiKV do MVCC GC. That deletes the second pass *and* the delete
writes: retention stops being work and becomes a property of compaction. It needs its
own correctness tests around never-updated keys first — a naive
"drop everything older than the watermark" filter would delete the only surviving
version of a row that has not been written in a while.

Until then the knobs are `INDEX_RETENTION_DELETE_PARALLEL` (default 4),
`INDEX_RETENTION_DELETE_CHUNK` (512) and `INDEX_RETENTION_DELAY`. Lengthening the delay
does not reduce the work, only defers it and widens the version fan-out that descending
scans skip past.

---

## 4. Index keys are computed twice per row

Two separate walks over every enabled index of every written document, per commit:

- `Committer::compute_writes` (`crates/database/src/committer.rs:962`) →
  `Snapshot::update` → `IndexRegistry::index_updates`
  (`crates/indexing/src/index_registry.rs:189`), which calls `self.index_keys(document)`
  for the old and new documents. This runs **on the committer task**, which is the one
  serialized point in the whole write path.
- `index_keys_from_full_documents` (`crates/database/src/write_log.rs:138`) →
  `IndexRegistry::document_index_keys` (`index_registry.rs:292`), which calls
  `index_keys_for_index` over the same indexes and the same documents, to build the write
  log entry that drives subscriptions and the index cache. This runs off the committer
  task but on the same core.

The two produce different shapes (`DatabaseIndexUpdate` vs `IndexKeyUpdate`) from
identical inputs. Deriving the second from the first is a refactor inside
`IndexRegistry` — no trait moves, no API change — but it touches a file upstream changes
often, so it is flagged rather than done. On a multi-core deployment it is invisible;
on the 1-CPU cell in `infra/k8s/base/cells/`, per-row CPU is the budget.

---

## 5. The module graph is re-instantiated on every call

Convex already has the fix and it is opt-in per module. `analyze.rs:449` looks for an
exported `experimental_reuseContext`:

```ts
export const experimental_reuseContext = true;
```

With it set, a successful execution saves its V8 context and `ModuleMap`
(`crates/isolate/src/context_cache.rs:116`), and the isolate scheduler routes the next
call for that module to the worker holding it (`crates/isolate/src/client.rs:1421`,
`CachedContexts::can_serve_request`). Without it, every invocation gets a fresh
`v8::Context` and re-instantiates and re-evaluates the entire module graph — for a
mutation module that transitively imports the schema and shared helpers, that is the
largest fixed cost of a call. Compiled code is cached (`ModuleCodeCacheResult`);
instantiation and evaluation are not.

The reuse is validated, not assumed: module-initialisation reads are snooped into a
`ContextReadSet` and checked before the context is handed back, so a context is never
reused across a change to what it read.

Two things to know before enabling it:

- **Each isolate caches exactly one context** (`context_cache.rs:45`, a single
  `Option<SavedContext>`). A parent module and a nested module that both set the flag
  will clobber each other. Set it on the hot entry point, not on everything.
- It applies to nested `ctx.runMutation` too — `run_nested` takes the same path
  (`crates/isolate/src/environment/udf/mod.rs:765`) — which matters if the ingest
  fan-out calls per-event mutations. Nested calls run in the *same* isolate, thread and
  transaction, so the cost of one is exactly a context plus a module graph, not a
  scheduler round trip.

This is an app-side change and costs one line.

---

## 6. Only reads batch across the isolate boundary

`AsyncSyscallBatch` (`crates/isolate/src/environment/udf/async_syscall.rs:244`) batches
exactly three syscalls:

```rust
"1.0/get"             => Self::Reads(…),
"1.0/queryStreamNext" => Self::Reads(…),
"1.0/storageGetUrl"   => Self::StorageGetUrls(…),
_                     => Self::Unbatched { name, args },
```

Up to `MAX_SYSCALL_BATCH_SIZE` (16) of them go across in one crossing. Everything
else — `1.0/insert`, `1.0/replace`, `1.0/shallowMerge`, `1.0/remove` — is `Unbatched`:
one JSON round trip and one promise resolution each.

Two consequences for a batched ingest mutation:

**Sequential awaits cannot batch.** Reads only batch if they are *pending at the same
time*. The RLE neighbour lookups in aa-app's `insertNewLocationPayload`
(`convex/deviceTraits/locations.ts:578` and `:587`) are two sequential `await`s with an
early return between them, so they are two crossings, not one. `Promise.all` on the pair
makes them one batched `queryStreamNext`. The cost is that the early return
(`if (last?.timestamp === timestamp) return null`) then happens after both reads, so the
skipped case reads one extra row and records one extra read-set interval — a slightly
wider OCC footprint for half the crossings.

**The per-event loop serializes everything.** `convex/telemetry/ingestBatch.ts:128`
applies events with `for (…) { await applyIngestEvent(…) }`. Events for the *same*
device must stay ordered — each RLE decision depends on the previous event's write — but
events for *different* devices are independent. Grouping the batch by device and running
the groups concurrently would let reads from different devices land in the same syscall
batch, which is where the 16-deep batching actually pays. That is an app-side
restructure, and the only one on this list with a real correctness argument to make
first.

Batching writes would be a backend change: `insert`/`replace` are pure transaction-local
operations with no I/O, so a `Writes` variant of `AsyncSyscallBatch` is mechanically
straightforward, but it changes the order in which per-write errors surface to JS. Not
attempted here.

---

## 7. OCC backoff

Unchanged from `crates/rocksdb_persistence/README.md`:
`UDF_EXECUTOR_OCC_INITIAL_BACKOFF_MS` defaults to 100 ms, and a conflict re-runs the
whole mutation after that delay. For a mutation that does single-digit milliseconds of
work, the first backoff alone is an order of magnitude more than the work being retried.
Check the function log for actual conflict rates before touching it — if OCC retries are
not what you are seeing, lowering the backoff buys nothing.

A batched ingest mutation is unusually exposed here because its read set is the union of
every event's reads: 64 events × 3 index reads is a wide surface for another writer to
collide with. Partitioning the upstream stream by device — so that one device's events
only ever reach one in-flight mutation — narrows it structurally.

---

## 8. Subscriptions

`SubscriptionManager::advance_log` (`crates/database/src/subscription.rs:492`) walks
each commit's index writes and matches them against the interval map of subscribed
queries. Indexes with no subscribers cost a map lookup and nothing else, so an ingest
table nobody watches is free. An ingest table a dashboard *does* watch costs one
invalidation — and one query re-run — per commit, at commit rate.

If live queries are pointed at the ingested tables, this is worth measuring before
anything else on this list: `SUBSCRIPTION_INVALIDATION_DELAY_THRESHOLD` and
`NUM_SUBSCRIPTION_MANAGERS` exist for exactly this shape, and a dashboard that polls a
narrow aggregate is cheaper than one that subscribes to the raw stream.

---

## 9. What was measured and what was not

Measured on `crates/persistence_bench`, which drives the real `Database` — committer,
conflict checking, index registry and cache, write log, retention, persistence — but
**not** the V8 isolate, the sync worker or HTTP: §1 and §2.

Reasoned from the source, not measured: §3–§8. §5 and §6 in particular sit above the
benchmark's boundary, so their effect on end-to-end mutation latency is unquantified
here. They are listed because the code says the cost is there, not because a number
says how big it is.
