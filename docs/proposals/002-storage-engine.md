# Replacing the storage engine without touching the Convex API

**Status:** phases 1–2 implemented — see `crates/rocksdb_persistence/`
**Date:** 2026-08-25
**Supersedes:** Layer 3 of [001-fast-write-path.md](./001-fast-write-path.md) (ephemeral tables), withdrawn — it changed developer-visible semantics
**Constraint:** zero change to the Convex developer API. No new `defineTable` options, no
new `ctx.db` methods, no changed query semantics. Everything here lives strictly below
`Arc<dyn Persistence>`.

---

## 0. Summary

Convex's storage schema is already an append-only, multi-version log: a document update
never overwrites a row, it writes a new `(id, ts)` revision plus a new
`(index_id, key, ts)` index entry. It then asks Postgres — an engine built around
in-place page updates — to store that log, paying B-tree maintenance, full-page writes
and vacuum for a workload that never overwrites anything.

An LSM engine's on-disk model *is* that log. And Convex asks remarkably little of its
storage layer: every read through `PersistenceReader` is explicitly timestamp-scoped —
`index_scan` takes a `read_timestamp`, `load_documents` a `TimestampRange`,
`previous_revisions` a set of `(id, ts)` pairs — so **Convex implements MVCC in its own
data model and needs none from the engine.** What is left is ordered keys, range scans,
point gets, and atomic durable batches: the minimum any KV store offers, and a set of
requirements almost every candidate satisfies. That makes the engine choice unusually
low-risk and unusually reversible.

The surface-level footprint is a new crate plus one enum variant threaded through the
handful of `match` arms in two small, rarely-touched files (§7). Nothing above the trait learns that anything changed, so no
developer-facing API moves.

---

## 1. What an LSM tree is, and why it fits

### B-tree (Postgres, InnoDB, LMDB, redb)

Keys live in a sorted tree of fixed-size pages, updated **in place**. A write finds the
target leaf, modifies it, logs the change to a WAL, and eventually flushes the whole page.

- **Write cost:** a random read of the leaf (if not cached), a page modification, a WAL
  record, and — in Postgres — a *full-page write* the first time a page is touched after
  each checkpoint. Every secondary index repeats this. Pages split as they fill.
- **Read cost:** O(log n) page traversals, and exactly one place to look. Predictable,
  low-latency.
- **Space:** one live copy per key, plus dead tuples awaiting vacuum.

### LSM tree (RocksDB, fjall, SurrealKV, LevelDB, Cassandra, ScyllaDB)

Writes go into a sorted in-memory table (a **memtable**, usually a skiplist) and a
sequential WAL append. When the memtable fills, it is written out whole and already sorted
as an immutable **SSTable**. Background **compaction** merges SSTables into larger,
level-organised files. Nothing is ever modified in place: an update is a newer entry that
shadows the old one, and a delete is a **tombstone**.

- **Write cost:** a memtable insert (sub-microsecond, no I/O) plus one sequential WAL
  append. No random reads, no page splits, no full-page writes. The sorting work is
  deferred to background compaction, off the critical path.
- **Read cost:** check the memtable, then each level's SSTables — **read amplification**.
  Mitigated by per-file **bloom filters** (skip files that cannot contain the key) and a
  block cache. A point get in a well-tuned LSM is typically one bloom check plus one block
  read.
- **Space:** shadowed versions linger until compaction reclaims them — **space
  amplification** — and compaction itself costs background I/O (**write amplification**,
  though sequential rather than random).

### The trade, and why this workload sits on the right side of it

An LSM converts random writes into sequential writes and pays for it with background
compaction and read amplification. That is a bad trade for an update-in-place OLTP table
and a good trade for an append-heavy versioned log.

Convex's storage is the latter, by construction:

| Convex property | Consequence |
|---|---|
| A document update writes a new `(id, ts)` row, never an update | Nothing is ever overwritten — the B-tree's in-place machinery is dead weight |
| One `indexes` row per index, per write (`index_registry.rs:189-222`) | Index maintenance is pure appends, not tree rebalancing |
| Ingest keys cluster by `(device, time)` — near-sequential | Compaction stays cheap; new data lands in a narrow key range |
| Old versions are deleted wholesale by the retention worker | Tombstone-and-compact is the natural shape; so is Postgres `DELETE` + vacuum, but the LSM version is sequential |
| Reads are dominated by a hot recent window, backed by a 512 MB `IndexCache` in front | Read amplification lands mostly on cold data the app rarely touches |

The one place the trade genuinely bites is a large **descending** index scan over
many retained versions — see §5.

---

## 2. What SurrealDB uses

SurrealDB is worth looking at because it made this exact call, twice.

Its storage layer is pluggable, with RocksDB the default for single-node persistent
deployments, TiKV for distributed clusters, and an in-memory engine for ephemeral use
([architecture docs](https://surrealdb.com/docs/surrealdb/introduction/architecture)).
Since 2.0 it also ships its own engine, **SurrealKV**.

The interesting detail is SurrealKV's history. It launched built on a **versioned adaptive
radix trie (VART)** — an in-memory index structure — and was subsequently **rewritten onto
an LSM tree**, explicitly to handle datasets larger than available memory
([README](https://github.com/surrealdb/surrealkv)). That is the same constraint driving
this document, resolved the same way, by a team that had already shipped the in-memory
version.

SurrealKV today is Apache-2.0, offers snapshot isolation with MVCC, time-travel reads
(`get_at`, `history`), two durability levels (`Eventual` = OS buffer, `Immediate` = fsync
before `commit()` returns), a WAL, and value-log separation for values over ~1 KB. It
remains **beta**; SurrealDB's own guidance is that RocksDB is the conservative choice for
production single-node deployments
([deployment guidance](https://surrealdb.com/learn/fundamentals/performance/deployment-storage)).

Note that SurrealKV's built-in versioning is redundant here: Convex already does MVCC in
its data model. We would be paying for a feature we cannot use.

---

## 3. Candidates

| Engine | Model | License | Notes for this use |
|---|---|---|---|
| **RocksDB** (`rocksdb` 0.24 / librocksdb-sys 10.4.2) | LSM | Apache-2.0 / GPLv2 | The conservative choice. Column families map to our keyspaces; concurrent writers are coalesced into one WAL fsync by its write-group mechanism, which matches Convex's up-to-16 concurrent `Persistence::write` calls. Compaction filters are exposed in the Rust binding (`compaction_filter::Decision`). Cost: a C++ dependency (build needs libclang), and a very large knob surface. |
| **fjall 3.1** | LSM | MIT | Pure Rust. Isolated LSM trees per keyspace with an atomic cross-keyspace `WriteBatch` and per-batch `PersistMode` — a direct match for `Persistence::write`. Key-value separation for large values; compaction filters since 3.1. [Feature development is winding down in favour of stability](https://fjall-rs.github.io/post/fjall-3/), which is what you want under a system of record. Younger and less battle-tested than RocksDB. |
| **SurrealKV 0.9** | LSM | Apache-2.0 | Interesting lineage (§2), but beta, and its versioning duplicates what Convex already does. |
| **redb 2.x**, **LMDB** | Copy-on-write B-tree | MIT/Apache-2.0 | Excellent read latency, one write transaction at a time. Architecturally the same side of the trade as Postgres, minus the network — useful as a *control* in benchmarking, not as the answer for write-heavy ingest. |
| **sled** | Hybrid | MIT/Apache-2.0 | Still pre-1.0, and its README states the on-disk format will change in ways requiring manual migration before 1.0. Not for a system of record. |

**Recommendation:** benchmark RocksDB and fjall on the real hardware (§6). On the
reference VM RocksDB led on every column (§6.1), but neither engine was tuned and the two
converge once fsync dominates — so treat that as a reason to measure, not as the answer.
Absent a clear win for fjall on your disk, default to RocksDB: its operational track
record under exactly this workload is the tiebreaker, and pure-Rust is a preference
rather than a requirement.

---

## 4. The mapping: `Persistence` over an ordered KV store

### 4.1 Keyspaces and encodings

Five keyspaces (RocksDB column families / fjall keyspaces). `!ts` means
`(u64::MAX - ts).to_be_bytes()`, so newer versions sort first within a key.

```
dlog : ts[8] ‖ table_id[16] ‖ id[16]        -> document bytes    (the "heap")
doc  : table_id[16] ‖ id[16] ‖ !ts[8]       -> ()                (revisions by id)
dtab : table_id[16] ‖ ts[8] ‖ id[16]        -> ()                (per-table by ts)
idx  : index_id[16] ‖ index_key[..] ‖ !ts[8] -> tag[1] ‖ table_id[16] ‖ id[16]
glob : key[..]                              -> json
```

This mirrors the Postgres schema exactly: `dlog` is the `documents` heap keyed by its
primary key `(ts, table_id, id)`, and `doc` / `dtab` are the two secondary indexes
`documents_by_table_and_id` and `documents_by_table_ts_and_id`
(`crates/postgres/src/sql.rs:85-99`). Same physical work, one fewer process.

Two encoding wins fall out for free:

- **The `key_sha256` column disappears.** Postgres splits index keys into
  `key_prefix` + `key_suffix` and carries a SHA-256 of the whole key, purely because its
  primary keys cap at 2730 bytes (`sql.rs:118-129`). A KV store takes arbitrary-length
  keys, so that's 32 bytes and one hash per index row, gone.
- **`DISTINCT ON` becomes a seek.** The index-scan query
  (`crates/sqlite/src/lib.rs:157-177`) asks for "the newest version at or before
  `read_ts`, per distinct key". With `!ts` in the key, that is a single forward seek to
  `index_id ‖ key ‖ !read_ts` — no sort, no grouping.

### 4.2 Method-by-method

**`Persistence`**

| Method | Implementation |
|---|---|
| `write(docs, indexes, strategy)` | One atomic write batch across all keyspaces, with fsync per the durability setting. See §5.1 for `ConflictStrategy::Error`. |
| `write_persistence_global` | Put into `glob`, same batch machinery |
| `load_index_chunk(cursor, n)` | Ordered scan of `idx` from `cursor` |
| `delete_index_entries` | Batch deletes in `idx` |
| `delete(documents)` | Batch deletes across `dlog` / `doc` / `dtab` |
| `delete_tablet_documents(tablet, n)` | Prefix scan of `dtab`, delete |
| `import_documents_batch` / `import_indexes_batch` | Large write batches; ideally bulk SST ingestion (`SstFileWriter` + `ingest_external_file`) for snapshot imports |
| `is_fresh` / `shutdown` / `finish_loading` | Trivial; `finish_loading` is a good place for a manual compaction after an import |

**`PersistenceReader`**

| Method | Implementation |
|---|---|
| `load_documents(range, order)` | Scan `dlog` over `[ts_lo, ts_hi]`, forward or reverse. Values are inline — sequential. |
| `load_documents_from_table(tablet, range, order)` | Scan `dtab` over `tablet ‖ [ts_lo, ts_hi]`, point-get each value from `dlog` |
| `previous_revisions(ids)` | Per `(id, ts)`: seek `doc` at `table_id ‖ id ‖ !(ts-1)`, take the first entry whose prefix still matches, point-get from `dlog` |
| `previous_revisions_of_documents(queries)` | Exact point gets at `dlog[prev_ts ‖ table_id ‖ id]` |
| `index_scan(index_id, tablet, read_ts, range, order)` | See §5.2 |
| `get_persistence_global` | Point get in `glob` |
| `max_ts` | Reverse seek to the last key of `dlog`, versus `glob[MaxRepeatableTimestamp]` |
| `version` | Constant |

Everything above the trait — the committer, the index cache, retention, subscriptions,
streaming export, search and vector indexes — is untouched. Search and vector never call
`Persistence` at all; they go through `Searcher` and file storage.

---

## 5. The four things that are genuinely hard

Anyone who tells you this is a mechanical port has not read `ConflictStrategy`.

### 5.1 `ConflictStrategy::Error` is free in a B-tree and is not free in an LSM

Every commit calls `persistence.write(&documents, &indexes, ConflictStrategy::Error)`
(`crates/database/src/write_batcher.rs:220`). In Postgres that is a plain `INSERT`
and the primary key raises the error for nothing (`crates/postgres/src/sql.rs:407`). An LSM has no unique
constraint: a `put` of an existing key silently shadows it. Detecting the collision costs
a point get per key — six per document at three indexes.

Those are bloom-filtered point gets, so roughly 1–3 µs each: about 10–20 µs per document,
or half a core at 50k documents/sec. Affordable, but not free, and it is on the write path.

It cannot simply be dropped. `Database::initialize` writes system-table bootstrap rows
straight to persistence, outside a real `Transaction`, and leans on this check as its only
protection against ID collision — the code says so:

> This is a little unsafe because we generated random IDs for this documents with
> `TransactionIdGenerator`, but aren't using a real `Transaction` so we don't have our
> usual protections against ID collisions. Our `ConflictStrategy::Error` should notice the
> problem but consider improving in the future (CX-2265).
> — `crates/database/src/database.rs:1618-1628`

But that path runs once, at database creation, over a handful of documents. On the *commit*
path the check is redundant: commit timestamps come from `next_commit_ts` and strictly
increase, so `(id, ts)` cannot collide, and `check_generated_ids`
(`crates/database/src/committer.rs:1566-1600`) already rejects reused document IDs a layer
up.

So: implement `ConflictStrategy::Error` faithfully with real point gets — imports and
bootstrap get exactly today's guarantee — and put the commit-path check behind a knob.
Whether that knob defaults on or off is a real decision, and it must be a **documented**
one, written into the crate docs, because it is the one place where the new engine is
weaker than the old one.

### 5.2 Descending index scans read every retained version

With `!ts` in the key, a **forward** scan meets each key's newest version first: one entry
read per key, then skip to the next key prefix. Ideal.

A **reverse** scan meets each key's versions in ascending `ts` order, so it reads every
retained version of a key before reaching the one it wants. The state machine is simple —
within a key's run, hold the last entry with `ts <= read_ts` and emit it when the prefix
changes — but the work is proportional to versions retained per key.

Index retention is four minutes by default (`knobs.rs:642`), so in steady state that is
typically one to three versions and the amplification is negligible. It stops being
negligible for a hot key updated many times inside the retention window — a per-device
"latest location" row, for instance. Two mitigations, both standard: seek per key rather
than scanning the run, or encode `ts` ascending and pay the cost on forward scans instead.
**Measure which direction dominates your query mix before choosing.** `.order('desc')` is
common in Convex apps, so do not assume forward scans win by default.

### 5.3 Retention could move into compaction — but not naively

This is the biggest structural prize and the easiest thing to get wrong.

Convex's retention worker walks the document log, finds each document's previous revision,
and deletes index entries at or below that revision's timestamp
(`crates/database/src/retention.rs:663-760`). It is a "delete superseded versions" walk,
not a blanket time cut. A compaction filter that simply dropped everything older than a
watermark would delete the only surviving version of a key that has not been written in a
while — silent data loss.

A *version-aware* filter is correct and is exactly how CockroachDB and TiKV do MVCC GC:
during compaction all versions of a key are adjacent and in descending-`ts` order, so the
filter keeps the first version at or before the retention watermark and drops every older
one. Both RocksDB and fjall 3.1 expose the hook.

Done right this makes retention nearly free — it rides along with compaction I/O that was
happening anyway, instead of issuing explicit `DELETE`s that compete with ingest for the
same Postgres core. **Do not attempt it in phase one.** Phase one maps
`delete_index_entries` and `delete` to ordinary batch deletes and keeps the existing
retention worker running unchanged. Phase two replaces it, behind a flag, with correctness
tests that specifically cover the never-updated-key case.

### 5.4 There is no lease

`PostgresPersistence` fences a stolen leadership with two extra statements per write
(`crates/postgres/src/lib.rs:1844`, `1867`). An embedded store has no such concept — the
process holding the file lock is the writer, full stop.

For a single-node cell that is strictly simpler and safer: exclusive file access is a
stronger guarantee than an advisory row. But it removes the "another backend stole the
lease, shut down cleanly" path, so a StatefulSet must guarantee one pod at a time — which
`replicas: 1` with `OnDelete` already does. It also means **no read replicas and no
failover via a shared database**: recovery is restore-from-backup plus WAL, not
promote-a-follower. That is a real operational change and belongs in the decision, not in
a footnote.

---

## 6. Measure it: `kvbench`

Published benchmarks for these engines measure `fillrandom`, which is not the shape Convex
writes. `tools/kvbench/` models the real shape instead:

- One document write expands to **1 + 3 + N keys** — `dlog`, `doc`, `dtab`, plus one
  `idx` entry per index — matching `index_updates` and the Postgres DDL.
- Writes are grouped into atomic batches of `COMMITTER_MAX_WRITE_BATCH_DOCUMENTS`
  documents, one batch per `Persistence::write` call.
- `--durable` fsyncs every batch; `--relaxed` does not, which is the like-for-like
  comparison against a Postgres running `synchronous_commit=off` — as the aa-app cells do.
- Reads exercise the two shapes that matter: the MVCC `index_scan` from
  `crates/sqlite/src/lib.rs:157-177`, and the `previous_revisions` point lookup.
- **SQLite is included as a control**, running Convex's own DDL from
  `crates/sqlite/src/lib.rs:624+`. Same process, same disk, no network — so the gap
  against the KV engines isolates the storage model from the client/server boundary.

Run it on the cell's actual PVC storage class, not on a laptop: on network-attached block
storage, fsync latency dominates, and that is exactly the axis these engines differ on.

```
cargo run --release -- --docs 200000 --batch 64 --indexes 3 --relaxed
cargo run --release -- --docs 200000 --batch 64 --indexes 3 --durable
```

### 6.1 A first run

Measured here, on a 4 vCPU Xeon @ 2.10 GHz / 15 GB RAM VM with a virtio disk of
unknown backing. **These are not your numbers** — the point of the harness is that you
run it on the cell's PVC. They are included because the *shape* of the result is
informative and reproducible.

500,000 documents, batches of 64, 3 indexes, 400-byte values — so 3,000,000 physical
keys. All four engines returned identical scan output (192,000 rows), checked before any
timing was reported.

**`--relaxed` (no fsync — what `synchronous_commit=off` gives you today)**

| engine | docs/s | rows/s | p50 ms | p99 ms | scans/s | prevrev/s | disk MB |
|---|--:|--:|--:|--:|--:|--:|--:|
| rocksdb | **114,813** | 688,880 | 0.55 | 0.88 | **42,046** | 153,366 | 634 |
| fjall | 53,414 | 320,483 | 0.96 | 2.40 | 9,099 | 94,000 | 685 |
| sqlite *(control)* | 42,940 | 257,639 | 0.90 | 6.20 | 2,208 | 148,187 | 570 |
| redb | 12,226 | 73,355 | 5.21 | 7.54 | 54,068 | **179,951** | 1,029 |

**`--durable` (fsync every batch)**

| engine | docs/s | rows/s | p50 ms | p99 ms | scans/s | prevrev/s | disk MB |
|---|--:|--:|--:|--:|--:|--:|--:|
| rocksdb | **53,121** | 318,727 | 1.11 | **1.84** | **41,278** | 156,436 | 634 |
| fjall | 45,500 | 273,003 | 1.03 | 2.93 | 13,347 | 89,961 | 685 |
| sqlite *(control)* | 16,448 | 98,690 | 2.02 | 18.80 | 2,186 | 147,329 | 570 |
| redb | 11,904 | 71,425 | 5.34 | 8.00 | 51,180 | 160,666 | 1,029 |

Five things worth reading off these.

**The axis is B-tree versus LSM, not embedded versus server.** redb is embedded,
in-process and has no network — and it is *slower than the SQLite control on writes*
(0.28× relaxed, 0.72× durable) while being the **fastest engine on reads**. That is the
trade from §1 showing up exactly where the theory says it should. Removing the socket is
not what wins here; changing the storage model is.

**The scan gap is larger than the write gap.** RocksDB scans ~19× faster than the
relational control in both modes. That is `DISTINCT ON` + `GROUP BY` + join versus a
single seek, and it lands on every `withIndex` query in the app, not just on ingest.

**Durability is where the LSM's shape pays.** Under fsync the write gap *widens*
(2.7× → 3.2×) and the tail separates hard: SQLite p99 18.80 ms against RocksDB 1.84 ms.
A sequential WAL append amortised by group commit degrades gracefully; B-tree page
flushing does not. This matters more than the throughput column if you ever want
`synchronous_commit` back on.

**The two LSMs are not interchangeable, and this run does not settle which.** RocksDB is
2.1× fjall relaxed but only 1.17× durable — under fsync the disk dominates and the
implementations converge. Neither has been tuned. Do not pick between them on this table.

**The SQLite control is a floor, not a stand-in for Postgres.** It runs in-process, with
no socket, no lease statements (§5.4), no MVCC row versions and no checkpoint full-page
writes — all of which the real Postgres pays on top. The measured 2.7–3.2× therefore
*understates* the gap against the deployment. The harness cannot say by how much; only a
run against the actual cell can.

One more understatement: the harness is single-threaded, while Convex issues up to 16
concurrent `Persistence::write` calls and RocksDB coalesces concurrent writers into one
WAL fsync via its write-group mechanism (`write_thread.h:319-333`). Real concurrency
should widen RocksDB's durable-mode lead, not narrow it.

### 6.2 Known gaps in the harness, deliberately

It measures forward index scans only, so it does not show the §5.2 descending-scan cost.
It uses layout A (document bytes in `doc`) rather than the layout B recommended in §4.1,
which changes the read numbers but not the write numbers. Its index keys are unique, so
version shadowing is barely exercised — add an index keyed on `device` alone to measure
that. And it does not charge the KV engines for §5.1's uniqueness check. See
[`tools/kvbench/README.md`](../../tools/kvbench/README.md) for the full list. Each is a
small extension; add the ones that matter to your decision rather than trusting the
defaults.

---

## 7. Phasing

**Phases 1 and 2 are built.** `crates/rocksdb_persistence/` implements both traits
over the five column families described in §4, and `--db rocksdb` selects it. What
changed outside the new crate is one `DbDriverTag` variant threaded through its match
arms in `crates/clusters/` and `crates/db_connection/`, plus one workspace dependency
entry. The committer, index cache, retention, subscriptions, streaming export, search
and vector indexes are untouched, and the Convex developer API is identical.

Three things are worth recording about what the implementation turned up.

**RocksDB was already in the tree.** `crates/vector` links it for its qdrant segments,
and only one package in a Cargo graph may provide a native library — so this backend is
pinned to the same `rocksdb 0.22` rather than introducing a second copy. That removes
the "new C++ dependency" objection from §8 entirely: the dependency was already there
and already shipped.

**Index keys needed escaping.** `idx` concatenates a variable-length index key with a
fixed-length timestamp, and naive concatenation is not order-preserving: `[1,2] ‖ !ts`
sorts *after* `[1,2,3] ‖ !ts`, because `0xFF… > 0x03`. Convex index keys are not
prefix-free, so the variable-length component is escaped into a self-terminating form
first. `keys.rs` proves the property with a proptest, and there is an integration test
that fails without it.

**Descending scans cost the same as ascending.** §5.2 predicted a reverse scan would
have to read every retained version of a key. It does not: resolving a key is a forward
seek to `key ‖ !read_ts` in either direction, and only the traversal between keys
differs — `seek(successor(prefix))` ascending, `seek_for_prev(prefix)` descending. The
per-key cost is one seek each way. What remains true is that a key with many versions
inside the retention window has more of them to skip past.

### Remaining phases

| Phase | Work | Exit criterion |
|---|---|---|
| 0 ✅ | Run `kvbench` on cell hardware, both durability modes | A measured ratio against the SQLite control on *your* disk. On the reference VM it was 2.7–3.2× on writes and ~19× on scans (§6.1), against a control that is already faster than the real Postgres. If your hardware shows materially less, stop — the bottleneck is elsewhere (see 001 §2) |
| 1 ✅ | `crates/rocksdb_persistence/`: the five column families, both traits, batch deletes, existing retention worker unchanged | Behavioural suite green: multi-version reads, tombstones, interval bounds and ordering, paging in both directions, `previous_revisions`, conflict detection, all three retention deletes, globals, `max_ts`, durability across a reopen |
| 2 ✅ | Wire-up: one `DbDriverTag` variant (`crates/clusters/src/db_driver_tag.rs`) and one arm each in `persistence_seed`, `connect_persistence`, `connect_persistence_reader` | `--db rocksdb <path>` selects the backend |
| 3 | Shadow-write: run both backends, compare reads, alert on divergence | A week clean under production ingest. **Not started** — do this before it holds anything you cannot lose |
| 4 | Retention via compaction filter (§5.3), behind a flag | Correctness tests covering never-updated keys. **Not started** — deliberately, see the crate README |

Phases 1 and 2 are the whole surface-level footprint: **one new crate, plus one enum
variant threaded through two existing files** (`crates/clusters/src/db_driver_tag.rs` and
`crates/db_connection/src/lib.rs`). Both are small, stable files that upstream rarely
touches, so rebases stay trivial — and the developer-facing Convex API is identical at
every phase.

---

## 8. Honest assessment

The strongest argument for this change is not that LSMs are faster than B-trees in the
abstract — that depends entirely on the workload. It is that **Convex's storage schema is
an append-only versioned log, and Postgres is being asked to emulate one on top of
update-in-place pages, in a separate process, over a socket, on one CPU.** An embedded LSM
removes the emulation, the process and the socket at once.

The strongest argument against is operational, not technical: Postgres is a database your
team can already debug, back up, replicate and hire for. `crates/kv_persistence` would be
software you own, in the write path of your system of record, with the §5.1 uniqueness
weakness and the §5.4 loss of lease-based failover as permanent properties.

Phase 0 exists so that trade is made against a number rather than an argument. The
reference run (§6.1) says the number is likely to be worth having — 2.7–3.2× on writes and
~19× on index scans against a control that is *already faster than the real Postgres* —
but it says so on a VM whose disk is nothing like a cell PVC. Run it there before writing
any of phase 1.
