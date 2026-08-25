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
| `ROCKSDB_SYNC_INTERVAL_MS` | `0` (off) | flush and fsync the WAL on a timer instead of per write; overrides `ROCKSDB_SYNC_WRITES` |
| `ROCKSDB_CHECK_CONFLICTS` | `true` | enforce `ConflictStrategy::Error` |
| `ROCKSDB_BLOCK_CACHE_BYTES` | derived from the cgroup limit | cached data, index and filter blocks *and* memtable charge; the largest, but not the only, consumer |
| `ROCKSDB_BLOCK_CACHE_PERCENT` | 25 | share of the container's memory limit to derive that from, when it is not set explicitly |
| `ROCKSDB_WRITE_BUFFER_BYTES` | ¼ of the cache | ceiling on memtable memory across all column families — a share of the cache, not memory on top of it |
| `ROCKSDB_MEMTABLE_BYTES` | 64 MiB | per-column-family memtable |
| `ROCKSDB_BACKGROUND_JOBS` | cores | flush and compaction threads. A PROCESS budget, not a per-database one: RocksDB's default `Env` is a singleton whose pools are shared, and this grows them to the largest value any database asked for rather than to their sum |
| `ROCKSDB_MAX_OPEN_FILES` | `-1` (unlimited) | table-file descriptors per database. Unlimited is right for one database and wrong for several — see *Several databases in one process* |
| `ROCKSDB_BLOB_THRESHOLD_BYTES` | 4096 | document size above which bodies move to blob files; `0` disables |
| `ROCKSDB_SCAN_PAGE_ROWS` | 1024 | rows per page in the streaming read paths |
| `ROCKSDB_SHUTDOWN_TIMEOUT_SECONDS` | 30 | how long `shutdown` waits for compactions |
| `ROCKSDB_HEALTH_POLL_SECONDS` | 15 | how often to check for a latched background error and a stalled write |
| `ROCKSDB_WRITE_STALL_TIMEOUT_SECONDS` | 120 | how long one write may be in flight, with no stall reported, before the process is stopped |
| `ROCKSDB_MIN_FLUSH_SILENCE_SECONDS` | 120 | floor on how long the WAL may go unflushed before the flusher is presumed dead |
| `ROCKSDB_BACKUP_DIR` | *unset* | where periodic backups go; unset disables them |
| `ROCKSDB_BACKUP_INTERVAL_SECONDS` | 3600 | how often the worker takes a generation (minimum 60) |
| `ROCKSDB_BACKUP_KEEP` | 24 | generations retained; `0` never prunes |

### The three sync modes

| Mode | Set with | On process crash | On host loss |
|---|---|---|---|
| every | `ROCKSDB_SYNC_WRITES=true` (default) | nothing | nothing |
| interval | `ROCKSDB_SYNC_INTERVAL_MS=100` | up to one interval | up to one interval |
| never | `ROCKSDB_SYNC_WRITES=false` | nothing | unbounded |

Interval mode opens the database with `manual_wal_flush`, so a write leaves its records
in RocksDB's own buffer and a background thread moves them with `flush_wal(true)` on the
interval. It is the only mode that can lose a write the *process* never handed to the
kernel — `never` still writes through to the page cache and only loses data if the
machine goes down, but with no bound on how much.

What it buys depends entirely on the commit rate, because RocksDB already coalesces
concurrent writers into one write group and fsyncs once for the group. Measured on the
device-location workload (`crates/persistence_bench`, fsync p50 0.17 ms on the test VM):

| events per commit | every | interval=100ms | never | interval's gain |
|--:|--:|--:|--:|--:|
| 1 | 403 ev/s | 532 ev/s | 549 ev/s | **+32 %** |
| 4 | 682 ev/s | 829 ev/s | 940 ev/s | **+22 %** |
| 16 | 1111 ev/s | 1202 ev/s | 1076 ev/s | +8 % |
| 64 | 1159 ev/s | 1291 ev/s | 1279 ev/s | +11 % |

Interval mode captures most of what `never` offers while keeping the loss window bounded,
so there is little reason to prefer `never` over a short interval. But the gain is a
function of how many commits per second reach the disk: batch writes into larger
transactions and the fsync stops mattering, which is the cheaper fix. These ratios are a
floor for slower storage — on a network-attached volume where an fsync costs milliseconds
rather than 0.17 ms, the same commit rate pays more for it.

(An earlier version of this table reported larger gains. It was measured through a code
path that opened the database with `manual_wal_flush` and *no flusher thread*, so the
"interval" column was `never` plus buffering and never fsynced at all. The default now
runs background work, and these numbers are from a configuration that actually flushes.)

`ROCKSDB_SYNC_WRITES=false` is the analogue of Postgres's
`synchronous_commit=off`: it trades a bounded window of recent writes on host
loss for throughput, and is only safe where an upstream log can replay into
idempotent appliers. "An upstream log" has to mean one whose checkpoint cannot
survive a crash that this store did not: if the checkpoint lives in a *different*
durability domain it can roll back less than this one does, and the difference is
lost writes rather than replayed ones. See
[`docs/proposals/004-rocksdb-in-kubernetes.md`](../../docs/proposals/004-rocksdb-in-kubernetes.md) §2.

### Memory

RocksDB reads no cgroup limit of its own, and an overrun kills the *backend* rather
than a subprocess. So the cache is not left at a constant: unset, it is derived from
the container's memory limit — cgroup v2 `memory.max` or v1 `memory.limit_in_bytes`,
walking up the hierarchy and taking the smallest limit that binds, capped at physical
memory. `ROCKSDB_BLOCK_CACHE_PERCENT` (default 25) is the share taken, clamped to
[64 MiB, 4 GiB]. The chosen size and where it came from are logged at startup.

A quarter rather than a half because the backend hosting this crate also runs V8
isolates, whose heaps are the other large consumer in the process. Raise it on a
deployment whose working set matters more than its isolate headroom; set
`ROCKSDB_BLOCK_CACHE_BYTES` to override the derivation entirely.

**The cache is the budget's largest term, not the whole of it.** Outside it: compaction
buffers (one set per background job, defaulting to the core count), the table readers
kept open by `max_open_files` (unlimited by default), blob file readers, and the batches retention builds
before writing. Size the container with headroom over the cache rather than treating the
cache as a ceiling.

## Several databases in one process

Opening more than one database in a process — which
`crates/multitenant_backend` does, one per tenant — changes what some of these
knobs mean, because three of the resources they govern are per PROCESS and one is
per directory.

**Memory is shared, and has to be.** `ROCKSDB_BLOCK_CACHE_BYTES` is derived from
the *container's* memory limit, so it is a statement about the process. The block
cache and the write-buffer manager are therefore process-wide singletons: every
database opened here shares one cache and one memtable budget. N databases each
claiming a quarter of the container would oversubscribe memory by N and get the
backend OOM-killed, which is exactly the failure the derived sizing exists to
prevent. For the single database a single-tenant backend opens, sharing is
indistinguishable from not sharing.

**Descriptors, memtable shape and backups are per database, and the caller sets
them.** `OpenOptions::tuning` (`options::DbTuning`) overrides three knobs per open,
each defaulting to the environment:

| field | why a multi-database process must set it |
|---|---|
| `max_open_files` | descriptors are a per-process resource; N unlimited databases race each other to `EMFILE` |
| `memtable_bytes` | the shared write-buffer manager cannot overcommit, but at the single-tenant 64 MiB per family, N × 5 families all want the whole budget and the manager answers by force-flushing the largest memtable — a healthy bound turned into premature flushes |
| `background_jobs` | rarely needed: the pools are already shared (above). Lower it only to cap how much of the pool one database may occupy at once |

`OpenOptions::backup_dir` overrides `ROCKSDB_BACKUP_DIR` per database, and a
multi-database process **must** set it. `BackupEngine` numbers generations per
directory with no record of which database wrote them, so two databases pointed at
one directory interleave their chains and each one's `purge_old_backups` deletes
the other's.

`OpenOptions::instance` labels this database's own gauges. Backup age, WAL-flush
age and latched background errors are *levels*: N unlabelled databases publishing
to one series would each overwrite the others, and it would report whichever wrote
last. Rates and latencies stay unlabelled, because summing them across a process
is what you want and a label per database would only add cardinality.

Not shared, and worth budgeting for: each database runs its own health monitor
thread, its own WAL flusher (in interval mode) and its own backup worker.

## Configuring a write-heavy deployment

Swapping the engine removes the storage-side cost. Two Convex knobs above the trait
are worth revisiting at the same time, because they are sized for interactive OLTP
rather than sustained ingest. Neither default is changed here — they belong to every
Convex deployment, not just this backend — but on an ingest-shaped workload both are
usually wrong:

| Knob | Default | Why it matters here |
|---|--:|---|
| `INDEX_CACHE_VERIFY_PERCENT` | 100 | At the default, every index-cache **hit** also performs the persistence read it was meant to avoid and compares the two pages. The cache cannot save a read until this is lowered. Worth 1.8–1.9× on read throughput; `1` keeps a sampled check running. |
| `UDF_EXECUTOR_OCC_INITIAL_BACKOFF_MS` | 100 | On an OCC conflict the whole mutation re-runs after this delay. For a mutation that takes single-digit milliseconds, the first backoff alone is an order of magnitude more than the work being retried, and two conflicts put one mutation past 300 ms before it has accomplished anything. |
| `COMMITTER_MAX_WRITE_BATCH_DOCUMENTS` / `_BYTES` | 64 / 64 KiB | The committer combines independent commits into one `Persistence::write`. Larger batches amortise per-write overhead — which matters far more against a network database than against this one, but still helps. |

Measure before changing any of them: if OCC retries are not what your function log shows,
lowering the backoff buys nothing. The first two are measured on an ingest-shaped
workload in [`docs/proposals/003-beyond-the-storage-layer.md`](../../docs/proposals/003-beyond-the-storage-layer.md),
which also surveys what the ingest path still pays for above this trait.

## Backups

Set `ROCKSDB_BACKUP_DIR` and a worker takes a `BackupEngine` generation on
`ROCKSDB_BACKUP_INTERVAL_SECONDS`, pruning to `ROCKSDB_BACKUP_KEEP`. Generations are
incremental — unchanged SST files are shared, so the *n*th backup writes only what
changed since *n-1* — and memtables are flushed first, so a backup never depends on
replaying a WAL. With `atomic_flush` on, that flush is a consistent cut across every
column family, so a backup can never hold an index entry whose document it missed.

`rocksdb-backup`, a separate binary in this crate, is the operator side:

```sh
rocksdb-backup backup   /convex/backup --db /convex/data/db   # only while stopped
rocksdb-backup list     /convex/backup
rocksdb-backup verify   /convex/backup [--id N]      # checksum the files
rocksdb-backup rehearse /convex/backup --scratch /tmp/r   # restore and read it
rocksdb-backup restore  /convex/backup --to /convex/data/db
```

Separate because a restore rewrites a database directory and RocksDB holds that
directory's lock while a database is open, so it cannot run inside the backend it is
restoring for — it belongs in an init container or a one-shot job. `restore` refuses a
non-empty target: move the old directory aside instead, so a live database is never
written underneath and the current one stays recoverable.

`rehearse` never deletes anything at the path you give it. It restores into a
uniquely-named subdirectory it creates and removes only that, leaving the rest of the
directory exactly as found — so `rehearse --scratch /convex/data/db` cannot destroy the live database. A backup directory also records which database owns it
and refuses a second one, since interleaved generations mean retention prunes the wrong
ones and a restore returns whichever wrote last.

**`verify` checks sizes; `rehearse` is the test.** RocksDB's `VerifyBackup` defaults to
size-and-presence rather than checksums, and the binding exposes no way to change that, so
verification catches a truncated or missing file — the common destination failure — and
not a corrupted one. Rehearsal restores into a scratch directory, opens the
database, and **decodes** every document and index entry it finds — not just iterating,
which would pass on bytes that no longer parse. It fails on an empty result, because a
backup of an empty database restores and scans perfectly and is exactly the false pass a
rehearsal exists to catch. Put it on a schedule; a backup nobody has restored is not a
backup.

Only one process may use a backup directory at a time, enforced by an advisory lock that
`list`, `verify`, `rehearse`, `restore` and the worker all take. RocksDB defines no
behaviour for concurrent backup engines on one directory — its own header allows for
trashing the directory — and `purge_old_backups` runs on every worker tick, so an
operator listing generations while the worker prunes is a real sequence rather than a
hypothetical one.

Each generation is verified before older ones are pruned. `create_new_backup_flush`
returning `Ok` says RocksDB wrote the files; it does not say the destination kept them,
and pruning on the strength of an unverified generation is how the last known-good ones
age out behind a silently bad new one.

Backups are written to a local path; the crate binds no object-store `Env`, so
replicating that directory off-node is an external job. A backup directory on the same
volume as the database protects against a bad migration, not against losing the volume.

Watch `rocksdb_backup_age_seconds`. It is a gauge, published by the health monitor on its
own short timer rather than by the backup worker, and emitted from process start rather
than from the first generation — a deployment whose backups have never worked is the case
most worth alerting on, and waiting for a first backup would leave the series absent — a level that keeps rising says "no
backup has landed", where a distribution that stops receiving samples says nothing at
all, and the case worth catching is precisely the one where the backup worker is the
thing that died.

**There is no point-in-time recovery, and there cannot be at this layer.** Postgres
reaches an arbitrary moment by replaying archived WAL onto a base backup; RocksDB
recycles its WAL once the matching memtables flush, so there is nothing to archive. The
recovery point is the last backup, narrowed by backing up more often, and — for data
that came through it — by whatever an upstream log can replay.
See [`docs/proposals/005-backup-and-restore.md`](../../docs/proposals/005-backup-and-restore.md)
for the runbook and for what a database backup does not cover.

## Health and failure escalation

RocksDB latches read-only on a background error — a full disk, a checksum failure — and
stays that way. Every subsequent write fails, the process keeps serving, and nothing
crashes. A Postgres deployment gets a pod that dies and a database another node can take
over; an embedded one would fail every mutation indefinitely with no signal.

**And the real symptom is a stall, not an error.** Measured against a full volume: writes
stop returning entirely rather than failing, each one parking a blocking-pool thread, so
nothing errors, nothing crashes, and no error counter moves. Only duration changes.

So a health thread polls every `ROCKSDB_HEALTH_POLL_SECONDS` (default 15) and raises the
backend's `ShutdownSignal` — the same one the relational backends raise on lease loss —
on any of:

- **a write in flight longer than `ROCKSDB_WRITE_STALL_TIMEOUT_SECONDS`** *and* no write
  stall reported by the engine. RocksDB also blocks writers deliberately, as
  backpressure — `allow_stall` on the write buffer manager, the L0 slowdown triggers —
  and that drains as compaction catches up. Separating the two takes more than
  `rocksdb.is-write-stopped`, which is the *write controller* and does not cover the
  write-buffer-manager stall this backend enables — running flushes and compactions are
  the signal that the engine is working through a backlog rather than stuck;
- **a latched background error**, read from the `default` column family. That detail
  matters: `rocksdb.background-errors` is served per column family but RocksDB only ever
  increments it on `default`, so polling the five families this backend defines returns a
  permanent zero. Measured on a full filesystem: those five read `0` while `default` read
  `3`.
- **a write-ahead log whose flushes are failing** in interval mode, ten intervals running.
  In that mode a write is acknowledged before it reaches the kernel, so persistent flush
  failure is acknowledged data accumulating unwritten — unbounded, not "one interval".
  Escalation needs consecutive *failures*, not just elapsed time, so one slow fsync on a
  contended volume cannot take the process down. A separate, much longer deadline —
  `max(interval × 60, ROCKSDB_MIN_FLUSH_SILENCE_SECONDS)` — catches a flusher that died
  without ever returning an error. The floor matters: without it, a 100 ms interval would
  make that deadline six seconds.

It also publishes `rocksdb_oldest_write_seconds` and `rocksdb_wal_flush_age_seconds`.

## Semantics that differ from the relational backends

Both are deliberate, and both are the reason this file exists rather than a
one-line "it's just a KV store".

**Uniqueness is detected, not enforced.** `ConflictStrategy::Error` costs nothing in a
B-tree, which gets it from a primary key *inside the transaction*; an LSM silently
shadows an existing key instead. It is checked here with one batched point get per row
written — but before the batch, not inside it, so two concurrent writes naming the same
key can both probe clean and both apply. Nor is it free: document keys are
bloom-filtered, but `idx` deliberately carries no filter (its reads are range scans), so
every index entry written costs a filterless negative lookup. That is weaker than
Postgres. It holds on the path that matters for a reason outside this crate: commits are
serialized through one committer assigning strictly increasing timestamps, so no two can
name the same `(ts, id)`. On the commit path that check is redundant — commit timestamps strictly
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
  instance beside the primary, which reads its files without taking the write lock and
  catches up on demand. It reads without an engine snapshot, because RocksDB rejects one
  on a secondary; that costs nothing, since a secondary's view only advances when it is
  told to catch up, which happens once at the start of a page and never during one.

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
Each page reads against one engine snapshot, so the index walk and the document bodies
it resolves see the same instant. Rows *can* disappear between pages — retention removes
them continuously, and `delete_tablet_documents` does so with no reference to the
retention window at all — and what makes that safe is that every cursor is a value rather
than a position: a resume seeks to the cursor's key and steps past it only if the seek
landed on it.

## Not implemented


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

`cargo test -p rocksdb_persistence` covers the encodings (unit plus proptest for the
order-preservation property the `idx` layout rests on) and the trait behaviour:
multi-version reads at explicit timestamps, tombstones, interval bounds and ordering,
paging in both directions **including across a row deleted mid-scan**, the
`previous_revisions` family, conflict detection, all three retention deletes, persistence
globals, `max_ts`, and durability across a reopen with and without a clean shutdown.

It also covers the failure paths that are easy to get wrong and impossible to notice: a
retention validator that *rejects*, so read-then-validate ordering is asserted rather
than assumed; dropping a database mid-flush-tick, which is the teardown production
actually performs; index entries whose keys share a truncated prefix, where storage order
and `IndexEntry` order diverge; and the backup lifecycle end to end — generations,
retention, a refused concurrent opener, a refused non-empty restore target, and a
rehearsal that refuses an empty database.
