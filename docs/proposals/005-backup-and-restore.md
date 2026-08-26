# The RocksDB backup and restore path

**Status:** **implemented.** `src/backup.rs` and the `rocksdb-backup` binary; §3's three pieces are built, §7's gaps are not. Tests cover the generation/retention/rehearsal/restore lifecycle, the refusal to restore over a populated directory, and a full destroy-and-restore across every column family including blob files.
**Date:** 2026-08-25
**Follows:** [004-rocksdb-in-kubernetes.md](./004-rocksdb-in-kubernetes.md) §6, which identifies this as the largest gap in adopting the backend
**Scope:** the persistence layer only. See §6 for what this deliberately does not cover.

---

## 0. Summary

Moving off Postgres gives up `pg_dump`, `pg_basebackup`, WAL archiving and PITR — four
things every operator already knows how to drive. This document says what replaces them.

The mechanisms are not the hard part: RocksDB has two, and the `rocksdb` 0.22 crate this
backend already depends on exposes both. The hard part is policy — where backups land,
how often, how many are kept, and how a restore is driven against a directory that a
running pod holds a lock on. That is what is written here.

| | |
|---|---|
| **Mechanism** | `BackupEngine`, incremental, to a second local directory |
| **Off-node** | an external sync of that directory; not this backend's job |
| **Cadence** | a background worker in the backend, on an interval knob |
| **Restore** | offline, before the backend opens the database — an init container or a one-shot job |
| **RPO** | the backup interval, narrowed by whatever the upstream log can replay |
| **Does not cover** | file storage, which lives outside persistence (§6) |

---

## 1. Choosing between the two APIs

| | `Checkpoint::create_checkpoint` | `BackupEngine` |
|---|---|---|
| Output | a directory that is itself a RocksDB database | a numbered backup in a backup directory |
| Cost of the *n*th | hard links — near-instant, near-zero space | copies only SSTs that changed since *n-1* |
| Destination | same filesystem only (hard links) | any path an `Env` can reach |
| Restore | open the directory as a database | `restore_from_latest_backup` / `restore_from_backup` |
| Retention | your problem | `purge_old_backups(n)` |
| Verification | none | `verify_backup(id)` |
| Inspection | none | `get_backup_info()` — id, timestamp, size, file count |

**`BackupEngine` is the right default.** It is the one that behaves like a backup system
rather than a snapshot primitive: numbered generations, a retention call, checksum
verification, and a restore path that does not require the operator to know RocksDB's
directory layout. Its incremental sharing also means the steady-state cost of a backup is
proportional to what changed, not to the size of the database — which matters for a
workload whose whole point is a high write rate.

`Checkpoint` still has a use: it is the cheapest possible way to pin a consistent view of
the database for something *else* to read — a one-off migration, a debugging copy, or a
snapshot taken immediately before a risky operation. It is not a backup, because it lives
on the same filesystem as the thing it is protecting against losing.

### Consistency

`create_new_backup_flush(db, true)` flushes memtables before copying, so a backup does
not depend on replaying a WAL to be complete. This backend opens RocksDB with
`atomic_flush` enabled, so that flush is a consistent cut across all five column
families — a backup can never contain an index entry whose document was not also
captured. That is the same invariant §"Durability and recovery" in the crate README
relies on for crash recovery, reused here.

The resulting snapshot is a point in Convex's timestamp order, not an arbitrary smear:
because every write carries its commit timestamp and nothing is ever updated in place,
the restored database is the state as of the last commit that made it into the backup.

---

## 2. Where backups land

`BackupEngine::open` takes a `BackupEngineOptions` and a RocksDB `Env`. The Rust crate
exposes the default POSIX `Env` and a memory `Env` — there is no S3 or object-store `Env`
binding. So the backend writes backups to a **local path**, and getting them off the node
is somebody else's job.

That separation is the right one anyway. In the reference cell topology:

```
/convex/data/db          the live database        (convex-data PVC)
/convex/backup           the backup directory     (a second PVC, or a subdirectory)
```

and an external process — a sidecar running `rclone`/`aws s3 sync`, or a `CronJob`
mounting the same volume — replicates `/convex/backup` to object storage. The backup
directory is append-mostly and content-addressed by file name, so a naive incremental
sync is correct and cheap.

**Putting the backup directory on the same PVC as the database is not a backup.** It
protects against a corrupted database or a bad migration; it does not protect against
losing the volume, which is the failure this exists for. A second PVC on different
underlying storage is the minimum, and off-node replication is the actual answer.

---

## 3. What was implemented

Three pieces, none large. All of it lives in `crates/rocksdb_persistence`; no upstream
Convex file is touched.

**A. `RocksDbPersistence::backup(&self, dir: &Path, keep: usize)`.** Opens a
`BackupEngine` against `dir`, calls `create_new_backup_flush(&self.inner.db, true)`, then
`purge_old_backups(keep)`. It needs the live database handle, held read-write, so it lives on the
persistence object rather than in a standalone tool. A secondary instance refuses.

**B. A background worker** that calls it on an interval, with these knobs:

| Knob | Suggested default | Meaning |
|---|--:|---|
| `ROCKSDB_BACKUP_DIR` | unset | where backups go; unset disables backups entirely |
| `ROCKSDB_BACKUP_INTERVAL_SECONDS` | 3600 | how often to take one |
| `ROCKSDB_BACKUP_KEEP` | 24 | generations retained before `purge_old_backups` |

Unset-means-off matters: a backend that silently starts writing backups into an
unconfigured path is worse than one that does nothing. The worker should log each
backup's id, size and duration at info, and its failures at error — a backup system that
fails quietly is the one failure mode worse than not having one.

**C. A restore entry point.** `restore_from_latest_backup` and `restore_from_backup` must
run with **no open database** — RocksDB holds a directory lock and a restore rewrites the
directory underneath it. So this cannot be an endpoint on the running backend. It is the
`rocksdb-backup` binary, shipped from this crate rather than added as a subcommand to
`convex-local-backend`, which keeps the upstream CLI untouched and makes it usable as an
init container or a one-shot `Job`:

```sh
rocksdb-backup list     <backup-dir>
rocksdb-backup verify   <backup-dir> [--id N]
rocksdb-backup rehearse <backup-dir> --scratch <dir> [--id N]
rocksdb-backup restore  <backup-dir> --to <db-dir> [--id N]
```

`restore` refuses a non-empty target directory. There is no reliable way to ask whether
another process holds RocksDB's directory lock — the `LOCK` file exists either way, and
the only way to find out is to take it, which is exactly what must not happen during a
restore. Requiring an empty target sidesteps the question and rules out the worse
mistake of writing into a live database.

**D. The rehearsal, which §7 called the highest-value gap.** `rocksdb-backup rehearse`
restores a generation into a scratch directory, opens it, and iterates every column
family touching values as well as keys — so a blob-stored document body is a read that
actually happens rather than one a key-only scan would skip. `verify` checks that every file is present and the expected size — **not** checksums;
this proves a database restored from them opens and can be read.

---

### E. Backing up a running deployment

Snapshot the volume. Under the default `SyncMode::Every` every acknowledged write
is in the write-ahead log before `write` returns, so a crash-consistent snapshot
recovers exactly as an unclean restart does — which this crate tests directly,
with a child process calling `_exit(0)` and no destructors.

`rocksdb-backup backup` cannot do this. It needs the database read-write, and a
read-only instance is not a workaround: RocksDB will not hold a file list still
for one (`DBImplSecondary::DisableFileDeletions` returns `NotSupported`, because
"the secondary instance does not own the database files"). A revision of this
crate allowed it anyway; a single flush landing in the window produced a
generation that was created `Ok`, passed `verify_backup`, and restored 10 of 210
acknowledged documents. The guard is now a hard refusal with a test pinning it.

## 4. Restore runbook

For a `StatefulSet` with `replicas: 1` and `updateStrategy: OnDelete`, where the pod
holds the volume:

1. **Stop the writer.** `kubectl scale statefulset/<cell> --replicas=0`. Wait for the pod
   to terminate — the RocksDB directory lock is released only when the process exits, and
   `terminationGracePeriodSeconds: 60` is generous, but note that nothing in the tree calls `Persistence::shutdown()` today, so every stop is an unclean one and recovery replays the write-ahead log.
2. **Confirm what you are restoring.** Run the restore subcommand's listing (from
   `get_backup_info`) against the backup directory and pick an id. Backup ids are
   always-increasing, so "latest" is well-defined; a corrupted-data incident is the case
   where you want an older one.
3. **Verify it before you destroy anything.** `verify_backup(id)` checksums the backup's
   files. A restore that overwrites a live directory with an unverified backup can turn a
   recoverable incident into an unrecoverable one.
4. **Restore into a fresh path**, not over the live one, if there is disk for it. Then
   swap directories. If there is not, take a `Checkpoint` of the live database first —
   hard links, so it costs almost nothing — and keep it until the restore is confirmed
   good.
5. **Start the writer.** `kubectl scale statefulset/<cell> --replicas=1`. The backend
   opens the restored directory and comes up normally; nothing above `Persistence` knows
   a restore happened.
6. **Reconcile the upstream log.** The restored state is as of the backup, so the relay's
   ingest checkpoint is now ahead of the database. Rewind it to at or before the backup's
   position and let the bus replay the difference into the idempotent appliers. **This
   step is not optional** — skipping it leaves a permanent gap between the backup point
   and wherever the checkpoint had reached.

Step 6 is the one that has no analogue in a Postgres restore, and the one most likely to
be forgotten, because in the current topology the checkpoint and the data are in the same
database and roll back together. See [004](./004-rocksdb-in-kubernetes.md) §2 — the same
split that governs the sync mode governs restore.

---

## 5. How this relates to Convex's own answer

**Convex's persistence layer implements no backup handling at all.** `backup` does not
appear anywhere in `crates/postgres`, `crates/mysql` or `crates/sqlite`. Durability below
the trait is entirely the database's problem, which for a Postgres deployment means
`pg_dump`, `pg_basebackup`, WAL archiving and PITR, driven by the operator or the cloud
provider.

What Convex *does* implement is a layer above: snapshot export and import
(`crates/exports`, `crates/application/src/snapshot_import`). And that is not a
second-class path — it is what Convex's own managed backups are built on.
`ExportRequestor` has exactly two variants, `SnapshotExport` and **`CloudBackup`**
(`crates/model/src/exports/types.rs:365`), and the `deployment:backups:create`,
`:import`, `:configurePeriodic`, `:disablePeriodic` and `:delete` permissions in
`crates/roles` are the control-plane surface over the same machinery. Periodic logical
export *is* the Convex backup product.

Two things follow.

**The logical path is faster than the SurrealDB comparison suggests.** SurrealDB made the
same "export is the backup" choice — `grep` for `Checkpoint`, `create_checkpoint`,
`BackupEngine` or `backup_engine` across its tree returns nothing — and its tracker
reports ~200 000 records taking 7+ hours to restore (issue #7189), because its restore is
sequential SQL statement parsing and per-statement execution. Convex's import is not that
shape: `import_objects` accumulates documents and commits them in batches bounded by
`TRANSACTION_MAX_NUM_USER_WRITES / 2` — 8 000 documents per transaction — straight through
`ImportFacingModel`, with no UDF execution per row. The restore-time argument against
logical export does not transfer.

**It also covers something a file-level backup cannot.** `ExportFormat::Zip {
include_storage }` walks `_file_storage` and writes user-uploaded files into the archive
alongside the data. A RocksDB backup contains none of them (§6). So the two are genuinely
complementary rather than redundant:

| | Engine backup (this document) | Snapshot export |
|---|---|---|
| Cost of taking one | incremental; proportional to what changed | full read of every table, every time |
| Restore | file copy, then open | re-insert everything through the write path |
| Covers file storage | no | yes, with `include_storage` |
| Portable across engines and versions | no | yes |
| Practical RPO | minutes | hours |

The engine backup is the one that can run often enough to bound an RPO on a
write-heavy cell. The export is the one that survives an engine change and captures file
storage. A production cell wants both, on different schedules.

---

## 6. What a database backup does not cover

Worth being explicit, because it is the difference between a restore that works and one
that half-works.

- **File storage.** User-uploaded files live on the `convex-data` volume, outside
  `Persistence`. Nothing in a RocksDB backup contains them, and nothing can rebuild them.
  They need their own backup: either a snapshot export with `include_storage` (§5), or —
  since files are immutable once written — a plain incremental file sync of the
  directory, which is cheaper and can run far more often.
- **Search and vector index segments.** Also outside persistence. Unlike file storage
  these are *derived*: Convex can rebuild them from the document log, so losing them
  costs backfill time rather than data. Worth capturing anyway to avoid the rebuild.
- **Anything below the process.** The instance secret, the admin key and the origins are
  configuration, held in a `Secret`, and restoring a database into a deployment
  configured differently will not do what you want.

A "cell backup" is therefore the RocksDB backup plus the file-storage directory plus the
cell's `Secret`. Only the first of those is what this document proposes building; the
other two are ordinary volume and secret backup, and they should be part of the same
runbook so that nobody discovers the gap during a restore.

---

## 7. What this plan does not make production-ready

§3 is enough to have backups. It is not enough to have a backup *system*, and the
difference is where people lose data. Each of these is a real gap, not a nice-to-have.

**There is no point-in-time recovery, and there cannot be at this layer.** Postgres gives
PITR by archiving WAL segments and replaying them onto a base backup, so the recovery
point is any moment you like. RocksDB's WAL is recycled or deleted once its memtables
flush — there is nothing to archive, and no `restore_to_timestamp`. The best this design
can do is RPO = backup interval. That is a genuine capability regression from Postgres,
and the honest mitigation is that the ingest bus can replay the gap for the data that
came through it. For anything that did not — user-authored records, anything written by a
mutation the bus did not drive — the recovery point really is the last backup.

**A backup nobody has restored is not a backup.** *Now built* — `rocksdb-backup rehearse`,
which decodes rather than counts and refuses an empty result — but building the command
is not the same as running it. It has to be on a schedule, and the schedule is still
yours to create.

**Nothing measures the backup's age.** *Now published* as `rocksdb_backup_age_seconds`,
on every worker tick whether the backup succeeded or not, because failures are events you
can miss and age is a level you cannot. The alert on it is still the deliverable, and is
still yours.

**Concurrent access is refused rather than coordinated.** One advisory lock per backup
directory means a listing during a backup fails instead of corrupting, which is the right
default but is still an error an operator will hit. Two backends must never share a
`ROCKSDB_BACKUP_DIR`.

**Backups are unencrypted.** RocksDB's backup files are the SSTs, in the clear. Whatever
holds them off-node needs encryption at rest and access control at least as tight as the
database's, or the backup becomes the easiest way to read the data. This is a property of
the destination, not of the backend, but it has to be somebody's job.

**Restore is per-cell and manual.** §4 is a runbook a human executes against one cell.
A fleet of cells needs that automated, with the cell's identity — `INSTANCE_NAME`, the
instance secret, the origins — restored alongside the data, because a database restored
into a deployment configured differently will not behave.

**The retention interaction is unexamined.** Convex's retention worker deletes superseded
versions on a timer, so a restored database resumes with whatever retention state the
backup captured. Restoring a backup older than the document retention window into a
deployment whose relay checkpoint expects a newer state is a case that has not been
thought through, and it is exactly the case a real incident produces.

**Backup I/O competes with the write path.** `create_new_backup_flush` forces a flush and
then copies SSTs, on a volume that foreground writes and compaction are already using. On
a cell at ingest saturation that is a throughput dip on a schedule. It wants rate
limiting and a measurement, neither of which exists.

None of these are reasons not to build §3 — they are the difference between "we take
backups" and "we can restore". The order that matters: build §3, then the restore
rehearsal, then the age alert. The rest can follow.
