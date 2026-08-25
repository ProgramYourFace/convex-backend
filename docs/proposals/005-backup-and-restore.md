# The RocksDB backup and restore path

**Status:** design and runbook — **not implemented.** No code in `rocksdb_persistence` calls either API yet.
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

## 3. What to implement

Three pieces, none large.

**A. `RocksDbPersistence::backup(&self, dir: &Path, keep: usize)`.** Opens a
`BackupEngine` against `dir`, calls `create_new_backup_flush(&self.inner.db, true)`, then
`purge_old_backups(keep)`. It needs the live database handle, so it lives on the
persistence object rather than in a standalone tool, and it runs on a blocking thread
like every other RocksDB call in this crate.

**B. A background worker** that calls it on an interval, with knobs alongside the
existing ones:

| Knob | Suggested default | Meaning |
|---|--:|---|
| `ROCKSDB_BACKUP_DIR` | unset | where backups go; unset disables backups entirely |
| `ROCKSDB_BACKUP_INTERVAL_SECONDS` | 3600 | how often to take one |
| `ROCKSDB_BACKUP_KEEP` | 24 | generations retained before `purge_old_backups` |

Unset-means-off matters: a backend that silently starts writing backups into an
unconfigured path is worse than one that does nothing. The worker should log each
backup's id, size and duration at info, and its failures at error — a backup system that
fails quietly is the one failure mode worse than not having one.

**C. A restore entry point.** `restore_from_latest_backup(db_dir, wal_dir)` and
`restore_from_backup(db_dir, wal_dir, id)` must run with **no open database** — RocksDB
holds a directory lock and the restore rewrites the directory underneath it. So this
cannot be an endpoint on the running backend. It should be a subcommand on the binary
(`convex-local-backend restore --from <dir> [--backup-id <n>] --to <path>`) that opens no
database of its own, so it can run as an init container or a one-shot `Job`.

---

## 4. Restore runbook

For a `StatefulSet` with `replicas: 1` and `updateStrategy: OnDelete`, where the pod
holds the volume:

1. **Stop the writer.** `kubectl scale statefulset/<cell> --replicas=0`. Wait for the pod
   to terminate — the RocksDB directory lock is released only when the process exits, and
   `terminationGracePeriodSeconds: 60` gives the backend time to `shutdown` cleanly.
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

## 5. Why not the logical-export answer

Convex has snapshot export/import above the trait, and it is tempting to call that the
backup story and stop. SurrealDB — the closest comparable, and also a Rust database
shipping RocksDB as its default single-node engine — made exactly that choice: `grep` for
`Checkpoint`, `create_checkpoint`, `BackupEngine` or `backup_engine` across its tree
returns nothing, and its documented answer is `surreal export` to SurrealQL plus volume
snapshots, with its own docs stating that point-in-time recovery "is not implicit in a
single export".

The cost shows up in their tracker. Issue #7189 reports **~200 000 records across 69
tables taking 7+ hours to restore** from an 850 MB export, because restore is sequential
statement parsing and per-statement execution, so it scales linearly with database size.
The top-ranked proposal in that issue is storage-engine-level backup and restore.

A logical export is genuinely better at one thing — it is portable across engines and
across versions, which a file-level backup is not. That makes it the right tool for a
migration and the wrong tool for an RPO. The two are complementary, and the engine-level
path is the one that is missing.

---

## 6. What a database backup does not cover

Worth being explicit, because it is the difference between a restore that works and one
that half-works.

- **File storage.** User-uploaded files live on the `convex-data` volume, outside
  `Persistence`. Nothing in a RocksDB backup contains them, and nothing can rebuild them.
  They need their own backup — and unlike the database, they are immutable once written,
  so a plain incremental file sync is sufficient.
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
