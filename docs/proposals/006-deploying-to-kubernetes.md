---
status: as-built
updated: 2026-08-26
---

# 006 — Deploying the RocksDB backend to Kubernetes, safely

What a cell needs beyond `--db rocksdb <path>`, and why each item is there. Every
claim here was measured against a running backend; where something is untested,
it says so.

[004](./004-rocksdb-in-kubernetes.md) covers the resource and volume consequences.
[005](./005-backup-and-restore.md) covers backup mechanics. This is the deployment
checklist.

---

## 0. The short version

| | Item | Why | Status |
|---|---|---|---|
| §1 | `replicas: 1`, `Recreate` strategy | RocksDB allows one writer. Two pods on one volume corrupt it. | **required** |
| §2 | `livenessProbe` on `/api/list_snapshot` | Nothing in-process detects a wedged volume. `/version` and `/health` cannot. | **required** |
| §3 | Alert on `HTTP_HANDLE_DURATION_SECONDS` | Catches degradation the probe misses, and read-mostly cells. | strongly recommended |
| §4 | `startupProbe` sized for WAL replay | Nothing calls `Persistence::shutdown()`, so every restart is unclean. | **required** |
| §5 | Volume snapshots for live backups | `rocksdb-backup` needs the writer stopped. | **required** |
| §6 | Memory limits and the cache | The cache is derived from the cgroup limit; table readers are not. | recommended |
| §7 | First-boot step on an upgraded volume | The backup identity is minted at open. | one-off |

---

## 1. One writer, enforced by the manifest

RocksDB takes a `LOCK` file in the data directory and permits exactly one
read-write process. That is not advisory: a second writer does not start.

```yaml
spec:
  replicas: 1
  strategy:
    type: Recreate        # NOT RollingUpdate
```

`RollingUpdate` is the trap. It starts the new pod before the old one
terminates, so the new pod fails to open the database and crashloops until the
old one goes away — a self-inflicted outage on every deploy. `Recreate` stops
first, starts second.

This is the same invariant the Postgres topology already satisfies with
`replicas: 1`; the difference is that Postgres would let a second replica start
and fail later, while RocksDB refuses immediately.

## 2. The liveness probe, and why the obvious endpoints are wrong

**RocksDB does not fail on a wedged volume — it blocks.** A full disk or a hung
mount makes `write_opt` never return. No error surfaces, no counter moves,
nothing crashes. Every writer parks on a blocking-pool thread while the process
keeps answering health checks and doing no work.

Nothing inside the process reliably detects this, and that is a deliberate
conclusion rather than an omission: every signal RocksDB exposes for it
(`num-running-flushes`, `num-running-compactions`, `is-write-stopped`,
`actual-delayed-write-rate`, `current-super-version-number`) is maintained by
the machinery that has stopped, so each latches in exactly the state it is meant
to report. Five successive attempts at an in-process detector each failed in a
new way. Liveness is the cluster's job.

**Do not probe `/version` or `/health`.** Measured: `/version` answers `200 OK`
in about a millisecond with the blocking pool fully parked and every concurrency
permit taken. It is a pure async closure returning a cached string, and it is
merged into the router *after* the timeout and concurrency layers, so it sits
outside both. `health_check_routes` — `/instance_name`, `/instance_version`,
`/` and `/echo` — has the same shape. A probe pointed at any of them reports the
cell healthy for exactly as long as the cell is dead.

Only two routes are outside the layer stack: `/version` and `/metrics`.
Everything under `/api` is inside it.

**Probe `/api/list_snapshot`.** It qualifies on three counts, all verified:

- It reads persistence rather than the index cache, through
  `Database::table_iterator` — the same path streaming export uses to walk
  history.
- Every RocksDB read in that path goes through `tokio_spawn_blocking`, so on a
  wedged volume the request queues behind the parked writers and never runs.
- It is inside the `TimeoutLayer`, so a hung read becomes `408 REQUEST_TIMEOUT`
  and the probe fails.

Measured: `200` in 12 ms on an empty database, 43 ms with 2 000 events, needing
only the admin key and no deployed function.

```yaml
livenessProbe:
  httpGet:
    path: /api/list_snapshot
    port: 3210
    httpHeaders:
      - name: Authorization
        value: "Convex $(CONVEX_ADMIN_KEY)"   # from a Secret
  periodSeconds: 30
  timeoutSeconds: 10       # the probe is the clock, not HTTP_SERVER_TIMEOUT_SECONDS
  failureThreshold: 4      # ~2 min before restart
```

`timeoutSeconds` matters. Without it the probe inherits
`HTTP_SERVER_TIMEOUT_SECONDS` (default 300), so each failed probe costs five
minutes.

**What this does not cover.** A volume that has gone *read-only* rather than
unresponsive still serves this probe. That case is caught inside the process
instead: five consecutive failed engine writes raise the backend's
`ShutdownSignal`. The two mechanisms are complementary — the probe catches
"stopped responding", the escalation catches "refusing writes".

**Cost.** One real persistence read every 30 s. Small at 2 000 events, but it
iterates tablets, so measure it at your data volume before settling on the
period.

## 3. Alerting, which the probe does not replace

Convex already instruments every request:

```
HTTP_HANDLE_DURATION_SECONDS{endpoint, method, status, client_version, is_test}
```

emitted by `RequestStatsGuard::drop`, which records `408` for a timed-out
request and `499` for a client-cancelled one. On a stalled cell you see 499s
first — clients give up long before the 300 s server timeout.

Alert on the collapse of successful mutations rather than on the errors alone:

```promql
sum(rate(HTTP_HANDLE_DURATION_SECONDS_count{endpoint="/api/mutation", status="200"}[5m]))
  < 0.5 * sum(rate(HTTP_HANDLE_DURATION_SECONDS_count{endpoint="/api/mutation", status="200"}[5m] offset 1h))
```

This matters most for the case the probe handles poorly: **a read-mostly cell**.
Persistence writes there come only from the committer's idle timestamp bump,
randomised up to an hour, so a wedged volume produces very few failing mutations
to alert on — and the probe is what carries the detection.

## 4. Startup, and why the window has to be generous

**Nothing in the tree calls `Persistence::shutdown()`.** `Database::shutdown`
stops the committer, subscriptions and retention workers and never touches
persistence; `local_backend` has no SIGTERM path to it. So `wait_for_compact`,
the per-column-family flushes and the closing `flush_wal` do not run in
production: **every restart is an unclean one**, recovered by replaying the
write-ahead log.

Under the default `SyncMode::Every` nothing is lost — every acknowledged write
is already in the WAL. But recovery replays up to `max_total_wal_size` (1 GiB)
through a large L0 before the HTTP port binds, and the liveness probe must not
fire during it.

```yaml
startupProbe:
  httpGet: { path: /version, port: 3210 }    # correct here: "is the port up"
  periodSeconds: 10
  failureThreshold: 120                       # 20 min, per proposal 004 §4
```

`/version` is the *right* endpoint for a startup probe — the question there is
"has the process finished booting and bound the port", which is exactly what it
answers. A `livenessProbe` only begins after the `startupProbe` succeeds, so
adding §2's probe does not shorten this window.

## 5. Backups

`rocksdb-backup backup` and `restore` open the database **read-write**, so the
backend must be stopped. `list`, `verify` and `rehearse` only read the backup
directory and run against a live cell.

**For a running deployment, snapshot the volume.** Under `SyncMode::Every` every
acknowledged write is in the WAL before `write` returns, so a crash-consistent
snapshot recovers exactly as an unclean restart does — which the crate tests
directly, with a child process calling `_exit(0)` and no destructors.

```yaml
# VolumeSnapshot on a schedule, via your CSI driver's snapshot class.
```

**Do not try to make `rocksdb-backup` do this from a read-only instance.** A
revision of this backend allowed it; a single flush landing in the window
between catch-up and the file listing produced a generation that was created
`Ok`, passed `verify`, and restored **10 of 210** acknowledged documents.
`BackupEngine` needs `DisableFileDeletions` to hold a file list still, and
`DBImplSecondary` answers that with `NotSupported` — RocksDB's own comment:
*"the secondary instance does not own the database files."* The refusal is now a
hard error with a test pinning it.

A `verify` pass is **not** a restore test: it checks that every file is present
and the expected size, not checksums. Run `rehearse` on a schedule — it restores
into a scratch directory and decodes every row.

## 6. Memory

The block cache is derived from the cgroup limit at `ROCKSDB_BLOCK_CACHE_PERCENT`
(default 25), capped at 4 GiB and **not** floored — a small container is meant to
get a small cache.

What that budget does *not* cover: `max_open_files = -1` keeps every table
reader open, and those structures live outside the cache and scale with file
count. This is unbounded by configuration and unmeasured at production data
volume. If the pod is OOMKilled with the cache well under its share, this is the
first thing to check.

## 7. First boot on a volume from an older build

The backup directory is claimed by a database identity row, minted when the
database is opened read-write. A volume created by a build that predates that
change has no such row, and a backup taken with the writer stopped will fail
with:

> this database has no backup identity yet, and a read-only instance cannot mint
> one. Start the backend against it once — it mints the identity at open — or
> take this backup with the writer stopped.

One normal start on the current build fixes it permanently. Do this before
pointing any backup automation at an upgraded cell.

---

## Open questions & TODOs

- **Untested at production scale.** Restore duration, snapshot size and probe
  cost were all measured on databases of a few MiB. None of them has been run at
  a real cell's data volume.
- **Never run on the target cluster.** Different kernel, storage class and
  memory limits. The cgroup cache derivation has unit tests but has never met a
  real container limit.
- **`Persistence::shutdown()` is dead** (§4). Wiring SIGTERM to it would make
  restarts clean and shrink §4's window considerably — but it also changes the
  teardown path, which is where several defects have lived, so it wants its own
  review rather than being slipped in.
