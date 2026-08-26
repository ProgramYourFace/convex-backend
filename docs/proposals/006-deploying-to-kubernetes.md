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
| §1 | One writer, enforced by the object kind | RocksDB allows one writer. Two pods on one volume corrupt it. | **required** |
| §2 | `livenessProbe` on `/health/storage` | Nothing in-process detects a wedged volume, and no in-memory endpoint can. | **required** |
| §2a | Know that the fatal-error exit code is `0` | It reads as a graceful shutdown, so exit-code alerts stay silent. | **required** |
| §3 | Alert on `http_handle_duration_seconds` | Catches degradation the probe misses, and read-mostly cells. | strongly recommended |
| §4 | `startupProbe` sized for WAL replay | Nothing calls `Persistence::shutdown()`, so every restart is unclean. | **required** |
| §5 | Volume snapshots + a `check` CronJob | `rocksdb-backup backup` needs the writer stopped; a snapshot needs verifying. | **required** |
| §6 | Memory limits and the cache | The cache is derived from the cgroup limit; table readers are not. | recommended |
| §7 | One backup directory per cell | The identity travels with the data, so a restored or cloned cell shares its parent's chain. | **required** |

---

## 1. One writer, enforced by the manifest

RocksDB takes a `LOCK` file in the data directory and permits exactly one
read-write process. That is not advisory: a second writer does not start.

Which field enforces it depends on the object kind, and [004](./004-rocksdb-in-kubernetes.md) §1
takes a `StatefulSet` as the reference topology:

```yaml
# StatefulSet — the reference topology
spec:
  replicas: 1
  podManagementPolicy: OrderedReady
  updateStrategy:
    type: OnDelete

# Deployment — if you use one instead
spec:
  replicas: 1
  strategy:
    type: Recreate        # NOT RollingUpdate
```

`spec.strategy` is a Deployment field; `kubectl apply` rejects it on a
StatefulSet. Use whichever matches your object.

`RollingUpdate` is the Deployment trap. It starts the new pod before the old one
terminates, so the new pod fails to open the database and crashloops until the
old one goes away — a self-inflicted outage on every deploy. `Recreate` stops
first, starts second. A `StatefulSet` with `replicas: 1` already terminates
before replacing, so it does not have this failure mode.

This is the same invariant the Postgres topology already satisfies with
`replicas: 1`; the difference is that Postgres would let a second replica start
and fail later, while RocksDB refuses immediately.

## 2. The liveness probe

**RocksDB does not fail on a wedged volume — it blocks.** A full disk or a hung mount
makes `write_opt` never return. No error surfaces, no counter moves, nothing crashes.
Every writer parks on a blocking-pool thread while the process keeps answering health
checks and doing no work.

Nothing inside the process reliably detects this, and that is a deliberate conclusion
rather than an omission: every signal RocksDB exposes for it (`num-running-flushes`,
`num-running-compactions`, `is-write-stopped`, `actual-delayed-write-rate`,
`current-super-version-number`) is maintained by the machinery that has stopped, so each
latches in exactly the state it is meant to report. Five successive attempts at an
in-process detector each failed in a new way. Liveness is the cluster's job.

### Probe `/health/storage`

```yaml
livenessProbe:
  httpGet:
    path: /health/storage
    port: 3210
  periodSeconds: 30
  timeoutSeconds: 10
  failureThreshold: 4      # ~2 min before restart
```

It is unauthenticated and it touches the filesystem, and it has to be both.

**Filesystem-touching, not a read.** An earlier revision of this probe did one `globals`
point get, and that was worthless: it read `MaxRepeatableTimestamp`, which the committer
rewrites every 5 s on any active cell, so it sat in the active memtable and the probe
answered `200` in microseconds on a volume nothing could reach. No RocksDB read fixes
that — a cached block, a pinned index, or a bloom-filter miss on an absent key all
short-circuit before the device.

So `PersistenceReader::check_storage` does not read. On RocksDB it does two things a
wedged volume cannot complete, both on the blocking pool:

1. `metadata()` on the data directory. A hung mount — NFS, EBS, a disconnected CSI volume
   — blocks every VFS call against it, in the kernel, before RocksDB is involved. This is
   the case with no other detector.
2. `flush_wal(true)`, an fsync of the write-ahead log, which reaches the block layer and
   is where the parked writers are stuck. Cheap under `SyncMode::Every`, where the WAL is
   already synced.

The method has a default of `Ok(())` on the trait, so the Postgres and SQLite backends are
unaffected: their storage is a process on the far side of a socket, where a broken
connection is already an error rather than a silence.

**Unauthenticated**, because a liveness probe that can fail for any reason other than
"this process is unwell" is a kill switch. The kubelet has no identity. It cannot read a
Secret into a probe header — Kubernetes expands `$(VAR)` in `command`, `args` and
`env[].value`, and **nowhere else**, so a header written as
`Authorization: Convex $(CONVEX_ADMIN_KEY)` is sent as that literal string and rejected.
And it cannot distinguish 401 from a hung disk: both are "not 2xx". An authenticated probe
therefore turns any credential change — an `INSTANCE_SECRET` rotation, a remounted Secret,
a projected-Secret refresh racing the probe — into a restart every two minutes that
restarting cannot fix. An earlier revision of this document recommended exactly that,
against `/api/list_snapshot`.

The cost is one `stat` and one fsync every 30 s, fixed: it does not grow with the
database, allocates no iterator, and books no usage.

Being unauthenticated, it is also reachable by anyone who can reach the port, and each
request occupies a blocking-pool thread and one of the 128 concurrency permits. Do not
expose port 3210 beyond the cluster, and keep `failureThreshold` at 4 or higher so a
transient saturation cannot restart a healthy pod.

**`timeoutSeconds` is not optional.** Kubernetes defaults it to **1 second**. (An earlier
revision of this section claimed the probe would otherwise inherit
`HTTP_SERVER_TIMEOUT_SECONDS`, which is backwards — that knob bounds how long the *server*
keeps working and reaches the kubelet through no channel at all. The two clocks are
unrelated.) One second is too tight for a cold start; 10 s is a reasonable floor.

### Why not the endpoints that look like health checks

`/version` and `/metrics` are the **only** two routes outside the timeout and concurrency
layers — they are merged in `serve()`, after the stack is applied. `/version` is a pure
async closure returning a cached string and answers `200 OK` in about a millisecond with
the blocking pool fully parked. Measured.

`health_check_routes` — `/instance_name`, `/instance_version`, `/` and `/echo` — is merged
*inside* the stack, not outside it. (An earlier revision of this section said it "has the
same shape" as `/version`; that was wrong, and it contradicted the correct statement four
lines below it.) Being inside the stack is necessary but not sufficient: all four still
answer from memory, so they report a dead cell healthy for as long as it stays dead.
`/health/storage` is in the same router precisely so it inherits that stack while actually
reading the volume.

Within that stack, note the ordering: `GlobalConcurrencyLimitLayer` is declared *before*
`TimeoutLayer` and is therefore the **outer** service. A request that cannot get one of the
128 permits waits upstream of the timeout and never becomes a `408` — it simply hangs.
That is fine for a liveness probe, because the kubelet's own `timeoutSeconds` is the real
clock, but it means the server-side timeout cannot be relied on to bound the probe.

### What this does not cover

**A volume that has gone read-only rather than unresponsive** still serves this probe,
because the probe is a read. That case is caught inside the process instead: consecutive
failed engine writes raise the backend's `ShutdownSignal` (§2a).

**A read-mostly cell.** On a cell with no user mutations the only persistence write is the
committer's idle `MaxRepeatableTimestamp` bump, whose interval is jittered between 1× and
2× `MAX_REPEATABLE_TIMESTAMP_IDLE_FREQUENCY` (3600 s) — so **one to two hours**, not "up to
an hour" as an earlier revision said. During that window a latched engine serves every
read, the probe stays green, and §3's mutation-rate alert has no mutations to observe.
This is the largest detection hole left after the in-process supervision layer was
removed. Lowering `MAX_REPEATABLE_TIMESTAMP_IDLE_FREQUENCY` on RocksDB cells shortens it
directly.

## 2a. What the in-process escalation does, and its exit code

Two mechanisms raise `ShutdownSignal` on a failing write, and they fire at very different
points:

- On the **commit path**, the *first* failed persistence write does it. `Committer::go`
  propagates the error and signals; there is no retry.
- On **retention deletes and the idle bump**, the persistence layer's own counter does it,
  after `ROCKSDB_WRITE_FAILURES_TO_ESCALATE` (default 5) *consecutive* failures.

The counter resets on any success, which is deliberate — one bad write should not take a
cell down — but it means an intermittent fault that fails half of all writes never
escalates. That state is observable even though it never stops the process:

```promql
rate(rocksdb_write_failures_total[5m]) > 0
```

which is the alert for a volume that has gone read-only or is failing intermittently.

**The process then exits with status 0.** `preempt_rx` fires, the shutdown loop breaks,
and `main` returns `Ok(())`. In `kubectl describe pod` this reads as
`Reason: Completed, Exit Code: 0` — a graceful shutdown, not a storage fault. Any fleet
alert keyed on a non-zero exit code or on
`kube_pod_container_status_last_terminated_reason{reason="Error"}` stays silent, and under
any `restartPolicy` other than `Always` the process does not come back at all. Alert on
§3's mutation-rate collapse, not on the exit code.

## 3. Alerting, which the probe does not replace

Convex already instruments every request:

```
http_handle_duration_seconds{endpoint, method, status, client_version, is_test}
```

The name is lower_snake_case in Prometheus: `register_convex_histogram_owned!` exports
`stringify!([<$NAME:lower>])`, so the Rust constant `HTTP_HANDLE_DURATION_SECONDS` becomes
`http_handle_duration_seconds`. Metric names are case-sensitive and a comparison over an
empty vector never fires, so an alert written in the constant's casing — as an earlier
revision of this section had it — is silently inert.

emitted by `RequestStatsGuard::drop`, which records `408` for a timed-out
request and `499` for a client-cancelled one. On a stalled cell you see 499s
first — clients give up long before the 300 s server timeout.

**Do not alert on `endpoint="/api/mutation"` alone.** The `endpoint` label is the matched
axum path, and mutations arrive by at least four of them — `/api/mutation`,
`/api/function`, `/api/run/{*functionIdentifier}`, and the `/api/{version}/sync` WebSocket,
which records one sample per *connection* at close with status 101 and so hides its
mutations entirely — plus any run inside an HTTP action, which the mapper collapses to
`/http/:user_http_action`. A cell driven by a reactive client or by HTTP actions never
produces `/api/mutation` at all, the series is never created, and a comparison over an
empty vector never fires. Same silent-inertness as the casing bug above, one layer up.

Alert below the HTTP layer instead, where there is one code path regardless of transport:

```promql
# Mutations have collapsed.
sum(rate(database_commit_seconds_count[5m]))
  < 0.5 * sum(rate(database_commit_seconds_count[5m] offset 1h))

# Heartbeat: the committer's idle timestamp bump has not succeeded in three hours.
# It only records on success, so this covers the read-mostly cell that the probe
# and the mutation-rate alert both miss.
increase(bump_repeatable_ts_seconds_count[3h]) < 1
```

The heartbeat is what covers **a read-mostly cell**. Persistence writes there come only
from the committer's idle timestamp bump, jittered between 1× and 2× a one-hour base, so
there are almost no mutations to observe a collapse in. `bump_repeatable_ts_seconds_count`
only increments once that write *succeeds*, so a three-hour window catches a latched or
wedged engine just past its longest jitter.

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

But a snapshot is only a backup once something has read it back, and the verbs
that do that (`verify`, `rehearse`) all take a *backup* directory — a snapshot
restores to a *database* directory. The two halves do not compose on their own,
which is what `rocksdb-backup check --db <dir>` is for: it is `rehearse`'s
read-back, pointed at a database directory.

The whole cycle, as one `CronJob` per schedule:

```yaml
# 1. A VolumeSnapshot on a schedule, via your CSI driver's snapshot class.
# 2. A CronJob that, per run:
#      - provisions a PVC from the newest snapshot (a clone: no writer holds it)
#      - runs: rocksdb-backup check --db /clone/db
#      - optionally: rocksdb-backup backup /backup --db /clone
#      - deletes the clone
# 3. Alert when the CronJob's lastSuccessfulTime falls behind its schedule.
```

`check` opens the clone **read-write**, replaying its write-ahead log exactly as
recovery would, then decodes every row. That is what turns a snapshot into a
tested snapshot. Step 2's optional `backup` is how you get `BackupEngine`
generations without a maintenance window: the clone has no writer, so the
read-write open succeeds.

**This rests on `SyncMode::Every`,** the default. Under `ROCKSDB_SYNC_WRITES=false`
a snapshot is host loss from the engine's point of view, and `Never`'s durability
row gives host loss as unbounded. Nothing warns at snapshot time, and the
snapshot does not record which mode produced it.

**Do not try to make `rocksdb-backup` do this from a read-only instance.** A
revision of this backend allowed it; a single flush landing in the window
between catch-up and the file listing produced a generation that was created
`Ok`, passed `verify`, and restored **10 of 210** acknowledged documents.
`BackupEngine` needs `DisableFileDeletions` to hold a file list still, and
`DBImplSecondary` answers that with `NotSupported` — RocksDB's own comment:
*"the secondary instance does not own the database files."* The refusal is now a
hard error with a test pinning it.

A `verify` pass is **not** a restore test: it checks that every file is present
and the expected size, not checksums — the `rocksdb` crate exposes no way to turn
checksum verification on. `rehearse` (for backup generations) and `check` (for
snapshots) are the two verbs that actually decode the data, and one of them
belongs on a schedule.

## 6. Memory

The block cache is derived from the cgroup limit at `ROCKSDB_BLOCK_CACHE_PERCENT`
(default 25), capped at 4 GiB and **not** floored — a small container is meant to
get a small cache.

What that budget does *not* cover: `max_open_files = -1` keeps every table
reader open, and those structures live outside the cache and scale with file
count. This is unbounded by configuration and unmeasured at production data
volume. Nor does it cover the blocking pool's stacks — tokio's default cap is 512
threads at `RUNTIME_STACK_SIZE` (4 MiB), so the virtual reservation can reach
2 GiB, and every RocksDB read and write runs on one of those threads. If the pod
is OOMKilled with the cache well under its share, these two are the first things
to check.

## 7. Backup directory ownership, and the fork it does not catch

The backup directory is claimed by a database identity row, minted when the
database is opened read-write. It exists so that two cells pointed at one backup
directory — a shared volume, a templated env var, a copy-pasted manifest — cannot
interleave their generations into one chain, where `purge_old_backups` would age
out the other database's generations and nobody would find out until a restore.

An earlier revision of this section prescribed a mandatory first-boot step
against a "no backup identity yet" error. That step is no longer needed: `backup`
opens read-write, which mints the identity at open, so an upgraded volume gets
one on the first `backup` as well as on the first normal start.

**What the check does not catch is a fork.** The identity lives in `globals`, so
it travels with the data — deliberately, so a restored database can continue its
chain. A database and a *copy* of it are therefore indistinguishable. Restore a
generation into a staging cell deployed from the same chart, and staging's
backups land in production's chain with a matching identity; `list`, `verify` and
`rehearse` all pass, because staging's data is perfectly valid data. A CSI clone
of the data volume does the same. Give each cell its own backup directory, and
change it when you restore into a new cell.

---

## Open questions & TODOs

- **Untested at production scale.** Restore duration, snapshot size and probe
  cost were all measured on databases of a few MiB. None of them has been run at
  a real cell's data volume.
- **Never run on the target cluster.** Different kernel, storage class and
  memory limits. The cgroup cache derivation has unit tests but has never met a
  real container limit.
- **`/health/storage` has not been compiled in this container.** The route and
  `Application::check_storage` could not be typechecked here: `crates/isolate`'s
  build script runs `pnpm install --frozen-lockfile`, and one transitive
  dependency (`get-convex/saffron`, a GitHub tarball) is unreachable through this
  environment's proxy. `Database::check_storage`, which holds the actual logic,
  does compile. Build the backend before relying on the manifest above.
- **No metric counts failed engine writes** (§2a), so the intermittent-fault case
  the counter deliberately does not escalate is also invisible. One counter in
  `engine_write`'s error arm would close it.
- **`Persistence::shutdown()` is dead** (§4). Wiring SIGTERM to it would make
  restarts clean and shrink §4's window considerably — but it also changes the
  teardown path, which is where several defects have lived, so it wants its own
  review rather than being slipped in.
