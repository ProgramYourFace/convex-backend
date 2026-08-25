# One process, N deployments: a multi-tenant Convex backend on RocksDB

**Status:** implemented; `crates/multitenant_backend` builds and its unit tests pass. Not
yet exercised end-to-end against a live cluster.
**Date:** 2026-08-25
**Follows:** [002-storage-engine.md](./002-storage-engine.md) (why RocksDB),
[004-rocksdb-in-kubernetes.md](./004-rocksdb-in-kubernetes.md) (the topology),
[005-backup-and-restore.md](./005-backup-and-restore.md) (the transfer mechanism)

---

## 0. Summary

A Convex backend is single tenant: one `Application`, one `Database`, one persistence
handle, one V8 isolate pool. Serving N tenants therefore means N processes — and, in the
reference topology, N `StatefulSet`s, 2N PVCs, N Secrets and N rollouts, with onboarding
one tenant restarting the pod its neighbours are served from.

Almost none of that cost is inherent. The parts of a backend that are expensive per
process are *already* partitioned by deployment internally; they were simply never hoisted
out of `make_app`, because upstream only ever calls it once. This proposal hosts N
deployments in one process, sharing exactly those parts, and keeps everything else per
tenant.

| | Item | Kind | Where |
|---|---|---|---|
| §1 | The four seams that already existed | — | no change |
| §2 | Sharing the isolate pool and the caches | new | `function_runner`, `local_backend` |
| §3 | One route table, not two | new | `local_backend::router` |
| §4 | A store, a committer and an OCC domain per tenant | property | `multitenant_backend::instance` |
| §5 | One RocksDB memory budget for N stores | **bug, fixed** | `rocksdb_persistence::options` |
| §6 | A directory per tenant, and what that buys | property | `multitenant_backend::naming` |
| §7 | Fault isolation, and its honest limit | design | `multitenant_backend::instance` |
| §8 | What is still shared, and what that costs | gap | below |

The whole change to files that already existed is **386 insertions across 18 files**, and
every one of them is a no-op for a single-tenant backend. Everything else is a new crate.
That ratio is the point: this has to survive merges from upstream.

---

## 1. Four seams that already existed

The reason this is tractable at all is that four things were already written for a
multi-tenant implementor, presumably because Convex's hosted product is one.

**Host resolution.** `ExtractResolvedHostname` checks an
`axum::Extension<ResolvedHostname>` *first*, before its own hostname parsing and before
its `CONVEX_SITE` fallback. Pre-routing middleware can therefore answer "which
deployment?" before the question is asked, with no change to any handler.

**Request dispatch.** Every method of `ApplicationApi` takes `host: &ResolvedHostname` as
its first argument. The single-tenant `impl ApplicationApi for Application<RT>` names that
parameter `_host` and ignores it. A multi-instance implementation is a dispatch table over
the same trait — one object, resolving per call.

**The legacy handlers.** All ~90 `/api/**` handlers extract `MtState<LocalAppState>`,
whose `FromMtState` trait — unlike axum's `FromRef` — is handed the request's `Parts`.
That is exactly the hook: the app becomes a function of the REQUEST rather than of the
router. One impl covers all of them.

**Per-deployment partitioning.** The V8 isolate pool is keyed by
`client_id == deployment name` and recreates its isolate when the client changes; the
module and code cache keys fold in the deployment name; `IndexCache` hands out a
`DeploymentId` per handle; every `CacheManager` allocates its own tenant id inside a shared
`QueryCache`. None of that had to be built.

What the seams did *not* cover is §2, §3 and §5.

---

## 2. Sharing the isolate pool and the caches

`InProcessFunctionRunner::new` unconditionally builds a fresh `FunctionRunnerCore` — a
fresh V8 `IsolateClient`, isolate pool and in-memory index/module/code caches — and
hardcodes `max_percent_per_client = 100` with the comment that it is single tenant. N
unpatched instances would mean N isolate pools (each up to `*MAX_ISOLATE_WORKERS` worker
threads), N node subprocess pools, N searcher scratch budgets and N index caches each sized
at `*INDEX_CACHE_SIZE`.

Two additions, both additive:

- `function_runner::in_process_function_runner::new_shared_core(rt, max_percent_per_client)`
  builds one core plus its concurrency-logger task, over a new `UnboundStorage` that fails
  closed rather than resolving to some other deployment's storage.
  `FunctionRunnerCore::with_storage` then mints a per-deployment handle onto the same pool
  and caches. `InProcessFunctionRunner::new` keeps its exact previous behaviour.
- `local_backend::make_app_with_shared(.., SharedResources)` takes the bundle;
  `make_app` builds a fresh one and is otherwise unchanged.

`max_percent_per_client` is now a knob rather than a constant, and the multi-tenant host
sets it to 25 by default. At 100 on a shared pool, one tenant's function storm occupies
every worker and its neighbours get `PerClientWorkerOverloaded`.

One incidental fix: `make_app` built a brand-new `IndexCache` per app and then took a
single handle off it, which defeated the cache's own per-deployment partitioning. Sharing
one cache is what that partitioning was for.

---

## 3. One route table, not two

The tempting shape — and the one an earlier draft of this took — is for the multi-tenant
host to own a copy of `router()`, substituting its own state type. That copy is 350 lines,
it silently skips whatever it could not reach (`local_only_dashboard_router` was hard-typed
to `LocalAppState`), and every upstream route added afterwards is missing from it until
somebody notices.

Instead, `router` is now generic over its state with two bounds:

```rust
pub fn router<S>(st: S) -> Router
where
    LocalAppState: FromMtState<S>,   // the ~90 legacy handlers, resolved per request
    RouterState:   FromRef<S>,       // the migrated routes, via ApplicationApi
    S: Clone + Send + Sync + 'static,
```

For the single-tenant backend `S = LocalAppState`: the first bound holds through axum's
blanket `FromRef<T> for T`, and the second through one new `impl FromRef<LocalAppState> for
RouterState` that projects the app's own `ApplicationApi`. **The call site in `main.rs` did
not change.** For the multi-tenant host, `state.rs` supplies both impls and mounts the real
route table.

Three mechanical changes went with it: nine handlers moved from `State<LocalAppState>` to
`MtState<LocalAppState>` (identical for a `Router<LocalAppState>`; the difference is only
visible when the app must come from the request), eight `platform_router()` builders
relaxed `LocalAppState: FromRef<S>` to `FromMtState<S>`, and the `RouterState` sub-router
moved into its own non-generic function.

That last one is worth stating, because it is a trap. `sync` and friends extract
`State<RouterState>`, which is satisfiable both by `RouterState` itself and by any `S` a
`RouterState` can be taken from. A `RouterState: FromRef<S>` bound in scope wins trait
selection over the impls, so with a generic parameter around, *every one of those handlers
resolves to `S`*. Keeping them in a function with no `S` in scope is the fix; annotating the
routers is not, because `get(sync)` resolves before it is unified with anything.

---

## 4. A committer per tenant, and therefore an OCC domain per tenant

Nothing had to be built for this, but it is half the reason to do the work, so it is worth
saying plainly: each instance gets its own `Database`, and a `Database` owns its own
committer. OCC conflicts are detected and retried within one committer's serialized
timestamp assignment.

So two tenants writing the same table name never contend, a write-heavy tenant's retry
storm stays inside its own store, and a tenant's conflict rate is a property of its own
workload rather than of whoever it happens to share a pod with. The alternative shape — one
database with a tenant column — has exactly the opposite property, and it is the reason
this design gives each instance its own store rather than sharing one.

---

## 5. One RocksDB memory budget for N stores

**This was a bug, and it is the one thing here that would have produced an outage rather
than a slowdown.**

`rocksdb_persistence::options::build` created a fresh `Cache::new_lru_cache(*BLOCK_CACHE_BYTES)`
per open. `BLOCK_CACHE_BYTES` is derived from the *container's* memory limit — 25% of it by
default — precisely because RocksDB reads no cgroup limit and a fixed default sized for a
generous host gets the backend OOM-killed on a small one (see
[004](./004-rocksdb-in-kubernetes.md) §3). That derivation is a statement about the
process. N databases each claiming a quarter of the container oversubscribes memory by N,
and the failure mode is the kernel killing the thing serving traffic.

The cache and the write-buffer manager are now process-wide (`SHARED_MEMORY`, a
`LazyLock`), so memtable memory and cached blocks come out of one number no matter how many
stores are open. For the single database a single-tenant backend opens, sharing is
indistinguishable from not sharing.

Three smaller multi-database defects went with it:

- **Descriptors.** `max_open_files` was `-1`, unlimited, which is right for one database
  and wrong for N: descriptors are a per-*process* resource, so N unlimited stores race
  each other to `EMFILE`. `DbTuning::max_open_files` divides the process's `RLIMIT_NOFILE`
  by `max_instances`, with a floor.
- **The shape of the memtable bound.** The shared write-buffer manager cannot overcommit,
  but left at the single-tenant 64 MiB per column family, N × 5 families all want the whole
  budget and the manager answers by force-flushing whichever memtable is largest — turning
  a healthy bound into a stream of premature flushes. `DbTuning::memtable_bytes` scales the
  per-family target down.
- **Backups.** `ROCKSDB_BACKUP_DIR` was a single process-wide variable. `BackupEngine`
  numbers generations per directory with no record of which database wrote them, so two
  stores pointed at one directory interleave their chains and each one's `purge_old_backups`
  deletes the other's. `OpenOptions::backup_dir` is now per open, and the host sets
  `<root>/<instance>`.
- **Gauges.** Backup age, WAL-flush age and latched background errors are *levels*, and N
  unlabelled databases publishing to one series report whichever wrote last. They now carry
  an `instance` label. Rates and latencies stay unlabelled, because summing them across a
  process is what you want.

What did *not* need fixing: background compaction threads. RocksDB's default `Env` is a
process-wide singleton whose thread pools are shared by every database opened against it,
and `max_background_jobs` grows those pools to the largest value any database asked for
rather than to their sum.

---

## 6. A directory per tenant

```text
<data_dir>/instances/<name>/db        the RocksDB database
<data_dir>/instances/<name>/storage   file storage and search indexes
```

A tenant's entire state is that subtree and nothing else. No shared tables, no rows keyed by
tenant in a neighbour's database, no entry in a global catalogue.

That is what makes a tenant **transferable**. Moving one to another host is
`rocksdb-backup` from this one and a restore into the other's `instances/<name>/db`, plus a
copy of `storage/` — the mechanism [005](./005-backup-and-restore.md) already built, applied
per instance rather than per pod. Retiring one is `rm -rf` of the subtree. Neither operation
touches a co-tenant, and neither needs a maintenance window on the pod.

An adopted instance — one whose data a single-tenant backend already wrote directly under
the data directory — keeps its paths where they lie (`MULTITENANT_LEGACY_INSTANCE`), so
adopting an existing deployment moves no bytes, and keeps its origin at the bare group host
so no client, deploy key or signed storage URL changes.

Per-instance deployment secrets are HKDF-derived from one root secret rather than stored:
`KeyBroker` derives its encryptors from the secret alone and keeps the instance name as a
plain field, so two instances sharing a secret would accept each other's admin keys. Distinct
secrets are mandatory, and deriving them takes the secret-store write off the tenant
onboarding path entirely — the host computes an instance's secret the moment it learns the
name, and whatever mints that instance's admin key computes the identical value
independently. The `info` prefix is versioned and overridable, so a deployment that already
mints keys against another prefix keeps them valid.

---

## 7. Fault isolation, and its honest limit

A single-tenant `main` builds one `ShutdownSignal` and exits the process when it fires —
a full disk, a corrupt SST, a lost lease. Here that would mean one tenant's failed store
killing every co-tenant, which is the exact blast radius this exists to shrink. So each
instance gets its own signal, and its firing is forwarded to the supervisor, which unloads
that instance and leaves the process serving.

The limit, stated plainly: `panic = "abort"` in the release profile, and the isolate pool is
shared. **A genuine panic still ends the process.** This is a bound on reported operational
failures, not a memory-safety boundary. Do not sell it as one.

Eviction is expressed by absence — an instance leaves by not being in the roster — which
makes the instance source's failure contract load-bearing. On ANY failure it keeps serving
the last known good set and retries with backoff; it never publishes an empty roster, which
the supervisor would faithfully read as "unload every tenant".

---

## 8. What is still shared, and what it costs

| | Shared thing | Consequence | Mitigation today |
|---|---|---|---|
| a | `MAX_CONCURRENT_REQUESTS` — one process-wide semaphore | a noisy tenant can starve co-tenants' request slots | none; the knob is process-wide |
| b | ~15 background workers per `Application` | N tenants is N × 15 tasks, mostly idle | bounded by `MULTITENANT_MAX_INSTANCES` |
| c | 2–3 OS threads per RocksDB store (WAL flusher, health, backup) | N × 3 threads, mostly asleep | as above |
| d | the process itself | a panic in any tenant's code path ends all of them | §7; `panic = "abort"` is upstream's choice |
| e | the shared block cache | one tenant's scan can evict a neighbour's hot blocks | none; this is the cost of one memory budget |

(a) is the one worth fixing next: a per-instance semaphore, or a fair queue in front of the
process-wide one, would turn the sharpest remaining shared resource into a bounded one.

Nothing reclaims an unhosted instance's directory. That is deliberate for now — an
automatic `rm -rf` driven by a roster is a data-loss button wired to a network response —
but it means retiring a tenant leaves its bytes behind until someone removes them.

---

## 9. Configuration

All environment, no arguments: the interesting half of a Convex config is per instance and
does not exist at process start.

| variable | default | notes |
|---|---|---|
| `MULTITENANT_GROUP` | *required* | the label every instance here shares; also the name of an adopted instance |
| `MULTITENANT_BASE_DOMAIN` | *required* | suffix of every instance hostname |
| `MULTITENANT_ORIGIN_SCHEME` | `http` | `http` or `https` |
| `MULTITENANT_ROOT_SECRET` | *required* | 64 lowercase hex chars. HKDF root. Never logged |
| `MULTITENANT_SECRET_INFO_PREFIX` | `convex-multitenant/instance-secret/v1/` | set it to keep already-minted admin keys valid |
| `MULTITENANT_INSTANCES` \| `_FILE` \| `MULTITENANT_ROSTER_URL` | *exactly one* | static list, JSON file, or HTTP control plane |
| `MULTITENANT_ROSTER_TOKEN` | unset | bearer for the HTTP source. Never logged |
| `MULTITENANT_POLL_MS` | `2000` | |
| `MULTITENANT_DB` | `rocksdb` | or `sqlite`, `postgres-v5`, `mysql-v5` |
| `MULTITENANT_DB_SPEC` | — | required for the relational drivers: a cluster URL with an EMPTY path |
| `MULTITENANT_DATA_DIR` | `/convex/data` | |
| `MULTITENANT_BACKUP_DIR` | unset | each instance backs up to `<root>/<instance>` |
| `MULTITENANT_MAX_INSTANCES` | `24` | admits beyond this are refused, loudly. Also divides the descriptor budget |
| `MULTITENANT_BOOT_CONCURRENCY` | `4` | |
| `MULTITENANT_ISOLATE_PERCENT_PER_CLIENT` | `25` | share of the shared isolate pool per instance |
| `MULTITENANT_LEGACY_INSTANCE` | unset | an instance whose data lies directly under the data dir |
| `MULTITENANT_INSTANCE_HEADER` | `x-convex-instance` | the in-cluster instance selector |
| `CONVEX_SITE` | **must be unset** | the process refuses to start otherwise — see below |

`CONVEX_SITE` is fatal because `ExtractResolvedHostname` falls back to it when a request
resolves to no deployment. On a multi-tenant host that silently routes an unrouted request
into whichever tenant the variable names, instead of 404ing. There is no safe value.

### Addressing an instance

```
X-Convex-Instance: <instance>                    # in-cluster
Host: <instance>.<group>.api.<base>              # public wildcard
Host: <group>.api.<base>                         # the adopted instance
```

Header, then wildcard Host, then bare group Host, then **404**. A header that contradicts
the Host is a **400**; a name that is not hosted is a **404**. There is no fallback.

The conflict rule, not the ingress, is the trust boundary: a public request that reached a
host rule matched it by definition, so its `Host` always resolves, so a client-supplied
header is always either redundant or a 400 — it can never select a co-tenant. Stripping the
header at the ingress is worthwhile defence in depth, but the design does not depend on it.

---

## 10. Status and what is untested

`cargo check` and `cargo test` pass for `multitenant_backend`, `local_backend`,
`function_runner` and `rocksdb_persistence`. The unit tests cover name validation and the
path/secret derivations (including pinned HKDF golden vectors and the RFC 4231 HMAC
vectors), the whole host-resolution order including the fail-closed and conflict rules, the
supervisor's reconcile planner including the cap and the relocation case, roster parsing and
sanitising, and the per-instance config derivation.

What is **not** exercised: anything end-to-end. No test boots two instances in one process,
pushes functions to both and asserts they cannot see each other; none measures what N
concurrent stores actually cost in memory or descriptors; none exercises a tenant transfer.
Those need a live backend, and they are the next thing to build.
