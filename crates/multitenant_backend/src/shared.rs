//! Process-wide resources, built once and handed to every instance.
//!
//! This is where the density comes from. Booting N unmodified backends in one
//! process would mean N V8 isolate pools (each up to `*MAX_ISOLATE_WORKERS`
//! threads), N node subprocess pools, N searcher scratch budgets and N index
//! caches each sized at `*INDEX_CACHE_SIZE`. Every one of those is designed to
//! be shared — three of them are ALREADY internally partitioned by deployment —
//! they were just never hoisted out of `make_app`, because a single-tenant
//! backend only ever calls it once.
//!
//! What is shared, and what makes it safe to share:
//!
//! | resource            | partitioned by                                            |
//! |---------------------|-----------------------------------------------------------|
//! | `IsolateClient`     | `client_id == deployment name`; the worker recreates its V8 isolate whenever the client changes |
//! | module + code cache | the cache key folds in the deployment name                |
//! | `IndexCache`        | a `DeploymentId` per handle                               |
//! | `QueryCache`        | a tenant id per `CacheManager`                            |
//! | `InProcessSearcher` | nothing — a stateless index reader over a scratch directory |
//! | `LocalNodeExecutor` | nothing — `NodeActions` carries the per-instance origin and deployment metadata, and its `shutdown()` is a no-op, so one instance unloading does not disturb the others |
//! | `SourceMapCache`    | the key is a source map's content                         |
//! | RocksDB block cache | nothing — one process-wide memory budget, by design (see `rocksdb_persistence::options`) |
//!
//! NOT shared, one per instance: persistence, `Database`, `Application`,
//! `KeyBroker`, file storage, the fetch clients, and the background workers
//! `Application::new` spawns. Notably, **the committer is per instance**, so
//! OCC is per instance: two tenants writing the same table name contend with
//! nobody, and a write-heavy tenant's retry storm is confined to its own
//! database.

use std::{
    sync::Arc,
    time::Duration,
};

use application::{
    QueryCache,
    SourceMapCache,
};
use common::{
    knobs::{
        INDEX_CACHE_SIZE,
        NODE_ACTION_USER_TIMEOUT,
        UDF_CACHE_MAX_SIZE,
    },
    runtime::SpawnHandle,
};
use function_runner::{
    in_process_function_runner::new_shared_core,
    server::{
        FunctionRunnerCore,
        UnboundStorage,
    },
};
use indexing::index_cache::{
    DeploymentId,
    IndexCache,
};
use local_backend::SharedResources as PerAppResources;
use node_executor::{
    local::LocalNodeExecutor,
    NodeExecutor,
};
use runtime::prod::ProdRuntime;
use search::searcher::InProcessSearcher;

use crate::config::MultitenantConfig;

pub struct SharedResources {
    /// One process-global index cache. Each instance gets a handle (and with it
    /// a `DeploymentId`) off this; the handle is what partitions the shared
    /// entries, and `remove_deployment` is what reclaims them on unload.
    index_cache: IndexCache,
    /// One process-global UDF query cache with one global size limit. Cloning
    /// shares the same underlying structure, and each `CacheManager` built from
    /// it allocates its own tenant id.
    query_cache: QueryCache,
    searcher: Arc<InProcessSearcher<ProdRuntime>>,
    node_executor: Arc<dyn NodeExecutor>,
    source_map_cache: SourceMapCache<ProdRuntime>,
    /// The shared V8 isolate pool and the module/code caches, unbound to any
    /// instance's storage. `make_app_with_shared` binds it with
    /// `FunctionRunnerCore::with_storage` once the instance's storage exists.
    function_runner_core: FunctionRunnerCore<ProdRuntime, UnboundStorage>,
    /// Keeps the isolate pool's concurrency-logger task alive for the life of
    /// the process. Dropping a `SpawnHandle` cancels its task, so this must not
    /// be per instance — the first instance to unload would silence the logger
    /// for every other one.
    _concurrency_logger: Box<dyn SpawnHandle>,
}

impl SharedResources {
    pub async fn new(runtime: &ProdRuntime, config: &MultitenantConfig) -> anyhow::Result<Self> {
        // Cap any one instance's share of the isolate pool.
        // `InProcessFunctionRunner::new` hardcodes 100% because it is single
        // tenant; at 100% here, one instance's function storm can occupy every
        // worker and its co-tenants get `PerClientWorkerOverloaded`.
        let (function_runner_core, concurrency_logger) =
            new_shared_core(runtime.clone(), config.isolate_percent_per_client)?;
        let node_process_timeout = *NODE_ACTION_USER_TIMEOUT + Duration::from_secs(5);
        Ok(Self {
            index_cache: IndexCache::new(*INDEX_CACHE_SIZE),
            query_cache: QueryCache::new(*UDF_CACHE_MAX_SIZE),
            searcher: Arc::new(InProcessSearcher::new(runtime.clone())?),
            node_executor: Arc::new(LocalNodeExecutor::new(node_process_timeout).await?),
            source_map_cache: SourceMapCache::new(runtime.clone()),
            function_runner_core,
            _concurrency_logger: concurrency_logger,
        })
    }

    /// The per-instance bundle handed to `make_app_with_shared`.
    ///
    /// Mints exactly one index-cache handle and returns its `DeploymentId`
    /// alongside, so the caller can hand it back to
    /// [`SharedResources::release`] when the instance goes away.
    /// `new_handle` allocates monotonically off a `u32` and asserts on
    /// overflow, so a leaked id is a slow leak of cache entries rather than
    /// a crash — but 2^32 admissions is not as far away as it sounds on a
    /// long-lived process that churns tenants.
    pub fn bundle(&self) -> (PerAppResources, DeploymentId) {
        let index_cache_handle = self.index_cache.new_handle();
        let deployment_id = index_cache_handle.deployment_id;
        (
            PerAppResources {
                index_cache_handle,
                query_cache: self.query_cache.clone(),
                searcher: self.searcher.clone(),
                node_executor: self.node_executor.clone(),
                source_map_cache: self.source_map_cache.clone(),
                function_runner_core: self.function_runner_core.clone(),
            },
            deployment_id,
        )
    }

    /// Drops every cached index interval belonging to an unloaded instance.
    ///
    /// Without this the entries survive until they are evicted by size, which
    /// on a process that churns tenants means the shared cache slowly fills
    /// with intervals nobody can ever read again.
    pub fn release(&self, deployment_id: DeploymentId) {
        self.index_cache.remove_deployment(deployment_id);
    }
}
