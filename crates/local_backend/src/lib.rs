#![feature(try_blocks)]
#![feature(try_blocks_heterogeneous)]
#![feature(iterator_try_collect)]
#![feature(coroutines)]
#![feature(exhaustive_patterns)]

use std::{
    self,
    sync::Arc,
    time::Duration,
};

use ::authentication::{
    access_token_auth::NullAccessTokenAuth,
    application_auth::ApplicationAuth,
};
use ::usage_limits::NoopUsageLimitNotifier;
use application::{
    self,
    api::ApplicationApi,
    log_visibility::RedactLogsToClient,
    Application,
    QueryCache,
    SourceMapCache,
};
use axum::extract::FromRef;
use common::{
    self,
    http::{
        fetch::ProxiedFetchClient,
        RouteMapper,
    },
    knobs::{
        DOCUMENT_RETENTION_RATE_LIMIT,
        INDEX_CACHE_SIZE,
        NODE_ACTION_USER_TIMEOUT,
        UDF_CACHE_MAX_SIZE,
    },
    persistence::Persistence,
    runtime::{
        new_rate_limiter,
        Runtime,
    },
    shutdown::ShutdownSignal,
    types::{
        ConvexOrigin,
        ConvexSite,
        DeploymentClass,
        DeploymentMetadata,
        TEST_REGION_NAME,
    },
};
use config::LocalConfig;
use database::Database;
use events::usage::NoOpUsageEventLogger;
use exports::interface::InProcessExportProvider;
use file_storage::{
    FileStorage,
    TransactionalFileStorage,
};
use function_runner::{
    in_process_function_runner::{
        new_shared_core,
        InProcessFunctionRunner,
    },
    server::{
        DeploymentStorage,
        FunctionRunnerCore,
        UnboundStorage,
    },
    FunctionRunner,
};
use governor::Quota;
use http_client::CachedHttpClient;
use indexing::index_cache::{
    IndexCache,
    IndexCacheHandleBuilder,
};
use model::{
    initialize_application_system_tables,
    virtual_system_mapping,
};
use node_executor::{
    local::LocalNodeExecutor,
    NodeActions,
    NodeExecutor,
};
use runtime::prod::ProdRuntime;
use search::{
    searcher::InProcessSearcher,
    Searcher,
    SegmentTermMetadataFetcher,
};
use serde::Serialize;
pub use sync::subscription_reconnect::SubscriptionReconnectRateLimiter;

pub mod admin;
mod app_metrics;
mod args_structs;
pub mod authentication;
pub mod beacon;
pub mod canonical_urls;
pub mod config;
pub mod custom_headers;
pub mod dashboard;
pub mod deploy_config;
pub mod deploy_config2;
pub mod deployment_audit_log;
pub mod deployment_info;
pub mod deployment_state;
pub mod environment_variables;
pub mod http_actions;
pub mod log_sinks;
pub mod logs;
pub mod node_action_callbacks;
pub mod parse;
pub mod proxy;
pub mod public_api;
pub mod router;
pub mod scheduling;
pub mod schema;
pub mod snapshot_export;
pub mod snapshot_import;
pub mod storage;
pub mod streaming_export;
pub mod streaming_import;
pub mod subs;
pub mod usage_limits;

pub const MAX_CONCURRENT_REQUESTS: usize = 128;

#[derive(Clone)]
pub struct LocalAppState {
    // Origin for the server (e.g. http://127.0.0.1:3210, https://demo.convex.cloud)
    pub origin: ConvexOrigin,
    // Origin for the corresponding convex.site (where we serve HTTP) (e.g. http://127.0.0.1:8001, https://crazy-giraffe-123.convex.site)
    pub site_origin: ConvexSite,
    // Name of the instance. (e.g. crazy-giraffe-123)
    pub instance_name: String,
    pub application: Application<ProdRuntime>,
    pub zombify_rx: async_broadcast::Receiver<()>,
}

impl LocalAppState {
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.application.shutdown().await?;

        Ok(())
    }
}

// Contains state needed to serve most http routes. Similar to LocalAppState,
// but uses ApplicationApi instead of Application, which allows it to be used
// in both Backend and Usher.
#[derive(Clone)]
pub struct RouterState {
    pub api: Arc<dyn ApplicationApi>,
    pub runtime: ProdRuntime,
    pub subscription_reconnect_rate_limiter: Option<Arc<SubscriptionReconnectRateLimiter>>,
}

/// A `RouterState` is a projection of a `LocalAppState`: the state the migrated
/// routes need is exactly this app's `ApplicationApi` and its runtime.
///
/// This is what lets [`router::router`] be generic over its state while the
/// single-tenant backend keeps passing a bare `LocalAppState`. A host that
/// serves several deployments from one router supplies its own `FromRef` impl
/// returning a dispatching `ApplicationApi` instead.
impl FromRef<LocalAppState> for RouterState {
    fn from_ref(st: &LocalAppState) -> Self {
        RouterState {
            api: Arc::new(st.application.clone()),
            runtime: st.application.runtime(),
            subscription_reconnect_rate_limiter: None,
        }
    }
}

#[derive(Serialize)]
pub struct EmptyResponse {}

/// The parts of a `LocalAppState` that are worth building once per *process*
/// rather than once per app: a V8 isolate pool, a node subprocess executor, a
/// search index reader and three caches that are already internally partitioned
/// by deployment.
///
/// [`make_app`] builds a fresh one of these, so the single-tenant backend is
/// unchanged. A host that runs several deployments in one process builds the
/// expensive parts once and assembles a cheap bundle per app: every field here
/// is either `Clone` over a shared allocation or a per-app handle minted off a
/// shared structure.
pub struct SharedResources {
    /// A handle onto the process-wide index cache, minted with
    /// `IndexCache::new_handle()`. Mint exactly one per app — the handle owns a
    /// `DeploymentId` that partitions the shared cache, and the minter is
    /// responsible for `IndexCache::remove_deployment`-ing it when the app goes
    /// away, or the dead deployment's cached intervals are retained until they
    /// are evicted by size.
    pub index_cache_handle: IndexCacheHandleBuilder,
    /// The process-wide UDF query cache. `QueryCache` is `Clone` over a shared
    /// `Arc<Mutex<..>>` and every `CacheManager` allocates its own tenant id
    /// within it, so one cache with one global size limit is how it is meant to
    /// be shared.
    pub query_cache: QueryCache,
    /// One in-process searcher, whose scratch budget is shared.
    pub searcher: Arc<InProcessSearcher<ProdRuntime>>,
    /// One node subprocess pool. `NodeActions` is a thin per-app wrapper around
    /// it carrying the app's origin and deployment metadata, and
    /// `LocalNodeExecutor::shutdown` is a no-op, so one app shutting down does
    /// not disturb the others.
    pub node_executor: Arc<dyn NodeExecutor>,
    pub source_map_cache: SourceMapCache<ProdRuntime>,
    /// The shared V8 isolate pool plus the in-memory index, module and code
    /// caches, not yet bound to this app's storage;
    /// [`make_app_with_shared`] rebinds it with
    /// `FunctionRunnerCore::with_storage`. The pool's per-client capacity share
    /// is fixed when the core is built.
    pub function_runner_core: FunctionRunnerCore<ProdRuntime, UnboundStorage>,
}

pub async fn make_app(
    runtime: ProdRuntime,
    config: LocalConfig,
    persistence: Arc<dyn Persistence>,
    zombify_rx: async_broadcast::Receiver<()>,
    preempt_tx: ShutdownSignal,
) -> anyhow::Result<LocalAppState> {
    let node_process_timeout = *NODE_ACTION_USER_TIMEOUT + Duration::from_secs(5);
    // A single-tenant backend may use the whole isolate pool.
    let (function_runner_core, concurrency_logger) = new_shared_core(runtime.clone(), 100)?;
    // The core outlives this scope, so its logger task must not be cancelled
    // when the handle is dropped here.
    concurrency_logger.detach();
    let shared = SharedResources {
        index_cache_handle: IndexCache::new(*INDEX_CACHE_SIZE).new_handle(),
        query_cache: QueryCache::new(*UDF_CACHE_MAX_SIZE),
        searcher: Arc::new(InProcessSearcher::new(runtime.clone())?),
        node_executor: Arc::new(LocalNodeExecutor::new(node_process_timeout).await?),
        source_map_cache: SourceMapCache::new(runtime.clone()),
        function_runner_core,
    };
    make_app_with_shared(runtime, config, persistence, zombify_rx, preempt_tx, shared).await
}

/// [`make_app`], but over resources the caller owns and may share between
/// several apps in the same process.
pub async fn make_app_with_shared(
    runtime: ProdRuntime,
    config: LocalConfig,
    persistence: Arc<dyn Persistence>,
    zombify_rx: async_broadcast::Receiver<()>,
    preempt_tx: ShutdownSignal,
    shared: SharedResources,
) -> anyhow::Result<LocalAppState> {
    let SharedResources {
        index_cache_handle,
        query_cache,
        searcher: in_process_searcher,
        node_executor,
        source_map_cache,
        function_runner_core,
    } = shared;
    let key_broker = config.key_broker()?;
    let searcher: Arc<dyn Searcher> = in_process_searcher.clone();
    // TODO(CX-6572) Separate `SegmentMetadataFetcher` from `SearcherImpl`
    let segment_metadata_fetcher: Arc<dyn SegmentTermMetadataFetcher> = in_process_searcher;
    let (deleted_tablet_sender, deleted_tablet_receiver) = tokio::sync::mpsc::channel(100);
    let usage_event_logger = Arc::new(NoOpUsageEventLogger);
    let database = Database::load(
        persistence.clone(),
        runtime.clone(),
        searcher.clone(),
        preempt_tx.clone(),
        virtual_system_mapping().clone(),
        index_cache_handle,
        Arc::new(new_rate_limiter(
            runtime.clone(),
            Quota::per_second(*DOCUMENT_RETENTION_RATE_LIMIT),
        )),
        deleted_tablet_sender,
        config.name(),
    )
    .await?;
    initialize_application_system_tables(&database).await?;
    let application_storage = Application::initialize_storage(
        runtime.clone(),
        &database,
        config.storage_tag_initializer(),
        config.name(),
    )
    .await?;

    let file_storage = FileStorage {
        transactional_file_storage: TransactionalFileStorage::new(
            runtime.clone(),
            application_storage.files_storage.clone(),
            config.convex_origin_url()?,
        ),
        database: database.clone(),
    };

    let deployment = DeploymentMetadata {
        name: config.name(),
        region: None,
        class: DeploymentClass::S16,
    };
    let node_actions = NodeActions::new(
        node_executor,
        config.convex_origin_url()?,
        *NODE_ACTION_USER_TIMEOUT,
        runtime.clone(),
        deployment.clone(),
    );

    #[cfg(not(debug_assertions))]
    if config.convex_http_proxy.is_none() {
        tracing::warn!(
            "Running without a proxy in release mode -- UDF `fetch` requests are unrestricted!"
        );
    }
    let fetch_client = Arc::new(ProxiedFetchClient::new(
        config.convex_http_proxy.clone(),
        config.name(),
        reqwest::redirect::Policy::none(),
    ));
    let oidc_http_client = CachedHttpClient::new(
        config.convex_http_proxy.clone(),
        config.name(),
        reqwest::redirect::Policy::default(),
    );
    let function_runner: Arc<dyn FunctionRunner<ProdRuntime>> =
        Arc::new(InProcessFunctionRunner::new_with_core(
            function_runner_core.with_storage(DeploymentStorage {
                files_storage: application_storage.files_storage.clone(),
                modules_storage: application_storage.modules_storage.clone(),
            }),
            deployment,
            key_broker.function_runner_keybroker(),
            config.convex_origin_url()?,
            persistence.reader(),
            database.clone(),
            fetch_client.clone(),
        ));

    let application = Application::new(
        runtime.clone(),
        database.clone(),
        file_storage.clone(),
        application_storage,
        usage_event_logger,
        Arc::new(NoopUsageLimitNotifier),
        key_broker.clone(),
        DeploymentMetadata {
            name: config.name(),
            region: Some(TEST_REGION_NAME.clone()),
            class: DeploymentClass::S16,
        },
        function_runner,
        config.convex_origin_url()?,
        config.convex_site_url()?,
        searcher.clone(),
        segment_metadata_fetcher,
        persistence,
        node_actions,
        Arc::new(RedactLogsToClient::new(config.redact_logs_to_client)),
        Arc::new(ApplicationAuth::new(
            key_broker.clone(),
            Arc::new(NullAccessTokenAuth),
            runtime.clone(),
        )),
        query_cache,
        fetch_client,
        config.local_log_sink.clone(),
        preempt_tx.clone(),
        Arc::new(InProcessExportProvider),
        deleted_tablet_receiver,
        oidc_http_client,
        None,
        source_map_cache,
    )
    .await?;

    let origin = config.convex_origin_url()?;
    let instance_name = config.name();

    if !config.disable_beacon {
        let beacon_future = beacon::start_beacon(
            runtime.clone(),
            database.clone(),
            config.beacon_tag.clone(),
            config.beacon_fields.clone(),
        );
        runtime.spawn_background("beacon_worker", beacon_future);
    }

    let app_state = LocalAppState {
        origin,
        site_origin: config.convex_site_url()?,
        instance_name,
        application,
        zombify_rx,
    };

    Ok(app_state)
}

#[derive(Clone)]
pub struct HttpActionRouteMapper;

impl RouteMapper for HttpActionRouteMapper {
    fn map_route(&self, route: String) -> String {
        // Backend can receive arbitrary HTTP requests, so group all of these
        // under one tag.
        if route.starts_with("/http/") {
            "/http/:user_http_action".into()
        } else {
            route
        }
    }
}
