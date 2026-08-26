//! The multi-tenant backend binary.
//!
//! Structurally this is `local_backend`'s `main` with the single-instance
//! middle replaced. That one is:
//!
//! ```text
//!   parse argv -> connect_persistence -> make_app -> router -> serve
//! ```
//!
//! and this one is:
//!
//! ```text
//!   read env -> shared resources -> router -> serve
//!                     |
//!                     +-> instance source -> supervisor -> N x (open store -> make_app)
//! ```
//!
//! Two differences are deliberate and worth stating.
//!
//! **The route table is `local_backend::router::router`, not a copy of it.**
//! It is generic over its state with exactly the two bounds
//! `multitenant_backend::state` satisfies, so every route a single-tenant
//! backend serves is served here, and a route added upstream tomorrow is served
//! here without an edit.
//!
//! **The listeners come up FIRST, before any instance exists.** `GET /version`
//! is a meta route on `ConvexHttpService`, so a readiness probe passes as soon
//! as the process is listening — which is what lets whatever manages placement
//! proceed to hand this process its first tenant. Until the first roster
//! arrives, instance-scoped routes 404, which is the documented cold-start
//! behaviour.

#![feature(try_blocks)]
#![feature(try_blocks_heterogeneous)]
#![feature(iterator_try_collect)]
#![feature(coroutines)]
#![feature(exhaustive_patterns)]

use std::{
    net::{
        Ipv4Addr,
        SocketAddr,
    },
    sync::Arc,
    time::Duration,
};

use cmd_util::env::config_service;
use common::{
    errors::MainError,
    http::ConvexHttpService,
    knobs::HTTP_SERVER_TIMEOUT_DURATION,
    runtime::Runtime,
    sentry::set_sentry_tags,
    version::SERVER_VERSION_STR,
};
use futures::{
    future,
    FutureExt,
};
use local_backend::{
    proxy::dev_site_proxy,
    router::router,
    HttpActionRouteMapper,
    MAX_CONCURRENT_REQUESTS,
};
use multitenant_backend::{
    config::MultitenantConfig,
    fleet,
    host::{
        hosted_from_map,
        inject_resolved_hostname,
        HostResolver,
    },
    instance::{
        API_PORT,
        SITE_PORT,
    },
    shared::SharedResources,
    source::{
        source_description,
        InstanceSource,
        Roster,
    },
    supervisor::Supervisor,
    MultitenantState,
};
use runtime::prod::ProdRuntime;
use tokio::{
    signal,
    sync::watch,
};

/// How long the process waits for in-flight requests to drain after a signal
/// before it gives up. A container runtime will SIGKILL at the end of its own
/// termination grace period regardless, so this is deliberately well inside a
/// typical one.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

fn main() -> Result<(), MainError> {
    let _guard = config_service();
    tracing::info!(
        "Starting a multi-tenant Convex backend {}",
        *SERVER_VERSION_STR
    );

    // Read and validate the whole configuration before anything expensive
    // happens, so a misconfigured process crash-loops with one clear line
    // instead of half-starting. This is also where CONVEX_SITE is asserted
    // unset: with it set, an unresolvable request lands silently on one
    // arbitrary tenant instead of 404ing.
    let config = Arc::new(MultitenantConfig::from_env()?);
    tracing::info!(
        "group {} on {} hosting up to {} instance(s) from {}",
        config.group,
        config.origins.base_domain,
        config.max_instances,
        source_description(&config.source),
    );

    let sentry = sentry::init(sentry::ClientOptions {
        release: Some(format!("multitenant-backend@{}", *SERVER_VERSION_STR).into()),
        ..Default::default()
    });
    if sentry.is_enabled() {
        let group = config.group.clone();
        sentry::configure_scope(move |scope| {
            scope.set_tag("group", group);
            set_sentry_tags(scope);
        });
    }

    let tokio = ProdRuntime::init_tokio()?;
    let runtime = ProdRuntime::new(&tokio);
    let runtime_ = runtime.clone();
    runtime.block_on("main", async move {
        run_server(runtime_, config).await?;
        Ok(())
    })
}

async fn run_server(runtime: ProdRuntime, config: Arc<MultitenantConfig>) -> anyhow::Result<()> {
    // One shutdown broadcast for the whole process. Unlike the single-tenant
    // backend there is no process-wide fatal-error channel: a fatal error
    // belongs to one instance and unloads that instance. See
    // `multitenant_backend::instance`.
    let (shutdown_tx, shutdown_rx) = async_broadcast::broadcast(1);

    let state = MultitenantState::new(runtime.clone(), &config.instance_header);
    let shared = Arc::new(SharedResources::new(&runtime, &config).await?);

    // Read the source once, synchronously, before the supervisor starts: a
    // restart should bring its tenants back before it starts answering, not one
    // poll interval later. A failure here is not fatal — the poll loop retries,
    // and any adopted instance is desired regardless of the roster.
    let source = InstanceSource::new(
        runtime.clone(),
        &config.group,
        config.source.clone(),
        config.poll_interval,
    )?;
    let initial = match source.read_once().await {
        Ok(roster) => roster,
        Err(e) => {
            tracing::error!(
                "could not read the instance source at startup; continuing with nothing hosted \
                 and retrying: {e:#}"
            );
            Roster::default()
        },
    };
    let (roster_tx, roster_rx) = watch::channel(initial);

    let supervisor = Supervisor::new(
        runtime.clone(),
        config.clone(),
        shared.clone(),
        state.clone(),
    );
    let supervisor_shutdown = shutdown_rx.clone();
    let supervisor_handle =
        runtime.spawn("supervisor", supervisor.run(roster_rx, supervisor_shutdown));
    runtime.spawn_background("instance_source", source.run(roster_tx));

    let resolver = HostResolver::new(
        &config.group,
        &config.origins.base_domain,
        &config.instance_header,
        hosted_from_map(state.instances.clone()),
    );

    let fleet_routes = config.admin_token.as_ref().map(|token| {
        fleet::router(fleet::FleetState {
            instances: state.instances.clone(),
            group: config.group.as_str().into(),
            token: token.as_str().into(),
        })
    });

    let http_service = ConvexHttpService::new(
        router(state),
        "multitenant-backend",
        SERVER_VERSION_STR.to_string(),
        MAX_CONCURRENT_REQUESTS,
        *HTTP_SERVER_TIMEOUT_DURATION,
        HttpActionRouteMapper,
    );

    // MOUNTED AHEAD OF THE RESOLVING MIDDLEWARE, next to `/version`.
    //
    // These are cell-wide operations: they carry no routable `Host` and name
    // their instances in the body. Merged into the normal route table they are
    // rejected by `inject_resolved_hostname` before reaching a handler —
    // correctly, since it fails closed on a Host it cannot resolve. That is not
    // a guess: the first cut mounted them there and answered `unknown_instance`
    // to every one of them.
    //
    // Unset token => not mounted at all, so a cell never configured for fleet
    // operations does not answer these at all rather than 401.
    let http_service = match fleet_routes {
        Some(routes) => {
            tracing::info!("cell fleet endpoints mounted at /api/cell/*");
            http_service.with_extra_meta_routes(routes)
        },
        None => {
            tracing::info!("cell fleet endpoints NOT mounted (MULTITENANT_ADMIN_TOKEN unset)");
            http_service
        },
    };

    let mut api_shutdown_rx = shutdown_rx.clone();
    let serve_api = http_service.serve_with_middleware(
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, API_PORT)),
        async move {
            let _ = api_shutdown_rx.recv().await;
        },
        inject_resolved_hostname(resolver),
    );
    // The site listener is the stock forwarder: it rewrites the URI to
    // `<api>/http{uri}` and preserves the headers, so an HTTP-action request is
    // resolved to an instance once, on the API listener, after the hop.
    let serve_site = dev_site_proxy(
        // All interfaces, like the API listener: this is the public HTTP-action
        // surface, reached from outside the process. Only the FORWARD target
        // below is loopback.
        Some((Ipv4Addr::UNSPECIFIED.octets(), SITE_PORT)),
        format!("http://127.0.0.1:{API_PORT}/http"),
        shutdown_rx,
    );

    let serve_future = future::try_join(serve_api, serve_site).fuse();
    futures::pin_mut!(serve_future);

    futures::select! {
        r = serve_future => {
            r?;
            anyhow::bail!("the listeners stopped unexpectedly")
        },
        r = signal::ctrl_c().fuse() => {
            r?;
            tracing::info!("received a termination signal; draining");
            let _: Result<_, _> = shutdown_tx.broadcast(()).await;
        },
    }

    // Drain requests and stop the listeners, then let the supervisor unload
    // every instance. The order matters: unloading an instance stops the
    // workers a still-running request would be using.
    let drained = async {
        serve_future.await?;
        Ok::<_, anyhow::Error>(())
    }
    .fuse();
    futures::pin_mut!(drained);
    let mut grace = runtime.wait(SHUTDOWN_GRACE).fuse();
    futures::select! {
        r = drained => r?,
        _ = grace => tracing::warn!(
            "requests did not drain within {SHUTDOWN_GRACE:?}; unloading instances anyway"
        ),
        r = signal::ctrl_c().fuse() => {
            r?;
            tracing::warn!("second termination signal; unloading instances now");
        },
    }
    supervisor_handle.join().await?;
    tracing::info!("shut down cleanly");
    Ok(())
}
