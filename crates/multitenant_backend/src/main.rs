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
//! **The listeners come up FIRST, before any instance exists**, so that
//! whatever manages placement can reach this process to hand it its first
//! tenant. Until the supervisor's first reconcile finishes, instance-scoped
//! routes 404.
//!
//! That window is exactly why readiness is `GET /ready` and NOT `GET /version`.
//! `/version` answers as soon as the socket binds, so a probe on it admits the
//! pod to its Service while stores are still opening — and a 404 is a permanent
//! answer, so a client that distinguishes "no such deployment" from "try again"
//! drops those writes rather than retrying them. `/ready` reports the first
//! reconcile. Both are meta routes on `ConvexHttpService`, mounted ahead of the
//! resolving middleware.

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
    sync::{
        atomic::{
            AtomicBool,
            Ordering,
        },
        Arc,
    },
    time::Duration,
};

use axum::{
    http::StatusCode,
    routing::get,
    Router,
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
const SHUTDOWN_GRACE: Duration = Duration::from_secs(20);

/// How long the supervisor gets to unload every instance once the listeners are
/// down. Bounded because `shutdown_all` closes stores SEQUENTIALLY and each
/// RocksDB close can wait for a flush: unbounded, one slow store eats the rest
/// of the pod's termination grace and every remaining instance is SIGKILLed
/// with an unflushed WAL — the exact harm the ordering exists to avoid.
///
/// `SHUTDOWN_GRACE + UNLOAD_BUDGET` plus the manifest's `preStop` sleep must
/// stay comfortably inside `terminationGracePeriodSeconds`.
const UNLOAD_BUDGET: Duration = Duration::from_secs(20);

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
    // TWO shutdown broadcasts, and the order between them is the whole point.
    //
    // `shutdown_tx` stops the listeners and lets in-flight requests drain.
    // `supervisor_tx` unloads the instances those requests are still using. A
    // single channel would wake both at once, and because the supervisor's
    // select arm is biased first it would close every RocksDB store while the
    // drain window was still open — turning "finish what you started" into
    // "fail everything for 30 seconds".
    //
    // Unlike the single-tenant backend there is no process-wide fatal-error
    // channel: a fatal error belongs to one instance and unloads that
    // instance. See `multitenant_backend::instance`.
    let (shutdown_tx, shutdown_rx) = async_broadcast::broadcast(1);
    let (supervisor_tx, supervisor_rx) = async_broadcast::broadcast(1);

    // REGISTERED HERE, before anything slow, and reused by reference below.
    //
    // `signal()` installs the OS handler when it is CALLED. Building the stream
    // inside the shutdown future would leave SIGTERM at its default disposition
    // for the whole of `SharedResources::new` and the source's first read (a
    // 10s HTTP timeout) — and this process is PID 1 in its container, where the
    // kernel discards a default-disposition signal. A pod deleted while
    // starting would hang until SIGKILL.
    //
    // ONE stream each, not one per wait: `Signal` is a `watch::Receiver` and
    // subscribing marks the current value as already seen, so a stream built
    // freshly for the second-signal wait would miss a signal that arrived while
    // the first was still being handled.
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())?;

    let state = MultitenantState::new(runtime.clone());
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

    let ready = Arc::new(AtomicBool::new(false));
    let supervisor = Supervisor::new(
        runtime.clone(),
        config.clone(),
        shared.clone(),
        state.clone(),
        ready.clone(),
    );
    let supervisor_handle = runtime.spawn("supervisor", supervisor.run(roster_rx, supervisor_rx));
    runtime.spawn_background("instance_source", source.run(roster_tx));

    let resolver = HostResolver::new(
        &config.group,
        &config.origins.base_domain,
        &config.instance_header,
        hosted_from_map(state.instances.clone()),
    );

    // `/ready` is a PROCESS property, so it belongs beside `/version` rather
    // than in the resolving route table — and unlike the fleet routes it is
    // always mounted.
    //
    // It reports whether the supervisor has finished its first reconcile.
    // `/version` cannot: it answers the moment the socket binds, which is before
    // any store is open. See `Supervisor::ready`.
    let mut meta_routes = Router::new().route(
        "/ready",
        get(move || {
            let ready = ready.load(Ordering::Acquire);
            async move {
                if ready {
                    (StatusCode::OK, "ready")
                } else {
                    (StatusCode::SERVICE_UNAVAILABLE, "loading instances")
                }
            }
        }),
    );
    if let Some(token) = config.admin_token.as_ref() {
        meta_routes = meta_routes.merge(fleet::router(fleet::FleetState {
            instances: state.instances.clone(),
            group: config.group.as_str().into(),
            token: token.as_str().into(),
        }));
        tracing::info!("cell fleet endpoints mounted at /api/cell/*");
    } else {
        tracing::info!("cell fleet endpoints NOT mounted (MULTITENANT_ADMIN_TOKEN unset)");
    }

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
    let http_service = http_service.with_extra_meta_routes(meta_routes);

    let mut api_shutdown_rx = shutdown_rx.clone();
    let serve_api = http_service.serve_with_middleware(
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, config.api_port)),
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
        Some((Ipv4Addr::UNSPECIFIED.octets(), config.site_port)),
        format!("http://127.0.0.1:{}/http", config.api_port),
        // A FRACTION of the API listener's budget, and neither the stock 4 nor
        // the full 128.
        //
        // 4 is a single-tenant dev default applied as one global semaphore, so
        // on a host forwarding for a dozen deployments four slow HTTP actions
        // in one tenant queue every other tenant's ingest behind them.
        //
        // But the full 128 is worse in the other direction: every site request
        // holds a site permit AND, after the loopback hop, an API permit, so
        // HTTP actions alone could occupy all 128 API permits and the cell
        // would stop answering queries, mutations and sync entirely. Waiting
        // for a permit also sits OUTSIDE the request timeout (the limiter is
        // applied outside it), so those requests block until the client gives
        // up. Half leaves the API listener a floor it cannot be pushed below.
        MAX_CONCURRENT_REQUESTS / 2,
        shutdown_rx,
    );

    let serve_future = future::try_join(serve_api, serve_site).fuse();
    futures::pin_mut!(serve_future);

    // Why the outcome is captured rather than returned from the arm: BOTH exits
    // must still unload the instances. Returning early from the listener-error
    // arm drops `supervisor_handle`, and dropping a `SpawnHandle` CANCELS its
    // task (only `spawn_background` detaches) — so a listener failing to bind
    // would abandon the supervisor at whatever await point it was on, quite
    // possibly mid-boot with a store open and no `Application::shutdown()`.
    let exit = futures::select! {
        r = serve_future => Exit::Listeners(match r {
            Ok(_) => Err(anyhow::anyhow!("the listeners stopped unexpectedly")),
            Err(e) => Err(e),
        }),
        () = next_termination(&mut sigterm, &mut sigint).fuse() => Exit::Signal,
    };

    if matches!(exit, Exit::Signal) {
        tracing::info!("received a termination signal; draining");
        let _: Result<_, _> = shutdown_tx.broadcast(()).await;

        // Drain requests and stop the listeners. Only then may the supervisor
        // unload anything: unloading an instance stops the workers a
        // still-running request would be using.
        let drained = async {
            serve_future.await?;
            Ok::<_, anyhow::Error>(())
        }
        .fuse();
        futures::pin_mut!(drained);
        let mut grace = runtime.wait(SHUTDOWN_GRACE).fuse();
        futures::select! {
            // Deliberately NOT `?`. Returning here would skip the unload
            // below — precisely the failure the `Exit` capture above exists to
            // prevent — and a listener error is not actionable once the process
            // is already terminating.
            r = drained => if let Err(e) = r {
                tracing::warn!("the listeners errored while draining: {e:#}");
            },
            _ = grace => tracing::warn!(
                "requests did not drain within {SHUTDOWN_GRACE:?}; unloading instances anyway"
            ),
            () = next_termination(&mut sigterm, &mut sigint).fuse() => tracing::warn!(
                "second termination signal; unloading instances now"
            ),
        }
    }

    // ALWAYS, on either exit: the listeners are down and nothing new can
    // arrive, so the instances the drained requests were using can be unloaded.
    let _: Result<_, _> = supervisor_tx.broadcast(()).await;
    let mut unload = supervisor_handle.join().fuse();
    let mut unload_budget = runtime.wait(UNLOAD_BUDGET).fuse();
    futures::select! {
        r = unload => r?,
        _ = unload_budget => tracing::error!(
            "instances did not finish unloading within {UNLOAD_BUDGET:?}; some stores may not \
             have been flushed. Lower ROCKSDB_SHUTDOWN_TIMEOUT_SECONDS or raise the pod's \
             terminationGracePeriodSeconds."
        ),
    }
    match exit {
        Exit::Signal => {
            tracing::info!("shut down cleanly");
            Ok(())
        },
        Exit::Listeners(r) => r,
    }
}

/// Why this exists rather than `matches!` on a bare `Result`: the listener
/// branch carries an error that must be returned AFTER the instances are
/// unloaded, not instead of unloading them.
enum Exit {
    Signal,
    Listeners(anyhow::Result<()>),
}

/// The next SIGINT **or SIGTERM**, on streams registered at startup.
///
/// Both matter: Kubernetes only ever sends SIGTERM, and `tokio::signal::ctrl_c`
/// — the obvious thing to reach for — is SIGINT only. With `ctrl_c()` alone
/// every pod replacement went: preStop sleep, SIGTERM discarded by the kernel
/// (PID 1, default disposition), the process serving on obliviously, then
/// SIGKILL at the end of the grace period. No drain, no
/// `Application::shutdown()`, every store killed open, so every restart was a
/// WAL recovery — none of the ordering above ran even once in a cluster.
async fn next_termination(sigterm: &mut signal::unix::Signal, sigint: &mut signal::unix::Signal) {
    futures::select! {
        _ = sigterm.recv().fuse() => {},
        _ = sigint.recv().fuse() => {},
    }
}
