//! One process, N Convex deployments — each with its own database, its own
//! committer and therefore its own OCC domain, and its own directory on disk.
//!
//! ## Why
//!
//! A Convex backend is single tenant: one `Application`, one `Database`, one
//! persistence handle, one V8 isolate pool. Serving N tenants therefore means N
//! processes, and on a container platform N processes means N pods, N volumes,
//! N secrets and N rollouts — and onboarding one tenant restarts the pod its
//! neighbours are served from. That cost is not in the database layer, which is
//! already partitioned per deployment almost everywhere it matters; it is in
//! the fact that nothing ever hoisted the expensive, shareable half of a
//! backend out of `make_app`.
//!
//! This crate hosts N instances in one process, sharing the isolate pool and
//! the caches, while every instance keeps:
//!
//! * **its own store.** With the RocksDB driver that is a directory —
//!   `<data>/instances/<name>/` — and nothing else. No shared tables, no tenant
//!   column, no row of a neighbour's database. *That is what makes a tenant
//!   transferable*: moving one is a backup-and-restore of one subtree, and
//!   retiring one is `rm -rf`.
//! * **its own committer.** OCC conflicts are resolved per `Database`, so two
//!   tenants writing the same table name never contend, and a write-heavy
//!   tenant's retry storm stays inside its own store.
//! * **its own deployment secret**, derived from the host's root secret so that
//!   one instance's admin key is rejected by every other. See [`naming`].
//! * **its own fatal-error channel.** A single-tenant backend exits the process
//!   when persistence reports a fatal error; here that unloads one instance and
//!   leaves the rest serving. See [`instance`].
//!
//! ## The shape of it
//!
//! ```text
//!            instance source (file / http / static)
//!                        │
//!                        ▼
//!                   supervisor ──── admit / evict / drain
//!                        │
//!         instances: ArcSwap<HashMap<name, LocalAppState>>
//!                   ▲              ▲               ▲
//!   :3210 ──▶ host::resolve ──▶ local_backend::router::router
//!   :3211 ──▶ dev_site_proxy ─▶ :3210/http         │
//!                                       api::MultitenantApplicationApi
//! ```
//!
//! ## Four seams, and why this is not a fork
//!
//! Everything below already existed; this crate only supplies the pieces that
//! plug into it.
//!
//! 1. **Host resolution.** `ExtractResolvedHostname` checks an
//!    `axum::Extension<ResolvedHostname>` BEFORE its own hostname parsing and
//!    before its `CONVEX_SITE` fallback, so pre-routing middleware can answer
//!    "which instance?" with no change to any handler. See [`host`].
//! 2. **Request dispatch.** `ApplicationApi` already takes `host:
//!    &ResolvedHostname` as the first argument of every method; the
//!    single-tenant implementation just ignores it. A multi-instance
//!    implementation is a dispatch table. See [`api`].
//! 3. **The legacy `/api/**` handlers.** All ~90 extract
//!    `MtState<LocalAppState>`, whose `FromMtState` trait is handed the
//!    request's `Parts`. One impl covers them all. See [`state`].
//! 4. **The route table itself.** `local_backend::router::router` is generic
//!    over its state with exactly the two bounds [`state`] satisfies, so this
//!    crate mounts the real route table rather than a copy of it — which is the
//!    difference between tracking upstream and re-deriving a 350-line router on
//!    every merge.
//!
//! What those seams did NOT already cover is small and lives upstream, in the
//! crates that own it: sharing the isolate pool
//! (`function_runner::in_process_function_runner::new_shared_core`), injecting
//! shared resources into `make_app`
//! (`local_backend::make_app_with_shared`), and making a process-wide RocksDB
//! memory budget serve N stores (`rocksdb_persistence::options`).

// Release-mode layout computation of the deeply nested async blocks behind
// `ApplicationApi::execute_public_query` overflows the default query depth
// ("queries overflow the depth limit!", first release build 2026-08-26);
// dev/test profiles never hit it because they don't compute those layouts.
#![recursion_limit = "256"]

use std::{
    collections::HashMap,
    sync::Arc,
};

use application::api::ApplicationApi;
use arc_swap::ArcSwap;
use local_backend::LocalAppState;
use runtime::prod::ProdRuntime;

use crate::api::MultitenantApplicationApi;

pub mod api;
pub mod config;
pub mod fleet;
pub mod host;
pub mod instance;
pub mod naming;
pub mod shared;
pub mod source;
pub mod state;
pub mod supervisor;

/// The router state for every route this process serves.
///
/// The instance map is behind an `ArcSwap` rather than an `RwLock`: reads are
/// on the hot path of every request and every `FromMtState` extraction, writes
/// happen at most once per source tick, and a reader must never be able to
/// block the supervisor (or vice versa) while holding a request open.
#[derive(Clone)]
pub struct MultitenantState {
    pub instances: Arc<ArcSwap<HashMap<String, LocalAppState>>>,
    /// The dispatching `ApplicationApi` handed to `RouterState`. Built once and
    /// shared, because it resolves per call rather than per construction.
    pub api: Arc<dyn ApplicationApi>,
    pub runtime: ProdRuntime,
}

impl MultitenantState {
    pub fn new(runtime: ProdRuntime) -> Self {
        let instances = Arc::new(ArcSwap::from_pointee(HashMap::new()));
        Self {
            api: Arc::new(MultitenantApplicationApi::new(instances.clone())),
            instances,
            runtime,
        }
    }

    /// The app serving `instance`, if it is hosted right now.
    pub fn lookup(&self, instance: &str) -> Option<LocalAppState> {
        self.instances.load().get(instance).cloned()
    }
}
