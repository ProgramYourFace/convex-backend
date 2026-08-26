//! Reconciles the live instance map against the desired set.
//!
//! The supervisor owns every [`HostedInstance`] and is the only writer of the
//! `ArcSwap` that the router and the `ApplicationApi` read. It reacts to two
//! inputs:
//!
//! * a new roster (admit what appeared, unload what disappeared);
//! * an [`InstanceFault`] — one instance's own `ShutdownSignal` fired.
//!
//! ## Eviction is expressed by absence, and that cuts both ways
//!
//! The roster is the whole desired set, so "not in it" is how a host learns to
//! stop serving a tenant. That makes the source's failure contract
//! load-bearing: it must never publish an empty roster on an error, or this
//! loop would dutifully unload every tenant. See [`crate::source`].
//!
//! ## Draining
//!
//! Removing an instance from the map only stops NEW requests; a handler that
//! already extracted its `LocalAppState` holds a clone. So eviction is two
//! phases: unroute and publish immediately, then stop the app's workers after
//! [`instance::DRAIN_GRACE`] on a detached task, so a slow drain never blocks
//! the next reconcile.

use std::{
    collections::{
        BTreeSet,
        HashMap,
    },
    sync::{
        atomic::{
            AtomicBool,
            Ordering,
        },
        Arc,
    },
    time::{
        Duration,
        Instant,
    },
};

use common::runtime::Runtime;
use futures::{
    FutureExt,
    StreamExt,
};
use local_backend::LocalAppState;
use runtime::prod::ProdRuntime;
use tokio::sync::{
    mpsc,
    watch,
};

use crate::{
    config::MultitenantConfig,
    instance::{
        self,
        HostedInstance,
        InstanceFault,
    },
    shared::SharedResources,
    source::Roster,
    MultitenantState,
};

/// At most one "boot failed" line per instance per minute. A tenant whose store
/// is unreadable is retried on every tick, and that must not drown the log.
const BOOT_ERROR_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// What one reconcile should do. Pure, so the interesting logic is testable
/// without a runtime, a database or V8.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct Plan {
    /// Hosted but no longer desired — unload these.
    pub evict: Vec<String>,
    /// Desired but not hosted — boot these.
    pub admit: Vec<String>,
    /// Desired, not hosted, and refused because the host is at
    /// `max_instances`. Reported so a capacity problem looks like a capacity
    /// problem rather than a silent placement failure.
    pub refused: Vec<String>,
}

/// Computes the plan.
///
/// Evictions count against the cap BEFORE admissions, so an instance leaving
/// and another arriving in the same tick both succeed — which is what a
/// relocation looks like from here.
pub fn plan(hosted: &BTreeSet<String>, desired: &BTreeSet<String>, max_instances: usize) -> Plan {
    let evict: Vec<String> = hosted.difference(desired).cloned().collect();
    let remaining = hosted.len() - evict.len();
    let mut headroom = max_instances.saturating_sub(remaining);
    let mut admit = Vec::new();
    let mut refused = Vec::new();
    // `desired` is a `BTreeSet`, so admission order is by name — deterministic,
    // which matters when the cap bites and someone has to explain which tenant
    // was refused.
    for name in desired.difference(hosted) {
        if headroom == 0 {
            refused.push(name.clone());
        } else {
            headroom -= 1;
            admit.push(name.clone());
        }
    }
    Plan {
        evict,
        admit,
        refused,
    }
}

pub struct Supervisor {
    runtime: ProdRuntime,
    config: Arc<MultitenantConfig>,
    shared: Arc<SharedResources>,
    state: MultitenantState,
    hosted: HashMap<String, HostedInstance>,
    fault_tx: mpsc::UnboundedSender<InstanceFault>,
    /// Taken by [`Supervisor::run`]; held in an `Option` so the loop can own
    /// the receiver outright and still call `&mut self` methods from a
    /// select arm.
    fault_rx: Option<mpsc::UnboundedReceiver<InstanceFault>>,
    /// Last time a boot failure was logged for a given instance.
    last_boot_error: HashMap<String, Instant>,
    /// Set once the FIRST reconcile has finished, and never cleared.
    ///
    /// This is what `/ready` reports. The listeners bind before any instance
    /// exists — deliberately, so placement can hand this process its first
    /// tenant — which means `/version` answers during a window in which every
    /// instance-scoped request would 404. A readiness probe on `/version`
    /// therefore puts the pod into the Service's endpoint list while it is
    /// still opening stores, and a rolling upgrade drops traffic at the seam.
    ready: Arc<AtomicBool>,
}

impl Supervisor {
    pub fn new(
        runtime: ProdRuntime,
        config: Arc<MultitenantConfig>,
        shared: Arc<SharedResources>,
        state: MultitenantState,
        ready: Arc<AtomicBool>,
    ) -> Self {
        let (fault_tx, fault_rx) = mpsc::unbounded_channel();
        Self {
            runtime,
            config,
            shared,
            state,
            hosted: HashMap::new(),
            fault_tx,
            fault_rx: Some(fault_rx),
            last_boot_error: HashMap::new(),
            ready,
        }
    }

    /// Runs until `shutdown_rx` fires, then unloads every instance.
    pub async fn run(
        mut self,
        mut roster_rx: watch::Receiver<Roster>,
        mut shutdown_rx: async_broadcast::Receiver<()>,
    ) {
        let mut fault_rx = self
            .fault_rx
            .take()
            .expect("Supervisor::run called more than once");
        // ONE reconcile against whatever the channel already holds, before
        // waiting for a change. The caller seeds it with the source's initial
        // read, so the instances a restart is supposed to bring back are up
        // before the listeners take their first request rather than one poll
        // interval later.
        {
            let initial = roster_rx.borrow_and_update().clone();
            // Raced against shutdown: booting N stores takes seconds each, and
            // this runs BEFORE the select loop below — so without the race a
            // pod deleted during cold start has to finish opening every
            // instance before it can begin closing any, and the termination
            // grace period expires mid-boot.
            futures::select! {
                () = self.reconcile(&initial).fuse() => {},
                _ = shutdown_rx.recv().fuse() => {
                    tracing::info!("shutdown during the first reconcile; unloading what booted");
                    self.shutdown_all().await;
                    return;
                },
            }
            // Only NOW is the pod fit to receive traffic. Note this is set even
            // when the initial roster was empty or every boot failed: readiness
            // means "this process has finished trying", not "N instances are
            // up". A cell that cannot boot its tenants must still answer, so
            // that the failure is visible as 404s and logs rather than as a pod
            // that never joins the Service.
            self.ready.store(true, Ordering::Release);
        }
        let mut source_live = true;
        loop {
            // Biased so shutdown always wins: during a termination grace period
            // there is no point admitting a new tenant.
            futures::select_biased! {
                _ = shutdown_rx.recv().fuse() => {
                    tracing::info!(
                        "supervisor shutting down {} instance(s)",
                        self.hosted.len()
                    );
                    self.shutdown_all().await;
                    return;
                },
                fault = fault_rx.recv().fuse() => {
                    let Some(fault) = fault else {
                        // Unreachable: the supervisor holds a sender itself.
                        return;
                    };
                    self.handle_fault(fault);
                },
                roster = next_roster(&mut roster_rx, source_live).fuse() => {
                    let Some(roster) = roster else {
                        // The source is gone. Keep serving what we have rather
                        // than unloading everything. Crucially this does NOT
                        // return: that would abandon the shutdown and fault
                        // arms too, leaving a process that neither closes its
                        // stores on SIGTERM nor unloads an instance that
                        // faults. Disarm just this arm instead.
                        source_live = false;
                        tracing::error!(
                            "the instance source stopped; continuing with the current instances"
                        );
                        continue;
                    };
                    self.reconcile(&roster).await;
                },
            }
        }
    }

    async fn reconcile(&mut self, roster: &Roster) {
        let hosted: BTreeSet<String> = self.hosted.keys().cloned().collect();
        let mut desired: BTreeSet<String> = roster.names().map(str::to_owned).collect();
        // AN ADOPTED INSTANCE IS ALWAYS DESIRED, roster or not.
        //
        // The instance named after the group is what the bare `<group>` host
        // resolves to, and that host is the deployment's own address — whatever
        // probes it, mints its keys or registers it uses that URL. Making the
        // process's own identity depend on a control-plane record arriving first
        // is a bring-up deadlock waiting to happen: an empty roster (a fresh
        // control plane, one whose migration has not run) would leave the bare
        // host 404ing, which fails whatever is supposed to write that record.
        // Seeding it here breaks the cycle for good and costs one instance.
        if let Some(legacy) = &self.config.legacy_instance {
            desired.insert(legacy.clone());
        }
        let plan = plan(&hosted, &desired, self.config.max_instances);

        if !plan.refused.is_empty() {
            tracing::error!(
                "at MULTITENANT_MAX_INSTANCES ({}); refusing to host {} instance(s): {}. Those \
                 tenants stay down until the cap is raised or instances are moved.",
                self.config.max_instances,
                plan.refused.len(),
                plan.refused.join(", ")
            );
        }

        if !plan.evict.is_empty() {
            let mut draining = Vec::with_capacity(plan.evict.len());
            for name in &plan.evict {
                if let Some(instance) = self.hosted.remove(name) {
                    tracing::info!("instance {name} is no longer desired; draining");
                    draining.push(instance);
                }
            }
            // Publish the unrouting BEFORE draining, so no new request can land
            // on an instance whose workers are about to stop.
            self.publish();
            for instance in draining {
                self.spawn_drain(instance);
            }
        }

        if plan.admit.is_empty() {
            return;
        }

        // Bounded fan-out: booting an instance opens a store and runs the
        // system-table bootstrap, so admitting fifty at once on a cold start
        // would stampede the disk (or, on a relational driver, the cluster).
        let runtime = self.runtime.clone();
        let config = self.config.clone();
        let shared = self.shared.clone();
        let fault_tx = self.fault_tx.clone();
        let booted: Vec<(String, anyhow::Result<HostedInstance>)> =
            futures::stream::iter(plan.admit.into_iter())
                .map(|name| {
                    let runtime = runtime.clone();
                    let config = config.clone();
                    let shared = shared.clone();
                    let fault_tx = fault_tx.clone();
                    async move {
                        let result =
                            instance::boot(&runtime, &config, &shared, &name, fault_tx).await;
                        (name, result)
                    }
                })
                .buffer_unordered(self.config.boot_concurrency)
                .collect()
                .await;

        let mut any_admitted = false;
        for (name, result) in booted {
            match result {
                Ok(instance) => {
                    self.last_boot_error.remove(&name);
                    self.hosted.insert(name, instance);
                    any_admitted = true;
                },
                Err(e) => {
                    // Retried on the next tick. One tenant whose store is
                    // unreadable must neither stop its co-tenants from being
                    // admitted nor bring the process down.
                    let should_log = self
                        .last_boot_error
                        .get(&name)
                        .is_none_or(|t| t.elapsed() >= BOOT_ERROR_LOG_INTERVAL);
                    if should_log {
                        self.last_boot_error.insert(name.clone(), Instant::now());
                        tracing::error!("failed to boot instance {name}; will retry: {e:#}");
                    }
                },
            }
        }
        if any_admitted {
            self.publish();
        }
    }

    fn handle_fault(&mut self, fault: InstanceFault) {
        tracing::error!(
            "instance {} reported a fatal error and is being unloaded; the rest of the process \
             keeps running: {:#}",
            fault.name,
            fault.error
        );
        let Some(instance) = self.hosted.remove(&fault.name) else {
            // Already unloaded — the fault raced the eviction.
            return;
        };
        self.publish();
        // A faulted instance's store has stopped accepting writes; there is
        // nothing useful left to drain, so stop it immediately.
        let shared = self.shared.clone();
        self.runtime
            .spawn_background("instance_unload", async move {
                let deployment_id = instance.deployment_id;
                instance.shutdown().await;
                shared.release(deployment_id);
            });
    }

    fn spawn_drain(&self, instance: HostedInstance) {
        let runtime = self.runtime.clone();
        let shared = self.shared.clone();
        self.runtime.spawn_background("instance_drain", async move {
            // In-flight handlers hold a clone of the `LocalAppState`; give them
            // a window before their `Application`'s workers stop.
            runtime.wait(instance::DRAIN_GRACE).await;
            let deployment_id = instance.deployment_id;
            instance.shutdown().await;
            shared.release(deployment_id);
        });
    }

    async fn shutdown_all(&mut self) {
        self.state.instances.store(Arc::new(HashMap::new()));
        let instances: Vec<HostedInstance> =
            self.hosted.drain().map(|(_, instance)| instance).collect();
        // Sequential: `Application::shutdown` stops a set of background workers
        // and closes a store per instance, and doing fifty of those at once
        // inside a termination grace period is a thundering herd.
        for instance in instances {
            let deployment_id = instance.deployment_id;
            instance.shutdown().await;
            self.shared.release(deployment_id);
        }
    }

    /// Publishes the current map to the router and the `ApplicationApi`.
    fn publish(&self) {
        let map: HashMap<String, LocalAppState> = self
            .hosted
            .iter()
            .map(|(name, instance)| (name.clone(), instance.app.clone()))
            .collect();
        tracing::info!("now hosting {} instance(s)", map.len());
        self.state.instances.store(Arc::new(map));
    }
}

/// Awaits the next roster, or `None` if the source is gone.
///
/// Split out of the select arm on purpose: the returned future is the only
/// thing borrowing `roster_rx`, so the arm's body is free to take `&mut self`.
/// The next published roster, or a future that never completes once the source
/// has stopped — so that arm stops firing instead of spinning on a closed
/// channel, while the shutdown and fault arms keep running.
async fn next_roster(rx: &mut watch::Receiver<Roster>, live: bool) -> Option<Roster> {
    if !live {
        std::future::pending::<()>().await;
    }
    rx.changed().await.ok()?;
    Some(rx.borrow_and_update().clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| (*n).to_owned()).collect()
    }

    #[test]
    fn nothing_to_do_when_they_match() {
        assert_eq!(
            plan(&set(&["a", "b"]), &set(&["a", "b"]), 10),
            Plan::default()
        );
    }

    #[test]
    fn admits_new_and_evicts_absent() {
        let p = plan(&set(&["a", "b"]), &set(&["b", "c"]), 10);
        assert_eq!(p.evict, vec!["a".to_owned()]);
        assert_eq!(p.admit, vec!["c".to_owned()]);
        assert!(p.refused.is_empty());
    }

    #[test]
    fn an_empty_roster_evicts_everything() {
        // Correct for a genuinely empty roster — and exactly why the source
        // must never synthesise one on failure.
        let p = plan(&set(&["a", "b"]), &set(&[]), 10);
        assert_eq!(p.evict, vec!["a".to_owned(), "b".to_owned()]);
        assert!(p.admit.is_empty());
    }

    #[test]
    fn the_cap_refuses_deterministically_by_name() {
        let p = plan(&set(&["a"]), &set(&["a", "b", "c", "d"]), 2);
        assert!(p.evict.is_empty());
        assert_eq!(p.admit, vec!["b".to_owned()]);
        assert_eq!(p.refused, vec!["c".to_owned(), "d".to_owned()]);
    }

    #[test]
    fn an_eviction_frees_a_slot_in_the_same_tick() {
        // Two hosted at a cap of two, one leaves and one arrives: the arrival
        // must fit, or a relocation could never complete.
        let p = plan(&set(&["a", "b"]), &set(&["a", "c"]), 2);
        assert_eq!(p.evict, vec!["b".to_owned()]);
        assert_eq!(p.admit, vec!["c".to_owned()]);
        assert!(p.refused.is_empty());
    }

    #[test]
    fn already_over_the_cap_still_evicts_and_admits_nothing() {
        let p = plan(&set(&["a", "b", "c"]), &set(&["a", "b", "c", "d"]), 2);
        assert!(p.evict.is_empty());
        assert!(p.admit.is_empty());
        assert_eq!(p.refused, vec!["d".to_owned()]);
    }
}
