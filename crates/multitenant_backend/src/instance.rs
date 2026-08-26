//! Booting and tearing down one hosted instance.
//!
//! An instance is a full `LocalAppState` — its own persistence, `Database`,
//! `Application`, `KeyBroker`, file storage and background workers — assembled
//! by `local_backend::make_app_with_shared` over the process-wide resources in
//! [`crate::shared`]. Nothing about it is a special case; the only thing this
//! does that a single-tenant `main` does not is DERIVE the per-instance config
//! instead of parsing it from argv.
//!
//! ## Per-instance fault isolation
//!
//! A single-tenant `main` builds ONE `ShutdownSignal` and exits the process
//! when it fires. Here that would mean one tenant's failed store killing every
//! co-tenant — the exact blast radius this crate exists to shrink. So each
//! instance gets its own signal, and its firing is forwarded to the supervisor
//! as an [`InstanceFault`] naming the instance; the supervisor unloads that one
//! and leaves the process running.
//!
//! That is fault isolation for *reported* fatal errors. A genuine panic still
//! ends the process — `panic = "abort"` in the release profile, and the isolate
//! pool is shared — so this is a bound on operational failures (a full disk, a
//! corrupt store, a lost lease), not a memory-safety boundary.
//!
//! ## Why the RocksDB driver is opened directly
//!
//! `db_connection::connect_persistence` is the right entry point for the
//! relational drivers, which derive one database per instance from the instance
//! name and need nothing else. The embedded driver needs three things it cannot
//! read from the environment in a multi-database process — a per-instance
//! backup directory, a divided descriptor budget, and a metric label — so it is
//! opened here with them set explicitly. See
//! [`crate::config::MultitenantConfig`].

use std::{
    sync::Arc,
    time::Duration,
};

use clusters::DbDriverTag;
use common::{
    persistence::Persistence,
    runtime::Runtime,
    shutdown::ShutdownSignal,
};
use db_connection::{
    connect_persistence,
    ConnectPersistenceFlags,
};
use indexing::index_cache::DeploymentId;
use local_backend::{
    config::LocalConfig,
    make_app_with_shared,
    LocalAppState,
};
use rocksdb_persistence::{
    OpenOptions,
    RocksDbPersistence,
};
use runtime::prod::ProdRuntime;
use tokio::sync::{
    mpsc,
    oneshot,
};

use crate::{
    config::MultitenantConfig,
    naming,
    shared::SharedResources,
};

/// The listener ports are process-wide, not per instance. These values only
/// reach `LocalConfig`'s origin defaults, which are always overridden here, and
/// its bind-address helpers, which only `main` uses.
pub const API_PORT: u16 = 3210;
pub const SITE_PORT: u16 = 3211;

/// How long an unrouted instance is left alone before its workers are stopped.
///
/// Removing an instance from the routing map stops NEW requests, but a handler
/// that already extracted its `LocalAppState` holds a clone and keeps running.
/// This is the window those requests get.
pub const DRAIN_GRACE: Duration = Duration::from_secs(15);

/// An instance reported a fatal error through its own `ShutdownSignal`.
pub struct InstanceFault {
    pub name: String,
    pub error: anyhow::Error,
}

/// A live instance.
pub struct HostedInstance {
    pub name: String,
    pub app: LocalAppState,
    /// The shared index cache's partition for this instance, handed back to
    /// [`SharedResources::release`] on unload.
    pub deployment_id: DeploymentId,
    /// Ends this instance's long-poll log streams on unload. One per instance,
    /// so unloading a tenant does not drop its co-tenants' dashboard streams.
    zombify_tx: async_broadcast::Sender<()>,
}

impl HostedInstance {
    /// Stops the instance's streams and its background workers.
    ///
    /// The caller must already have removed this instance from the routing map
    /// and given in-flight requests time to finish: a handler holds a CLONE of
    /// the `LocalAppState`, so unrouting only stops NEW requests.
    ///
    /// Note what this does NOT do: it never touches the shared V8 isolate pool.
    /// The pool's scheduler tracks in-progress work by `client_id` and panics
    /// on an inconsistent count, so tearing a pool down under in-flight
    /// requests is a real hazard — one that does not arise here precisely
    /// because the pool is process-wide and outlives every instance.
    pub async fn shutdown(self) {
        let name = self.name;
        // Fails only if every receiver is already gone, which is fine.
        let _: Result<_, _> = self.zombify_tx.broadcast(()).await;
        if let Err(e) = self.app.shutdown().await {
            tracing::error!("instance {name} did not shut down cleanly: {e:#}");
        } else {
            tracing::info!("instance {name} shut down");
        }
    }
}

/// Boots one instance.
///
/// Returns an error rather than panicking or exiting for every failure mode;
/// the supervisor retries on the next source tick.
pub async fn boot(
    runtime: &ProdRuntime,
    config: &MultitenantConfig,
    shared: &SharedResources,
    name: &str,
    faults: mpsc::UnboundedSender<InstanceFault>,
) -> anyhow::Result<HostedInstance> {
    // Trust boundary: `name` came off the network. It is about to become a
    // directory name, an identifier and a hostname.
    naming::validate_instance_name(name)?;
    tracing::info!("booting instance {name}");

    let local_config = instance_config(config, name)?;

    // One fatal-error channel per instance. Its firing unloads THIS instance.
    let (preempt_tx, preempt_rx) = oneshot::channel();
    let preempt_signal = ShutdownSignal::new(preempt_tx);
    {
        let name = name.to_owned();
        runtime.spawn_background("instance_fault_watch", async move {
            // Err means the signal was dropped, i.e. the instance was unloaded
            // normally. Only a real fatal error reaches the supervisor.
            if let Ok(error) = preempt_rx.await {
                let _ = faults.send(InstanceFault { name, error });
            }
        });
    }

    let persistence = open_persistence(runtime, config, &local_config, name, &preempt_signal)
        .await
        .map_err(|e| e.context(format!("failed to open persistence for instance {name}")))?;

    let (zombify_tx, zombify_rx) = async_broadcast::broadcast(1);
    let (bundle, deployment_id) = shared.bundle();
    let app = match make_app_with_shared(
        runtime.clone(),
        local_config,
        persistence,
        zombify_rx,
        preempt_signal,
        bundle,
    )
    .await
    {
        Ok(app) => app,
        Err(e) => {
            // The handle minted above owns a `DeploymentId` off the shared index
            // cache. A failed boot that does not hand it back leaks a partition
            // — and, once entries exist under it, memory — for the life of the
            // process.
            shared.release(deployment_id);
            return Err(e.context(format!("failed to build application for instance {name}")));
        },
    };

    tracing::info!("instance {name} is live at {}", app.origin);
    Ok(HostedInstance {
        name: name.to_owned(),
        app,
        deployment_id,
        zombify_tx,
    })
}

/// Opens this instance's store.
async fn open_persistence(
    runtime: &ProdRuntime,
    config: &MultitenantConfig,
    local_config: &LocalConfig,
    name: &str,
    shutdown: &ShutdownSignal,
) -> anyhow::Result<Arc<dyn Persistence>> {
    match config.db {
        DbDriverTag::RocksDb => {
            let paths = config.instance_paths(name);
            std::fs::create_dir_all(&paths.db)?;
            let persistence = RocksDbPersistence::open_with(
                &paths.db,
                OpenOptions {
                    shutdown: Some(shutdown.clone()),
                    // Descriptors and memtable shape, divided by the instance
                    // cap. Memory needs nothing here: the block cache and the
                    // write-buffer manager are process-wide singletons, so
                    // every instance already shares one budget.
                    tuning: config.rocksdb_tuning,
                    ..OpenOptions::default()
                },
            )?;
            tracing::info!("instance {name} opened RocksDB at {}", paths.db.display());
            Ok(Arc::new(persistence) as Arc<dyn Persistence>)
        },
        // The relational and SQLite drivers already derive one database per
        // instance from `config.name()`, so the stock path needs nothing added.
        DbDriverTag::Sqlite
        | DbDriverTag::Postgres(_)
        | DbDriverTag::MySql(_)
        | DbDriverTag::MySqlMultitenant(_) => {
            connect_persistence(
                local_config.db,
                &local_config.db_spec,
                ConnectPersistenceFlags {
                    require_ssl: !local_config.do_not_require_ssl,
                    allow_read_only: false,
                    skip_index_creation: false,
                },
                &local_config.name(),
                runtime.clone(),
                shutdown.clone(),
            )
            .await
        },
    }
}

/// The per-instance `LocalConfig`.
///
/// Built as a struct literal rather than by synthesising an argv and calling
/// `LocalConfig::parse_from`: clap's `requires=` pairs (`convex_origin` <->
/// `convex_site`, `instance_name` <-> `instance_secret`) and the `storage`
/// argument group would turn a programming mistake into a runtime parse failure
/// discovered by the first tenant placed here.
fn instance_config(config: &MultitenantConfig, name: &str) -> anyhow::Result<LocalConfig> {
    let paths = config.instance_paths(name);
    let local_storage = paths
        .storage
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("the data directory is not valid UTF-8"))?
        .to_owned();
    let db_spec = match config.db {
        // The embedded driver addresses a database by path, and this instance's
        // path is derived, not configured.
        DbDriverTag::RocksDb | DbDriverTag::Sqlite => paths
            .db
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("the data directory is not valid UTF-8"))?
            .to_owned(),
        // The relational drivers take the cluster URL and append the database
        // name they derive from the instance name themselves.
        DbDriverTag::Postgres(_) | DbDriverTag::MySql(_) | DbDriverTag::MySqlMultitenant(_) => {
            config.db_spec.clone()
        },
    };

    Ok(LocalConfig {
        db_spec,
        db: config.db,
        interface: std::net::Ipv4Addr::UNSPECIFIED,
        port: config.api_port,
        site_proxy_port: config.site_port,
        // Always explicit, so the `convex_origin_url` / `convex_site_url`
        // localhost defaults never apply. These are what the instance reports to
        // clients and what it signs file-storage URLs with.
        convex_origin: Some(config.origins.cloud_origin(name).into()),
        convex_site: Some(config.origins.site_origin(name).into()),
        convex_http_proxy: None,
        instance_name: Some(name.to_owned()),
        // Derived, never stored. Distinct per instance is MANDATORY: `KeyBroker`
        // derives its encryptors from the secret alone and keeps the instance
        // name as a plain field, so instances sharing a secret would accept each
        // other's admin keys.
        instance_secret: Some(config.instance_secret(name)?),
        sentry_identifier: None,
        local_storage,
        s3_storage: false,
        do_not_require_ssl: !config.require_ssl,
        // Forced on, not read from the environment. `make_app` spawns a beacon
        // worker per app; N of those from one process is N times the telemetry,
        // all describing the same self-hosted deployment.
        disable_beacon: true,
        beacon_tag: "self-host".to_owned(),
        beacon_fields: None,
        redact_logs_to_client: config.redact_logs_to_client,
        local_log_sink: config.local_log_sink.clone(),
        subcommand: None,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use common::types::PersistenceVersion;

    use super::*;
    use crate::{
        config::SourceConfig,
        naming::OriginTemplate,
    };

    fn config() -> MultitenantConfig {
        MultitenantConfig {
            group: "cell-01".to_owned(),
            origins: OriginTemplate {
                scheme: "http".to_owned(),
                group: "cell-01".to_owned(),
                base_domain: "127.0.0.1.nip.io".to_owned(),
            },
            instance_header: crate::host::DEFAULT_INSTANCE_HEADER.to_owned(),
            root_secret_hex: "0".repeat(63) + "1",
            secret_info_prefix: naming::DEFAULT_SECRET_INFO_PREFIX.to_owned(),
            source: SourceConfig::Static { names: vec![] },
            poll_interval: Duration::from_secs(2),
            data_dir: PathBuf::from("/convex/data"),
            backup_dir: None,
            admin_token: None,
            api_port: crate::instance::API_PORT,
            site_port: crate::instance::SITE_PORT,
            max_instances: 24,
            boot_concurrency: 4,
            isolate_percent_per_client: 25,
            db: DbDriverTag::RocksDb,
            db_spec: String::new(),
            legacy_instance: None,
            require_ssl: false,
            redact_logs_to_client: false,
            local_log_sink: None,
            rocksdb_tuning: Default::default(),
        }
    }

    #[test]
    fn derives_per_instance_origins_paths_and_secret() {
        let cfg = instance_config(&config(), "i-0068a1f39c2b4d5e6f708192").unwrap();
        assert_eq!(
            cfg.convex_origin_url().unwrap().as_str(),
            "http://i-0068a1f39c2b4d5e6f708192.cell-01.api.127.0.0.1.nip.io"
        );
        assert_eq!(
            cfg.convex_site_url().unwrap().as_str(),
            "http://i-0068a1f39c2b4d5e6f708192.cell-01.site.127.0.0.1.nip.io"
        );
        assert_eq!(
            cfg.local_storage,
            "/convex/data/instances/i-0068a1f39c2b4d5e6f708192/storage"
        );
        assert_eq!(
            cfg.db_spec,
            "/convex/data/instances/i-0068a1f39c2b4d5e6f708192/db"
        );
        assert_eq!(cfg.name(), "i-0068a1f39c2b4d5e6f708192");
        // The secret must be a valid `DeploymentSecret`, or every admin key and
        // every encryptor for this instance is unusable.
        assert!(cfg.key_broker().is_ok());
    }

    #[test]
    fn two_instances_never_share_a_secret_or_a_directory() {
        let c = config();
        let a = instance_config(&c, "i-0068a1f39c2b4d5e6f708192").unwrap();
        let b = instance_config(&c, "i-0068a1f4a1b2c3d4e5f60718").unwrap();
        assert_ne!(a.instance_secret, b.instance_secret);
        assert_ne!(a.db_spec, b.db_spec);
        assert_ne!(a.local_storage, b.local_storage);
    }

    #[test]
    fn an_adopted_instance_keeps_the_paths_and_origin_it_already_had() {
        let mut c = config();
        c.legacy_instance = Some("cell-01".to_owned());
        let cfg = instance_config(&c, "cell-01").unwrap();
        // A single-tenant backend wrote these; adopting it must move no bytes.
        assert_eq!(cfg.db_spec, "/convex/data/db");
        assert_eq!(cfg.local_storage, "/convex/data/storage");
        // ...and its origin is the bare group host, unchanged.
        assert_eq!(
            cfg.convex_origin_url().unwrap().as_str(),
            "http://cell-01.api.127.0.0.1.nip.io"
        );
    }

    #[test]
    fn the_relational_drivers_get_the_cluster_url_verbatim() {
        let mut c = config();
        c.db = DbDriverTag::Postgres(PersistenceVersion::V5);
        c.db_spec = "postgresql://postgres:pw@localhost:5432".to_owned();
        let cfg = instance_config(&c, "i-0068a1f39c2b4d5e6f708192").unwrap();
        // The driver appends the database name it derives from `name()`; the
        // URL must therefore still have an empty path here.
        assert_eq!(cfg.db_spec, "postgresql://postgres:pw@localhost:5432");
    }

    #[test]
    fn the_beacon_is_always_off_and_storage_is_local() {
        let cfg = instance_config(&config(), "i-0068a1f39c2b4d5e6f708192").unwrap();
        assert!(cfg.disable_beacon);
        assert!(!cfg.s3_storage);
        assert!(cfg.subcommand.is_none());
    }

    #[test]
    fn https_origins_propagate() {
        let mut c = config();
        c.origins.scheme = "https".to_owned();
        c.origins.base_domain = "example.com".to_owned();
        let cfg = instance_config(&c, "i-0068a1f39c2b4d5e6f708192").unwrap();
        assert_eq!(
            cfg.convex_origin_url().unwrap().as_str(),
            "https://i-0068a1f39c2b4d5e6f708192.cell-01.api.example.com"
        );
    }

    #[test]
    fn a_bad_name_never_becomes_a_config() {
        // The secret derivation validates the name, so a name that slipped past
        // the source's sanitiser and the host middleware still cannot become a
        // storage path or an identifier.
        for bad in ["../../etc", "Foo", "1abc", "a b"] {
            assert!(instance_config(&config(), bad).is_err(), "{bad}");
        }
    }
}
