//! Process configuration, read once from the environment at boot.
//!
//! The binary takes no command-line arguments. `local_backend`'s `LocalConfig`
//! is a clap parser whose interesting half (`--instance-name`,
//! `--instance-secret`, `--convex-origin`, `--convex-site`, `--local-storage`)
//! is PER INSTANCE, and those values do not exist at process start — they
//! arrive later, from the instance source. So the process reads a small
//! host-scoped configuration here and synthesises one `LocalConfig` per
//! instance in [`crate::instance`].
//!
//! Every knob is validated at boot rather than at first use: a host whose base
//! domain is empty should crash-loop visibly, not serve 404s for every request
//! until someone reads the logs.

use std::{
    path::PathBuf,
    time::Duration,
};

use clusters::DbDriverTag;
use common::types::PersistenceVersion;

use crate::{
    host::DEFAULT_INSTANCE_HEADER,
    naming::{
        self,
        OriginTemplate,
    },
};

const DEFAULT_ORIGIN_SCHEME: &str = "http";
const DEFAULT_DATA_DIR: &str = "/convex/data";
const DEFAULT_POLL_MS: u64 = 2_000;
const DEFAULT_MAX_INSTANCES: usize = 24;
const DEFAULT_BOOT_CONCURRENCY: usize = 4;
/// Share of the shared V8 isolate pool any one instance may occupy.
///
/// `InProcessFunctionRunner::new` hardcodes 100 because it is single tenant. At
/// 100 here, one instance's function storm occupies every isolate worker and
/// its co-tenants get `PerClientWorkerOverloaded`.
const DEFAULT_ISOLATE_PERCENT_PER_CLIENT: usize = 25;
/// Descriptors reserved for everything that is not a RocksDB table file: the
/// listeners, the node subprocess pipes, the search index readers, the HTTP
/// client pools.
const NON_DB_FD_RESERVE: u64 = 512;
/// Floor on the per-database descriptor budget. Below this RocksDB reopens
/// table files constantly and every read pays an `open`.
const MIN_FDS_PER_DB: i32 = 64;
/// What `MULTITENANT_DATA_DIR` is expected to be able to open in total when the
/// process's own `RLIMIT_NOFILE` cannot be read.
const ASSUMED_FD_LIMIT: u64 = 65_536;

/// Where the roster of instances to host comes from.
#[derive(Clone, Debug)]
pub enum SourceConfig {
    /// A JSON file on disk, re-read on an interval. The file is the whole
    /// desired set — an instance is retired by removing it — which makes this
    /// usable both for a fixed deployment (write it once) and for one driven by
    /// a config-map projection that rewrites it.
    File { path: PathBuf },
    /// An HTTP control plane, polled on an interval with `If-None-Match`.
    Http {
        url: String,
        /// Bearer token. NEVER LOG THIS.
        bearer: Option<String>,
    },
    /// A fixed list from the environment. Nothing polls; the set never changes
    /// for the life of the process. This is the single-node and test shape.
    Static { names: Vec<String> },
}

#[derive(Clone)]
pub struct MultitenantConfig {
    /// The label every instance on this host shares — the first non-instance
    /// label of a public hostname, and the name of the adopted single-tenant
    /// instance if there is one.
    pub group: String,
    pub origins: OriginTemplate,
    /// The header that selects an instance for callers with no per-instance
    /// hostname to use. Overridable so a deployment that already sends its own
    /// selector keeps working.
    pub instance_header: String,
    /// The 32-byte root secret, 64 lowercase hex chars, from which every
    /// per-instance deployment secret is derived. NEVER LOG THIS.
    pub root_secret_hex: String,
    /// HKDF `info` prefix. Overridable so a deployment that already mints admin
    /// keys against another prefix keeps them valid.
    pub secret_info_prefix: String,
    pub source: SourceConfig,
    pub poll_interval: Duration,
    pub data_dir: PathBuf,
    /// Backup root. Each instance gets `<root>/<instance>`; sharing one
    /// directory between databases would interleave their backup chains.
    pub backup_dir: Option<PathBuf>,
    /// Bearer token for the cell-wide fleet endpoints (`crate::fleet`).
    ///
    /// `None` means those routes are NOT MOUNTED. Absence is the safe default:
    /// a cell never configured for fleet operations should not answer them at
    /// all, rather than answer 401 and invite guessing. This authorises an
    /// operation across every hosted instance, so it must not be any single
    /// instance's admin key.
    pub admin_token: Option<String>,
    pub max_instances: usize,
    pub boot_concurrency: usize,
    pub isolate_percent_per_client: usize,
    /// The persistence driver. `rocksdb` gives each instance its own embedded
    /// store under its own directory, which is the shape this host is for;
    /// `postgres-v5` and `mysql-v5` give each instance its own database in one
    /// cluster, which the stock drivers already derive from the instance name.
    pub db: DbDriverTag,
    /// For the relational drivers, the cluster URL with an EMPTY path — the
    /// driver appends the per-instance database name and refuses a URL that
    /// already names one. Unused (and empty) for `rocksdb`, whose per-instance
    /// path comes from `data_dir`. May carry a password. NEVER LOG THIS.
    pub db_spec: String,
    /// The instance, if any, whose data was written by a single-tenant backend
    /// directly under `data_dir` and must be reopened where it lies. Always
    /// desired, roster or not — see [`crate::supervisor`].
    pub legacy_instance: Option<String>,
    pub require_ssl: bool,
    pub redact_logs_to_client: bool,
    pub local_log_sink: Option<String>,
    /// Per-database RocksDB tuning, derived from `max_instances` so that N
    /// stores divide one process's descriptors and memtable budget rather than
    /// each taking a single-tenant share. See [`crate::instance`].
    pub rocksdb_tuning: rocksdb_persistence::options::DbTuning,
}

/// Hand-written so a stray `{:?}` cannot print the root secret, the source
/// bearer token, or a password embedded in the cluster URL. `#[derive(Debug)]`
/// on a struct holding three secrets is one careless `tracing::debug!` away
/// from a credential in a log aggregator.
impl std::fmt::Debug for MultitenantConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultitenantConfig")
            .field("group", &self.group)
            .field("origins", &self.origins)
            .field("instance_header", &self.instance_header)
            .field("secret_info_prefix", &self.secret_info_prefix)
            .field("source", &self.source_for_debug())
            .field("poll_interval", &self.poll_interval)
            .field("data_dir", &self.data_dir)
            .field("backup_dir", &self.backup_dir)
            .field(
                "admin_token",
                &self.admin_token.as_ref().map(|_| "<redacted>"),
            )
            .field("max_instances", &self.max_instances)
            .field("boot_concurrency", &self.boot_concurrency)
            .field(
                "isolate_percent_per_client",
                &self.isolate_percent_per_client,
            )
            .field("db", &self.db)
            .field("legacy_instance", &self.legacy_instance)
            .field("require_ssl", &self.require_ssl)
            .field("redact_logs_to_client", &self.redact_logs_to_client)
            .field("local_log_sink", &self.local_log_sink)
            .field("rocksdb_tuning", &self.rocksdb_tuning)
            .finish_non_exhaustive()
    }
}

impl MultitenantConfig {
    fn source_for_debug(&self) -> String {
        match &self.source {
            SourceConfig::File { path } => format!("file:{}", path.display()),
            SourceConfig::Http { url, .. } => format!("http:{url}"),
            SourceConfig::Static { names } => format!("static:{}", names.join(",")),
        }
    }

    pub fn from_env() -> anyhow::Result<Self> {
        // FAIL CLOSED ON CONVEX_SITE. `ExtractResolvedHostname` falls back to
        // it when a request resolves to no deployment, which on a host serving
        // several would silently route an unrouted request into whichever
        // instance the variable happens to name instead of 404ing. There is no
        // safe value, so refuse to start.
        if std::env::var("CONVEX_SITE").is_ok_and(|site| !site.is_empty()) {
            anyhow::bail!(
                "CONVEX_SITE is set and must not be: the request-to-deployment fallback would \
                 silently route unrouted requests into one arbitrary tenant. Unset it."
            );
        }

        let group = required("MULTITENANT_GROUP")?;
        naming::validate_instance_name(&group)
            .map_err(|e| anyhow::anyhow!("MULTITENANT_GROUP is not a usable instance name: {e}"))?;

        let base_domain = required("MULTITENANT_BASE_DOMAIN")?;
        anyhow::ensure!(
            !base_domain.contains('/') && !base_domain.contains(':'),
            "MULTITENANT_BASE_DOMAIN must be a bare domain, got {base_domain:?}"
        );
        let scheme = optional("MULTITENANT_ORIGIN_SCHEME")
            .unwrap_or_else(|| DEFAULT_ORIGIN_SCHEME.to_owned());
        anyhow::ensure!(
            scheme == "http" || scheme == "https",
            "MULTITENANT_ORIGIN_SCHEME must be http or https, got {scheme:?}"
        );

        let root_secret_hex = required("MULTITENANT_ROOT_SECRET")?;
        anyhow::ensure!(
            root_secret_hex.len() == 64
                && root_secret_hex
                    .chars()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "MULTITENANT_ROOT_SECRET must be 64 lowercase hex characters (openssl rand -hex 32)"
        );
        let secret_info_prefix = optional("MULTITENANT_SECRET_INFO_PREFIX")
            .unwrap_or_else(|| naming::DEFAULT_SECRET_INFO_PREFIX.to_owned());

        let source = source_from_env()?;

        let data_dir = PathBuf::from(
            optional("MULTITENANT_DATA_DIR").unwrap_or_else(|| DEFAULT_DATA_DIR.into()),
        );
        let backup_dir = optional("MULTITENANT_BACKUP_DIR").map(PathBuf::from);
        let admin_token = optional("MULTITENANT_ADMIN_TOKEN");
        if let Some(token) = &admin_token {
            anyhow::ensure!(
                token.len() >= 32,
                "MULTITENANT_ADMIN_TOKEN must be at least 32 characters; it authorises a \
                 cell-wide operation"
            );
        }

        let max_instances = parse_or("MULTITENANT_MAX_INSTANCES", DEFAULT_MAX_INSTANCES)?;
        anyhow::ensure!(max_instances > 0, "MULTITENANT_MAX_INSTANCES must be > 0");
        let boot_concurrency = parse_or("MULTITENANT_BOOT_CONCURRENCY", DEFAULT_BOOT_CONCURRENCY)?;
        anyhow::ensure!(
            boot_concurrency > 0,
            "MULTITENANT_BOOT_CONCURRENCY must be > 0"
        );
        let isolate_percent_per_client = parse_or(
            "MULTITENANT_ISOLATE_PERCENT_PER_CLIENT",
            DEFAULT_ISOLATE_PERCENT_PER_CLIENT,
        )?;
        anyhow::ensure!(
            (1..=100).contains(&isolate_percent_per_client),
            "MULTITENANT_ISOLATE_PERCENT_PER_CLIENT must be between 1 and 100"
        );

        let db = parse_driver(optional("MULTITENANT_DB").as_deref().unwrap_or("rocksdb"))?;
        let db_spec = optional("MULTITENANT_DB_SPEC").unwrap_or_default();
        if !matches!(db, DbDriverTag::RocksDb) {
            anyhow::ensure!(
                !db_spec.is_empty(),
                "MULTITENANT_DB_SPEC is required for the {db:?} driver: a cluster URL with an \
                 EMPTY path, from which the driver derives one database per instance"
            );
        }

        let legacy_instance = match optional("MULTITENANT_LEGACY_INSTANCE") {
            Some(name) => {
                naming::validate_instance_name(&name)?;
                Some(name)
            },
            None => None,
        };

        let poll_interval =
            Duration::from_millis(parse_or("MULTITENANT_POLL_MS", DEFAULT_POLL_MS)?);
        anyhow::ensure!(
            poll_interval >= Duration::from_millis(100),
            "MULTITENANT_POLL_MS must be at least 100"
        );

        let instance_header = optional("MULTITENANT_INSTANCE_HEADER")
            .unwrap_or_else(|| DEFAULT_INSTANCE_HEADER.to_owned())
            .to_ascii_lowercase();
        anyhow::ensure!(
            http::HeaderName::try_from(instance_header.as_str()).is_ok(),
            "MULTITENANT_INSTANCE_HEADER is not a valid header name: {instance_header:?}"
        );

        Ok(Self {
            origins: OriginTemplate {
                scheme,
                group: group.clone(),
                base_domain,
            },
            group,
            instance_header,
            root_secret_hex,
            secret_info_prefix,
            source,
            poll_interval,
            data_dir,
            backup_dir,
            admin_token,
            max_instances,
            boot_concurrency,
            isolate_percent_per_client,
            db,
            db_spec,
            legacy_instance,
            require_ssl: !flag("DO_NOT_REQUIRE_SSL"),
            redact_logs_to_client: flag("REDACT_LOGS_TO_CLIENT"),
            local_log_sink: optional("LOCAL_LOG_SINK"),
            rocksdb_tuning: rocksdb_tuning(max_instances),
        })
    }

    /// The per-instance deployment secret.
    pub fn instance_secret(&self, instance: &str) -> anyhow::Result<String> {
        naming::derive_instance_secret(&self.root_secret_hex, &self.secret_info_prefix, instance)
    }

    /// Where this instance's data lives.
    pub fn instance_paths(&self, instance: &str) -> naming::InstancePaths {
        naming::instance_paths(&self.data_dir, instance, self.legacy_instance.as_deref())
    }

    /// Where this instance's backups go, or `None` if backups are off.
    ///
    /// A directory per instance, never a shared one: `BackupEngine` numbers
    /// generations per directory with no record of which database wrote them,
    /// so two databases pointed at one directory interleave their chains and
    /// each one's pruning deletes the other's generations.
    ///
    /// NOT read by this process. `rocksdb_persistence` no longer schedules
    /// backups in-process — they are operations driven by the `rocksdb-backup`
    /// binary from a CronJob, the same shape as `pg_basebackup`. This stays
    /// because the naming convention has to be stated and tested somewhere, and
    /// the host is what knows an instance's layout: a scheduler asking "where
    /// do this tenant's backups live?" should ask here rather than re-deriving
    /// `<root>/<instance>` and getting it subtly wrong.
    pub fn instance_backup_dir(&self, instance: &str) -> Option<PathBuf> {
        self.backup_dir.as_ref().map(|root| root.join(instance))
    }
}

/// Divides the process's descriptor and memtable budgets between the databases
/// it may open.
///
/// The block cache and the write-buffer manager are already shared process-wide
/// by `rocksdb_persistence`, so memory does not need dividing — but the SHAPE
/// of the memtable bound does, and descriptors are a hard per-process resource
/// that nothing shares. Both are derived from `max_instances` rather than from
/// the number currently hosted, because a database opened under a light load
/// must not have to be reconfigured when the host fills up.
fn rocksdb_tuning(max_instances: usize) -> rocksdb_persistence::options::DbTuning {
    let per_db_fds = (fd_limit().saturating_sub(NON_DB_FD_RESERVE) / max_instances as u64)
        .min(i32::MAX as u64) as i32;
    rocksdb_persistence::options::DbTuning {
        max_open_files: Some(per_db_fds.max(MIN_FDS_PER_DB)),
        // Five column families per database. Keeping the per-family target
        // small is what stops the shared write-buffer manager from spending its
        // whole budget on one tenant and then force-flushing everyone.
        memtable_bytes: Some(memtable_bytes(max_instances)),
        // Leave the compaction concurrency alone: the thread pools are already
        // process-wide (see `rocksdb_persistence::options::BACKGROUND_JOBS`), so
        // the only thing a lower value would buy is a per-tenant cap on
        // simultaneous compactions, and starving compaction is a worse failure
        // than one tenant briefly occupying the pool.
        background_jobs: None,
    }
}

/// A per-column-family memtable target that leaves room for `max_instances`
/// databases in the shared write-buffer budget, clamped to a range where
/// RocksDB still behaves: too small and every flush produces a tiny L0 file,
/// too large and one tenant's memtables are the whole budget.
fn memtable_bytes(max_instances: usize) -> usize {
    const COLUMN_FAMILIES: usize = 5;
    const MIN: usize = 4 << 20;
    const MAX: usize = 64 << 20;
    let budget = *rocksdb_persistence::options::WRITE_BUFFER_BYTES;
    (budget / (max_instances * COLUMN_FAMILIES)).clamp(MIN, MAX)
}

/// This process's soft `RLIMIT_NOFILE`, or a conservative assumption.
fn fd_limit() -> u64 {
    // SAFETY: `getrlimit` writes into the `rlimit` we hand it and returns
    // nonzero on failure, in which case the value is left untouched — which is
    // why it is initialised before the call.
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    if rc != 0 || limit.rlim_cur == 0 || limit.rlim_cur == libc::RLIM_INFINITY {
        return ASSUMED_FD_LIMIT;
    }
    limit.rlim_cur as u64
}

fn source_from_env() -> anyhow::Result<SourceConfig> {
    let file = optional("MULTITENANT_INSTANCES_FILE");
    let url = optional("MULTITENANT_ROSTER_URL");
    let list = optional("MULTITENANT_INSTANCES");
    let set = [file.is_some(), url.is_some(), list.is_some()]
        .iter()
        .filter(|s| **s)
        .count();
    anyhow::ensure!(
        set == 1,
        "set exactly one of MULTITENANT_INSTANCES_FILE, MULTITENANT_ROSTER_URL or \
         MULTITENANT_INSTANCES (got {set})"
    );
    if let Some(path) = file {
        return Ok(SourceConfig::File {
            path: PathBuf::from(path),
        });
    }
    if let Some(url) = url {
        anyhow::ensure!(
            url.starts_with("http://") || url.starts_with("https://"),
            "MULTITENANT_ROSTER_URL must be an http(s) URL, got {url:?}"
        );
        return Ok(SourceConfig::Http {
            url: url.trim_end_matches('/').to_owned(),
            bearer: optional("MULTITENANT_ROSTER_TOKEN"),
        });
    }
    let names: Vec<String> = list
        .expect("exactly one source is set")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    for name in &names {
        naming::validate_instance_name(name)
            .map_err(|e| anyhow::anyhow!("MULTITENANT_INSTANCES: {e}"))?;
    }
    Ok(SourceConfig::Static { names })
}

fn parse_driver(raw: &str) -> anyhow::Result<DbDriverTag> {
    Ok(match raw {
        "rocksdb" => DbDriverTag::RocksDb,
        "sqlite" => DbDriverTag::Sqlite,
        "postgres-v5" => DbDriverTag::Postgres(PersistenceVersion::V5),
        "mysql-v5" => DbDriverTag::MySql(PersistenceVersion::V5),
        other => anyhow::bail!(
            "MULTITENANT_DB must be one of rocksdb, sqlite, postgres-v5, mysql-v5; got {other:?}"
        ),
    })
}

fn required(key: &str) -> anyhow::Result<String> {
    optional(key).ok_or_else(|| anyhow::anyhow!("{key} is required"))
}

fn optional(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

fn flag(key: &str) -> bool {
    optional(key).is_some_and(|v| {
        let v = v.to_ascii_lowercase();
        v == "1" || v == "true" || v == "yes"
    })
}

fn parse_or<T: std::str::FromStr>(key: &str, default: T) -> anyhow::Result<T>
where
    T::Err: std::fmt::Display,
{
    match optional(key) {
        None => Ok(default),
        Some(raw) => raw
            .parse()
            .map_err(|e| anyhow::anyhow!("{key}={raw:?} is not valid: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_debug_impl_never_prints_a_secret() {
        let config = MultitenantConfig {
            group: "cell-01".to_owned(),
            origins: OriginTemplate {
                scheme: "http".to_owned(),
                group: "cell-01".to_owned(),
                base_domain: "example.com".to_owned(),
            },
            instance_header: DEFAULT_INSTANCE_HEADER.to_owned(),
            root_secret_hex: "deadbeef".repeat(8),
            secret_info_prefix: naming::DEFAULT_SECRET_INFO_PREFIX.to_owned(),
            source: SourceConfig::Http {
                url: "http://control".to_owned(),
                bearer: Some("hunter2".to_owned()),
            },
            poll_interval: Duration::from_secs(2),
            data_dir: "/convex/data".into(),
            backup_dir: None,
            max_instances: 24,
            boot_concurrency: 4,
            isolate_percent_per_client: 25,
            db: DbDriverTag::RocksDb,
            db_spec: "postgresql://user:hunter3@host:5432".to_owned(),
            legacy_instance: None,
            require_ssl: true,
            redact_logs_to_client: false,
            local_log_sink: None,
            rocksdb_tuning: Default::default(),
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("deadbeef"), "{rendered}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(!rendered.contains("hunter3"), "{rendered}");
        // ...but it still says enough to debug a misconfiguration.
        assert!(rendered.contains("cell-01"));
        assert!(rendered.contains("http://control"));
    }

    #[test]
    fn backups_get_a_directory_per_instance() {
        let mut config = base_config();
        config.backup_dir = Some("/backups".into());
        assert_eq!(
            config.instance_backup_dir("i-0068a1f3"),
            Some(PathBuf::from("/backups/i-0068a1f3"))
        );
        config.backup_dir = None;
        assert_eq!(config.instance_backup_dir("i-0068a1f3"), None);
    }

    #[test]
    fn the_descriptor_budget_is_divided_and_floored() {
        // A generous limit divides cleanly...
        let generous = rocksdb_tuning(8);
        assert!(generous.max_open_files.unwrap() >= MIN_FDS_PER_DB);
        // ...and an implausible instance count still leaves each database
        // enough descriptors to function rather than a number near zero.
        let crowded = rocksdb_tuning(100_000);
        assert_eq!(crowded.max_open_files, Some(MIN_FDS_PER_DB));
        // Never unlimited: that is the single-tenant default and the thing this
        // exists to override.
        assert_ne!(generous.max_open_files, Some(-1));
    }

    #[test]
    fn the_memtable_target_shrinks_with_the_instance_count() {
        assert!(memtable_bytes(64) <= memtable_bytes(2));
        assert!(memtable_bytes(1_000_000) >= 4 << 20);
        assert!(memtable_bytes(1) <= 64 << 20);
    }

    fn base_config() -> MultitenantConfig {
        MultitenantConfig {
            group: "cell-01".to_owned(),
            origins: OriginTemplate {
                scheme: "http".to_owned(),
                group: "cell-01".to_owned(),
                base_domain: "example.com".to_owned(),
            },
            instance_header: DEFAULT_INSTANCE_HEADER.to_owned(),
            root_secret_hex: "0".repeat(63) + "1",
            secret_info_prefix: naming::DEFAULT_SECRET_INFO_PREFIX.to_owned(),
            source: SourceConfig::Static { names: vec![] },
            poll_interval: Duration::from_secs(2),
            data_dir: "/convex/data".into(),
            backup_dir: None,
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
}
