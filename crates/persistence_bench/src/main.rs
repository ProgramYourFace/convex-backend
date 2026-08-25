//! Compares persistence backends by driving the **real Convex commit path**.
//!
//! `tools/kvbench` measures storage engines against a hand-written model of the
//! keys Convex writes. This measures the actual database: a real
//! [`Database`] is loaded on each backend and every write goes through
//! `begin` → insert → `commit`, so the committer, conflict checking, the index
//! registry, the index cache, the write log, retention and the persistence
//! layer are all in the measurement.
//!
//! What is *not* in the measurement: the V8 isolate, the sync worker, and HTTP.
//! A real mutation pays those on top, identically on every backend, so they
//! would compress the ratio without changing which backend is faster. Treat the
//! numbers here as the storage-attributable difference, not as end-to-end
//! mutation latency.
//!
//! `--backends postgres` needs a server the benchmark does not manage; point
//! `--pg-url` (or `PERSISTENCE_BENCH_PG_URL`) at one and the run clears its
//! `public` schema first, the way the embedded backends start by deleting their
//! directory.
//!
//! # Workload
//!
//! Modelled on aa-app's device-location ingest path — see [`workload`] for what
//! it does and why that shape matters. In short: two run-length-encoding
//! neighbour reads, an insert-or-merge into `deviceLocations`, and a
//! read-modify-write upsert of the device's `deviceLatestLocations` row. Nine
//! physical rows and three index reads per event, against a hot per-device key.
//!
//! # Usage
//!
//! ```text
//! persistence-bench [--docs N] [--batch N] [--merge-percent N] [--devices N]
//!                   [--reads N] [--pin table,table]
//!                   [--backends sqlite,rocksdb,postgres] [--pg-url URL]
//!                   [--dir PATH]
//! ```

use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

use common::{
    knobs::{
        DOCUMENT_RETENTION_RATE_LIMIT,
        INDEX_CACHE_SIZE,
    },
    persistence::Persistence,
    runtime::new_rate_limiter,
    shutdown::ShutdownSignal,
    types::PersistenceVersion,
};
use database::Database;
use governor::Quota;
use indexing::index_cache::IndexCache;
use keybroker::Identity;
use model::virtual_system_mapping;
use runtime::prod::ProdRuntime;
use search::{
    searcher::InProcessSearcher,
    Searcher,
};
use value::TableName;
mod workload;

struct Config {
    docs: usize,
    /// Documents per transaction. One transaction is one commit, so this is the
    /// unit the committer batches and the persistence layer writes.
    batch: usize,
    /// Fraction of events that extend the previous cluster instead of
    /// starting a new row, in percent. A parked vehicle merges; a moving one
    /// inserts. aa-app's RLE thresholds decide this from drift radius and
    /// speed; here it is a knob because the ratio is what changes the storage
    /// shape.
    merge_percent: u64,
    /// Fleet size. `docs / devices` is events per device, which sets how many
    /// versions pile up on each hot spine key.
    devices: u64,
    reads: usize,
    /// Tables pinned into memory before the run, the way
    /// `TABLES_TO_LOAD_IN_MEMORY` pins them in a real deployment. Reads against
    /// a pinned table's indexes never reach persistence.
    pin: Vec<String>,
    /// Connection URL for the `postgres` backend. Required to run it, since
    /// unlike the embedded backends the benchmark cannot create the server.
    pg_url: Option<String>,
    dir: PathBuf,
    backends: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            docs: 20_000,
            batch: 64,
            merge_percent: 30,
            devices: workload::DEFAULT_DEVICES,
            reads: 2_000,
            pin: Vec::new(),
            pg_url: std::env::var("PERSISTENCE_BENCH_PG_URL").ok(),
            dir: PathBuf::from("/tmp/persistence-bench"),
            backends: vec!["sqlite".to_string(), "rocksdb".to_string()],
        }
    }
}

struct Report {
    backend: &'static str,
    load: Duration,
    stats: workload::EventStats,
    write_docs_per_s: f64,
    commits_per_s: f64,
    commit_p50: Duration,
    commit_p99: Duration,
    reads_per_s: f64,
    disk_bytes: u64,
}

fn percentile(sorted: &[u64], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let i = (((sorted.len() - 1) as f64) * p).round() as usize;
    Duration::from_nanos(sorted[i])
}

/// Where a backend's bytes live, so the run can report a comparable on-disk
/// size. The embedded backends own a path; Postgres owns a database inside a
/// server that also holds unrelated files, so it is asked directly.
enum DiskUsage {
    Path(PathBuf),
    PostgresDatabase(String),
}

impl DiskUsage {
    async fn measure(&self) -> u64 {
        match self {
            Self::Path(path) => dir_size(path),
            // `pg_database_size` counts the database's relations and their
            // indexes — the same thing `dir_size` counts for the others. It
            // excludes the WAL, which is cluster-wide and would otherwise
            // charge this workload for the whole server.
            Self::PostgresDatabase(url) => pg_database_size(url).await.unwrap_or(0),
        }
    }
}

async fn pg_database_size(url: &str) -> anyhow::Result<u64> {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
    let handle = tokio::spawn(connection);
    let row = client
        .query_one("SELECT pg_database_size(current_database())", &[])
        .await;
    drop(client);
    handle.abort();
    Ok(row?.get::<_, i64>(0).max(0) as u64)
}

/// Drops and recreates the public schema so a Postgres run starts from the same
/// clean slate the embedded backends get from deleting their directory.
async fn reset_pg_schema(url: &str) -> anyhow::Result<()> {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
    let handle = tokio::spawn(connection);
    let result: anyhow::Result<()> = async {
        client
            .batch_execute("DROP SCHEMA IF EXISTS public CASCADE")
            .await?;
        client.batch_execute("CREATE SCHEMA public").await?;
        Ok(())
    }
    .await;
    drop(client);
    handle.abort();
    result
}

fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(meta) = std::fs::metadata(path)
        && meta.is_file()
    {
        return meta.len();
    }
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        total += if meta.is_dir() {
            dir_size(&entry.path())
        } else {
            meta.len()
        };
    }
    total
}

async fn run_backend(
    runtime: ProdRuntime,
    backend: &'static str,
    persistence: Arc<dyn Persistence>,
    disk: DiskUsage,
    cfg: &Config,
) -> anyhow::Result<Report> {
    let in_process_searcher = Arc::new(InProcessSearcher::new(runtime.clone())?);
    let searcher: Arc<dyn Searcher> = in_process_searcher;
    let (deleted_tablet_sender, _deleted_tablet_receiver) = tokio::sync::mpsc::channel(100);
    let (shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();

    let load_start = Instant::now();
    let database = Database::load(
        persistence,
        runtime.clone(),
        searcher,
        ShutdownSignal::new(shutdown_tx),
        virtual_system_mapping().clone(),
        IndexCache::new(*INDEX_CACHE_SIZE).new_handle(),
        Arc::new(new_rate_limiter(
            runtime.clone(),
            Quota::per_second(*DOCUMENT_RETENTION_RATE_LIMIT),
        )),
        deleted_tablet_sender,
        "persistence-bench".to_string(),
    )
    .await?;
    let load = load_start.elapsed();

    // Tables and their user indexes, before any timing starts.
    let mut tx = database.begin(Identity::system()).await?;
    workload::create_schema(&mut tx).await?;
    database
        .commit_with_write_source(tx, "persistence_bench_schema")
        .await?;

    // Pin the requested tables, as `initialize_application_system_tables` does
    // at backend startup for the tables `TABLES_TO_LOAD_IN_MEMORY` names.
    if !cfg.pin.is_empty() {
        let tables: BTreeSet<TableName> = cfg
            .pin
            .iter()
            .map(|name| name.parse())
            .collect::<anyhow::Result<_>>()?;
        database.load_indexes_into_memory(tables).await?;
    }

    // --- ingest -----------------------------------------------------------
    let mut commit_latencies = Vec::with_capacity(cfg.docs / cfg.batch + 1);
    let mut stats = workload::EventStats::default();
    let write_start = Instant::now();
    let mut applied = 0;
    while applied < cfg.docs {
        let this_batch = cfg.batch.min(cfg.docs - applied);
        let started = Instant::now();
        let mut tx = database.begin(Identity::system()).await?;
        for i in 0..this_batch {
            let n = (applied + i) as u64;
            // Devices report round-robin, so each device's rows interleave with
            // every other device's in the log — the same way a real fleet's
            // fixes arrive through a partitioned bus.
            let device = n % cfg.devices;
            let timestamp = 1_700_000_000_000.0 + (n / cfg.devices) as f64 * 1_000.0;
            let merge = cfg.merge_percent > 0 && (n % 100) < cfg.merge_percent;
            stats.add(workload::apply_location_event(&mut tx, device, timestamp, merge).await?);
        }
        database
            .commit_with_write_source(tx, "persistence_bench_ingest")
            .await?;
        commit_latencies.push(started.elapsed().as_nanos() as u64);
        applied += this_batch;
    }
    let write_elapsed = write_start.elapsed().as_secs_f64();

    // --- reads ------------------------------------------------------------
    // The dashboard read: "where is this device right now", one indexed lookup
    // per device through `deviceLatestLocations.by_device`. This is the hot
    // key — every event rewrote it — so it is the version-shadowing case.
    let read_start = Instant::now();
    let mut read_count = 0;
    let mut device = 0u64;
    while read_count < cfg.reads {
        let mut tx = database.begin(Identity::system()).await?;
        for _ in 0..64.min(cfg.reads - read_count) {
            if workload::latest_for_device(&mut tx, device % cfg.devices)
                .await?
                .is_some()
            {
                read_count += 1;
            }
            device += 1;
        }
        drop(tx);
    }
    let read_elapsed = read_start.elapsed().as_secs_f64();
    anyhow::ensure!(read_count > 0, "read phase returned nothing");

    database.shutdown().await?;
    // Give the persistence layer a moment to flush before measuring the files.
    tokio::time::sleep(Duration::from_millis(500)).await;

    commit_latencies.sort_unstable();
    Ok(Report {
        backend,
        load,
        stats,
        write_docs_per_s: cfg.docs as f64 / write_elapsed,
        commits_per_s: commit_latencies.len() as f64 / write_elapsed,
        commit_p50: percentile(&commit_latencies, 0.50),
        commit_p99: percentile(&commit_latencies, 0.99),
        reads_per_s: read_count as f64 / read_elapsed,
        disk_bytes: disk.measure().await,
    })
}

fn parse_config() -> anyhow::Result<Config> {
    let mut cfg = Config::default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let flag = args[i].clone();
        if flag == "--help" || flag == "-h" {
            println!("{HELP}");
            std::process::exit(0);
        }
        i += 1;
        let value = args
            .get(i)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("{flag} needs a value"))?;
        match flag.as_str() {
            "--docs" => cfg.docs = value.parse()?,
            "--batch" => cfg.batch = value.parse::<usize>()?.max(1),
            "--merge-percent" => cfg.merge_percent = value.parse::<u64>()?.min(100),
            "--devices" => cfg.devices = value.parse::<u64>()?.max(1),
            "--reads" => cfg.reads = value.parse()?,
            "--pin" => {
                cfg.pin = value
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            },
            "--pg-url" => cfg.pg_url = Some(value),
            "--dir" => cfg.dir = PathBuf::from(value),
            "--backends" => cfg.backends = value.split(',').map(|s| s.trim().to_string()).collect(),
            other => anyhow::bail!("unknown flag {other}"),
        }
        i += 1;
    }
    Ok(cfg)
}

fn main() -> anyhow::Result<()> {
    let cfg = parse_config()?;
    let tokio = ProdRuntime::init_tokio()?;
    let runtime = ProdRuntime::new(&tokio);

    println!(
        "convex device-location ingest benchmark\nevents={} batch={} merge={}% devices={} \
         reads={}\ndir={}\n",
        cfg.docs,
        cfg.batch,
        cfg.merge_percent,
        cfg.devices,
        cfg.reads,
        cfg.dir.display(),
    );

    let rt = runtime.clone();
    let cfg_ref = &cfg;
    let reports = runtime.block_on("persistence_bench", async move {
        let mut reports = Vec::new();
        for name in &cfg_ref.backends {
            let _ = std::fs::remove_dir_all(&cfg_ref.dir);
            std::fs::create_dir_all(&cfg_ref.dir)?;

            let (backend, persistence, disk): (&'static str, Arc<dyn Persistence>, DiskUsage) =
                match name.as_str() {
                    "sqlite" => {
                        let path = cfg_ref.dir.join("convex.sqlite3");
                        (
                            "sqlite",
                            Arc::new(sqlite::SqlitePersistence::new(
                                path.to_str().expect("non-utf8 path"),
                            )?),
                            DiskUsage::Path(path),
                        )
                    },
                    "rocksdb" => {
                        let path = cfg_ref.dir.join("rocksdb");
                        (
                            "rocksdb",
                            Arc::new(rocksdb_persistence::RocksDbPersistence::new(&path)?),
                            DiskUsage::Path(path),
                        )
                    },
                    // Unlike the embedded backends this talks to a server the
                    // benchmark does not own, so the run starts by clearing the
                    // schema rather than by deleting a directory.
                    "postgres" => {
                        let url = cfg_ref
                            .pg_url
                            .clone()
                            .ok_or_else(|| anyhow::anyhow!("--backends postgres needs --pg-url"))?;
                        reset_pg_schema(&url).await?;
                        let (pg_shutdown_tx, _pg_shutdown_rx) = tokio::sync::oneshot::channel();
                        let persistence = postgres::PostgresPersistence::new(
                            &url,
                            postgres::PostgresOptions {
                                allow_read_only: false,
                                version: PersistenceVersion::V5,
                                schema: None,
                                instance_name: "persistence-bench".into(),
                                multitenant: false,
                                skip_index_creation: false,
                            },
                            ShutdownSignal::new(pg_shutdown_tx),
                        )
                        .await?;
                        (
                            "postgres",
                            Arc::new(persistence),
                            DiskUsage::PostgresDatabase(url),
                        )
                    },
                    other => anyhow::bail!("unknown backend {other}"),
                };

            eprint!("running {backend} ... ");
            let started = Instant::now();
            let report = run_backend(rt.clone(), backend, persistence, disk, cfg_ref).await?;
            eprintln!("{:.1}s", started.elapsed().as_secs_f64());
            reports.push(report);
        }
        anyhow::Ok(reports)
    })?;

    let ms = |d: Duration| format!("{:.2}", d.as_secs_f64() * 1000.0);
    println!(
        "\n{:<9} {:>10} {:>11} {:>10} {:>10} {:>10} {:>9}",
        "backend", "events/s", "commits/s", "p50 ms", "p99 ms", "reads/s", "disk MB",
    );
    println!("{}", "-".repeat(76));
    for r in &reports {
        println!(
            "{:<9} {:>10.0} {:>11.1} {:>10} {:>10} {:>10.0} {:>9.1}",
            r.backend,
            r.write_docs_per_s,
            r.commits_per_s,
            ms(r.commit_p50),
            ms(r.commit_p99),
            r.reads_per_s,
            r.disk_bytes as f64 / (1024.0 * 1024.0),
        );
    }
    println!();
    for r in &reports {
        println!(
            "{:<9} load {:>7}   {} inserts, {} merges, {} index reads",
            r.backend,
            ms(r.load),
            r.stats.inserts,
            r.stats.merges,
            r.stats.reads,
        );
    }
    let _ = std::fs::remove_dir_all(&cfg.dir);
    Ok(())
}

const HELP: &str = "\
persistence-bench — compare persistence backends through the real Convex commit path

  --docs N            location events to apply    (default 20000)
  --batch N           events per transaction      (default 64)
  --merge-percent N   share of events that RLE-merge instead of inserting (default 30)
  --devices N         fleet size; docs/devices is events per device (default 512)
  --reads N           latest-location lookups     (default 2000)
  --pin a,b           tables to hold in memory, as TABLES_TO_LOAD_IN_MEMORY
                      does in a deployment        (default none)
  --backends a,b    subset of sqlite,rocksdb,postgres (default sqlite,rocksdb)
  --pg-url URL      postgres connection URL, or PERSISTENCE_BENCH_PG_URL
  --dir PATH        scratch directory             (default /tmp/persistence-bench)

Covers the committer, conflict checking, the index registry and cache, the write
log, retention and persistence. Does not cover the V8 isolate, the sync worker or
HTTP, which a real mutation pays identically on both backends.
";
