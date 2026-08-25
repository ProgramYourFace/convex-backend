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
//! A real mutation pays those on top, identically on both backends, so they
//! would compress the ratio without changing which backend is faster. Treat the
//! numbers here as the storage-attributable difference, not as end-to-end
//! mutation latency.
//!
//! # Usage
//!
//! ```text
//! persistence-bench [--docs N] [--batch N] [--fields N] [--reads N]
//!                   [--backends sqlite,rocksdb] [--dir PATH]
//! ```

use std::{
    path::PathBuf,
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

use common::{
    identity::Identity,
    knobs::{
        DOCUMENT_RETENTION_RATE_LIMIT,
        INDEX_CACHE_SIZE,
    },
    persistence::Persistence,
    runtime::new_rate_limiter,
    shutdown::ShutdownSignal,
};
use database::{
    Database,
    TestFacingModel,
};
use governor::Quota;
use indexing::index_cache::IndexCache;
use model::virtual_system_mapping;
use runtime::prod::ProdRuntime;
use search::{
    searcher::InProcessSearcher,
    Searcher,
};
use value::{
    ConvexObject,
    ConvexValue,
    FieldName,
    TableName,
};

struct Config {
    docs: usize,
    /// Documents per transaction. One transaction is one commit, so this is the
    /// unit the committer batches and the persistence layer writes.
    batch: usize,
    /// Fields per document, which sets how much there is to serialize.
    fields: usize,
    reads: usize,
    dir: PathBuf,
    backends: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            docs: 20_000,
            batch: 64,
            fields: 8,
            reads: 2_000,
            dir: PathBuf::from("/tmp/persistence-bench"),
            backends: vec!["sqlite".to_string(), "rocksdb".to_string()],
        }
    }
}

struct Report {
    backend: &'static str,
    load: Duration,
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

/// A document with `fields` string fields, so each commit has real bytes to
/// serialize and index rather than an empty object.
fn document(n: usize, fields: usize) -> anyhow::Result<ConvexObject> {
    let mut map = std::collections::BTreeMap::new();
    map.insert(
        "seq".parse::<FieldName>()?,
        ConvexValue::Int64(n as i64),
    );
    map.insert(
        "device".parse::<FieldName>()?,
        ConvexValue::try_from(format!("device-{}", n % 4096))?,
    );
    for f in 0..fields.saturating_sub(2) {
        map.insert(
            format!("f{f}").parse::<FieldName>()?,
            ConvexValue::try_from(format!("value-{n}-{f}"))?,
        );
    }
    Ok(ConvexObject::try_from(map)?)
}

async fn run_backend(
    runtime: ProdRuntime,
    backend: &'static str,
    persistence: Arc<dyn Persistence>,
    data_path: PathBuf,
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

    let table: TableName = "events".parse()?;

    // --- writes -----------------------------------------------------------
    let mut commit_latencies = Vec::with_capacity(cfg.docs / cfg.batch + 1);
    let mut ids = Vec::with_capacity(cfg.reads);
    let write_start = Instant::now();
    let mut written = 0;
    while written < cfg.docs {
        let this_batch = cfg.batch.min(cfg.docs - written);
        let started = Instant::now();
        let mut tx = database.begin(Identity::system()).await?;
        for i in 0..this_batch {
            let id = TestFacingModel::new(&mut tx)
                .insert(&table, document(written + i, cfg.fields)?)
                .await?;
            // Keep a spread of ids so the read phase is not confined to the
            // most recently written pages.
            if ids.len() < cfg.reads && (written + i) % 7 == 0 {
                ids.push(id);
            }
        }
        database
            .commit_with_write_source(tx, "persistence_bench_write")
            .await?;
        commit_latencies.push(started.elapsed().as_nanos() as u64);
        written += this_batch;
    }
    let write_elapsed = write_start.elapsed().as_secs_f64();

    // --- reads ------------------------------------------------------------
    // A `get` by id through the same transaction machinery a query uses, so
    // this covers the index cache and, on a miss, the persistence reader.
    let read_start = Instant::now();
    let mut read_count = 0;
    for chunk in ids.chunks(64.max(1)) {
        let mut tx = database.begin(Identity::system()).await?;
        for id in chunk {
            if tx.get(*id).await?.is_some() {
                read_count += 1;
            }
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
        write_docs_per_s: cfg.docs as f64 / write_elapsed,
        commits_per_s: commit_latencies.len() as f64 / write_elapsed,
        commit_p50: percentile(&commit_latencies, 0.50),
        commit_p99: percentile(&commit_latencies, 0.99),
        reads_per_s: read_count as f64 / read_elapsed,
        disk_bytes: dir_size(&data_path),
    })
}

fn parse_config() -> anyhow::Result<Config> {
    let mut cfg = Config::default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let flag = args[i].clone();
        let mut next = || -> anyhow::Result<String> {
            i += 1;
            args.get(i)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("{flag} needs a value"))
        };
        match args[i].as_str() {
            "--docs" => cfg.docs = next()?.parse()?,
            "--batch" => cfg.batch = next()?.parse::<usize>()?.max(1),
            "--fields" => cfg.fields = next()?.parse::<usize>()?.max(2),
            "--reads" => cfg.reads = next()?.parse()?,
            "--dir" => cfg.dir = PathBuf::from(next()?),
            "--backends" => {
                cfg.backends = next()?.split(',').map(|s| s.trim().to_string()).collect()
            },
            "--help" | "-h" => {
                println!("{HELP}");
                std::process::exit(0);
            },
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
        "convex commit-path benchmark\ndocs={} batch={} fields={} reads={}\ndir={}\n",
        cfg.docs,
        cfg.batch,
        cfg.fields,
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

            let (backend, persistence, data_path): (&'static str, Arc<dyn Persistence>, PathBuf) =
                match name.as_str() {
                    "sqlite" => {
                        let path = cfg_ref.dir.join("convex.sqlite3");
                        (
                            "sqlite",
                            Arc::new(sqlite::SqlitePersistence::new(
                                path.to_str().expect("non-utf8 path"),
                            )?),
                            path,
                        )
                    },
                    "rocksdb" => {
                        let path = cfg_ref.dir.join("rocksdb");
                        (
                            "rocksdb",
                            Arc::new(rocksdb_persistence::RocksDbPersistence::new(&path)?),
                            path,
                        )
                    },
                    other => anyhow::bail!("unknown backend {other}"),
                };

            eprint!("running {backend} ... ");
            let started = Instant::now();
            let report =
                run_backend(rt.clone(), backend, persistence, data_path, cfg_ref).await?;
            eprintln!("{:.1}s", started.elapsed().as_secs_f64());
            reports.push(report);
        }
        anyhow::Ok(reports)
    })?;

    let ms = |d: Duration| format!("{:.2}", d.as_secs_f64() * 1000.0);
    println!(
        "\n{:<9} {:>10} {:>11} {:>10} {:>10} {:>10} {:>9}",
        "backend", "docs/s", "commits/s", "p50 ms", "p99 ms", "reads/s", "disk MB",
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
        println!("{:<9} database load took {}", r.backend, ms(r.load));
    }
    let _ = std::fs::remove_dir_all(&cfg.dir);
    Ok(())
}

const HELP: &str = "\
persistence-bench — compare persistence backends through the real Convex commit path

  --docs N          documents to write            (default 20000)
  --batch N         documents per transaction     (default 64)
  --fields N        fields per document           (default 8)
  --reads N         documents to read back        (default 2000)
  --backends a,b    subset of sqlite,rocksdb      (default both)
  --dir PATH        scratch directory             (default /tmp/persistence-bench)

Covers the committer, conflict checking, the index registry and cache, the write
log, retention and persistence. Does not cover the V8 isolate, the sync worker or
HTTP, which a real mutation pays identically on both backends.
";
