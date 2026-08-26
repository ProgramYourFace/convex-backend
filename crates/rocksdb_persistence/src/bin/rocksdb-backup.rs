//! Operator tooling for RocksDB backups: take one, list them, check one, and
//! restore.
//!
//! # When each command needs the backend stopped
//!
//! `backup` and `restore` open the database read-write, so the backend must be
//! stopped. RocksDB allows one writer, and that is not a lock this tool can
//! work around: a read-only instance cannot make RocksDB hold a file list
//! still, so a backup taken from one can silently omit data. See
//! `backup::backup_inner`.
//!
//! `list`, `verify` and `rehearse` only read the backup directory, so they run
//! against a live deployment.
//!
//! # Backing up a running deployment
//!
//! Snapshot the volume. Under the default `SyncMode::Every` every acknowledged
//! write is in the write-ahead log before `write` returns, so a
//! crash-consistent snapshot recovers exactly as an unclean restart does —
//! which this crate tests. That is the live-backup story; this tool is for
//! maintenance windows and for everything on the restore side.

use std::path::PathBuf;

use anyhow::Context as _;
use rocksdb_persistence::backup;

const HELP: &str = "\
rocksdb-backup — administer backups of an embedded RocksDB Convex database

  backup   <backup-dir> --db <db-dir> [--keep N]   take a generation now
  list     <backup-dir>                            generations, newest last
  verify   <backup-dir> [--id N]                   check a generation's file sizes
  rehearse <backup-dir> --scratch <dir> [--id N]   restore to scratch and read it
  restore  <backup-dir> --to <db-dir> [--id N]     restore for real
  check    --db <db-dir>                           read back a database directory

Without --id, the newest generation is used.

`check` is the odd one out: it takes a *database* directory rather than a
backup directory, because that is what a volume snapshot restores to. Snapshots
are the only backup that can be taken from a running cell, and every other verb
here needs a backup directory, so without `check` a snapshot could never be
verified. It opens read-write and replays the write-ahead log — what recovery
actually does — so give it a clone, not the live volume.

`backup` is for the database being *stopped* — before an upgrade or a risky
migration. RocksDB allows one writer, so it fails while the backend is running.
To back up a running deployment, snapshot the volume: under the default
`SyncMode::Every` every acknowledged write is in the write-ahead log before
`write` returns, so a crash-consistent snapshot recovers exactly as an unclean
restart does.

`verify` checks that the files are intact. `rehearse` checks the thing you
actually need to know — that a database restored from them opens and reads —
and is what belongs on a schedule. A backup nobody has restored is not a backup.

`restore` requires an empty or absent target: move the existing directory aside
rather than deleting it, since until the restore is confirmed good it is the only
other copy you have.

`rehearse` clears its scratch directory, but only one it created itself — so
repeated rehearsals work and pointing it at a populated directory refuses rather
than deleting.
";

struct Args {
    command: String,
    dir: PathBuf,
    to: Option<PathBuf>,
    db: Option<PathBuf>,
    keep: usize,
    scratch: Option<PathBuf>,
    id: Option<u32>,
}

fn parse() -> anyhow::Result<Args> {
    let mut raw: Vec<String> = std::env::args().skip(1).collect();
    if raw.is_empty() || raw[0] == "--help" || raw[0] == "-h" {
        println!("{HELP}");
        std::process::exit(0);
    }
    let command = raw.remove(0);
    // Every verb but `check` names a backup directory first; `check` reads a
    // database directory, which it takes through --db like `backup` does.
    let dir = if command == "check" {
        PathBuf::new()
    } else {
        anyhow::ensure!(!raw.is_empty(), "{command} needs a backup directory");
        PathBuf::from(raw.remove(0))
    };
    let mut args = Args {
        command,
        dir,
        to: None,
        db: None,
        keep: *rocksdb_persistence::options::BACKUP_KEEP,
        scratch: None,
        id: None,
    };
    let mut i = 0;
    while i < raw.len() {
        let flag = raw[i].clone();
        i += 1;
        let value = raw
            .get(i)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("{flag} needs a value"))?;
        match flag.as_str() {
            "--to" => args.to = Some(PathBuf::from(value)),
            "--db" => args.db = Some(PathBuf::from(value)),
            "--keep" => args.keep = value.parse()?,
            "--scratch" => args.scratch = Some(PathBuf::from(value)),
            "--id" => args.id = Some(value.parse()?),
            other => anyhow::bail!("unknown flag {other}"),
        }
        i += 1;
    }
    Ok(args)
}

fn resolve_id(dir: &std::path::Path, id: Option<u32>) -> anyhow::Result<u32> {
    match id {
        Some(id) => Ok(id),
        None => Ok(backup::list(dir)?
            .last()
            .ok_or_else(|| anyhow::anyhow!("no backups in {}", dir.display()))?
            .backup_id),
    }
}

fn main() {
    // `backup.rs` reports several non-fatal conditions through `tracing::warn!`
    // — a purge that could not delete old generations, a target that could not
    // be cleared after a failed restore. Without a subscriber every one of them
    // was discarded, so a backup job could exit 0 with its retention silently
    // broken. stderr, so it interleaves with the messages below rather than
    // with the tool's own stdout output.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_target(false)
        .without_time()
        .init();
    // A backtrace is right for a server and wrong for an operator tool: the
    // failures here are things like "that directory is not empty", where the
    // message is the whole answer and a stack trace buries it.
    if let Err(e) = run() {
        eprintln!("rocksdb-backup: {e:#}");
        std::process::exit(1);
    }
}

fn run() -> anyhow::Result<()> {
    let args = parse()?;
    match args.command.as_str() {
        "backup" => {
            let db_dir = args
                .db
                .ok_or_else(|| anyhow::anyhow!("backup needs --db <db-dir>"))?;
            anyhow::ensure!(
                db_dir.join("CURRENT").exists(),
                "{} is not a RocksDB database. Refusing to create one — a mistyped --db would \
                 otherwise back up an empty database over a real chain.",
                db_dir.display(),
            );
            // Opened read-write, which is what makes the backup trustworthy.
            // `BackupEngine` has to hold a file list still while it copies, and
            // only the owner of the files can do that; a secondary answers
            // `DisableFileDeletions` with `NotSupported` and the copy proceeds
            // unpinned. `backup::backup_inner` refuses a secondary outright for
            // that reason, so opening one here would fail every invocation.
            //
            // The cost is that this fails while the backend is running, since
            // RocksDB allows one writer. That is intended: backing up a live
            // deployment is a volume snapshot's job, not this tool's.
            let persistence = rocksdb_persistence::RocksDbPersistence::new(&db_dir)
                .with_context(|| {
                    format!(
                        "could not open {} for writing. `backup` needs exclusive access, so the \
                         backend must be stopped; to back up a running deployment, snapshot the \
                         volume instead.",
                        db_dir.display(),
                    )
                })?;
            let info = persistence.backup(&args.dir, args.keep)?;
            println!(
                "backup {} written: {} files, {} MiB",
                info.backup_id,
                info.num_files,
                info.size_bytes >> 20,
            );
        },
        "list" => {
            let generations = backup::list(&args.dir)?;
            if generations.is_empty() {
                println!("no backups in {}", args.dir.display());
                return Ok(());
            }
            println!(
                "{:>6}  {:>20}  {:>12}  {:>7}",
                "id", "timestamp", "size", "files"
            );
            for g in generations {
                println!(
                    "{:>6}  {:>20}  {:>9} MiB  {:>7}",
                    g.backup_id,
                    g.timestamp,
                    g.size_bytes >> 20,
                    g.num_files,
                );
            }
        },
        "verify" => {
            let id = resolve_id(&args.dir, args.id)?;
            backup::verify(&args.dir, id)?;
            println!("backup {id} verified: every file present and the expected size");
            println!("note: sizes, not checksums, and not a restore. Run `rehearse` for both.");
        },
        "rehearse" => {
            let scratch = args
                .scratch
                .ok_or_else(|| anyhow::anyhow!("rehearse needs --scratch <dir>"))?;
            let (info, read) = backup::rehearse(&args.dir, &scratch, args.id)?;
            println!(
                "backup {} restored into {} ({} files, {} MiB)",
                info.backup_id,
                scratch.display(),
                info.num_files,
                info.size_bytes >> 20,
            );
            println!(
                "read back: {} documents and {} index entries decoded, {} rows scanned",
                read.documents, read.index_entries, read.rows,
            );
        },
        "restore" => {
            let to = args
                .to
                .ok_or_else(|| anyhow::anyhow!("restore needs --to <db-dir>"))?;
            let id = resolve_id(&args.dir, args.id)?;
            backup::verify(&args.dir, id)?;
            backup::restore(&args.dir, &to, Some(id))?;
            println!("backup {id} restored into {}", to.display());
            println!(
                "the upstream log's checkpoint is now ahead of this database — rewind it to at or \
                 before this backup and let it replay, or the gap is permanent"
            );
        },
        "check" => {
            let db_dir = args
                .db
                .ok_or_else(|| anyhow::anyhow!("check needs --db <db-dir>"))?;
            let read = rocksdb_persistence::check_readable(&db_dir)?;
            println!(
                "{} opened and read back: {} documents and {} index entries decoded, {} rows \
                 scanned",
                db_dir.display(),
                read.documents,
                read.index_entries,
                read.rows,
            );
        },
        other => anyhow::bail!("unknown command {other}. Try --help."),
    }
    Ok(())
}
