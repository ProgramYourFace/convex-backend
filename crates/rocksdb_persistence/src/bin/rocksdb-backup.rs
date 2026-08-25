//! Backup administration for the embedded RocksDB backend.
//!
//! A separate binary rather than a subcommand on the backend, for one
//! structural reason: restoring rewrites a database directory, and RocksDB
//! holds that directory's lock for as long as a database is open. A restore
//! therefore cannot run inside the process it is restoring for. Shipping it as
//! its own binary makes it usable as an init container or a one-shot `Job`,
//! which is where a restore actually happens.
//!
//! ```text
//! rocksdb-backup list    <backup-dir>
//! rocksdb-backup verify  <backup-dir> [--id N]
//! rocksdb-backup rehearse <backup-dir> --scratch <dir> [--id N]
//! rocksdb-backup restore <backup-dir> --to <db-dir> [--id N]
//! ```

use std::path::PathBuf;

use rocksdb_persistence::backup;

const HELP: &str = "\
rocksdb-backup — administer backups of an embedded RocksDB Convex database

  list     <backup-dir>                            generations, newest last
  verify   <backup-dir> [--id N]                   checksum a generation's files
  rehearse <backup-dir> --scratch <dir> [--id N]   restore to scratch and read it
  restore  <backup-dir> --to <db-dir> [--id N]     restore for real

Without --id, the newest generation is used.

`verify` checks that the files are intact. `rehearse` checks the thing you
actually need to know — that a database restored from them opens and reads —
and is what belongs on a schedule. A backup nobody has restored is not a backup.

`restore` requires an empty or absent target directory. Move the existing one
aside rather than deleting it: until the restore is confirmed good, it is the
only other copy you have.
";

struct Args {
    command: String,
    dir: PathBuf,
    to: Option<PathBuf>,
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
    anyhow::ensure!(!raw.is_empty(), "{command} needs a backup directory");
    let dir = PathBuf::from(raw.remove(0));
    let mut args = Args {
        command,
        dir,
        to: None,
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
            println!("note: this is a checksum, not a restore. Run `rehearse` for that.");
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
        other => anyhow::bail!("unknown command {other}. Try --help."),
    }
    Ok(())
}
