//! End-to-end tests for the `rocksdb-backup` binary.
//!
//! These exist because the library had 83 tests and the binary had none, and a
//! revert that was correct in the library left the binary calling the path the
//! library had just been taught to refuse: `backup` opened a *secondary*, and
//! `backup_inner` rejects a secondary outright, so the operator's only
//! backup command failed on every invocation while the whole suite stayed
//! green. Testing the library's `backup()` cannot catch that — the defect is in
//! which constructor the binary chooses. Only running the binary can.

use std::{
    path::Path,
    process::{
        Command,
        Output,
    },
};

use common::{
    document::{
        CreationTime,
        ResolvedDocument,
    },
    index::IndexKeyBytes,
    obj,
    persistence::{
        ConflictStrategy,
        DocumentLogEntry,
        Persistence,
        PersistenceIndexEntry,
    },
    types::{
        IndexId,
        Timestamp,
    },
    value::{
        InternalDocumentId,
        ResolvedDocumentId,
        TabletId,
    },
};
use rocksdb_persistence::RocksDbPersistence;
use value::{
    DeveloperDocumentId,
    InternalId,
    TableNumber,
};

const ID_LEN: usize = 16;
const TABLE_NUMBER: u32 = 7;

fn internal_id(n: u32) -> InternalId {
    let mut bytes = [0u8; ID_LEN];
    bytes[..4].copy_from_slice(&n.to_be_bytes());
    InternalId(bytes)
}

fn document(n: u32, body: &str) -> anyhow::Result<ResolvedDocument> {
    let id = ResolvedDocumentId {
        tablet_id: TabletId(InternalId([1u8; ID_LEN])),
        developer_id: DeveloperDocumentId::new(
            TableNumber::try_from(TABLE_NUMBER)?,
            internal_id(n),
        ),
    };
    Ok(ResolvedDocument::new(
        id,
        CreationTime::try_from(1.0)?,
        obj!("body" => body)?,
    )?)
}

/// Writes a handful of documents and index entries, so `rehearse` has something
/// to decode. It refuses an empty database by design — a backup of nothing
/// restores and scans perfectly, which would make the rehearsal a false pass.
async fn populate(persistence: &RocksDbPersistence) -> anyhow::Result<()> {
    let documents: Vec<DocumentLogEntry> = (1..=4u32)
        .map(|n| {
            Ok(DocumentLogEntry {
                ts: Timestamp::try_from(u64::from(n) * 10)?,
                id: InternalDocumentId::new(TabletId(InternalId([1u8; ID_LEN])), internal_id(n)),
                value: Some(document(n, "body")?),
                prev_ts: None,
            })
        })
        .collect::<anyhow::Result<_>>()?;
    let indexes: Vec<PersistenceIndexEntry> = (1..=4u32)
        .map(|n| {
            Ok(PersistenceIndexEntry {
                ts: Timestamp::try_from(u64::from(n) * 10)?,
                index_id: IndexId(InternalId([0xA1; ID_LEN])),
                key: IndexKeyBytes(n.to_be_bytes().to_vec()),
                value: Some(InternalDocumentId::new(
                    TabletId(InternalId([1u8; ID_LEN])),
                    internal_id(n),
                )),
            })
        })
        .collect::<anyhow::Result<_>>()?;
    persistence
        .write(&documents, &indexes, ConflictStrategy::Error)
        .await?;
    Ok(())
}

fn cli(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocksdb-backup"))
        .args(args)
        .output()
        .expect("failed to run rocksdb-backup")
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// Creates a real database at `path` and closes it, leaving no writer.
fn make_stopped_database(path: &Path) {
    let persistence = RocksDbPersistence::new(path).expect("failed to create the database");
    drop(persistence);
    assert!(path.join("CURRENT").exists(), "no database was created");
}

#[test]
fn backup_succeeds_against_a_stopped_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let backups = dir.path().join("backup");
    make_stopped_database(&db);

    let out = cli(&[
        "backup",
        backups.to_str().unwrap(),
        "--db",
        db.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "backup failed against a stopped database.\nstdout: {}\nstderr: {}",
        stdout(&out),
        stderr(&out),
    );
    let reported = stdout(&out);
    assert!(
        reported.contains("written") && reported.contains("backup 1"),
        "backup did not report a generation by id: {reported}",
    );

    let listed = cli(&["list", backups.to_str().unwrap()]);
    assert!(listed.status.success(), "list failed: {}", stderr(&listed));
    assert!(
        !stdout(&listed).contains("no backups"),
        "the generation backup just wrote is not listed: {}",
        stdout(&listed),
    );
}

/// The whole point of the secondary refusal, seen from the operator's side: it
/// has to fail, and the message has to say what to do instead. A refusal that
/// reads as an internal error sends someone hunting for a bug in the tool.
#[test]
fn backup_refuses_while_the_backend_is_running_and_says_why() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let backups = dir.path().join("backup");

    // Held open for the duration of the call — this is the running backend.
    let _running = RocksDbPersistence::new(&db).expect("failed to create the database");

    let out = cli(&[
        "backup",
        backups.to_str().unwrap(),
        "--db",
        db.to_str().unwrap(),
    ]);
    assert!(
        !out.status.success(),
        "backup must not claim success while another process holds the write lock",
    );
    let message = stderr(&out);
    // Assert on the *lock contention* wording, not on "stopped"/"snapshot".
    // The defect this file exists for — opening a secondary — also fails, and
    // its refusal says "Take this backup with the writer stopped, or snapshot
    // the volume instead", which satisfies both of those words. Matching them
    // would make this test pass against the broken binary.
    assert!(
        message.contains("exclusive access") || message.contains("could not open"),
        "the failure must be the write lock, not a secondary refusal: {message}",
    );
}

/// `rehearse` is what belongs on a schedule, so it has to work against a
/// backup the CLI itself produced.
#[tokio::test]
async fn rehearse_reads_back_a_backup_the_cli_took() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let backups = dir.path().join("backup");
    let scratch = dir.path().join("scratch");
    {
        let persistence = RocksDbPersistence::new(&db).expect("failed to create the database");
        populate(&persistence).await.expect("failed to write");
    }

    let out = cli(&[
        "backup",
        backups.to_str().unwrap(),
        "--db",
        db.to_str().unwrap(),
    ]);
    assert!(out.status.success(), "backup failed: {}", stderr(&out));

    let out = cli(&[
        "rehearse",
        backups.to_str().unwrap(),
        "--scratch",
        scratch.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "rehearse failed against a backup this tool took.\nstdout: {}\nstderr: {}",
        stdout(&out),
        stderr(&out),
    );
    assert!(
        stdout(&out).contains("read back"),
        "rehearse did not report what it decoded: {}",
        stdout(&out),
    );
}

#[test]
fn backup_refuses_a_directory_that_is_not_a_database() {
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("not-a-db");
    std::fs::create_dir_all(&empty).unwrap();

    let out = cli(&[
        "backup",
        dir.path().join("backup").to_str().unwrap(),
        "--db",
        empty.to_str().unwrap(),
    ]);
    assert!(
        !out.status.success(),
        "backup must refuse a directory with no database in it",
    );
    assert!(
        stderr(&out).contains("not a RocksDB database"),
        "the refusal must name the reason: {}",
        stderr(&out),
    );
}

/// `check` is what turns a volume snapshot into a verified one, and the
/// documented backup cycle depends on it, so it needs coverage of its own.
///
/// A copy of a *cleanly closed* database, which is a weaker exercise than it
/// looks: RocksDB flushes every column family in `~DBImpl`, so the copy has its
/// memtables in SSTs and a MANIFEST already past the WAL. It therefore never
/// replays a write-ahead log — which is exactly what `check` does against a
/// real crash-consistent snapshot. This is a smoke test for the verb, not a
/// stand-in for a snapshot; the WAL-replay path is covered by
/// `writes_survive_a_reopen_without_a_clean_shutdown` in the crate's own tests,
/// which kills a child process with `_exit(0)`.
#[tokio::test]
async fn check_reads_back_a_copy_of_a_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let clone = dir.path().join("clone");
    {
        let persistence = RocksDbPersistence::new(&db).expect("failed to create the database");
        populate(&persistence).await.expect("failed to write");
    }
    copy_dir(&db, &clone);

    let out = cli(&["check", "--db", clone.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "check failed against a copy of a closed database.\nstdout: {}\nstderr: {}",
        stdout(&out),
        stderr(&out),
    );
    assert!(
        stdout(&out).contains("read back") && stdout(&out).contains("4 documents"),
        "check did not report what it decoded: {}",
        stdout(&out),
    );
}

/// Pointed at a backup directory instead of a database, `check` has to say so —
/// it is the mistake the two directory shapes invite, and the error is the only
/// thing standing between an operator and a CronJob that verifies nothing.
#[tokio::test]
async fn check_refuses_a_backup_directory_and_names_the_right_verb() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db");
    let backups = dir.path().join("backup");
    // A real backup directory, not an empty one — otherwise this passes through
    // the missing-`CURRENT` branch without ever meeting the shape it is named
    // for.
    {
        let persistence = RocksDbPersistence::new(&db).expect("failed to create the database");
        populate(&persistence).await.expect("failed to write");
        persistence.backup(&backups, 4).expect("failed to back up");
    }

    let out = cli(&["check", "--db", backups.to_str().unwrap()]);
    assert!(!out.status.success(), "check must refuse a non-database");
    let message = stderr(&out);
    assert!(
        message.contains("rehearse"),
        "the refusal must point at the verb that does take a backup directory: {message}",
    );
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap().flatten() {
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}
