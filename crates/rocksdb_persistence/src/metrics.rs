use metrics::{
    log_counter,
    log_counter_with_labels,
    log_distribution,
    log_gauge_with_labels,
    register_convex_counter,
    register_convex_gauge,
    register_convex_histogram,
    MetricLabel,
    StatusTimer,
    Timer,
    STATUS_LABEL,
};
use prometheus::VMHistogram;

/// Labels the metrics that describe ONE database rather than the process.
///
/// Levels — the age of the newest backup, the age of the last WAL flush — are
/// only meaningful per database: a process that opens several would otherwise
/// have each one overwrite the others and the series would report whichever
/// wrote last, which is the opposite of an alertable signal. Rates and
/// latencies are left unlabelled, because summing them across the databases in
/// a process is exactly what you want and a label per database only adds
/// cardinality.
const INSTANCE_LABEL: &str = "instance";

fn instance_label(instance: &str) -> MetricLabel<'static> {
    MetricLabel::new(INSTANCE_LABEL, instance.to_owned())
}

register_convex_histogram!(
    ROCKSDB_WRITE_SECONDS,
    "Time for RocksDB to apply one Persistence::write batch"
);
pub fn write_timer() -> Timer<VMHistogram> {
    Timer::new(&ROCKSDB_WRITE_SECONDS)
}

register_convex_histogram!(
    ROCKSDB_WAL_FLUSH_SECONDS,
    "Time for one interval-mode WAL flush and fsync",
    &STATUS_LABEL
);
/// The interval flusher's own latency. A gap here — ticks that stop arriving,
/// or arrive slower than the configured interval — is how a persistently
/// failing flush becomes visible, since the writes themselves keep succeeding.
pub fn wal_flush_timer() -> StatusTimer {
    StatusTimer::new(&ROCKSDB_WAL_FLUSH_SECONDS)
}

register_convex_histogram!(
    ROCKSDB_BACKUP_SECONDS,
    "Time to create one backup generation",
    &STATUS_LABEL
);
pub fn backup_timer() -> StatusTimer {
    StatusTimer::new(&ROCKSDB_BACKUP_SECONDS)
}

register_convex_histogram!(ROCKSDB_BACKUP_BYTES, "Size of a backup generation");
register_convex_histogram!(ROCKSDB_BACKUP_FILES_TOTAL, "Files in a backup generation");
pub fn log_backup(size_bytes: u64, num_files: u32) {
    log_distribution(&ROCKSDB_BACKUP_BYTES, size_bytes as f64);
    log_distribution(&ROCKSDB_BACKUP_FILES_TOTAL, num_files as f64);
}

register_convex_counter!(
    ROCKSDB_BACKUP_FAILURES_TOTAL,
    "Backup attempts that failed",
    &[INSTANCE_LABEL],
);
pub fn log_backup_failure(instance: &str) {
    log_counter_with_labels(
        &ROCKSDB_BACKUP_FAILURES_TOTAL,
        1,
        vec![instance_label(instance)],
    );
}

register_convex_gauge!(
    ROCKSDB_BACKUP_AGE_SECONDS,
    "Age of the newest backup generation",
    &[INSTANCE_LABEL],
);
/// The metric to alert on, and a gauge rather than a histogram: a level that
/// keeps rising says "no backup has landed", where a distribution that stops
/// receiving samples says nothing at all. Published by the health monitor on
/// its own short timer rather than by the backup worker, so it keeps moving
/// even if the backup worker is the thing that died.
pub fn log_backup_age(instance: &str, seconds: f64) {
    log_gauge_with_labels(
        &ROCKSDB_BACKUP_AGE_SECONDS,
        seconds,
        vec![instance_label(instance)],
    );
}

register_convex_gauge!(
    ROCKSDB_WAL_FLUSH_AGE_SECONDS,
    "Time since the write-ahead log was last successfully flushed, in interval sync mode",
    &[INSTANCE_LABEL],
);
/// In interval mode a write is acknowledged before it reaches the kernel, so
/// this is the durability measurement: how much acknowledged data could be
/// lost right now.
pub fn log_wal_flush_age(instance: &str, seconds: f64) {
    log_gauge_with_labels(
        &ROCKSDB_WAL_FLUSH_AGE_SECONDS,
        seconds,
        vec![instance_label(instance)],
    );
}

register_convex_gauge!(
    ROCKSDB_OLDEST_WRITE_SECONDS,
    "How long the oldest in-flight write has been running",
    &[INSTANCE_LABEL],
);
/// The signal a stall actually produces. A write RocksDB cannot make progress
/// for blocks rather than failing, so no error counter moves and no `Result` is
/// ever returned — only this number grows.
///
/// Published by the per-database health monitor, so it carries the label for
/// the reason [`INSTANCE_LABEL`] gives: unlabelled, N monitors would each
/// overwrite the last and the series would report whichever polled most
/// recently rather than the database that is actually stuck.
pub fn log_oldest_write(instance: &str, seconds: f64) {
    log_gauge_with_labels(
        &ROCKSDB_OLDEST_WRITE_SECONDS,
        seconds,
        vec![instance_label(instance)],
    );
}

register_convex_gauge!(
    ROCKSDB_BACKGROUND_ERRORS_TOTAL,
    "Latched RocksDB background errors, which stop the database accepting writes",
    &[INSTANCE_LABEL],
);
/// A gauge, not a counter: the property is the *total* latched so far, so
/// adding it to a counter on every poll would multiply one error by the poll
/// rate. Being a level, it is also per database — see [`INSTANCE_LABEL`].
pub fn log_background_errors(instance: &str, count: u64) {
    log_gauge_with_labels(
        &ROCKSDB_BACKGROUND_ERRORS_TOTAL,
        count as f64,
        vec![instance_label(instance)],
    );
}

register_convex_gauge!(
    ROCKSDB_WRITE_STOPPED_TOTAL,
    "Whether RocksDB is deliberately stalling writers as backpressure",
    &[INSTANCE_LABEL],
);
/// Tells a deliberate stall apart from a hang — the difference between an
/// ingest burst and a volume that has stopped accepting writes.
///
/// One database's deliberate backpressure says nothing about its neighbours',
/// so this is labelled even though the unlabelled form would be cheaper.
pub fn log_write_stopped(instance: &str, stopped: bool) {
    log_gauge_with_labels(
        &ROCKSDB_WRITE_STOPPED_TOTAL,
        if stopped { 1.0 } else { 0.0 },
        vec![instance_label(instance)],
    );
}

register_convex_histogram!(
    ROCKSDB_CONFLICT_CHECK_SECONDS,
    "Time spent enforcing ConflictStrategy::Error before a write"
);
pub fn conflict_check_timer() -> Timer<VMHistogram> {
    Timer::new(&ROCKSDB_CONFLICT_CHECK_SECONDS)
}

register_convex_histogram!(
    ROCKSDB_WRITE_DOCUMENTS_TOTAL,
    "Documents per Persistence::write batch"
);
register_convex_histogram!(
    ROCKSDB_WRITE_INDEX_ENTRIES_TOTAL,
    "Index entries per Persistence::write batch"
);
pub fn log_write(documents: usize, index_entries: usize) {
    log_distribution(&ROCKSDB_WRITE_DOCUMENTS_TOTAL, documents as f64);
    log_distribution(&ROCKSDB_WRITE_INDEX_ENTRIES_TOTAL, index_entries as f64);
}

register_convex_histogram!(
    ROCKSDB_LOAD_DOCUMENTS_SECONDS,
    "Time to stream a document log range",
    &STATUS_LABEL
);
pub fn load_documents_timer() -> StatusTimer {
    StatusTimer::new(&ROCKSDB_LOAD_DOCUMENTS_SECONDS)
}

register_convex_counter!(
    ROCKSDB_DOCUMENTS_LOADED_TOTAL,
    "Documents returned by document log scans"
);
pub fn finish_load_documents_timer(timer: StatusTimer, loaded: usize) {
    log_counter(&ROCKSDB_DOCUMENTS_LOADED_TOTAL, loaded as u64);
    timer.finish();
}

register_convex_histogram!(
    ROCKSDB_INDEX_SCAN_SECONDS,
    "Time to stream an index scan",
    &STATUS_LABEL
);
pub fn index_scan_timer() -> StatusTimer {
    StatusTimer::new(&ROCKSDB_INDEX_SCAN_SECONDS)
}

register_convex_counter!(
    ROCKSDB_INDEX_ROWS_SCANNED_TOTAL,
    "Rows returned by index scans"
);
pub fn finish_index_scan_timer(timer: StatusTimer, rows: usize) {
    log_counter(&ROCKSDB_INDEX_ROWS_SCANNED_TOTAL, rows as u64);
    timer.finish();
}

register_convex_counter!(
    ROCKSDB_DOCUMENTS_DELETED_TOTAL,
    "Document revisions deleted by retention"
);
pub fn log_documents_deleted(count: usize) {
    log_counter(&ROCKSDB_DOCUMENTS_DELETED_TOTAL, count as u64);
}

register_convex_counter!(
    ROCKSDB_INDEX_ENTRIES_DELETED_TOTAL,
    "Index entries deleted by retention"
);
pub fn log_index_entries_deleted(count: usize) {
    log_counter(&ROCKSDB_INDEX_ENTRIES_DELETED_TOTAL, count as u64);
}
