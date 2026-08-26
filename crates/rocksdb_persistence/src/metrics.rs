use metrics::{
    log_counter,
    log_distribution,
    register_convex_counter,
    register_convex_histogram,
    StatusTimer,
    Timer,
    STATUS_LABEL,
};
use prometheus::VMHistogram;

register_convex_histogram!(
    ROCKSDB_WRITE_SECONDS,
    "Time for RocksDB to apply one Persistence::write batch"
);
pub fn write_timer() -> Timer<VMHistogram> {
    Timer::new(&ROCKSDB_WRITE_SECONDS)
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
