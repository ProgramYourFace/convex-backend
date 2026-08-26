//! The two places this backend has to leave portable Rust.
//!
//! Kept in one file so the portability story is legible rather than scattered:
//! an exclusive directory lock for backups, and a read that has to reach the
//! device rather than the page cache. Both are POSIX ideas with no portable
//! equivalent, and the crate builds for macOS and Windows even though a
//! deployment of it is a Linux container — `db_connection` depends on this
//! crate unconditionally, and the release workflow builds five targets.

use std::{
    fs::File,
    io::{
        self,
        Read as _,
    },
};

/// Takes an exclusive, non-blocking advisory lock on an open file, returning
/// `false` if another process holds it.
///
/// `flock` and not a lock file, because the kernel releases it when the process
/// dies: a `SIGKILL` mid-backup would otherwise strand the directory until
/// somebody deleted a file they had no way to know was stale.
#[cfg(unix)]
pub(crate) fn try_lock_exclusive(file: &File) -> io::Result<bool> {
    use std::os::fd::AsRawFd as _;

    // Safe: `fd` is owned by `file` and outlives the call.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EWOULDBLOCK) => Ok(false),
        _ => Err(error),
    }
}

/// Windows has `LockFileEx`, but wiring it up would mean taking a dependency on
/// `windows-sys` for a code path that cannot be exercised: a RocksDB-backed
/// Convex cell is a Linux container, and `rocksdb-backup` runs beside it. An
/// error is honest; a no-op lock would let two concurrent backups interleave
/// generations into one chain, which is the failure the lock exists to prevent.
#[cfg(not(unix))]
pub(crate) fn try_lock_exclusive(_file: &File) -> io::Result<bool> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "backups need an exclusive directory lock, which is only implemented for unix",
    ))
}

/// Reads `file` in a way that has to reach the storage device, filling `buf`.
///
/// The point is defeating the page cache. A liveness probe that reads a file
/// the process has already touched proves nothing — it is answered from memory
/// on a volume nothing can reach, which is how three successive versions of
/// this backend's probe shipped broken.
///
/// Returns whether the cache was actually bypassed, because on some platforms
/// it cannot be and the caller should not claim otherwise.
#[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
pub(crate) fn read_bypassing_cache(file: &mut File, buf: &mut Vec<u8>) -> io::Result<bool> {
    use std::os::fd::AsRawFd as _;

    // `len = 0` means "to the end of the file". Only clean, unmapped pages are
    // dropped, which is exactly what a file nobody is writing consists of.
    // Safe: `fd` is owned by `file` and outlives the call.
    let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    file.read_to_end(buf)?;
    // `posix_fadvise` returns the error number directly rather than setting
    // errno, and it is advisory: a filesystem that ignores it leaves the read a
    // cache hit. tmpfs is the case that matters — it has no backing device, so
    // there is nothing to detect there anyway.
    Ok(rc == 0)
}

/// macOS has no `posix_fadvise`. `F_NOCACHE` is the equivalent, and it is a
/// property of the *descriptor* rather than of the file, so the read has to
/// happen through this handle.
#[cfg(target_vendor = "apple")]
pub(crate) fn read_bypassing_cache(file: &mut File, buf: &mut Vec<u8>) -> io::Result<bool> {
    use std::os::fd::AsRawFd as _;

    // Safe: `fd` is owned by `file` and outlives the call.
    let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    file.read_to_end(buf)?;
    Ok(rc != -1)
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_vendor = "apple"
)))]
pub(crate) fn read_bypassing_cache(file: &mut File, buf: &mut Vec<u8>) -> io::Result<bool> {
    file.read_to_end(buf)?;
    Ok(false)
}
