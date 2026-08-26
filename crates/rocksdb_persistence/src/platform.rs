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

/// What a platform can do to keep a read from being served out of memory.
///
/// Named rather than reduced to a bool, because the honest answer differs per
/// platform and the difference matters: a probe that reports "the device is
/// alive" when it never left the page cache is precisely the defect four
/// consecutive review rounds found in this backend.
// Exactly one variant is constructible per target, so the other two are always
// dead on any given build. Allowed rather than silenced per-variant, because
// the point of the enum is that all three exist somewhere.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CacheBypass {
    /// `posix_fadvise(POSIX_FADV_DONTNEED)`. Evicts clean, unmapped pages, so
    /// the read that follows has to fault them back from the device. Advisory:
    /// a filesystem may accept the call and do nothing — tmpfs does, measurably
    /// — so this is the strongest available, not a guarantee.
    Fadvise,
    /// `F_NOCACHE`. Stops the descriptor caching *future* I/O; it does not
    /// evict pages already resident, so a small file the process has recently
    /// read can still be served from the unified buffer cache. Weaker than
    /// [`Self::Fadvise`], and weaker than its name suggests.
    FNoCache,
    /// Nothing. The read is an ordinary cached read and proves only that the
    /// path is still openable.
    None,
}

/// What this build will do. A constant, so it can be reported once at open
/// rather than guessed at from a probe's return value.
pub(crate) const fn cache_bypass() -> CacheBypass {
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd",
        target_os = "illumos"
    ))]
    {
        CacheBypass::Fadvise
    }
    #[cfg(target_vendor = "apple")]
    {
        CacheBypass::FNoCache
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd",
        target_os = "illumos",
        target_vendor = "apple"
    )))]
    {
        CacheBypass::None
    }
}

/// Reads `file`, doing whatever this platform offers to keep the read from
/// being answered out of the page cache.
///
/// The point is defeating that cache. A liveness probe that reads a file the
/// process has already touched proves nothing — it is answered from memory on a
/// volume nothing can reach, which is how three successive versions of this
/// backend's probe shipped broken.
///
/// It returns no claim about whether that succeeded, deliberately. None of the
/// mechanisms below can report it: `posix_fadvise` returns 0 for "advice
/// accepted", not "pages evicted", and `fcntl` reports only that the flag was
/// set. [`cache_bypass`] says what was attempted; nothing here can say what the
/// kernel did with it.
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "illumos"
))]
pub(crate) fn read_bypassing_cache(file: &mut File, buf: &mut Vec<u8>) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;

    // `len = 0` means "to the end of the file". Only clean, unmapped pages are
    // dropped, which is exactly what a file nobody is writing consists of. The
    // fd stays open across this: the page cache is per-inode, so evicting and
    // then reading through the same handle is enough — measured.
    //
    // Safe: `fd` is owned by `file` and outlives the call.
    unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    file.read_to_end(buf)?;
    Ok(())
}

/// macOS has no `posix_fadvise`. `F_NOCACHE` is the nearest thing, and it is a
/// property of the *descriptor*, so the read has to happen through this handle.
///
/// It is genuinely weaker: it stops the descriptor *retaining* what it reads,
/// but does not invalidate pages already in the unified buffer cache, and a
/// small unaligned read takes XNU's copy path rather than its direct one. For a
/// 16-byte file the process opened a moment ago, that may well still be a
/// memory hit. See [`CacheBypass::FNoCache`]; macOS is a developer platform for
/// this backend, not a deployment one.
#[cfg(target_vendor = "apple")]
pub(crate) fn read_bypassing_cache(file: &mut File, buf: &mut Vec<u8>) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;

    // Safe: `fd` is owned by `file` and outlives the call.
    unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    file.read_to_end(buf)?;
    Ok(())
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "illumos",
    target_vendor = "apple"
)))]
pub(crate) fn read_bypassing_cache(file: &mut File, buf: &mut Vec<u8>) -> io::Result<()> {
    file.read_to_end(buf)?;
    Ok(())
}
