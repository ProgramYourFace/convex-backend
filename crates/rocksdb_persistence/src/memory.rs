//! Finding out how much memory this process is actually allowed to use.
//!
//! RocksDB reads no cgroup limit. Left at a fixed default, a block cache sized
//! for a generous host will happily grow past a container's limit and the
//! kernel will kill the *backend* — not a sidecar, not a subprocess, the thing
//! serving traffic. So the default has to come from the limit rather than from
//! a constant, and the limit has to be read from the cgroup.
//!
//! Both cgroup versions are handled, and in both the limit can be set on any
//! ancestor of this process's cgroup, so the whole chain is walked and the
//! smallest limit wins. A limit larger than physical memory is not a limit, so
//! the result is also capped at `MemTotal`.

use std::{
    fs,
    path::{
        Path,
        PathBuf,
    },
};

/// Anything at or above this is a sentinel for "no limit" rather than a real
/// one. cgroup v1 reports `PAGE_SIZE << 63`-ish values for unlimited, which
/// differ between kernels, so a threshold is more robust than an equality test.
const NO_LIMIT: u64 = 1 << 62;

/// The memory this process may use, in bytes, or `None` if it is unconstrained
/// or the limit could not be determined.
pub fn container_limit_bytes() -> Option<u64> {
    let cgroup = fs::read_to_string("/proc/self/cgroup").ok()?;
    let limit = cgroup_v2_limit(&cgroup).or_else(|| cgroup_v1_limit(&cgroup))?;
    // A limit above physical memory is not a limit.
    Some(match total_ram_bytes() {
        Some(ram) => limit.min(ram),
        None => limit,
    })
    .filter(|bytes| *bytes > 0)
}

/// The unified-hierarchy line of `/proc/self/cgroup`, which cgroup v2 writes as
/// `0::<path>`.
fn v2_relative_path(proc_cgroup: &str) -> Option<&str> {
    proc_cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .map(str::trim)
}

/// The `memory` controller's line, which cgroup v1 writes as
/// `<id>:<controllers>:<path>`. The controller field can list several
/// co-mounted controllers, so the match is on membership, not equality.
fn v1_relative_path(proc_cgroup: &str) -> Option<&str> {
    proc_cgroup
        .lines()
        .find_map(|line| {
            let mut parts = line.splitn(3, ':');
            let _id = parts.next()?;
            let controllers = parts.next()?;
            let path = parts.next()?;
            controllers
                .split(',')
                .any(|c| c == "memory")
                .then_some(path)
        })
        .map(str::trim)
}

/// Parses a `memory.max` value. cgroup v2 spells unlimited as the literal
/// `max`.
fn parse_v2_value(raw: &str) -> Option<u64> {
    if raw == "max" {
        return None;
    }
    raw.parse().ok().filter(|v| *v < NO_LIMIT)
}

/// Parses a `memory.limit_in_bytes` value. cgroup v1 spells unlimited as a
/// number near `u64::MAX`, whose exact value varies with the kernel's page
/// size, so it is filtered by magnitude rather than by equality.
fn parse_v1_value(raw: &str) -> Option<u64> {
    raw.parse().ok().filter(|v| *v < NO_LIMIT)
}

/// cgroup v2: one unified hierarchy, `memory.max` per directory.
fn cgroup_v2_limit(proc_cgroup: &str) -> Option<u64> {
    let rel = v2_relative_path(proc_cgroup)?;
    walk_up(
        Path::new("/sys/fs/cgroup"),
        rel,
        "memory.max",
        parse_v2_value,
    )
}

/// cgroup v1: a hierarchy per controller, mounted under
/// `/sys/fs/cgroup/memory`.
fn cgroup_v1_limit(proc_cgroup: &str) -> Option<u64> {
    let rel = v1_relative_path(proc_cgroup)?;
    walk_up(
        Path::new("/sys/fs/cgroup/memory"),
        rel,
        "memory.limit_in_bytes",
        parse_v1_value,
    )
}

/// Reads `file` in every directory from `root/rel` up to `root`, and returns
/// the smallest value any of them parsed to. A limit on an ancestor binds this
/// process just as much as one on its own cgroup.
fn walk_up(root: &Path, rel: &str, file: &str, parse: impl Fn(&str) -> Option<u64>) -> Option<u64> {
    let mut dir = PathBuf::from(root);
    dir.push(rel.trim_start_matches('/'));
    let mut best: Option<u64> = None;
    loop {
        if let Ok(raw) = fs::read_to_string(dir.join(file))
            && let Some(value) = parse(raw.trim())
        {
            best = Some(best.map_or(value, |b: u64| b.min(value)));
        }
        if dir == root || !dir.pop() {
            break;
        }
    }
    best
}

fn total_ram_bytes() -> Option<u64> {
    let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
    let kb: u64 = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some(kb * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_path_is_read_from_the_unified_line() {
        assert_eq!(
            v2_relative_path("9:cpu:/a\n0::/kubepods/pod123/container456\n"),
            Some("/kubepods/pod123/container456"),
        );
        assert_eq!(v2_relative_path("9:cpu:/a\n"), None);
    }

    #[test]
    fn v1_line_is_matched_on_the_memory_controller() {
        // The memory controller is often co-mounted with others, so the match
        // has to be on a controller in the list, not on the whole field.
        assert_eq!(
            v1_relative_path("9:cpu,cpuacct:/a\n4:memory,blkio:/b\n0::/c\n"),
            Some("/b"),
        );
        assert_eq!(v1_relative_path("9:cpu:/a\n"), None);
    }

    /// Both versions spell "unlimited" differently, and v1's spelling is a
    /// number that varies with the kernel's page size — so it is filtered by
    /// magnitude. Letting either through would size the cache off a nonsense
    /// number, which is the failure this module exists to prevent.
    #[test]
    fn unlimited_is_not_mistaken_for_a_limit() {
        assert_eq!(parse_v2_value("max"), None);
        assert_eq!(parse_v2_value("1073741824"), Some(1 << 30));
        assert_eq!(parse_v1_value("9223372036854771712"), None);
        assert_eq!(parse_v1_value("9223372036854775807"), None);
        assert_eq!(parse_v1_value("1073741824"), Some(1 << 30));
        assert_eq!(parse_v1_value("not a number"), None);
    }

    #[test]
    fn walk_up_takes_the_smallest_limit_in_the_chain() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let nested = root.path().join("parent/child");
        std::fs::create_dir_all(&nested)?;
        // The parent is the tighter of the two, and binds the child.
        std::fs::write(root.path().join("parent/memory.max"), "1000\n")?;
        std::fs::write(nested.join("memory.max"), "5000\n")?;
        assert_eq!(
            walk_up(root.path(), "parent/child", "memory.max", parse_v2_value),
            Some(1000),
        );
        Ok(())
    }

    #[test]
    fn walk_up_ignores_unlimited_markers() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let nested = root.path().join("c");
        std::fs::create_dir_all(&nested)?;
        std::fs::write(nested.join("memory.max"), "max\n")?;
        assert_eq!(
            walk_up(root.path(), "c", "memory.max", parse_v2_value),
            None,
        );
        Ok(())
    }

    /// Whatever this machine reports, the answer has to be self-consistent:
    /// never zero, and never above physical memory.
    #[test]
    fn detected_limit_is_sane_on_this_host() {
        if let Some(limit) = container_limit_bytes() {
            assert!(limit > 0);
            if let Some(ram) = total_ram_bytes() {
                assert!(limit <= ram);
            }
        }
    }
}
