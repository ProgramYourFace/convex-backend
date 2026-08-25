//! Instance names, the paths derived from them, and the per-instance
//! deployment secret.
//!
//! An instance name is the single identifier the whole host is keyed by. It
//! becomes a DNS label, a directory under the data root, a `KeyBroker`
//! identity, a relational database name when the Postgres driver is in use, and
//! a metric label. Names arrive over the network (from the instance source), so
//! everything in this module treats them as untrusted until
//! [`validate_instance_name`] has run.
//!
//! ## The charset
//!
//! ```text
//! ^[a-z][a-z0-9-]{0,38}$
//! ```
//!
//! The intersection of a DNS label, an unquoted Postgres identifier and a
//! filesystem path component. 39 characters keeps `<name>_<suffix>` under the
//! 63-byte limit that both DNS labels and Postgres identifiers impose, and the
//! leading letter is what makes `name.replace('-', "_")` legal unquoted.
//!
//! Nothing here mints names. Minting belongs to whatever control plane owns
//! placement; this host only ever validates names it was handed.

use std::path::{
    Path,
    PathBuf,
};

use sha2::{
    Digest,
    Sha256,
};

/// The longest name any instance may have. See the module docs for why 39.
pub const MAX_INSTANCE_NAME_LEN: usize = 39;

/// The default HKDF `info` prefix for a derived per-instance deployment secret.
///
/// Changing this string rotates every instance secret and invalidates every
/// admin key already minted against them, so it is versioned and overridable
/// (`MULTITENANT_SECRET_INFO_PREFIX`) — a deployment that already mints keys
/// against a different prefix keeps its existing keys valid by naming it.
pub const DEFAULT_SECRET_INFO_PREFIX: &str = "convex-multitenant/instance-secret/v1/";

/// `true` iff `name` matches `^[a-z][a-z0-9-]{0,38}$`.
///
/// Every path that turns a name into a hostname, an identifier, a file path or
/// a secret MUST go through this first.
pub fn is_valid_instance_name(name: &str) -> bool {
    if name.is_empty() || name.len() > MAX_INSTANCE_NAME_LEN {
        return false;
    }
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// Validates `name` and returns it, or an error naming the offending value.
pub fn validate_instance_name(name: &str) -> anyhow::Result<&str> {
    anyhow::ensure!(
        is_valid_instance_name(name),
        "invalid instance name {name:?}: expected ^[a-z][a-z0-9-]{{0,38}}$"
    );
    Ok(name)
}

/// The relational database name the stock driver derives for this instance.
///
/// Mirrors `clusters::` — `deployment_name.replace('-', "_")`, set as the
/// cluster URL's path. Reproduced here only so a host that must `CREATE
/// DATABASE` first can name the same database the driver will open. Callers
/// MUST have validated `name`; `-` is the only character in the accepted
/// charset that is illegal in an unquoted identifier.
pub fn db_name(instance: &str) -> String {
    instance.replace('-', "_")
}

/// Everything one instance owns on disk, under a single directory.
///
/// ```text
/// <data_dir>/instances/<name>/db        the RocksDB database
/// <data_dir>/instances/<name>/storage   file storage and search indexes
/// ```
///
/// THE DIRECTORY IS THE UNIT OF TRANSFER. A tenant's whole state is that one
/// subtree and nothing else: no shared tables, no rows keyed by tenant in a
/// neighbour's database, no entry in a global catalogue. Moving a tenant to
/// another host is `rocksdb-backup` from one and restore into the other's
/// `instances/<name>/db`, plus a copy of `storage/`; retiring one is `rm -rf`
/// of the subtree. That is the property multi-tenancy usually gives up, and the
/// reason each instance gets its own embedded store rather than a shared one
/// with a tenant column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstancePaths {
    pub root: PathBuf,
    pub db: PathBuf,
    pub storage: PathBuf,
}

/// Where an instance's data lives under `data_dir`.
///
/// `legacy` names an instance that predates this host — one whose data was
/// written by a single-tenant backend directly under `data_dir` — and keeps its
/// paths exactly where that backend left them, so adopting an existing
/// deployment moves no bytes. Every other instance is namespaced under
/// `instances/`.
pub fn instance_paths(data_dir: &Path, name: &str, legacy: Option<&str>) -> InstancePaths {
    if legacy == Some(name) {
        return InstancePaths {
            root: data_dir.to_path_buf(),
            db: data_dir.join("db"),
            storage: data_dir.join("storage"),
        };
    }
    let root = data_dir.join("instances").join(name);
    InstancePaths {
        db: root.join("db"),
        storage: root.join("storage"),
        root,
    }
}

/// How an instance is addressed from outside the process.
///
/// A host serves many instances behind one pair of listeners, so the instance
/// has to be recoverable from the request. `<name>.<group>.api.<base>` is the
/// public form (one wildcard DNS record and one wildcard certificate cover the
/// whole host); the `X-Convex-Instance` header is the in-cluster form, for
/// callers that reach the process over loopback or a service DNS name and have
/// no per-instance hostname to use.
#[derive(Clone, Debug)]
pub struct OriginTemplate {
    pub scheme: String,
    /// The label shared by every instance on this host — a cell id, a pod name,
    /// whatever the deployment calls the unit that owns the process.
    pub group: String,
    pub base_domain: String,
}

impl OriginTemplate {
    /// `<scheme>://<instance>.<group>.api.<base>`.
    pub fn cloud_origin(&self, instance: &str) -> String {
        format!(
            "{}://{}api.{}",
            self.scheme,
            self.host_prefix(instance),
            self.base_domain
        )
    }

    /// `<scheme>://<instance>.<group>.site.<base>`.
    pub fn site_origin(&self, instance: &str) -> String {
        format!(
            "{}://{}site.{}",
            self.scheme,
            self.host_prefix(instance),
            self.base_domain
        )
    }

    /// `"<instance>.<group>."`, or just `"<group>."` when the instance IS the
    /// group.
    ///
    /// THE FIRST-LABEL-ALREADY-EQUALS RULE. An adopted single-tenant deployment
    /// is named after the group it now shares, and its origin must stay exactly
    /// what it was — clients, deploy keys and signed file-storage URLs all
    /// carry it. Prepending the label blindly would produce
    /// `group.group.api.<base>`, which no wildcard record routes.
    fn host_prefix(&self, instance: &str) -> String {
        if instance == self.group {
            format!("{}.", self.group)
        } else {
            format!("{instance}.{}.", self.group)
        }
    }
}

/// HMAC-SHA256, RFC 2104.
///
/// Written against `sha2` rather than pulling in the `hmac` crate: this is the
/// construction's six lines, it is pinned by RFC 4231 vectors in the tests
/// below, and there is no secret-dependent branching or comparison here, so
/// constant-time behaviour is not at stake.
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut padded = [0u8; BLOCK];
    if key.len() > BLOCK {
        let digest = Sha256::digest(key);
        padded[..digest.as_slice().len()].copy_from_slice(digest.as_slice());
    } else {
        padded[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for ((i, o), p) in ipad.iter_mut().zip(opad.iter_mut()).zip(padded.iter()) {
        *i ^= *p;
        *o ^= *p;
    }
    let inner = Sha256::new()
        .chain_update(ipad)
        .chain_update(data)
        .finalize();
    let outer = Sha256::new()
        .chain_update(opad)
        .chain_update(inner)
        .finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(outer.as_slice());
    out
}

/// The per-instance deployment secret, HKDF-SHA256 from the host's one root
/// secret.
///
/// ```text
/// ikm  = hex_decode(root_secret_hex)          // exactly 32 bytes
/// salt = <empty>                              // RFC 5869: 32 zero bytes
/// info = <prefix> + instance
/// L    = 32
/// ```
///
/// WHY DERIVE RATHER THAN STORE. `KeyBroker::new(instance_name, secret)`
/// derives all of its encryptors from the SECRET ALONE — the instance name is
/// kept as a plain field — so two instances sharing one secret would accept
/// each other's admin keys. Distinct secrets are therefore mandatory, not a
/// nicety. Deriving them takes the secret-store write off the onboarding path
/// entirely: the host computes an instance's secret the moment it learns the
/// name, and whatever mints that instance's admin key computes the identical
/// value independently, with no ordering dependency between them.
///
/// Only one HKDF-Expand block is needed (L = 32 = HashLen), so this is
/// `T(1) = HMAC(prk, info || 0x01)`.
pub fn derive_instance_secret(
    root_secret_hex: &str,
    prefix: &str,
    instance: &str,
) -> anyhow::Result<String> {
    validate_instance_name(instance)?;
    let ikm = decode_hex32(root_secret_hex).ok_or_else(|| {
        anyhow::anyhow!("the root instance secret must be 64 lowercase hex chars")
    })?;
    // HKDF-Extract with an empty salt, which RFC 5869 defines as HashLen zeros.
    let prk = hmac_sha256(&[0u8; 32], &ikm);
    let mut info = Vec::with_capacity(prefix.len() + instance.len() + 1);
    info.extend_from_slice(prefix.as_bytes());
    info.extend_from_slice(instance.as_bytes());
    info.push(0x01);
    let okm = hmac_sha256(&prk, &info);
    Ok(hex_encode(&okm))
}

fn decode_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_the_documented_charset() {
        for name in [
            "a",
            "cell-01",
            "i-0068a1f39c2b4d5e6f708192",
            &"a".repeat(MAX_INSTANCE_NAME_LEN),
        ] {
            assert!(is_valid_instance_name(name), "{name}");
        }
    }

    #[test]
    fn rejects_everything_else() {
        for name in [
            "",
            "Foo",              // uppercase is not a legal DNS label
            "1abc",             // must start with a letter to be an identifier
            "-abc",             //
            "a_b",              // underscore is not a legal DNS label
            "a b",              //
            "../../etc/passwd", // the path-traversal case this gate exists for
            "a/b",
            "a.b",
            &"a".repeat(MAX_INSTANCE_NAME_LEN + 1),
        ] {
            assert!(!is_valid_instance_name(name), "{name}");
        }
    }

    #[test]
    fn paths_are_namespaced_per_instance() {
        let paths = instance_paths(Path::new("/convex/data"), "i-0068a1f3", None);
        assert_eq!(paths.db, Path::new("/convex/data/instances/i-0068a1f3/db"));
        assert_eq!(
            paths.storage,
            Path::new("/convex/data/instances/i-0068a1f3/storage")
        );
        assert_eq!(paths.root, Path::new("/convex/data/instances/i-0068a1f3"));
    }

    #[test]
    fn the_legacy_instance_keeps_the_paths_a_single_tenant_backend_wrote() {
        // A single-tenant backend was started with `--local-storage
        // <data>/storage` and a RocksDB db_spec of `<data>/db`. Adopting it
        // must reopen exactly those, or its uploaded files and built search
        // indexes sit one directory above where it looks for them.
        let paths = instance_paths(Path::new("/convex/data"), "cell-01", Some("cell-01"));
        assert_eq!(paths.db, Path::new("/convex/data/db"));
        assert_eq!(paths.storage, Path::new("/convex/data/storage"));
        // ...and every other instance is still namespaced.
        let other = instance_paths(Path::new("/convex/data"), "i-0068a1f3", Some("cell-01"));
        assert_eq!(other.db, Path::new("/convex/data/instances/i-0068a1f3/db"));
    }

    #[test]
    fn origins_follow_the_wildcard_shape() {
        let t = OriginTemplate {
            scheme: "https".to_owned(),
            group: "cell-01".to_owned(),
            base_domain: "example.com".to_owned(),
        };
        assert_eq!(
            t.cloud_origin("i-0068a1f3"),
            "https://i-0068a1f3.cell-01.api.example.com"
        );
        assert_eq!(
            t.site_origin("i-0068a1f3"),
            "https://i-0068a1f3.cell-01.site.example.com"
        );
    }

    #[test]
    fn the_group_named_instance_does_not_double_its_label() {
        let t = OriginTemplate {
            scheme: "http".to_owned(),
            group: "cell-01".to_owned(),
            base_domain: "example.com".to_owned(),
        };
        assert_eq!(t.cloud_origin("cell-01"), "http://cell-01.api.example.com");
        assert_eq!(t.site_origin("cell-01"), "http://cell-01.site.example.com");
    }

    #[test]
    fn db_names_are_legal_unquoted_identifiers() {
        assert_eq!(db_name("cell-01"), "cell_01");
        assert_eq!(db_name("i-0068a1f3"), "i_0068a1f3");
    }

    /// RFC 4231 test cases 1 and 2. If the HMAC construction is wrong, every
    /// derived secret is wrong in a way no other test would catch — the
    /// derivation would still be deterministic and still look like a secret.
    #[test]
    fn hmac_matches_rfc_4231() {
        assert_eq!(
            hex_encode(&hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        assert_eq!(
            hex_encode(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    /// Golden vectors. Whatever mints admin keys for these instances derives
    /// the same values independently; if the two ever disagree, every key it
    /// mints is rejected, so these are pinned rather than round-tripped.
    #[test]
    fn instance_secrets_are_pinned() {
        let root = "0000000000000000000000000000000000000000000000000000000000000001";
        assert_eq!(
            derive_instance_secret(root, DEFAULT_SECRET_INFO_PREFIX, "cell-01").unwrap(),
            "8e137a50df342703678a5931310849a9c1e5ca01c08beb893f951d48247b7639"
        );
        assert_eq!(
            derive_instance_secret(root, DEFAULT_SECRET_INFO_PREFIX, "i-0068a1f3").unwrap(),
            "05b1ae35c1caaf425ca2939d70a415ad4eda54dfbdd5fb5e917c866b76e9543f"
        );
    }

    #[test]
    fn instance_secrets_differ_per_instance_and_per_prefix() {
        let root = "0000000000000000000000000000000000000000000000000000000000000001";
        let a = derive_instance_secret(root, DEFAULT_SECRET_INFO_PREFIX, "a-one").unwrap();
        let b = derive_instance_secret(root, DEFAULT_SECRET_INFO_PREFIX, "a-two").unwrap();
        let c = derive_instance_secret(root, "other/v1/", "a-one").unwrap();
        assert_ne!(a, b, "two instances must not share a secret");
        assert_ne!(a, c, "the prefix must change the derivation");
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn a_bad_name_or_root_never_yields_a_secret() {
        let root = "0000000000000000000000000000000000000000000000000000000000000001";
        for bad in ["../../etc", "Foo", "1abc", "a b", ""] {
            assert!(
                derive_instance_secret(root, DEFAULT_SECRET_INFO_PREFIX, bad).is_err(),
                "{bad}"
            );
        }
        for bad_root in ["", "abc", &"z".repeat(64)] {
            assert!(
                derive_instance_secret(bad_root, DEFAULT_SECRET_INFO_PREFIX, "a-one").is_err(),
                "{bad_root}"
            );
        }
    }
}
