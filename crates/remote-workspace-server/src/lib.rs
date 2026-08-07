pub mod config;
pub mod exec;
pub mod fs_ops;
pub mod fsync;
pub mod hash;
pub mod install;
mod locking;
mod process_group;
pub mod scratch;
pub mod server;
pub mod session_log;
pub mod store;
pub mod transfer;
pub mod workspace;

pub use server::{Server, ServerOptions};

/// State directory for a workspace root under `base`: `<base>/state/
/// <name>-<hash12>`, where `name` is the root's final path component and
/// `hash12` is the first 12 hex chars of sha256 over the canonical root path.
/// Keyed by path so every workspace gets its own isolated store, outside the
/// workspace itself. `base` defaults to `~/.remote-workspace` (see main.rs) and
/// can be redirected with `--state-base`, e.g. when home is nearly full.
pub fn state_dir_under(
    base: &std::path::Path,
    root: &std::path::Path,
) -> anyhow::Result<std::path::PathBuf> {
    let canonical = root
        .canonicalize()
        .map_err(|e| anyhow::anyhow!("cannot canonicalize workspace root {root:?}: {e}"))?;
    use sha2::Digest;
    let digest = sha2::Sha256::digest(canonical.as_os_str().as_encoded_bytes());
    let hash12 = hex::encode(&digest[..6]);
    let name = canonical
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "root".into());
    Ok(base.join("state").join(format!("{name}-{hash12}")))
}

#[cfg(test)]
mod state_dir_tests {
    use super::state_dir_under;

    #[test]
    fn keyed_by_canonical_root() {
        let base = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let a = state_dir_under(base.path(), root.path()).unwrap();
        // A relative-ish spelling of the same root maps to the same directory.
        let dotted = root.path().join(".");
        let b = state_dir_under(base.path(), &dotted).unwrap();
        assert_eq!(a, b);
        assert!(a.starts_with(base.path().join("state")));
        let leaf = a.file_name().unwrap().to_string_lossy().into_owned();
        let root_name = root.path().file_name().unwrap().to_string_lossy();
        assert!(leaf.starts_with(&format!("{root_name}-")));
    }

    #[test]
    fn different_bases_give_disjoint_dirs_same_key() {
        let base1 = tempfile::tempdir().unwrap();
        let base2 = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let a = state_dir_under(base1.path(), root.path()).unwrap();
        let b = state_dir_under(base2.path(), root.path()).unwrap();
        assert_ne!(a, b);
        // Same per-root key under both bases.
        assert_eq!(a.file_name(), b.file_name());
    }

    #[test]
    fn missing_root_is_an_error() {
        let base = tempfile::tempdir().unwrap();
        assert!(state_dir_under(base.path(), std::path::Path::new("/nonexistent-xyz")).is_err());
    }
}
