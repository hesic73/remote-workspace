use std::path::{Component, Path, PathBuf};

use remote_workspace_protocol::{ErrorCode, ProtocolError};

#[derive(Clone)]
pub struct Workspace {
    pub root: PathBuf,
    pub scratch_root: PathBuf,
}

impl Workspace {
    pub fn new(root: PathBuf, scratch_root: PathBuf) -> std::io::Result<Self> {
        let canonical = root.canonicalize()?;
        std::fs::create_dir_all(&scratch_root)?;
        let scratch_root = scratch_root.canonicalize()?;
        Ok(Self {
            root: canonical,
            scratch_root,
        })
    }

    /// Resolve a client-supplied relative path against root, rejecting `..`
    /// escapes and symlink escapes. The result is guaranteed to stay inside the
    /// workspace root -- or the scratch root, for `@scratch/...` paths -- even
    /// when intermediate components are symlinks pointing out.
    pub fn resolve(&self, rel: &str) -> Result<PathBuf, ProtocolError> {
        if rel.is_empty() {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "path must not be empty",
            ));
        }

        let (base, relative) = match rel.strip_prefix("@scratch") {
            Some("") => (&self.scratch_root, "."),
            Some(rest) if rest.starts_with('/') => (&self.scratch_root, &rest[1..]),
            _ => (&self.root, rel),
        };
        let raw = Path::new(relative);
        if !raw.is_relative() {
            return Err(ProtocolError::new(
                ErrorCode::PathOutsideRoot,
                "path must be relative to workspace root",
            ));
        }
        validate_platform_path(raw)?;
        let base = base.clone();

        // Reject any `..` components outright. This is simpler and stricter
        // than canonicalize-and-prefix-check, and matches the design intent
        // of avoiding accidental escapes.
        for comp in raw.components() {
            match comp {
                Component::CurDir | Component::Normal(_) => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(ProtocolError::new(
                        ErrorCode::PathOutsideRoot,
                        "path escapes workspace root",
                    ));
                }
            }
        }

        // Canonicalize the deepest existing ancestor of the joined path, then
        // re-attach the non-existent tail. This is what makes the boundary
        // safe: a leaf that does not yet exist (e.g. `escape/new.txt` where
        // `escape` is a symlink out of root) is resolved against the
        // *already-validated* ancestor instead of being accepted unchecked.
        let joined = base.join(raw);
        let safe = canonicalize_ancestor(&joined).map_err(|e| {
            ProtocolError::new(ErrorCode::IoError, format!("failed to resolve path: {e}"))
        })?;

        if !safe.starts_with(&base) {
            return Err(ProtocolError::new(
                ErrorCode::PathOutsideRoot,
                "resolved path escapes workspace root",
            ));
        }
        Ok(safe)
    }

    /// Relative form (posix, forward slashes) of an absolute in-root path, for
    /// reporting in protocol messages.
    pub fn relative(&self, abs: &Path) -> String {
        if let Ok(path) = abs.strip_prefix(&self.scratch_root) {
            let path = path.to_string_lossy().replace('\\', "/");
            return if path.is_empty() {
                "@scratch".into()
            } else {
                format!("@scratch/{path}")
            };
        }
        abs.strip_prefix(&self.root)
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default()
    }
}

#[cfg(unix)]
fn validate_platform_path(_path: &Path) -> Result<(), ProtocolError> {
    Ok(())
}

#[cfg(windows)]
fn validate_platform_path(path: &Path) -> Result<(), ProtocolError> {
    // Apply the same strict namespace to reads and writes. Windows normally
    // aliases trailing dots/spaces and device names, so accepting legacy names
    // here would make path identity ambiguous.
    for component in path.components() {
        let Component::Normal(name) = component else {
            continue;
        };
        let name = name.to_string_lossy();
        if name.ends_with(['.', ' '])
            || name
                .chars()
                .any(|c| c < ' ' || matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*'))
            || is_windows_device_name(&name)
        {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "path contains a Windows-reserved name or syntax",
            ));
        }
    }
    Ok(())
}

#[cfg(windows)]
fn is_windows_device_name(name: &str) -> bool {
    let basename = name.split('.').next().unwrap_or(name).to_ascii_uppercase();
    matches!(
        basename.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) || matches!(
        basename.strip_prefix("COM"),
        Some("1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
    ) || matches!(
        basename.strip_prefix("LPT"),
        Some("1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
    )
}

/// Canonicalize the deepest existing ancestor of `path`, re-attaching the
/// non-existent tail components. Unlike `Path::canonicalize`, this handles
/// paths whose final component does not exist yet, by walking up until an
/// existing entry is found and resolving symlinks on the way.
fn canonicalize_ancestor(path: &Path) -> std::io::Result<PathBuf> {
    // Fast path: the whole thing exists.
    match path.canonicalize() {
        Ok(p) => return Ok(p),
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
        Err(_) => {}
    }

    // Collect the non-existent trailing components from the leaf upward.
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    let mut cur = path.to_path_buf();
    loop {
        match cur.canonicalize() {
            Ok(existing) => {
                let mut result = existing;
                for comp in tail.into_iter().rev() {
                    result.push(comp);
                }
                return Ok(result);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let parent = cur.parent().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::NotFound, "no existing ancestor")
                })?;
                let last = cur
                    .file_name()
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "path has no file name",
                        )
                    })?
                    .to_os_string();
                tail.push(last);
                cur = parent.to_path_buf();
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn ws() -> (tempfile::TempDir, Workspace) {
        let dir = tempdir().unwrap();
        let workspace =
            Workspace::new(dir.path().to_path_buf(), dir.path().join("scratch")).unwrap();
        (dir, workspace)
    }

    #[test]
    fn rejects_parent_dir() {
        let (_dir, w) = ws();
        let err = w.resolve("../etc/passwd").unwrap_err();
        assert_eq!(err.code, ErrorCode::PathOutsideRoot);
    }

    #[test]
    fn rejects_absolute() {
        let (_dir, w) = ws();
        let err = w.resolve("/etc/passwd").unwrap_err();
        assert_eq!(err.code, ErrorCode::PathOutsideRoot);
    }

    #[test]
    fn rejects_empty() {
        let (_dir, w) = ws();
        let err = w.resolve("").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidRequest);
    }

    #[test]
    fn allows_normal_relative() {
        let (_dir, w) = ws();
        let p = w.resolve("src/main.py").unwrap();
        assert!(p.starts_with(&w.root));
        assert!(p.ends_with("src/main.py"));
    }

    #[test]
    fn resolves_scratch_namespace_separately() {
        let dir = tempdir().unwrap();
        let scratch = tempdir().unwrap();
        let w = Workspace::new(dir.path().to_path_buf(), scratch.path().to_path_buf()).unwrap();
        let path = w.resolve("@scratch/logs/test.log").unwrap();
        assert!(path.starts_with(&w.scratch_root));
        assert_eq!(w.relative(&path), "@scratch/logs/test.log");
    }

    #[test]
    fn scratch_rejects_parent_escape() {
        let dir = tempdir().unwrap();
        let scratch = tempdir().unwrap();
        let w = Workspace::new(dir.path().to_path_buf(), scratch.path().to_path_buf()).unwrap();
        let err = w.resolve("@scratch/../operations.jsonl").unwrap_err();
        assert_eq!(err.code, ErrorCode::PathOutsideRoot);
    }

    #[test]
    fn allows_dot_components() {
        let (_dir, w) = ws();
        let p = w.resolve("./src/./main.py").unwrap();
        assert!(p.ends_with("src/main.py"));
    }

    #[test]
    #[cfg(unix)]
    fn rejects_symlink_escape() {
        let dir = tempdir().unwrap();
        let outside = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("src/escape")).unwrap();
        let w = Workspace::new(dir.path().to_path_buf(), dir.path().join("scratch")).unwrap();
        let err = w.resolve("src/escape").unwrap_err();
        assert_eq!(err.code, ErrorCode::PathOutsideRoot);
    }

    #[test]
    #[cfg(unix)]
    fn rejects_symlinked_ancestor_with_nonexistent_leaf() {
        // Regression: a non-existent leaf under a symlinked parent must not
        // be accepted just because the leaf itself does not exist yet.
        let dir = tempdir().unwrap();
        let outside = tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("escape")).unwrap();
        let w = Workspace::new(dir.path().to_path_buf(), dir.path().join("scratch")).unwrap();
        let err = w.resolve("escape/new.txt").unwrap_err();
        assert_eq!(err.code, ErrorCode::PathOutsideRoot);

        // And even deeper nesting through the symlink.
        let err = w.resolve("escape/sub/deep.txt").unwrap_err();
        assert_eq!(err.code, ErrorCode::PathOutsideRoot);
    }

    #[test]
    fn allows_nonexistent_leaf_under_real_dir() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        let w = Workspace::new(dir.path().to_path_buf(), dir.path().join("scratch")).unwrap();
        let p = w.resolve("src/new/file.txt").unwrap();
        assert!(p.starts_with(&w.root));
        assert!(p.ends_with("src/new/file.txt"));
        assert!(!p.exists(), "should not have created the file");
    }

    #[test]
    #[cfg(unix)]
    fn rejects_symlink_escape_deep() {
        // A symlink several levels down, pointing outside.
        let dir = tempdir().unwrap();
        let outside = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("a/b")).unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("a/b/link")).unwrap();
        let w = Workspace::new(dir.path().to_path_buf(), dir.path().join("scratch")).unwrap();
        let err = w.resolve("a/b/link/x").unwrap_err();
        assert_eq!(err.code, ErrorCode::PathOutsideRoot);
    }

    #[cfg(windows)]
    #[test]
    fn rejects_windows_reserved_paths() {
        let (_dir, w) = ws();
        for path in [
            "CON",
            "con.txt",
            "dir/NUL.log",
            "COM1",
            "lpt9.log",
            "file.txt:stream",
            "trailing.",
            "trailing ",
            "bad<name",
        ] {
            let err = w.resolve(path).unwrap_err();
            assert_eq!(err.code, ErrorCode::InvalidRequest, "accepted {path:?}");
        }
    }

    #[cfg(windows)]
    #[test]
    fn allows_similar_non_reserved_windows_names() {
        let (_dir, w) = ws();
        for path in ["console.txt", "COM10", "LPT0", "normal~1", "a.b"] {
            assert!(w.resolve(path).is_ok(), "rejected {path:?}");
        }
    }
}
