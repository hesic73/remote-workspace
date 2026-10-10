#[cfg(unix)]
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;

/// Sync a file after a write or rename. Unix also syncs its parent directory;
/// Windows cannot explicitly flush directory metadata.
pub fn fsync_file_or_dir(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    let f = File::open(path)?;
    // FlushFileBuffers requires a writable handle. Mutation paths sync before
    // exposing any read-only attribute; returning AccessDenied is preferable
    // to claiming durability for an unflushed external read-only file.
    #[cfg(windows)]
    let f = OpenOptions::new().read(true).write(true).open(path)?;
    f.sync_all()?;
    // Also sync the parent directory so the file entry is durable in the
    // directory metadata itself (important for newly created files).
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        let dir = OpenOptions::new().read(true).open(parent)?;
        dir.sync_all()?;
    }
    Ok(())
}

/// fsync a directory (only the dir metadata, not any file within it). Use
/// after file deletion where the target file no longer exists to open.
#[cfg(unix)]
pub fn fsync_dir(path: &Path) -> std::io::Result<()> {
    let dir = OpenOptions::new().read(true).open(path)?;
    dir.sync_all()
}

#[cfg(windows)]
pub fn fsync_dir(_path: &Path) -> std::io::Result<()> {
    // Windows does not expose a stable equivalent of fsync for directory
    // handles. File contents are flushed, but rename/delete metadata relies on
    // the filesystem journal and is therefore a weaker durability boundary.
    Ok(())
}

/// Sync the entries of the directories `create_dir_all` just made, from `dir`
/// up to and including `top`: each one exists only in its parent's metadata.
pub fn fsync_new_dirs(dir: &Path, top: &Path) -> std::io::Result<()> {
    let mut current = dir;
    loop {
        let parent = current
            .parent()
            .ok_or_else(|| std::io::Error::other(format!("{current:?} has no parent")))?;
        fsync_dir(parent)?;
        if current == top {
            return Ok(());
        }
        current = parent;
    }
}
