use std::fs::{File, OpenOptions};
use std::path::Path;

/// fsync a single file and its parent directory. Use after writes/renames
/// where the file itself still exists.
pub fn fsync_file_or_dir(path: &Path) -> std::io::Result<()> {
    let f = File::open(path)?;
    f.sync_all()?;
    // Also sync the parent directory so the file entry is durable in the
    // directory metadata itself (important for newly created files/blobs).
    if let Some(parent) = path.parent() {
        let dir = OpenOptions::new().read(true).open(parent)?;
        dir.sync_all()?;
    }
    Ok(())
}

/// fsync a directory (only the dir metadata, not any file within it). Use
/// after file deletion where the target file no longer exists to open.
pub fn fsync_dir(path: &Path) -> std::io::Result<()> {
    let dir = OpenOptions::new().read(true).open(path)?;
    dir.sync_all()
}
