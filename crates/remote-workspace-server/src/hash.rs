use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::Path;

pub fn hash_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

/// Streams the file through a fixed buffer: hashing must not cost memory
/// proportional to file size, since it runs on paths (exec logs, datasets)
/// that are far larger than anything the text operations accept.
pub fn hash_file(path: &Path) -> std::io::Result<Option<String>> {
    match std::fs::metadata(path) {
        Ok(meta) if meta.is_file() => {
            let mut file = std::fs::File::open(path)?;
            let mut hasher = Sha256::new();
            let mut buf = [0u8; 64 * 1024];
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Ok(Some(format!("sha256:{}", hex::encode(hasher.finalize()))))
        }
        Ok(_) => Ok(None),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

pub fn hash_str(s: &str) -> String {
    hash_bytes(s.as_bytes())
}
