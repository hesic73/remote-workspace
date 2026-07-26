use sha2::{Digest, Sha256};
use std::path::Path;

pub fn hash_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

pub fn hash_file(path: &Path) -> std::io::Result<Option<String>> {
    match std::fs::metadata(path) {
        Ok(meta) if meta.is_file() => {
            let bytes = std::fs::read(path)?;
            Ok(Some(hash_bytes(&bytes)))
        }
        Ok(_) => Ok(None),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

pub fn hash_str(s: &str) -> String {
    hash_bytes(s.as_bytes())
}
