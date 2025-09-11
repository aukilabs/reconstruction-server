use anyhow::{Context, Result};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

pub const NODE_SECRET_PATH: &str = "data/node_secret";
pub const NODE_SECRET_TMP_SUFFIX: &str = ".tmp";

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut p = PathBuf::from(path);
    let tmp = match p.file_name().and_then(|n| n.to_str()) {
        Some(name) => format!("{}{}", name, NODE_SECRET_TMP_SUFFIX),
        None => String::from("node_secret.tmp"),
    };
    p.set_file_name(tmp);
    p
}

/// Atomically write secret bytes to `path` by writing to a tmp file and renaming.
pub fn write_node_secret_to_path(path: &Path, secret: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create dir {}", parent.display()))?;
    }
    let tmp = tmp_path_for(path);
    let mut f = File::create(&tmp).with_context(|| format!("create tmp {}", tmp.display()))?;
    f.write_all(secret.as_bytes())
        .with_context(|| format!("write tmp {}", tmp.display()))?;
    f.sync_all().ok();
    drop(f);
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    // Best effort to sync directory for durability
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

/// Read secret contents from `path`. Returns Ok(None) if missing.
pub fn read_node_secret_from_path(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(s) => Ok(Some(s)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

/// Convenience: write to default path `data/node_secret`.
pub fn write_node_secret(secret: &str) -> Result<()> {
    write_node_secret_to_path(Path::new(NODE_SECRET_PATH), secret)
}

/// Convenience: read from default path `data/node_secret`.
pub fn read_node_secret() -> Result<Option<String>> {
    read_node_secret_from_path(Path::new(NODE_SECRET_PATH))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_and_read_roundtrip() {
        let base = std::env::temp_dir().join(format!("dds_persist_test_{}", uuid::Uuid::new_v4()));
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();
        let file = base.join("node_secret");

        write_node_secret_to_path(&file, "first").unwrap();
        let got = read_node_secret_from_path(&file).unwrap();
        assert_eq!(got.as_deref(), Some("first"));

        // overwrite
        write_node_secret_to_path(&file, "second").unwrap();
        let got2 = read_node_secret_from_path(&file).unwrap();
        assert_eq!(got2.as_deref(), Some("second"));

        // tmp file should not remain
        let tmp = tmp_path_for(&file);
        assert!(!tmp.exists(), "tmp file should be renamed away");

        let _ = fs::remove_dir_all(&base);
    }
}
