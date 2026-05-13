use std::path::Path;

use anyhow::Result;
use tokio::task;
use zip::ZipArchive;

pub async fn unzip_bytes_to_dir(zip_bytes: &[u8], unzip_root: &Path) -> Result<()> {
    let zip_bytes = zip_bytes.to_vec();
    let unzip_root = unzip_root.to_path_buf();
    task::spawn_blocking(move || {
        std::fs::create_dir_all(&unzip_root)?;
        let cursor = std::io::Cursor::new(zip_bytes);
        let mut archive = ZipArchive::new(cursor)?;
        for idx in 0..archive.len() {
            let mut file = archive.by_index(idx)?;
            if file.is_dir() {
                continue;
            }
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut file, &mut buf)?;
            let out_path = unzip_root.join(file.name());
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&out_path, &buf)?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("unzip join: {}", e))??;
    Ok(())
}
