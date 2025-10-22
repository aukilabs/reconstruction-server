use std::{
    collections::HashMap,
    io::Read,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use compute_runner_api::runner::{DomainArtifactContent, DomainArtifactRequest};
use compute_runner_api::ArtifactSink;
use runner_reconstruction_legacy::{refined::RefinedUploader, workspace::Workspace};
use tokio::runtime::Runtime;
use zip::read::ZipArchive;

type UploadEntry = (String, Vec<u8>);
type UploadLog = Arc<Mutex<Vec<UploadEntry>>>;

#[derive(Default)]
struct RecordingSink {
    uploads: UploadLog,
}

#[async_trait]
impl ArtifactSink for RecordingSink {
    async fn put_bytes(&self, rel_path: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.uploads
            .lock()
            .unwrap()
            .push((rel_path.to_string(), bytes.to_vec()));
        Ok(())
    }

    async fn put_file(&self, rel_path: &str, file_path: &std::path::Path) -> anyhow::Result<()> {
        let bytes = std::fs::read(file_path)?;
        self.put_bytes(rel_path, &bytes).await
    }

    async fn put_domain_artifact(
        &self,
        request: DomainArtifactRequest<'_>,
    ) -> anyhow::Result<Option<String>> {
        match request.content {
            DomainArtifactContent::Bytes(bytes) => {
                self.put_bytes(request.rel_path, bytes).await?;
            }
            DomainArtifactContent::File(path) => {
                self.put_file(request.rel_path, path).await?;
            }
        }
        Ok(None)
    }
}

fn create_workspace() -> Workspace {
    Workspace::create(None, "dom", Some("job"), "task").unwrap()
}

fn populate_scan(workspace: &Workspace, scan: &str, files: &[(&str, &str)]) -> PathBuf {
    let sfm_dir = workspace.refined_local().join(scan).join("sfm");
    std::fs::create_dir_all(&sfm_dir).unwrap();
    for (path, contents) in files {
        let path = sfm_dir.join(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, contents).unwrap();
    }
    sfm_dir
}

#[test]
fn uploader_zips_and_uploads_new_scans_once() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let workspace = create_workspace();
        populate_scan(
            &workspace,
            "scan_a",
            &[
                ("images.bin", "img"),
                ("cameras.bin", "cam"),
                ("points3D.bin", "pts"),
                ("portals.csv", "1,2"),
                ("config.txt", "hello"),
                ("nested/data.bin", "123"),
                ("notes.md", "skip"),
            ],
        );
        populate_scan(
            &workspace,
            "scan_b",
            &[
                ("images.bin", "img"),
                ("cameras.bin", "cam"),
                ("points3D.bin", "pts"),
                ("portals.csv", "1,2"),
                ("points.csv", "1,2,3"),
            ],
        );

        let sink = RecordingSink::default();
        let mut uploader = RefinedUploader::new();

        let uploaded = uploader
            .process(&workspace, &sink, true)
            .await
            .expect("process scans");
        assert_eq!(uploaded.len(), 2);

        // Second run should not reupload completed scans.
        let second = uploader
            .process(&workspace, &sink, true)
            .await
            .expect("process scans second time");
        assert!(second.is_empty());

        let uploads = sink.uploads.lock().unwrap();
        assert_eq!(uploads.len(), 2);

        let mut uploads_map: HashMap<String, Vec<u8>> = HashMap::new();
        for (path, bytes) in uploads.iter() {
            uploads_map.insert(path.clone(), bytes.clone());
        }
        let zip_bytes = uploads_map
            .get("refined/local/scan_a/RefinedScan.zip")
            .expect("scan_a uploaded");

        let reader = std::io::Cursor::new(zip_bytes);
        let mut archive = ZipArchive::new(reader).unwrap();
        let mut contents: HashMap<String, Vec<u8>> = HashMap::new();
        for i in 0..archive.len() {
            let mut file = archive.by_index(i).unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).unwrap();
            contents.insert(file.name().to_string(), buf);
        }
        assert!(contents.contains_key("config.txt"));
        assert!(contents.contains_key("nested/data.bin"));
        assert!(
            !contents.contains_key("notes.md"),
            "non-whitelisted extension should be skipped"
        );
    });
}

#[test]
fn uploader_ignores_scans_without_sfm() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let workspace = create_workspace();
        let scan_dir = workspace.refined_local().join("scan_empty");
        std::fs::create_dir_all(scan_dir).unwrap();

        let sink = RecordingSink::default();
        let mut uploader = RefinedUploader::new();
        let uploaded = uploader
            .process(&workspace, &sink, true)
            .await
            .expect("process scans");
        assert!(uploaded.is_empty());
        assert!(sink.uploads.lock().unwrap().is_empty());
    });
}

#[test]
fn uploader_skips_when_required_files_missing() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let workspace = create_workspace();
        populate_scan(
            &workspace,
            "scan_incomplete",
            &[("images.bin", "img"), ("cameras.bin", "cam")],
        );

        let sink = RecordingSink::default();
        let mut uploader = RefinedUploader::new();
        let uploaded = uploader
            .process(&workspace, &sink, true)
            .await
            .expect("process scans");
        assert!(uploaded.is_empty());
        assert!(sink.uploads.lock().unwrap().is_empty());
    });
}

#[test]
fn uploader_respects_upload_toggle() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let workspace = create_workspace();
        populate_scan(
            &workspace,
            "scan_disabled",
            &[
                ("images.bin", "img"),
                ("cameras.bin", "cam"),
                ("points3D.bin", "pts"),
                ("portals.csv", "1,2"),
            ],
        );

        let sink = RecordingSink::default();
        let mut uploader = RefinedUploader::new();
        let uploaded = uploader
            .process(&workspace, &sink, false)
            .await
            .expect("process scans");
        assert!(uploaded.is_empty());
        assert!(sink.uploads.lock().unwrap().is_empty());
    });
}
