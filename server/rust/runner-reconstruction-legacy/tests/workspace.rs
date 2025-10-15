use runner_reconstruction_legacy::workspace::Workspace;
use std::path::PathBuf;
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn creates_structure_under_provided_root() {
    let base = tempdir().unwrap();
    let domain = Uuid::new_v4().to_string();
    let job = Uuid::new_v4().to_string();
    let task = Uuid::new_v4().to_string();

    let workspace =
        Workspace::create(Some(base.path()), &domain, Some(&job), &task).expect("workspace");
    let expected_root = base
        .path()
        .join("jobs")
        .join(&domain)
        .join(format!("job_{}", job));
    assert_eq!(workspace.root(), expected_root.as_path());
    assert!(workspace.datasets().exists());
    assert!(workspace.refined_local().exists());
    assert!(workspace.refined_global().exists());
    assert_eq!(
        workspace.job_manifest_path(),
        workspace.root().join("job_manifest.json")
    );
    assert_eq!(
        workspace.scan_data_summary_path(),
        workspace.root().join("scan_data_summary.json")
    );

    drop(workspace);
    // Provided root should remain on disk.
    assert!(expected_root.exists());
}

#[test]
fn temp_workspace_is_cleaned_up_on_drop() {
    let domain = Uuid::new_v4().to_string();
    let task = Uuid::new_v4().to_string();

    let root_path: PathBuf;
    {
        let workspace =
            Workspace::create(None, &domain, None, &task).expect("workspace without base path");
        root_path = workspace.root().to_path_buf();
        assert!(root_path.exists());
        assert!(workspace.datasets().exists());
        assert!(workspace.refined_global().exists());
    }
    assert!(
        !root_path.exists(),
        "temporary workspace should be deleted after drop"
    );
}
