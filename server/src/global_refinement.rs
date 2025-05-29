use std::{collections::HashMap, fs::{self, File}, io::{self, Cursor, Error, ErrorKind}, path::{Path, PathBuf}, pin::Pin, sync::{Arc, Mutex}, task::{Context, Poll}};
use posemesh_domain::{auth::{handshake, TaskTokenClaim}, capabilities::public_key::PublicKeyStorage, datastore::{common::{data_id_generator, Datastore}, remote::RemoteDatastore}, message::read_prefix_size_message, protobuf::{domain_data::{self, Metadata, Query, UpsertMetadata}, task}};
use posemesh_networking::{client::{Client, TClient}, AsyncStream};
use quick_protobuf::{deserialize_from_slice, serialize_into_vec};
use regex::Regex;
use tokio::{self, sync::watch, time::{sleep, Duration}};
use futures::AsyncWrite;
use zip::ZipArchive;

use crate::utils::{execute_python, health};

async fn upload_results(domain_id: &str, output_path: PathBuf, datastore: &mut RemoteDatastore) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut uploader = datastore.upsert(domain_id.to_string()).await?;
    // open output_path and upload to datastore
    let files = fs::read_dir(output_path)?;
    for file in files {
        let file = file?;
        let path = file.path();
        let metadata: UpsertMetadata = match file.file_name().to_str().unwrap() {
            "refined_manifest.json" => UpsertMetadata {
                name: "refined_manifest".to_string(),
                data_type: "refined_manifest_json".to_string(),
                size: file.metadata()?.len() as u32,
                id: data_id_generator(),
                properties: HashMap::new(),
                is_new: true,
            },
            "RefinedPointCloud.ply" => UpsertMetadata {
                name: "refined_pointcloud".to_string(),
                data_type: "refined_pointcloud_ply".to_string(),
                size: file.metadata()?.len() as u32,
                id: data_id_generator(),
                properties: HashMap::new(),
                is_new: true
            },
            "BasicStitchPointCloud.ply" => UpsertMetadata {
                name: "unrefined_pointcloud".to_string(),
                data_type: "unrefined_pointcloud_ply".to_string(),
                size: file.metadata()?.len() as u32,
                id: data_id_generator(),
                properties: HashMap::new(),
                is_new: true
            },
            _ => continue
        };
        let content = fs::read(path)?;
        let mut producer = uploader.push(&metadata).await?;
        producer.next_chunk(&content, false).await?;
    }

    while !uploader.is_completed().await {
        sleep(Duration::from_secs(3)).await;
    }
    uploader.close().await;
    Ok(())
}

fn unzip_bytes(path: PathBuf, zip_bytes: &[u8]) -> io::Result<()> {
    let cursor = Cursor::new(zip_bytes);
    let mut archive = ZipArchive::new(cursor)?;
    tracing::info!("Zip archive opened, contains {} files", archive.len());
    
    for i in 0..archive.len() {
        let mut input_file = archive.by_index(i)?;
        let file_name = input_file.enclosed_name().unwrap();
        let file_name = file_name.file_name().unwrap().to_str().unwrap();
        
        let file_path = path.join(file_name);
        
        // Create parent directory if it doesn't exist
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        let mut output_file: File = File::create(&file_path)?;
        let _ = std::io::copy(&mut input_file, &mut output_file)?;
    }
    Ok(())
}

async fn initialize_task<S: AsyncStream, P: PublicKeyStorage, C: TClient + Clone + Send + Sync + 'static>(
    base_path: &str,
    stream: &mut S,
    c: &mut C,
    key_loader: P,
) -> Result<(task::Task, String, TaskTokenClaim, PathBuf, PathBuf, PathBuf), Box<dyn std::error::Error + Send + Sync>> {
    let claim = handshake(stream, key_loader).await.expect("Failed to handshake");
    let job_id = claim.job_id.clone();
    c.subscribe(job_id.clone()).await.expect("Failed to subscribe to job");
    let task = task::Task {
        name: claim.task_name.clone(),
        receiver: Some(claim.receiver.clone()),
        sender: claim.sender.clone(),
        endpoint: "/global-refinement/v1".to_string(),
        status: task::Status::STARTED,
        access_token: None,
        job_id: job_id.clone(),
        output: None,
    };

    let task_path = Path::new(base_path).join(&job_id);
    let dataset_path = task_path.join("datasets");
    let input_path = task_path.join("refined").join("local");
    let output_path = task_path.join("refined").join("global");

    /*
        | volumn/node_name
        | | job_id
        | | | datasets/suffix
        | | | refined/local/suffix/sfm => input for global refinement
        | | | refined/global => output for global refinement
    */

    fs::create_dir_all(&input_path)?;
    fs::create_dir_all(&dataset_path)?;
    fs::create_dir_all(&output_path)?;

    Ok((task, job_id, claim, task_path, input_path, output_path))
}

fn download_local_refinement_result(input_path: &Path, dataset_path: &Path, name: &str, data: &[u8]) -> io::Result<String> {
    let suffix = extract_suffix(name).map_err(|_| Error::new(ErrorKind::Other, "Failed to extract suffix"))?;
    fs::create_dir_all(dataset_path.join(&suffix))?;
    let path = input_path.join(&suffix).join("sfm");
    unzip_bytes(path, data)?;
    Ok(suffix)
}

#[derive(Clone)]
struct ToZipArchive {
    input_path: PathBuf,
    dataset_path: PathBuf,
    size: u32,
    name: String,
    content: Vec<u8>,
    scan_ids: Arc<Mutex<Vec<String>>>,
}
impl ToZipArchive {
    fn new(input_path: PathBuf, dataset_path: PathBuf) -> Self {
        Self {
            input_path,
            dataset_path,
            size: 0,
            name: "".to_string(),
            content: vec![],
            scan_ids: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn scan_ids(&self) -> Vec<String> {
        self.scan_ids.lock().unwrap().clone()
    }
}

impl AsyncWrite for ToZipArchive {
    fn poll_write(mut self: Pin<&mut Self>, _: &mut Context<'_>, content: &[u8]) -> Poll<Result<usize, Error>> {
        if self.size == 0 {
            if content.len() < 4 {
                return Poll::Ready(Err(Error::new(ErrorKind::Other, "Content is too short")));
            }
            let metadata = deserialize_from_slice::<Metadata>(&content[..4]).map_err(|_| Error::new(ErrorKind::Other, "Failed to deserialize metadata"))?;
            self.size = metadata.size;
            self.name = metadata.name;
            self.content = vec![];
            return Poll::Ready(Ok(content.len()));
        } else {
            self.content.extend_from_slice(content);
            return Poll::Ready(Ok(content.len()));
        }
    }
    
    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.size == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.size > self.content.len() as u32 {
            return Poll::Ready(Ok(()));
        }
        let suffix = download_local_refinement_result(&self.input_path, &self.dataset_path, &self.name, &self.content)?;
        self.scan_ids.lock().unwrap().push(suffix);
        self.content = vec![];
        self.size = 0;
        self.name = "".to_string();
        
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.content = vec![];
        self.size = 0;
        self.name = "".to_string();
        Poll::Ready(Ok(()))
    }
}

async fn download_data(
    datastore: &mut RemoteDatastore,
    domain_id: &str,
    query: Query,
    input_path: &Path,
    dataset_path: &Path,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let to_file_system = ToZipArchive::new(input_path.to_path_buf(), dataset_path.to_path_buf());
    let mut downloader = datastore.load(domain_id.to_string(), query, false, to_file_system.clone()).await?;
    downloader.wait_for_done().await?;

    if to_file_system.scan_ids().is_empty() {
        return Err("No scans to refine".into());
    }

    Ok(to_file_system.scan_ids())
}

fn extract_suffix(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let date_time_regex = Regex::new(r"\d{4}-\d{2}-\d{2}[_-]\d{2}-\d{2}-\d{2}")?;
    date_time_regex
        .find(name)
        .map(|m| m.as_str().to_string())
        .ok_or_else(|| "Failed to parse suffix from data.metadata.name".into())
}

pub(crate) async fn v1<S: AsyncStream, P: PublicKeyStorage>(
    base_path: String,
    mut stream: S,
    datastore: RemoteDatastore,
    mut c: Client,
    key_loader: P,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut t, job_id, claim, task_path, input_path, output_path) =
        initialize_task(&base_path, &mut stream, &mut c, key_loader).await?;

    let input = read_prefix_size_message::<task::GlobalRefinementInputV1, _>(&mut stream).await?;
    tracing::info!("Received global refinement input: {:?}", input);

    let mut query = Query::default();
    for result in input.local_refinement_results {
        query.ids.extend(result.result_ids);
    }

    c.publish(job_id.clone(), serialize_into_vec(&t).expect("failed to serialize task update")).await?;

    let (tx, rx) = watch::channel(false);

    let heartbeat_handle = health(t.clone(), c.clone(), &job_id, rx).await;

    let _cleanup = scopeguard::guard(tx.clone(), |tx| {
        let _ = tx.send(true); // Signal heartbeat task to stop
    });

    let domain_id = claim.domain_id.clone();
    let res = run(&domain_id, &input_path, &task_path, &output_path, datastore, query, &job_id).await;

    if let Err(e) = res {
        t.status = task::Status::FAILED;
        t.output = Some(task::Any {
            type_url: "Error".to_string(),
            value: serialize_into_vec(&task::Error {
                message: e.to_string(),
            }).expect("failed to serialize local refinement output"),
        });
        tracing::error!("Error: {}", e);
    } else {
        t.status = task::Status::DONE;
    }

    tx.send(true).unwrap();
    let _ = heartbeat_handle.await;

    let buf = serialize_into_vec(&t)?;
    c.publish(job_id.to_string(), buf).await?;

    tracing::info!("Finished executing {}", claim.task_name);

    Ok(())
}

async fn run(
    domain_id: &str,
    input_path: &Path,
    task_path: &Path,
    output_path: &Path,
    mut datastore: RemoteDatastore,
    query: Query,
    job_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let scan_ids = download_data(&mut datastore, &domain_id, query, &input_path, &task_path.join("datasets")).await?;
        
    let mut params = vec![
        "-u",
        "main.py",
        "--mode", "global_refinement",
        "--job_root_path", task_path.to_str().unwrap(),
        "--output", output_path.to_str().unwrap(),
        "--domain_id", &domain_id,
        "--job_id", &job_id,
        "--scans",
    ];
    params.extend(scan_ids.iter().map(|s| s.as_str()));

    execute_python(params).await?;
    upload_results(&domain_id, output_path.to_path_buf(), &mut datastore).await?;
    std::fs::remove_dir_all(output_path)?;
    Ok(())
}
