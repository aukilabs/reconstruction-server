use std::{collections::HashMap, fs::{self, OpenOptions}, io::{Error, ErrorKind}, path::{Path, PathBuf}, pin::Pin, sync::{Arc, Mutex}, task::{Context, Poll}, time::Duration};
use posemesh_domain::{auth::handshake, capabilities::public_key::PublicKeyStorage, datastore::{common::{data_id_generator, Datastore}, remote::RemoteDatastore}, message::read_prefix_size_message, protobuf::{domain_data::{Metadata, Query, UpsertMetadata},task::{self, LocalRefinementInputV1, LocalRefinementOutputV1}}};
use posemesh_networking::{client::TClient, AsyncStream};
use quick_protobuf::{deserialize_from_slice, serialize_into_vec};
use futures::{AsyncWrite, StreamExt};
use tokio::{sync::watch, time::sleep};
use std::io::{BufReader, Read, Write};
use zip::write::SimpleFileOptions;
use crate::utils::{execute_python, health, write_scan_data_summary};

const REFINED_SCAN_NAME: &str = "refined_scan_";

#[derive(Clone)]
struct ToFileSystem {
    size: u32,
    content: Option<Vec<u8>>,
    path: Option<PathBuf>,
    input_folder: PathBuf,
    count: Arc<Mutex<u32>>,
    written: u32,
    refined_res_id: Arc<Mutex<Option<String>>>,
}
impl ToFileSystem {
    fn new(input_folder: PathBuf) -> Self {
        Self {
            input_folder,
            path: None,
            size: 0,
            content: None,
            count: Arc::new(Mutex::new(0)),
            written: 0,
            refined_res_id: Arc::new(Mutex::new(None)),
        }
    }

    fn count(&self) -> u32 {
        self.count.lock().unwrap().clone()
    }

    fn refined_res_id(&self) -> Option<String> {
        self.refined_res_id.lock().unwrap().clone()
    }
}

impl AsyncWrite for ToFileSystem {
    fn poll_write(mut self: Pin<&mut Self>, _: &mut Context<'_>, content: &[u8]) -> Poll<Result<usize, Error>> {
        if self.path.is_none() {
            if content.len() < 4 {
                return Poll::Ready(Err(Error::new(ErrorKind::Other, "Content is too short")));
            }
            match deserialize_from_slice::<Metadata>(&content[..4]) {
                Ok(metadata) => {
                    if let Some(_) = metadata.name.strip_prefix("refined_scan_") {
                        self.refined_res_id.lock().unwrap().insert(metadata.id.clone());
                        return Poll::Ready(Err(Error::new(ErrorKind::Other, format!("RefinedAlready({})", metadata.id))));
                    }
                    let filename = match metadata.name.as_str() {
                        "Manifest.json" => "Manifest.json".to_string(),
                        "FeaturePoints.ply" => "FeaturePoints.ply".to_string(),
                        "ARposes.csv" => "ARposes.csv".to_string(),
                        "PortalDetections.csv" => "PortalDetections.csv".to_string(),
                        "CameraIntrinsics.csv" => "CameraIntrinsics.csv".to_string(),
                        "Frames.csv" => "Frames.csv".to_string(),
                        "Gyro.csv" => "Gyro.csv".to_string(),
                        "Accel.csv" => "Accel.csv".to_string(),
                        "gyro_accel.csv" => "gyro_accel.csv".to_string(),
                        "Frames.mp4" => "Frames.mp4".to_string(),
                        _ => {
                            match metadata.data_type.as_str() {
                                "dmt_manifest_json" => "Manifest.json".to_string(),
                                "dmt_featurepoints_ply" | "dmt_pointcloud_ply" => "FeaturePoints.ply".to_string(),
                                "dmt_arposes_csv" => "ARposes.csv".to_string(),
                                "dmt_portal_detections_csv" | "dmt_observations_csv" => "PortalDetections.csv".to_string(),
                                "dmt_intrinsics_csv" | "dmt_cameraintrinsics_csv" => "CameraIntrinsics.csv".to_string(),
                                "dmt_frames_csv" => "Frames.csv".to_string(),
                                "dmt_gyro_csv" => "Gyro.csv".to_string(),
                                "dmt_accel_csv" => "Accel.csv".to_string(),
                                "dmt_gyroaccel_csv" => "gyro_accel.csv".to_string(),
                                "dmt_recording_mp4" => "Frames.mp4".to_string(),
                                _ => {
                                    tracing::info!("unknown domain data type: {}", metadata.data_type);
                                    format!("{}.{}", metadata.name, metadata.data_type)
                                }
                            }
                        }
                    };

                    self.path = Some(self.input_folder.join(&filename));
                    self.written = 0;
                    return Poll::Ready(Ok(content.len()));
                }
                Err(_) => {
                    return Poll::Ready(Err(Error::new(ErrorKind::Other, "Failed to deserialize metadata")));
                }
            }
        } else {
            self.content
                .get_or_insert_with(Vec::new)
                .extend_from_slice(content);
            return Poll::Ready(Ok(content.len()));
        }
    }
    
    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.path.is_none() {
            return Poll::Ready(Ok(()));
        }
        let path = self.path.as_ref().unwrap();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let content = self.content.take().unwrap_or_default();
        file.write_all(&content)?;

        self.written += content.len() as u32;
        if self.written == self.size {
            self.path = None;
            self.written = 0;
            let mut count = self.count.lock().unwrap();
            *count += 1;
        }
        
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.content = None;
        self.path = None;
        Poll::Ready(Ok(()))
    }
}

pub(crate) async fn v1<S: AsyncStream, P: PublicKeyStorage, C: TClient + Clone + Send + Sync + 'static>(base_path: String, mut stream: S, datastore: RemoteDatastore, mut c: C, key_loader: P) {
    let claim = handshake(&mut stream, key_loader).await.expect("Failed to handshake");
    let job_id = claim.job_id.clone();
    c.subscribe(job_id.clone()).await.expect("Failed to subscribe to job");
    let t = &mut task::Task {
        name: claim.task_name.clone(),
        receiver: Some(claim.receiver.clone()),
        sender: claim.sender.clone(),
        endpoint: "/local-refinement/v1".to_string(),
        status: task::Status::STARTED,
        access_token: None,
        job_id: job_id.clone(),
        output: None,
    };

    let res = start(base_path, &job_id, &claim.task_name, &mut stream).await;
    match res {
        Ok(context) => {
            t.status = task::Status::STARTED;
            let message = serialize_into_vec(t).expect("failed to serialize task update");
            c.publish(job_id.clone(), message).await.expect("failed to publish task update");
            let (tx, rx) = watch::channel(false);

            let heartbeat_handle = health(t.clone(), c.clone(), &job_id, rx).await;

            let _cleanup = scopeguard::guard(tx.clone(), |tx| {
                let _ = tx.send(true); // Signal heartbeat task to stop
            });

            let output = run(&claim.domain_id, &job_id, &claim.task_name, context, datastore).await;
            match output {
                Ok(output) => {
                    t.output = Some(task::Any {
                        type_url: "LocalRefinementOutputV1".to_string(),
                        value: serialize_into_vec(&output).expect("failed to serialize local refinement output"),
                    });
                    t.status = task::Status::DONE;
                }
                Err(e) => {
                    tracing::error!("Failed to run local refinement: {}", e);
                    t.status = task::Status::FAILED;
                    t.output = Some(task::Any {
                        type_url: "Error".to_string(),
                        value: serialize_into_vec(&task::Error {
                            message: e.to_string(),
                        }).expect("failed to serialize local refinement output"),
                    });
                }
            }

            tx.send(true).unwrap();
            let _ = heartbeat_handle.await;
        }
        Err(e) => {
            tracing::error!("Failed to start local refinement: {}", e);
            t.status = task::Status::FAILED;
            t.output = Some(task::Any {
                type_url: "Error".to_string(),
                value: serialize_into_vec(&task::Error {
                    message: e.to_string(),
                }).expect("failed to serialize local refinement output"),
            });
        }
    }
    let message = serialize_into_vec(t).expect("failed to serialize task update");
    c.publish(job_id.clone(), message).await.expect("failed to publish task update");

    tracing::info!("finished local refinement {}", job_id);
}

struct LocalRefinementContext {
    query: Query,
    task_folder: PathBuf,
    scan_folder: PathBuf,
    input_folder: PathBuf,
    output_folder: PathBuf,
    suffix: String,
}

async fn start<S: AsyncStream>(base_path: String, job_id: &str, task_name: &str, mut stream: S) -> Result<LocalRefinementContext, Box<dyn std::error::Error + Send + Sync>> {
    let input = read_prefix_size_message::<LocalRefinementInputV1, _>(&mut stream).await?;

    tracing::info!("Start executing {}, {:?}", task_name, input);

    // input.name_regexp looks .*_date
    // get date from regexp
    let query = input.query;
    let query_clone = query.clone();
    let name_regexp = query.name_regexp;
    if name_regexp.is_none() {
        return Err("Name regexp is empty".into());
    }

    /*
        get suffix from regexp

        | volumn/node_name
        | | job_id
        | | | datasets/suffix => input for local refinement
        | | | refined/local/suffix
        | | | | sfm/.(txt|bin|csv) => output for local refinement
        upload refined_scan_2024-12-03_22-57-12 with type refined_scan_zip_v1

     */
    let suffix = name_regexp.unwrap().replace(".*_", "");

    let task_folder = Path::new(&base_path).join(job_id);

    let scan_folder = Path::new(&task_folder).join("datasets");
    let input_folder = Path::new(&scan_folder).join(&suffix.clone());
    let output_folder = Path::new(&task_folder).join("refined").join("local");
    fs::create_dir_all(&scan_folder)?;
    fs::create_dir_all(&output_folder)?;
    fs::create_dir_all(&input_folder)?;

    Ok(LocalRefinementContext {
        query: query_clone,
        task_folder,
        scan_folder,
        input_folder,
        output_folder,
        suffix,
    })
}

async fn run(domain_id: &str, job_id: &str, task_name: &str, context: LocalRefinementContext, mut datastore: RemoteDatastore) -> Result<LocalRefinementOutputV1, Box<dyn std::error::Error + Send + Sync>> { 
    let query = context.query;

    let writer = ToFileSystem::new(context.output_folder.clone());
    let mut downloader = datastore.load(domain_id.to_string(), Query {
        metadata_only: false,
        ..query
    }, false, writer.clone()).await?;
    if let Err(e) = downloader.wait_for_done().await {
        let refined_res_id = writer.refined_res_id();
        if refined_res_id.is_some() {
            return Ok(LocalRefinementOutputV1 {
                result_ids: vec![refined_res_id.unwrap()],
            });
        }
        return Err(Box::new(e));
    }

    tracing::info!("Finished downloading {} data for {}", writer.count(), task_name);

    write_scan_data_summary(context.input_folder.as_path(), context.task_folder.as_path().join("scan_data_summary.json").as_path())?;

    let params = vec![
        "-u",
        "main.py",
        "--mode", "local_refinement",
        "--job_root_path", context.task_folder.to_str().unwrap(),
        "--output", context.output_folder.to_str().unwrap(),
        "--domain_id", domain_id,
        "--job_id", job_id,
        "--scans", context.suffix.as_str(),
    ];

    execute_python(params).await?;

    let sfm = context.output_folder.join(context.suffix.clone()).join("sfm");
    fs::create_dir_all(&sfm)?;
    let zip_path = sfm.join(context.suffix.clone() + ".zip");
    let mut zip = zip::ZipWriter::new(fs::File::create(&zip_path)?);
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored); // No compression
    
    // open output folder/sfm, zip all txt, bin, csv files and upload
    for entry in fs::read_dir(sfm)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            continue;
        }
        let ext = path.extension().expect("Failed to get extension").to_str().expect("Failed to convert extension to string");
        if ext == "txt" || ext == "bin" || ext == "csv" {
            let str_path = path.to_str().expect("Failed to convert path to string");
            let file = fs::File::open(str_path)?;
            let mut reader = BufReader::new(file);
            zip.start_file(str_path, options)?;

            let mut buffer = [0u8; 8192]; // Use a buffer to stream in chunks
            loop {
                let bytes_read = reader.read(&mut buffer)?;
                if bytes_read == 0 {
                    break;
                }
                zip.write_all(&buffer[..bytes_read])?;
            }
        }
    }
    zip.finish()?;
    let zip_file_metadata = fs::metadata(&zip_path)?;

    let mut producer = datastore.upsert(domain_id.to_string()).await?;
    let res_id = data_id_generator();
    let mut chunks = producer.push(
        &UpsertMetadata {
            size: zip_file_metadata.len() as u32,
            name: format!("refined_scan_{}", context.suffix.clone()),
            data_type: "refined_scan_zip_v1".to_string(),
            id: res_id.clone(),
            is_new: true,
            properties: HashMap::new(),
        },
    ).await?;
    
    let _ = chunks.next_chunk(&fs::read(&zip_path)?, false).await?;
    while !producer.is_completed().await {
        sleep(Duration::from_secs(3)).await;
    }
    producer.close().await;
    std::fs::remove_dir_all(&context.task_folder)?;
    
    let output = LocalRefinementOutputV1 {
        result_ids: vec![res_id],
    };
    
    return Ok(output);
}
