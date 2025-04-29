use std::{collections::HashMap, fs, path::{Path, PathBuf}, time::Duration};
use domain::{datastore::{common::Datastore, remote::RemoteDatastore}, message::read_prefix_size_message, protobuf::{domain_data::{Metadata, Query},task::{self, LocalRefinementInputV1, LocalRefinementOutputV1}}};
use networking::{client::Client, AsyncStream};
use quick_protobuf::serialize_into_vec;
use futures::StreamExt;
use tokio::{sync::watch, time::sleep};
use std::io::{BufReader, Read, Write};
use zip::write::SimpleFileOptions;
use crate::utils::{handshake, health, write_scan_data_summary, execute_python};

pub(crate) async fn v1<S: AsyncStream>(base_path: String, mut stream: S, datastore: RemoteDatastore, mut c: Client) {
    let claim = handshake(&mut stream).await.expect("Failed to handshake");
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

            let heartbeat_handle = health(t.clone(), c.clone(), &job_id, rx);

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
                    eprintln!("Failed to run local refinement: {}", e);
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
            eprintln!("Failed to start local refinement: {}", e);
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
}

struct LocalRefinementContext {
    query: Query,
    task_folder: PathBuf,
    scan_folder: PathBuf,
    input_folder: PathBuf,
    output_folder: PathBuf,
    suffix: String,
}

async fn start<S: AsyncStream>(base_path: String, job_id: &str, task_name: &str, stream: S) -> Result<LocalRefinementContext, Box<dyn std::error::Error + Send + Sync>> {
    let input = read_prefix_size_message::<LocalRefinementInputV1>(stream).await?;
    // if let Err(e) = res {
    //     eprintln!("Failed to load local refinement input {}", e);
    //     t.status = task::Status::FAILED;
    //     let message = serialize_into_vec(t).expect("failed to serialize task update");
    //     c.publish(job_id.clone(), message).await.expect("failed to publish task update");
    //     return;
    // }
    // let input = res.unwrap();

    println!("Start executing {}, {:?}", task_name, input);

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

    let mut downloader = datastore.load(domain_id.to_string(), query.clone(), false).await;
    let mut i = 0;
    loop {
        match downloader.next().await {
            Some(Ok(data)) => {
                let filename = match data.metadata.name.as_str() {
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
                        match data.metadata.data_type.as_str() {
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
                                println!("unknown domain data type: {}", data.metadata.data_type);
                                format!("{}.{}", data.metadata.name, data.metadata.data_type)
                            }
                        }
                    }
                };
                let path = context.input_folder.join(&filename);
                fs::write(path, &data.content)?;
                i+=1;
                println!("downloaded {}", filename);
            }
            Some(Err(e)) => {
                return Err(e.into());
            }
            None => {
                break;
            }
        }
    }
    println!("Finished downloading {} data for {}", i, task_name);

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

    let mut producer = datastore.upsert(domain_id.to_string()).await;
    let mut chunks = producer.push(
        &Metadata {
            size: zip_file_metadata.len() as u32,
            name: format!("refined_scan_{}", context.suffix.clone()),
            data_type: "refined_scan_zip_v1".to_string(),
            id: None,
            properties: HashMap::new(),
            link: None,
            hash: None,
        },
    ).await?;
    
    let hash = chunks.push_chunk(&fs::read(&zip_path)?, false).await?;
    while !producer.is_completed().await {
        sleep(Duration::from_secs(3)).await;
    }
    producer.close().await;
    println!("Finished uploading refined scan for {}", context.suffix.clone());
    std::fs::remove_dir_all(&context.task_folder)?;

    let output = LocalRefinementOutputV1 {
        result_ids: vec![hash],
    };
    
    return Ok(output);
}
