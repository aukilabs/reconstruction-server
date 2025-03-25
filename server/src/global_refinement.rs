use std::{collections::HashMap, fs::{self, File}, io::{self, BufReader, BufWriter, Read, Write, BufRead, Cursor}, path::{Path, PathBuf}, process::{Command, Stdio}, time::SystemTime};
use chrono::Utc;
use domain::{cluster::DomainCluster, datastore::{self, common::Datastore, remote::RemoteDatastore}, message::read_prefix_size_message, protobuf::{domain_data::{Data, Metadata, Query},task::{self, LocalRefinementInputV1, LocalRefinementOutputV1}}};
use jsonwebtoken::{decode, DecodingKey,Validation, Algorithm};
use libp2p::Stream;
use networking::{client::Client, libp2p::Networking};
use quick_protobuf::{deserialize_from_slice, serialize_into_vec, BytesReader};
use regex::Regex;
use tokio::{self, select, sync::watch, time::{interval, sleep, Duration}};
use futures::{stream::Zip, AsyncReadExt, StreamExt};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use zip::ZipArchive;

use crate::utils::handshake;

fn unzip_bytes(path: PathBuf, zip_bytes: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Starting to unzip bytes to path: {:?}", path);
    let cursor = Cursor::new(zip_bytes);
    let mut archive = ZipArchive::new(cursor)?;
    println!("Zip archive opened, contains {} files", archive.len());
    
    for i in 0..archive.len() {
        println!("Processing file {}/{}", i + 1, archive.len());
        let mut input_file = archive.by_index(i)?;
        let file_name = input_file.name().to_string();
        println!("Extracting file: {}", file_name);
        
        let file_path = path.join(&file_name);
        
        // Create parent directory if it doesn't exist
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        let mut output_file: File = File::create(&file_path)?;
        let bytes_copied = std::io::copy(&mut input_file, &mut output_file)?;
        println!("Extracted {} bytes to {:?}", bytes_copied, file_path);
    }
    println!("Finished unzipping all files");
    Ok(())
}

pub(crate) async fn v1(base_path: String, mut stream: Stream, mut datastore: Box<dyn Datastore>, mut c: Client) {
    let claim = handshake(&mut stream).await.expect("Failed to handshake");
    let job_id = claim.job_id.clone();
    c.subscribe(job_id.clone()).await.expect("Failed to subscribe to job");
    let t = &mut task::Task {
        name: claim.task_name.clone(),
        receiver: Some(claim.receiver.clone()),
        sender: claim.sender.clone(),
        endpoint: "/global-refinement/v1".to_string(),
        status: task::Status::STARTED,
        access_token: None,
        job_id: job_id.clone(),
        output: None,
    };
    /*
        | volumn/node_name
        | | job_id
        | | | datasets/suffix
        | | | refined/local/suffix/sfm => input for global refinement
        | | | refined/global => output for global refinement
    
    */

    let input = read_prefix_size_message::<task::GlobalRefinementInputV1>(stream).await.expect("Failed to read global refinement input");
    println!("Received global refinement input: {:?}", input);
    // merge input.local_refinement_output into query
    let mut query = Query::default();
    for result in input.local_refinement_results {
        query.ids.extend(result.result_ids);
    }
    
    c.publish(job_id.clone(), serialize_into_vec(t).expect("failed to serialize task update")).await.expect("failed to publish task update");

    // download the local refinement output
    let task_path = Path::new(&base_path).join(job_id.clone());
    let dataset_path = Path::new(&task_path).join("datasets");
    let input_path = Path::new(&task_path).join("refined").join("local");
    let output_path = Path::new(&task_path).join("refined").join("global");
    fs::create_dir_all(input_path.clone()).expect("Failed to create input folder");
    fs::create_dir_all(dataset_path.clone()).expect("Failed to create dataset folder");
    fs::create_dir_all(output_path.clone()).expect("Failed to create output folder");

    let (tx, rx) = watch::channel(false);
    let mut c_clone = c.clone();
    let job_id_clone = job_id.clone();
    let task_clone = t.clone();
    let heartbeat_handle = tokio::spawn(async move {
        let mut rx = rx;
        let mut interval = interval(Duration::from_secs(30)); // Send heartbeat every 30 seconds
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let mut progress_task = task_clone.clone();
                    progress_task.status = task::Status::PROCESSING;
                    if let Ok(message) = serialize_into_vec(&progress_task) {
                        let _ = c_clone.publish(job_id_clone.clone(), message).await;
                    }
                }
                Ok(_) = rx.changed() => {
                    break;
                }
            }
        }
    });

    let _cleanup = scopeguard::guard(tx, |tx| {
        let _ = tx.send(true); // Signal heartbeat task to stop
    });

    let mut downloader = datastore.consume(claim.domain_id.clone(), query, false).await;

    loop {
        match downloader.next().await {
            Some(Ok(data)) => {
                // parse suffix from data.metadata.name
                let date_time_regex = Regex::new(r"\d{4}-\d{2}-\d{2}[_-]\d{2}-\d{2}-\d{2}").unwrap();
                let res = date_time_regex.find(&data.metadata.name)
                    .map(|m| m.as_str().to_string());
                if res.is_none() {
                    t.status = task::Status::FAILED;
                    t.output = Some(task::Any {
                        type_url: "Error".to_string(),
                        value: "Failed to parse suffix from data.metadata.name".to_string().into_bytes(),
                    });
                    let message = serialize_into_vec(t).expect("failed to serialize task update");
                    c.publish(job_id.clone(), message).await.expect("failed to publish task update");
                    return;
                }
                let suffix = res.unwrap();
                
                fs::create_dir_all(Path::new(&dataset_path).join(&suffix)).expect("Failed to create dataset folder");
                let path = Path::new(&input_path).join(&suffix).join("sfm");
                if let Err(e) = unzip_bytes(path, data.content) {
                    t.status = task::Status::FAILED;
                    t.output = Some(task::Any {
                        type_url: "Error".to_string(),
                        value: e.to_string().into_bytes(),
                    });
                    let message = serialize_into_vec(t).expect("failed to serialize task update");
                    c.publish(job_id.clone(), message).await.expect("failed to publish task update");
                    return;
                }
                println!("downloaded {}", data.metadata.name);
            }
            Some(Err(_)) => {
                t.status = task::Status::RETRY;
                let message = serialize_into_vec(t).expect("failed to serialize task update");
                c.publish(job_id.clone(), message).await.expect("failed to publish task update");
                return;
            }
            None => {
                break;
            }
        }
    }
    let params = vec![
        "main.py",
        "--mode", "global_refinement",
        "--job_root_path", task_path.to_str().unwrap(),
        "--output", output_path.to_str().unwrap(),
        "--domain_id", &claim.domain_id,
        "--job_id", &claim.job_id,
        "--scans"
    ];
    let child = Command::new("python3")
    .args(params)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn();

    if let Err(e) = child {
        eprintln!("Failed to execute global refinement: {}", e);
        t.status = task::Status::FAILED;
        let message = serialize_into_vec(t).expect("failed to serialize task update");
        c.publish(job_id.clone(), message).await.expect("failed to publish task update");
        return;
    }
    let mut child = child.unwrap();

    // Read stdout in real-time
    if let Some(stdout) = child.stdout.take() {
        let stdout_reader = BufReader::new(stdout);
        tokio::spawn(async move {
            for line in stdout_reader.lines() {
                if let Ok(line) = line {
                    println!("stdout: {}", line);
                }
            }
        });
    }

    // Read stderr in real-time
    if let Some(stderr) = child.stderr.take() {
        let stderr_reader = BufReader::new(stderr);
        tokio::spawn(async move {
            for line in stderr_reader.lines() {
                if let Ok(line) = line {
                    eprintln!("stderr: {}", line);
                }
            }
        });
    }

    // Wait for the process to complete
    match child.wait() {
        Ok(status) => {
            if !status.success() {
                eprintln!("Python process exited with status: {}", status);
                t.status = task::Status::FAILED;
                t.output = Some(task::Any {
                    type_url: "Error".to_string(),
                    value: serialize_into_vec(&task::Error {
                        message: format!("Python process exited with status: {}", status),
                    }).unwrap(),
                });
                let message = serialize_into_vec(t).expect("failed to serialize task update");
                c.publish(job_id.clone(), message).await.expect("failed to publish task update");
                return;
            }
            println!("Finished executing {}", claim.task_name);
        }
        Err(e) => {
            eprintln!("Failed to wait for Python process: {}", e);
            t.status = task::Status::FAILED;
            t.output = Some(task::Any {
                type_url: "Error".to_string(),
                value: serialize_into_vec(&task::Error {
                    message: format!("Failed to wait for Python process: {}", e),
                }).unwrap(),
            });
            let message = serialize_into_vec(t).expect("failed to serialize task update");
            c.publish(job_id.clone(), message).await.expect("failed to publish task update");
            return;
        }
    }

    let mut uploader = datastore.produce(claim.domain_id.clone()).await;
    // open output_path and upload to datastore
    let files = fs::read_dir(output_path).expect("Failed to read output folder");
    for file in files {
        let file = file.unwrap();
        let path = file.path();
        let content = fs::read(path).expect("Failed to read file");
        let metadata: Metadata = match file.file_name().to_str().unwrap() {
            "refined_manifest.json" => Metadata {
                name: "refined_manifest".to_string(),
                data_type: "refined_manifest_json".to_string(),
                size: content.len() as u32,
                id: None,
                properties: HashMap::new(),
            },
            "RefinedPointCloud.ply" => Metadata {
                name: "refined_pointcloud".to_string(),
                data_type: "refined_pointcloud_ply".to_string(),
                size: content.len() as u32,
                id: None,
                properties: HashMap::new(),
            },
            "BasicStitchPointCloud.ply" => Metadata {
                name: "unrefined_pointcloud".to_string(),
                data_type: "unrefined_pointcloud_ply".to_string(),
                size: content.len() as u32,
                id: None,
                properties: HashMap::new(),
            },
            _ => continue
        };
        uploader.push(&Data {
            domain_id: claim.domain_id.clone(),
            metadata,
            content,
        }).await.expect("failed to publish file");
    }

    while !uploader.is_completed().await {
        sleep(Duration::from_secs(3)).await;
    }
    uploader.close().await;

    let event = task::Task {
        name: claim.task_name.clone(),
        receiver: Some(claim.receiver.clone()),
        sender: claim.sender.clone(),
        endpoint: "/global-refinement/v1".to_string(),
        status: task::Status::DONE,
        access_token: None,
        job_id: job_id.clone(),
        output: None,
    };
    let buf = serialize_into_vec(&event).expect("failed to serialize task update");
    c.publish(job_id.clone(), buf).await.expect("failed to publish task update");
    println!("Finished executing {}", claim.task_name);

    let _ = heartbeat_handle.await;
    println!("Heartbeat task stopped");
    return;
}
