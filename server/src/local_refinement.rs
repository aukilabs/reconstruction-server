use std::{collections::HashMap, fs, path::Path, process::{Command, ExitCode, ExitStatus, Stdio}, time::Duration};
use domain::{datastore::common::Datastore, message::read_prefix_size_message, protobuf::{domain_data::{Data, Metadata, Query},task::{self, LocalRefinementInputV1, LocalRefinementOutputV1}}};
use libp2p::Stream;
use networking::client::Client;
use quick_protobuf::serialize_into_vec;
use futures::StreamExt;
use tokio::{sync::watch, time::{interval, sleep}};
use uuid::Uuid;
use std::io::{self, BufReader, BufWriter, Read, Write};
use zip::{write::{FileOptions, SimpleFileOptions}, ZipWriter};
use std::io::BufRead;

use crate::utils::handshake;

pub(crate) async fn v1(base_path: String, mut stream: Stream, mut datastore: Box<dyn Datastore>, mut c: Client) {
    let claim = handshake(&mut stream).await.expect("Failed to handshake");
    let job_id = claim.job_id.clone();
    c.subscribe(job_id.clone()).await.expect("Failed to subscribe to job");
    let t = &mut task::Task {
        name: claim.task_name.clone(),
        receiver: claim.receiver.clone(),
        sender: claim.sender.clone(),
        endpoint: "/local-refinement/v1".to_string(),
        status: task::Status::STARTED,
        access_token: "".to_string(),
        job_id: job_id.clone(),
        output: None,
    };

    let res = read_prefix_size_message::<LocalRefinementInputV1>(stream).await;
    if let Err(e) = res {
        eprintln!("Failed to load local refinement input {}", e);
        t.status = task::Status::FAILED;
        let message = serialize_into_vec(t).expect("failed to serialize task update");
        c.publish(job_id.clone(), message).await.expect("failed to publish task update");
        return;
    }
    let input = res.unwrap();

    println!("Start executing {}, {:?}", claim.task_name, input);

    // input.name_regexp looks .*_date
    // get date from regexp
    if input.query.is_none() {
        t.status = task::Status::FAILED;
        t.output = Some(task::Any {
            type_url: "Error".to_string(),
            value: "Query is empty".as_bytes().to_vec(),
        });
        let message = serialize_into_vec(t).expect("failed to serialize task update");
        c.publish(job_id.clone(), message).await.expect("failed to publish task update");
        return;
    }
    let query = input.query.clone().unwrap();
    let query_clone = query.clone();
    let name_regexp = query.name_regexp;
    if name_regexp.is_none() {
        t.status = task::Status::FAILED;
        t.output = Some(task::Any {
            type_url: "Error".to_string(),
            value: "Name regexp is empty".as_bytes().to_vec(),
        });
        let message = serialize_into_vec(t).expect("failed to serialize task update");
        c.publish(job_id.clone(), message).await.expect("failed to publish task update");
        return;
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

    let task_folder = Path::new(&base_path).join(&claim.job_id);
    let scan_folder = Path::new(&task_folder).join("datasets");
    let input_folder = Path::new(&scan_folder).join(&suffix.clone());
    let output_folder = Path::new(&task_folder).join("refined").join("local").join(&suffix);
    fs::create_dir_all(&scan_folder).expect("Failed to create directory");
    fs::create_dir_all(&output_folder).expect("Failed to create directory");
    fs::create_dir_all(&input_folder).expect("Failed to create directory");

    c.publish(job_id.clone(), serialize_into_vec(t).expect("failed to serialize task update")).await.expect("failed to publish task update");

    let (tx, rx) = watch::channel(false);
    let mut c_clone = c.clone();
    let task_clone = t.clone();
    let job_id_clone = job_id.clone();
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
                // Check if we should stop
                Ok(_) = rx.changed() => {
                    break;
                }
            }
        }
    });

    let _cleanup = scopeguard::guard(tx, |tx| {
        let _ = tx.send(true); // Signal heartbeat task to stop
    });

    let mut downloader = datastore.consume("".to_string(), query_clone, false).await;
    let mut i = 0;
    loop {
        match downloader.next().await {
            Some(Ok(data)) => {
                let filename = match data.metadata.data_type.as_str() {
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
                };
                let path = input_folder.join(&filename);
                fs::write(path, &data
                    .content)
                    .expect("Failed to write data to file");
                i+=1;
                println!("downloaded {}", filename);
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
    println!("Finished downloading {} data for {}", i, claim.task_name);

    let params = vec![
        "main.py",
        "--mode", "local_refinement",
        "--job_root_path", task_folder.to_str().unwrap(),
        "--output", output_folder.to_str().unwrap(),
        "--domain_id", &claim.domain_id,
        "--job_id", &claim.job_id,
        "--scans", &suffix.clone(),
    ];
    let child = Command::new("python3")
    .args(params)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn();

    if let Err(e) = child {
        eprintln!("Failed to execute local refinement: {}", e);
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

    // Wait for the command to complete
    let status = child.wait();

    match status {
        Err(e) => {
            eprintln!("Failed to execute local refinement: {}", e);
            t.status = task::Status::FAILED;
            t.output = Some(task::Any {
                type_url: "Error".to_string(),
                value: serialize_into_vec(&task::Error {
                    message: e.to_string(),
                }).unwrap(),
            });
            let message = serialize_into_vec(t).expect("failed to serialize task update");
            c.publish(job_id.clone(), message).await.expect("failed to publish task update");
            return;
        }
        Ok(exit_status) => {
            if !exit_status.success() {
                eprintln!("Failed to execute local refinement: {}", exit_status.code().unwrap_or(-1));
                t.status = task::Status::FAILED;
                let message = serialize_into_vec(t).expect("failed to serialize task update");
                c.publish(job_id.clone(), message).await.expect("failed to publish task update");
                return;
            }
            println!("Finished executing {}", claim.task_name);
        }
    }

    let mut producer = datastore.produce(claim.domain_id.clone()).await;

    let zip_path = output_folder.join("sfm").join(suffix.clone() + ".zip");
    let mut zip = zip::ZipWriter::new(fs::File::create(&zip_path).expect("Failed to create zip file"));
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored); // No compression
    
    // open output folder/sfm, zip all txt, bin, csv files and upload
    for entry in fs::read_dir(output_folder.join("sfm")).expect("Failed to read directory") {
        let entry = entry.expect("Failed to read entry");
        let path = entry.path();
        let ext = path.extension().expect("Failed to get extension").to_str().expect("Failed to convert extension to string");
        if ext == "txt" || ext == "bin" || ext == "csv" {
            let str_path = path.to_str().expect("Failed to convert path to string");
            let file = fs::File::open(str_path).expect("Failed to open file");
            let mut reader = BufReader::new(file);
            zip.start_file(str_path, options).expect("Failed to start zip file");

            let mut buffer = [0u8; 8192]; // Use a buffer to stream in chunks
            loop {
                let bytes_read = reader.read(&mut buffer).expect("Failed to read file");
                if bytes_read == 0 {
                    break;
                }
                zip.write_all(&buffer[..bytes_read]).expect("Failed to write to zip");
            }
        }
    }
    zip.finish().expect("Failed to finish zip");
    let zip_file_metadata = fs::metadata(&zip_path).expect("Failed to get metadata");

    let res = producer.push(&Data {
        domain_id: claim.domain_id.clone(),
        metadata: Metadata {
            size: zip_file_metadata.len() as u32,
            name: format!("refined_scan_{}", suffix.clone()),
            data_type: "refined_scan_zip_v1".to_string(),
            id: None,
            properties: HashMap::new(),
        },
        content: fs::read(&zip_path).expect("Failed to read zip file"),
    }).await;
    if let Err(e) = res {
        eprintln!("Failed to upload refined scan: {}", e);
        t.status = task::Status::FAILED;
        t.output = Some(task::Any {
            type_url: "Error".to_string(),
            value: serialize_into_vec(&task::Error {
                message: e.to_string(),
            }).unwrap(),
        });
        let message = serialize_into_vec(t).expect("failed to serialize task update");
        c.publish(job_id.clone(), message).await.expect("failed to publish task update");
        return;
    }
    while !producer.is_completed().await {
        sleep(Duration::from_secs(3)).await;
    }
    producer.close().await;
    println!("Finished uploading refined scan for {}", suffix.clone());

    let output = LocalRefinementOutputV1 {
        result_ids: vec![res.unwrap()],
    };
    let event = task::Task {
        name: claim.task_name.clone(),
        receiver: claim.receiver.clone(),
        sender: claim.sender.clone(),
        endpoint: "/local-refinement/v1".to_string(),
        status: task::Status::DONE,
        access_token: "".to_string(),
        job_id: job_id.clone(),
        output: Some(task::Any {
            type_url: "LocalRefinementOutputV1".to_string(),
            value: serialize_into_vec(&output).expect("Failed to serialize local refinement output"),
        }),
    };
    let buf = serialize_into_vec(&event).expect("failed to serialize task update");
    c.publish(job_id.clone(), buf).await.expect("failed to publish task update");

    let _ = heartbeat_handle.await;
    println!("Heartbeat task stopped");
    return;
}
