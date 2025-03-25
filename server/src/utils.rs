use domain::{cluster::DomainCluster, datastore::{common::Datastore, remote::RemoteDatastore}, message::read_prefix_size_message, protobuf::{domain_data::Query,task::{self, LocalRefinementInputV1, LocalRefinementOutputV1}}};
use jsonwebtoken::{decode, DecodingKey,Validation, Algorithm};
use libp2p::Stream;
use networking::{client::Client, libp2p::Networking};
use quick_protobuf::{deserialize_from_slice, serialize_into_vec};
use tokio::{self, select, time::{sleep, Duration}};
use futures::{AsyncReadExt, StreamExt};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use std::fs::{self, DirEntry};
use std::path::Path;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::error::Error;

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskTokenClaim {
    pub domain_id: String,
    pub task_name: String,
    pub job_id: String,
    pub sender: String,
    pub receiver: String,
    pub exp: usize,
    pub iat: usize,
    pub sub: String,
}

pub fn decode_jwt(token: &str) -> Result<TaskTokenClaim, Box<dyn std::error::Error + Send + Sync>> {
    let token_data = decode::<TaskTokenClaim>(token, &DecodingKey::from_secret("secret".as_ref()), &Validation::new(Algorithm::HS256))?;
    Ok(token_data.claims)
}

pub async fn handshake(stream: &mut Stream) -> Result<TaskTokenClaim, Box<dyn std::error::Error + Send + Sync>> {
    let mut length_buf = [0u8; 4];
    stream.read_exact(&mut length_buf).await?;

    let length = u32::from_be_bytes(length_buf) as usize;
    let mut buffer = vec![0u8; length];
    stream.read_exact(&mut buffer).await?;
        
    let header = deserialize_from_slice::<task::DomainClusterHandshake>(&buffer)?;

    decode_jwt(header.access_token.as_str())
}

pub fn write_scan_data_summary(
    scan_folder:&Path,
    summary_json_path: &Path,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut scan_count = 0;
    let mut total_frame_count = 0;
    let mut total_scan_duration = 0.0;
    let mut scan_durations = Vec::new();
    let mut unique_portal_ids = HashSet::new();
    let mut portal_sizes = Vec::new();
    let mut devices_used = HashSet::new();
    let mut app_versions_used = HashSet::new();
    
    let manifest_path = scan_folder.join("Manifest.json");
    if !manifest_path.exists() {
        return Err("Manifest.json not found".into());
    }

    let manifest_data = fs::read_to_string(&manifest_path)?;
    let manifest: Value = serde_json::from_str(&manifest_data)?;

    scan_count += 1;

    let frame_count = manifest["frameCount"].as_f64().unwrap_or(0.0) as i32;
    let duration = manifest["duration"].as_f64().unwrap_or(0.0);
    total_frame_count += frame_count;
    total_scan_duration += duration;
    scan_durations.push(duration);

    if let Some(portals) = manifest.get("portals").and_then(|p| p.as_array()) {
        for portal in portals {
            if let Some(portal_map) = portal.as_object() {
                if let Some(portal_id) = portal_map.get("shortId").and_then(|id| id.as_str()) {
                    if unique_portal_ids.insert(portal_id.to_string()) {
                        if let Some(size) = portal_map.get("physicalSize").and_then(|s| s.as_f64()) {
                            portal_sizes.push(size);
                        }
                    }
                }
            }
        }
    }

    let device = if let (Some(brand), Some(model), Some(system_name), Some(system_version)) = (
        manifest.get("brand").and_then(|b| b.as_str()),
        manifest.get("model").and_then(|m| m.as_str()),
        manifest.get("systemName").and_then(|s| s.as_str()),
        manifest.get("systemVersion").and_then(|v| v.as_str()),
    ) {
        format!("{} {} {} {}", brand, model, system_name, system_version)
    } else {
        "unknown".to_string()
    };
    devices_used.insert(device);

    let app_version = if let (Some(version), Some(build_id)) = (
        manifest.get("appVersion").and_then(|v| v.as_str()),
        manifest.get("buildId").and_then(|b| b.as_str()),
    ) {
        format!("{} (build {})", version, build_id)
    } else {
        "unknown".to_string()
    };
    app_versions_used.insert(app_version);
    

    scan_durations.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let shortest_scan_duration = scan_durations.first().unwrap_or(&0.0);
    let longest_scan_duration = scan_durations.last().unwrap_or(&0.0);
    let median_scan_duration = scan_durations.get(scan_durations.len() / 2).unwrap_or(&0.0);

    let average_scan_duration = total_scan_duration / scan_count as f64;
    let average_scan_frame_count = total_frame_count as f64 / scan_count as f64;
    let average_scan_frame_rate = total_frame_count as f64 / total_scan_duration;

    let summary = json!({
        "scanCount": scan_count,
        "totalFrameCount": total_frame_count,
        "totalScanDuration": total_scan_duration,
        "averageScanDuration": average_scan_duration,
        "averageScanFrameCount": average_scan_frame_count,
        "averageFrameRate": average_scan_frame_rate,
        "shortestScanDuration": shortest_scan_duration,
        "longestScanDuration": longest_scan_duration,
        "medianScanDuration": median_scan_duration,
        "portalCount": unique_portal_ids.len(),
        "portalIDs": unique_portal_ids,
        "portalSizes": portal_sizes,
        "deviceVersionsUsed": devices_used,
        "appVersionsUsed": app_versions_used,
    });

    fs::write(
        summary_json_path,
        serde_json::to_string_pretty(&summary)?,
    )?;

    Ok(())
} 
