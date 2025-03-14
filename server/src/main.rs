use domain::{cluster::DomainCluster, datastore::{common::Datastore, remote::{self, RemoteDatastore}}, message::read_prefix_size_message, protobuf::{domain_data::Query,task::{self, LocalRefinementInputV1, LocalRefinementOutputV1}}};
use jsonwebtoken::{decode, DecodingKey,Validation, Algorithm};
use libp2p::Stream;
use networking::{client::Client, libp2p::Networking};
use quick_protobuf::{deserialize_from_slice, serialize_into_vec};
use tokio::{self, select, signal::unix::{signal, SignalKind}, time::{sleep, Duration}};
use futures::{AsyncReadExt, StreamExt};
use uuid::Uuid;
use regex::Regex;
mod local_refinement;
mod global_refinement;
mod utils;

async fn shutdown_signal() {
    let mut term_signal = signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
    let mut int_signal = signal(SignalKind::interrupt()).expect("Failed to register SIGINT handler");

    tokio::select! {
        _ = term_signal.recv() => println!("Received SIGTERM, exiting..."),
        _ = int_signal.recv() => println!("Received SIGINT, exiting..."),
    }
}
/*
    * This is a simple example of a reconstruction node. It will connect to a set of bootstraps and execute reconstruction jobs.
    * Usage: cargo run <port> <name> <domain_manager> 
    * Example: cargo run 18808 reconstruction /ip4/127.0.0.1/udp/18800/quic-v1/p2p/12D3KooWDHaDQeuYeLM8b5zhNjqS7Pkh7KefqzCpDGpdwj5iE8pq
 */
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        println!("Usage: {} <port> <name> <domain_manager>", args[0]);
        return Ok(());
    }
    let port = args[1].parse::<u16>().unwrap();
    let name = args[2].clone();
    let base_path = format!("./volume/{}", name);
    let domain_manager = args[3].clone();
    let private_key_path = format!("{}/pkey", base_path);

    let domain_manager_id = domain_manager.split("/").last().unwrap().to_string();
    let domain_cluster = DomainCluster::new(domain_manager.clone(), name, false, None, Some(private_key_path));
    let mut n = domain_cluster.peer.clone();
    let mut local_refinement_v1_handler = n.client.set_stream_handler("/local-refinement/v1".to_string()).await.unwrap();
    let mut global_refinement_v1_handler = n.client.set_stream_handler("/global-refinement/v1".to_string()).await.unwrap();
    let remote_storage = RemoteDatastore::new(domain_cluster);

    loop {
        select! {
            Some((_, stream)) = local_refinement_v1_handler.next() => {
                let _ = tokio::spawn(local_refinement::v1(base_path.clone(), stream, Box::new(remote_storage.clone()), n.client.clone()));
            }
            Some((_, stream)) = global_refinement_v1_handler.next() => {
                let _ = tokio::spawn(global_refinement::v1(base_path.clone(), stream, Box::new(remote_storage.clone()), n.client.clone()));
            }
            _ = shutdown_signal() => {
                println!("Received termination signal, shutting down...");
                break;
            }
            else => break
        }
    }

    Ok(())
}
