use std::{collections::HashMap, sync::{atomic::{AtomicI32, Ordering}, Arc}, time::Duration};

use async_trait::async_trait;
use posemesh_domain::{auth::{AuthClient, AuthError}, capabilities::public_key::PublicKeyStorage, cluster::DomainCluster, datastore::remote::RemoteDatastore, protobuf::discovery::Capability};
use posemesh_networking::client::Client;
use tokio::{self, select, signal::unix::{signal, SignalKind}};
use futures::{lock::Mutex, StreamExt};
use tracing_subscriber::{fmt, prelude::__tracing_subscriber_SubscriberExt, EnvFilter, Registry};
mod local_refinement;
mod global_refinement;
mod utils;

async fn shutdown_signal() {
    let mut term_signal = signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
    let mut int_signal = signal(SignalKind::interrupt()).expect("Failed to register SIGINT handler");

    tokio::select! {
        _ = term_signal.recv() => tracing::info!("Received SIGTERM, exiting..."),
        _ = int_signal.recv() => tracing::info!("Received SIGINT, exiting..."),
    }
}

#[derive(Clone)]
struct PublicKeyLoader {
    auth_clients: Arc<Mutex<HashMap<String, AuthClient>>>,
    client: Client,
    cache_ttl: Duration,
    domain_manager_id: String
}

#[async_trait]
impl PublicKeyStorage for PublicKeyLoader {
    async fn get_by_domain_id(&self, domain_id: String) -> Result<Vec<u8>, AuthError> {
        let auth_clients = self.auth_clients.lock().await;
        if let Some(client) = auth_clients.get(&domain_id) {
            Ok(client.public_key().await)
        } else {
            drop(auth_clients);
            let auth_client = AuthClient::initialize(self.client.clone(), &self.domain_manager_id.clone(), self.cache_ttl, &domain_id).await?;
            let public_key = auth_client.public_key().await;
            let mut auth_clients = self.auth_clients.lock().await;
            auth_clients.insert(domain_id.clone(), auth_client);
            Ok(public_key)
        }
    }
}

impl PublicKeyLoader {
    async fn cleanup(&self) {
        let mut auth_clients = self.auth_clients.lock().await;
        auth_clients.clear();
    }
}

/*
    * This is a simple example of a reconstruction node. It will connect to a set of bootstraps and execute reconstruction jobs.
    * Usage: cargo run <port> <name> <domain_manager_addr> 
    * Example: cargo run 18808 reconstruction /ip4/127.0.0.1/udp/18800/quic-v1/p2p/12D3KooWDHaDQeuYeLM8b5zhNjqS7Pkh7KefqzCpDGpdwj5iE8pq
 */
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        println!("Usage: {} <port> <name> <domain_manager_addr> <initial_slot>", args[0]);
        return Ok(());
    }
    let port = args[1].parse::<u16>().unwrap();
    let name = args[2].clone();
    let base_path = format!("./volume/{}", name);
    let domain_manager = args[3].clone();
    let private_key_path = format!("{}/pkey", base_path);
    let mut slots: i32 = 1;
    if args.len() == 5 {
        slots = args[4].parse::<i32>().unwrap();
    }
    let left_slots: Arc<AtomicI32> = Arc::new(AtomicI32::new(slots));
    let subscriber = Registry::default()
            .with(fmt::layer().with_file(true).with_line_number(true))
            .with(EnvFilter::try_new(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string())).unwrap_or_else(|_| EnvFilter::new("info")));
    tracing::subscriber::set_global_default(subscriber).expect("failed to set subscriber");


    let local_refinement_capability = Capability {
        endpoint: "/local-refinement/v1".to_string(),
        capacity: slots,
    };
    let global_refinement_capability = Capability {
        endpoint: "/global-refinement/v1".to_string(),
        capacity: -1, // unlimited capacity
    };
    let mut domain_cluster = DomainCluster::join(&domain_manager, &name, false, port, false, false, None, Some(private_key_path), vec![domain_manager.clone()]).await?;
    let domain_manager_id = domain_cluster.manager_id.clone();
    let n = domain_cluster.peer.clone();
    let mut handlers = domain_cluster.with_capabilities(&vec![local_refinement_capability, global_refinement_capability]).await?;
    let mut local_refinement_v1_handler = handlers.remove(0);
    let mut global_refinement_v1_handler = handlers.remove(0);
    let remote_storage = RemoteDatastore::new(domain_cluster);
    let keys_loader = PublicKeyLoader {
        auth_clients: Arc::new(Mutex::new(HashMap::new())),
        client: n.client.clone(),
        cache_ttl: Duration::from_secs(60*60*24),
        domain_manager_id,
    };

    loop {
        let mut c = n.client.clone();
        select! {
            Some((_, stream)) = local_refinement_v1_handler.next() => {
                let base_path = base_path.clone();
                let remote_storage = remote_storage.clone();
                let c = c.clone();
                let keys_loader = keys_loader.clone();
                let left_slots = left_slots.clone();
                tokio::spawn(async move {
                    if let Err(e) = local_refinement::v1(left_slots, base_path, stream, remote_storage, c, keys_loader).await {
                        tracing::error!("Local refinement error: {}", e);
                    }
                });
            }
            Some((_, stream)) = global_refinement_v1_handler.next() => {
                let base_path = base_path.clone();
                let remote_storage = remote_storage.clone();
                let keys_loader = keys_loader.clone();
                let c = c.clone();
                tokio::spawn(async move {
                    if let Err(e) = global_refinement::v1(base_path, stream, remote_storage, c, keys_loader).await {
                        tracing::error!("Global Refinement Error: {}", e);
                    }
                });
            }
            _ = shutdown_signal() => {
                c.cancel().await.unwrap_or_else(|e| {
                    tracing::error!("Failed to cancel client: {}", e);
                });
                keys_loader.cleanup().await;
                tracing::info!("Received termination signal, shutting down...");
                break;
            }
            else => break
        }
    }

    Ok(())
}
