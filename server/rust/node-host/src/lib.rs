//! Compute-node host on the Auki SDK task runtime.
//!
//! The SDK owns machine registration, authentication, DMS leases and
//! heartbeats. This crate keeps the wire behaviour of the former
//! `posemesh-compute-node` 0.3.2 host: Domain input layout, artifact upserts
//! and the `{job, artifacts}` completion/failure receipts.

pub mod config;
pub mod host;
pub mod io;
pub mod process;
pub mod router;
pub mod telemetry;

pub use auki_sdk;
pub use auki_sdk::{TaskContext, TaskSpec};
pub use config::HostConfig;
pub use host::{run, run_with_shutdown};
pub use io::{ArtifactContent, ArtifactRequest, Materialized, StorageError, TaskIo};
pub use router::{NodeRunner, Router};
