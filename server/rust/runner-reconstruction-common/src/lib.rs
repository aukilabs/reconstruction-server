//! Code the reconstruction runners (local, global, update) share: starting the Python pipeline,
//! the job workspace on disk, and uploading the global and update outputs.

pub mod output;
pub mod python;
pub mod strategy;
pub mod workspace;
