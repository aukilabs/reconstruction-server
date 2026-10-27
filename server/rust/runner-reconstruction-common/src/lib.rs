//! Code the reconstruction runners share: starting the Python pipeline, the job workspace on
//! disk and its metadata, and uploading the task log and outputs.

pub mod job;
pub mod output;
pub mod python;
pub mod refined;
pub mod strategy;
pub mod workspace;
