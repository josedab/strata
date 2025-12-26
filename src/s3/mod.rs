//! S3-compatible gateway for Strata.
//!
//! This module provides an S3-compatible API for accessing Strata storage,
//! allowing cloud-native applications to use Strata as an S3 backend.

mod gateway;
mod handlers;

pub use gateway::run_s3_gateway;
