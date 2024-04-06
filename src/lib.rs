//! A key-value store, inspired by [Bitcask](https://en.wikipedia.org/wiki/Bitcask).
//!
//! This crate provides a simple key-value store that persists data to disk by
//! utilising a write-ahead log (WAL) and an in-memory hash map which maintains
//! an index of keys to their positions in the log or compacted log files.

/// A thin TCP client to interact with the key-value store server.
pub mod client;
/// Generic trait implementation for pluggable engines.
///
/// # Note
///
/// [`KvsEngine`] is the only current implementation of this trait.
mod engine;
/// Errors that may originate from operating the store.
mod error;
pub mod raft;
/// Implementation of the key-value store.
mod store;
pub use engine::KvsEngine;
pub use error::Error as KvStoreError;
pub use store::{Cluster, KvStore};

pub mod proto {
    tonic::include_proto!("sqrlraft");
}


/// Prefix for log files.
pub const LOG_PREFIX: &str = "sqrl-";
/// Name of the engine in use.
pub const ENGINE_FILE: &str = ".engine";

// The maximum size of a log file before it should be compacted.
//
// NOTE: For tests, a local value should be tests for speed. The recommended
// value is 1024.
thread_local! {
static MAX_LOG_FILE_SIZE: u64 = std::env::var("KVS_MAX_LOG_FILE_SIZE")
        .map(|s| s.parse().expect("KVS_MAX_LOG_FILE_SIZE must be a number"))
        .unwrap_or(1024 * 1024)
}

pub type Result<T> = anyhow::Result<T, KvStoreError>;
