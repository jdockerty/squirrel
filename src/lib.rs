pub mod client;
mod engine;
mod error;
mod store;
pub use engine::KvsEngine;
pub use error::Error as KvStoreError;
pub use store::KvStore;

pub const LOG_PREFIX: &str = "kvs";
pub const KEYDIR_NAME: &str = "kvs-keydir";
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
