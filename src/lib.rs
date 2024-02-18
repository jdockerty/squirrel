pub mod client;
mod engine;
mod error;
mod store;
pub use engine::KvsEngine;
pub use error::Error as KvStoreError;
pub use store::KvStore;

pub const LOG_PREFIX: &str = "kvs.log";
pub const KEYDIR_NAME: &str = "kvs-keydir";
pub const ENGINE_FILE: &str = ".engine";

// Smaller sizes for forcing compaction in tests.
const MAX_LOG_FILE_SIZE: u64 = 1024; // 1 MiB
const MAX_NUM_LOG_FILES: usize = 5;

pub type Result<T> = std::result::Result<T, error::Error>;
