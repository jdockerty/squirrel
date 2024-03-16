pub mod client;
mod engine;
mod error;
mod store;
pub mod thread_pool;
pub use engine::KvsEngine;
pub use error::Error as KvStoreError;
pub use store::KvStore;

pub const LOG_PREFIX: &str = "kvs";
pub const KEYDIR_NAME: &str = "kvs-keydir";
pub const ENGINE_FILE: &str = ".engine";

// Smaller sizes for forcing compaction in tests.
const MAX_LOG_FILE_SIZE: u64 = 1024 * 1024;

pub type Result<T> = std::result::Result<T, error::Error>;
