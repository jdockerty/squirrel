use thiserror::Error;
mod engine;
mod store;
pub use engine::KvsEngine;
pub use store::KvStore;

pub const LOG_PREFIX: &str = "kvs.log";
pub const KEYDIR_NAME: &str = "kvs-keydir";

// Smaller sizes for forcing compaction in tests.
const MAX_LOG_FILE_SIZE: u64 = 1024; // 1 MiB
const MAX_NUM_LOG_FILES: usize = 5;

pub type Result<T> = std::result::Result<T, KvStoreError>;

#[derive(Debug, Error)]
pub enum KvStoreError {
    #[error("I/O error on file {filename}: {source}")]
    IoError {
        source: std::io::Error,
        filename: String,
    },

    #[error("Get operation was stored in the log")]
    GetOperationInLog,

    #[error("Cannot remove non-existent key")]
    RemoveOperationWithNoKey,

    #[error("Tried compacting the active file")]
    ActiveFileCompaction,

    #[error("Unable to serialize: {0}")]
    BincodeSerialization(#[from] bincode::Error),

    #[error("Unable to setup tracing: {0}")]
    TracingError(#[from] tracing::subscriber::SetGlobalDefaultError),
}
