#[derive(Debug, thiserror::Error)]
pub enum Error {
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

    #[error("No active log file found")]
    NoActiveLogFile,

    #[error("No keydir set")]
    NoKeydir,

    #[error("Current engine is {current}, previously opened with '{previous}'")]
    IncorrectEngine { current: String, previous: String },

    #[error("Unable to serialize: {0}")]
    BincodeSerialization(#[from] bincode::Error),

    #[error("Unable to setup tracing: {0}")]
    TracingError(#[from] tracing::subscriber::SetGlobalDefaultError),
}
