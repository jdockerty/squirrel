use bson::Bson;
use serde::{Deserialize, Serialize};
use std::{io::Write, path::PathBuf};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, KvStoreError>;

#[derive(Debug, Error)]
pub enum KvStoreError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Operation {
    Set,
    Get,
    Remove,
}

/// An in-memory key-value store, backed by a [`HashMap`] from the standard library.
pub struct KvStore {
    pub log_location: PathBuf,
}

/// A ['LogEntry'] is a single line entry in the log file.
/// It can be used to rebuild the state of the store.
#[derive(Debug, Serialize, Deserialize)]
struct LogEntry {
    /// The operation that was performed.
    operation: Operation,
    key: String,
    value: String,

    /// Byte offset of the entry in the log file, this is used for lookups in
    /// the future.
    offset: u64,
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            log_location: PathBuf::from(""),
        }
    }

    pub fn open<P>(path: P) -> Result<KvStore>
    where
        P: Into<PathBuf>,
    {
        let mut store = KvStore::new();
        store.log_location = path.into();
        Ok(store)
    }

    /// Set the value of a key by inserting the value into the store for the given key.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let mut log_file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(self.log_location.as_path())?;

        let current_offset = log_file.metadata()?.len();

        let entry = LogEntry {
            operation: Operation::Set,
            key,
            value,
            offset: current_offset,
        };

        let d = bson::to_document(&entry).unwrap();
        d.to_writer(&mut log_file).unwrap();

        Ok(())
    }

    /// Retrieve the value of a key from the store.
    /// If the key does not exist, then [`None`] is returned.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(None)
    }

    /// Remove a key from the store.
    pub fn remove(&mut self, key: String) -> Result<()> {
        Ok(())
    }
}
