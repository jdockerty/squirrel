use serde::{Deserialize, Serialize};
use std::io::{prelude::*, SeekFrom};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};
use thiserror::Error;

pub const LOG_PREFIX: &str = "kvs.log";
pub const KEYDIR_NAME: &str = "kvs-keydir";

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

    #[error("Unable to serialize entry: {0}")]
    SerializeError(#[from] serde_json::Error),

    #[error("Unable to serialize keydir: {0}")]
    BincodeSerialization(#[from] bincode::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Operation {
    Set,
    Get,
    Remove,
}

/// An in-memory key-value store, backed by a [`HashMap`] from the standard library.
#[derive(Debug)]
pub struct KvStore {
    pub log_location: PathBuf,

    /// Keydir maps key entries in the log to their offset in the log file.
    /// The byte offset is used to seek to the correct position in the log file.
    keydir_location: PathBuf,

    /// Keydir is an in-memory hashmap of keys to their respective log file, which
    /// contains the offset to the entry in the file.
    keydir: HashMap<String, KeydirEntry>,

    active_log_file: PathBuf,

    /// Multiple log files are used to store the entries in the store.
    ///
    /// When a file reaches a certain size, defined by [`max_log_file_size`], a new file is
    /// created.
    log_files: Vec<LogFile>,

    /// The maximum size of a log file in bytes.
    max_log_file_size: u64,

    file_index: usize,
}

#[derive(Debug)]
struct LogFile {
    /// Whether the file is active or not.
    ///
    /// Once a file is inactive, it will never become active again.
    ///
    /// Only inactive files are marked for compaction/merging.
    active: bool,

    /// Name of the file, the full name can be constructed by appending this to the
    /// [`log location`].
    name: String,
}

/// A ['LogEntry'] is a single line entry in the log file.
/// Multiple entries can be used to rebuild the state of the store.
#[derive(Debug, Serialize, Deserialize)]
struct LogEntry {
    /// The operation that was performed.
    operation: Operation,

    timestamp: i64,

    key_size: usize,
    value_size: usize,
    key: String,
    value: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct KeydirEntry {
    /// The offset of the entry in the log file.
    offset: u64,

    /// The size of the entry in bytes.
    size: u64,

    timestamp: i64,
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new(1024)
    }
}

impl KvStore {
    /// Create a new KvStore.
    ///
    /// The store is created in memory and is not persisted to disk.
    fn new(max_log_file_size: u64) -> KvStore {
        KvStore {
            active_log_file: PathBuf::default(),
            log_location: PathBuf::default(),
            keydir_location: PathBuf::default(),
            keydir: HashMap::new(),
            log_files: Vec::new(),
            max_log_file_size, // TODO: increase
            file_index: 0,
        }
    }

    /// Open a KvStore at the given path.
    pub fn open<P>(path: P) -> Result<KvStore>
    where
        P: Into<PathBuf>,
    {
        let mut store = KvStore::new(200);

        let path = path.into();
        let mut keydir_path = PathBuf::default();

        if path.is_dir() {
            keydir_path = path.join(KEYDIR_NAME);
        }

        store.log_location = path.clone();
        store.keydir_location = keydir_path;

        // TODO: the create_new_log_file logic is the same here, but this is used
        // for initialisation.
        // Should we change the create_new_log_file signature to accept a KvStore
        // instead of self.
        let full_path = format!(
            "{}{}",
            store.log_location.join(LOG_PREFIX).display(),
            store.file_index
        );

        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(PathBuf::from(full_path.clone()))
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: full_path.clone(),
            })?;

        store.log_files.push(LogFile {
            active: true,
            name: full_path.clone(),
        });

        store.active_log_file = full_path.clone().into();

        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(store.keydir_location.as_path())
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: store.keydir_location.display().to_string(),
            })?;
        Ok(store)
    }

    fn create_new_log_file(&mut self) -> Result<()> {
        let full_path = format!(
            "{}{}",
            self.log_location.join(LOG_PREFIX).display(),
            self.file_index
        );
        let current_file = &mut self.log_files[self.file_index];
        self.file_index += 1;
        current_file.active = false;

        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(full_path.clone())
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: full_path.clone(),
            })?;

        self.active_log_file = PathBuf::from(full_path.clone());

        self.log_files.push(LogFile {
            active: true,
            name: full_path,
        });

        Ok(())
    }

    fn append_to_log(&mut self, entry: &LogEntry) -> Result<u64> {
        let mut log_file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(self.active_log_file.as_path())
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: self.active_log_file.as_path().to_string_lossy().to_string(),
            })?;

        // The offset is where the last entry in the log file ends, this is because
        // the log file is an append-only log.
        let offset = log_file
            .metadata()
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: self.active_log_file.as_path().to_string_lossy().to_string(),
            })?
            .len();

        if offset > self.max_log_file_size {
            println!("Splitting file");
            self.create_new_log_file()?;
        }

        // TODO: JSON likely isn't the best format for this, but it's easy to work with for now.
        // Investigate another serialisation format in the future (bincode?).
        serde_json::to_writer(&mut log_file, &entry).map_err(KvStoreError::SerializeError)?;
        writeln!(log_file).map_err(|e| KvStoreError::IoError {
            source: e,
            filename: self.active_log_file.as_path().to_string_lossy().to_string(),
        })?;

        // Returning the offset of the entry in the log file after it has been written.
        // This means that the next entry is written after this one.
        Ok(offset)
    }

    fn load_keydir(&mut self) -> Result<HashMap<String, KeydirEntry>> {
        let keydir_location = self.keydir_location.display().to_string();
        let keydir_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true) // To enable creation in the case where the file doesn't exist.
            .create(true)
            .open(self.keydir_location.as_path())
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: keydir_location.clone(),
            })?;

        let keydir_file_size = keydir_file
            .metadata()
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: keydir_location.clone(),
            })?
            .len();

        if keydir_file_size == 0 {
            return Ok(HashMap::new());
        }

        let keydir: HashMap<String, KeydirEntry> =
            bincode::deserialize_from(keydir_file).map_err(KvStoreError::BincodeSerialization)?;
        self.keydir = keydir.clone();
        Ok(keydir)
    }

    fn write_keydir(&mut self, key: String, offset: u64) -> Result<()> {
        let keydir_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(self.keydir_location.as_path())
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: self.keydir_location.display().to_string(),
            })?;

        let mut keydir = self.load_keydir()?;
        let entry = KeydirEntry {
            offset,
            size: 0, // TODO: calculate size
            timestamp: chrono::Utc::now().timestamp(),
        };
        keydir.insert(key, entry);

        bincode::serialize_into(keydir_file, &keydir)
            .map_err(KvStoreError::BincodeSerialization)?;
        Ok(())
    }

    /// Set the value of a key by inserting the value into the store for the given key.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let entry = LogEntry {
            timestamp: chrono::Utc::now().timestamp(),
            operation: Operation::Set,
            key_size: key.len(),
            key,
            value_size: value.len(),
            value: Some(value),
        };
        println!("{}", self.active_log_file.clone().display());

        let offset = self.append_to_log(&entry)?;
        self.write_keydir(entry.key.clone(), offset)?;
        Ok(())
    }

    /// Retrieve the value of a key from the store.
    /// If the key does not exist, then [`None`] is returned.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let keydir = self.load_keydir()?;
        match keydir.get(&key) {
            Some(entry) => {
                let log_file = std::fs::OpenOptions::new()
                    .read(true)
                    .open(self.active_log_file.as_path())
                    .map_err(|e| KvStoreError::IoError {
                        source: e,
                        filename: self.active_log_file.as_path().to_string_lossy().to_string(),
                    })?;

                let log_file_size = log_file
                    .metadata()
                    .map_err(|e| KvStoreError::IoError {
                        source: e,
                        filename: self.active_log_file.as_path().to_string_lossy().to_string(),
                    })?
                    .len();

                if log_file_size == 0 {
                    // Edge case for the log file being empty, there is no value to return.
                    return Ok(None);
                }

                let mut buf = BufReader::new(log_file);

                // Seek to the position provided by the keydir and serialize the entry.
                let mut v = Vec::new();
                buf.seek(SeekFrom::Start(entry.offset))
                    .map_err(|e| KvStoreError::IoError {
                        source: e,
                        filename: self.active_log_file.as_path().to_string_lossy().to_string(),
                    })?;
                buf.read_until(b'\n', &mut v)
                    .map_err(|e| KvStoreError::IoError {
                        source: e,
                        filename: self.active_log_file.as_path().to_string_lossy().to_string(),
                    })?;
                let entry: LogEntry = serde_json::from_slice(&v)?;
                match entry.value {
                    Some(value) => Ok(Some(value)),
                    // TODO: this is a tombstone value.
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    /// Remove a key from the store.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let keydir = self.load_keydir()?;

        match keydir.get(&key) {
            Some(_offset) => {
                let entry = LogEntry {
                    timestamp: chrono::Utc::now().timestamp(),
                    operation: Operation::Remove,
                    key_size: key.len(),
                    key,
                    value: None,
                    value_size: 0,
                };
                let offset = self.append_to_log(&entry)?;
                self.write_keydir(entry.key.clone(), offset)?;
                Ok(())
            }
            None => Err(KvStoreError::RemoveOperationWithNoKey),
        }
    }
}
