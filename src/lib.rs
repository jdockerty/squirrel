use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
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
    log_files: VecDeque<LogFile>,

    /// The maximum size of a log file in bytes.
    max_log_file_size: u64,
    /// The maximum number of log files before older versions are deleted.
    max_num_log_files: u64,

    /// Index used for the log file.
    file_index: usize,
}

#[derive(Debug, Clone)]
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
    /// The file where the entry is stored.
    file_id: PathBuf,

    /// The offset of the entry in the log file.
    offset: u64,

    /// The size of the entry in bytes.
    size: u64,

    timestamp: i64,
}

impl KvStore {
    /// Create a new KvStore.
    ///
    /// The store is created in memory and is not persisted to disk.
    fn new(max_log_file_size: u64, max_num_log_files: u64) -> KvStore {
        KvStore {
            active_log_file: PathBuf::default(),
            log_location: PathBuf::default(),
            keydir_location: PathBuf::default(),
            keydir: HashMap::new(),
            log_files: VecDeque::new(),
            max_log_file_size, // TODO: increase
            max_num_log_files,
            file_index: 0,
        }
    }

    /// Open a KvStore at the given path.
    pub fn open<P>(path: P) -> Result<KvStore>
    where
        P: Into<PathBuf>,
    {
        let mut store = KvStore::new(1024, 3);

        let path = path.into();
        let keydir_path = path.join(KEYDIR_NAME);

        store.log_location = path.clone();
        store.keydir_location = keydir_path;

        store.initial_log_file()?;

        Ok(store)
    }

    fn initial_log_file(&mut self) -> Result<std::fs::File> {
        let init_name = format!("{}0", self.log_location.join(LOG_PREFIX).display());
        // We do not use File::create here as it would be called on every time open is
        // used, which happens in the CLI and would cause the file to be truncated each
        // time.
        let log_file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .open(&init_name)
            .unwrap();
        let active_file = LogFile {
            active: true,
            name: init_name.clone(),
        };

        // Active file is always at the back of the queue.
        self.log_files.push_back(active_file);
        self.active_log_file = PathBuf::from(init_name.clone());
        Ok(log_file)
    }

    /// Set the current active file as inactive.
    ///
    /// This is used when a new file is created, the old file is marked as inactive
    /// and can be compacted at a later time.
    fn set_current_file_inactive(&mut self) {
        let (mut current_active_file, idx) = self.get_active_file();
        current_active_file.active = false;
        self.log_files[idx] = current_active_file;
    }

    fn create_log_file(&mut self) -> Result<std::fs::File> {
        self.set_current_file_inactive();

        let next_log_file_name = self.next_log_file_name();
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&next_log_file_name)
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: next_log_file_name.clone(),
            })?;
        self.active_log_file = PathBuf::from(next_log_file_name.clone());

        let new_file = LogFile {
            active: true,
            name: next_log_file_name,
        };

        // Active file is always at the back of the queue.
        self.log_files.push_back(new_file);

        if self.log_files.len() > self.max_num_log_files as usize {
            self.compact()?;
        }
        Ok(log_file)
    }

    fn next_log_file_name(&mut self) -> String {
        self.increment_file_index();
        format!(
            "{}{}",
            self.log_location.join(LOG_PREFIX).display(),
            self.file_index
        )
    }

    fn increment_file_index(&mut self) {
        self.file_index += 1;
    }

    /// Retrieve the active file and its index, since this is always at the back of the
    /// queue we know it is the last index.
    fn get_active_file(&self) -> (LogFile, usize) {
        (
            self.log_files
                .back()
                .expect("No active file found")
                .to_owned(),
            self.log_files.len() - 1,
        )
    }

    fn open_active_log_file(&mut self) -> Result<std::fs::File> {
        if std::path::Path::exists(self.active_log_file.as_path()) {
            // Open the file if it exists.
            Ok(std::fs::OpenOptions::new()
                .read(true)
                .create(true)
                .append(true)
                .open(&self.active_log_file.as_path())
                .map_err(|e| KvStoreError::IoError {
                    source: e,
                    filename: self.active_log_file.as_path().to_string_lossy().to_string(),
                })?)
        } else {
            // File needs to be created.
            Ok(self.create_log_file()?)
        }
    }

    /// Perform compaction on the inactive log files.
    fn compact(&mut self) -> Result<()> {
        println!("Compacting");

        while self.log_files.len() > self.max_num_log_files as usize {
            let file = self
                .log_files
                .pop_front()
                .expect("Compaction requires some files to occur");
            std::fs::remove_file(&file.name).map_err(|e| KvStoreError::IoError {
                source: e,
                filename: file.name.clone(),
            })?;
        }

        Ok(())
    }

    fn append_to_log(&mut self, entry: &LogEntry) -> Result<u64> {
        let mut log_file = self.open_active_log_file()?;

        // The offset is where the last entry in the log file ends, this is because
        // the log file is an append-only log.
        let mut offset = log_file
            .metadata()
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: self.active_log_file.as_path().to_string_lossy().to_string(),
            })?
            .len();

        // Create a new log file when our file size is too large.
        if offset > self.max_log_file_size {
            log_file = self.create_log_file()?;
            offset = 0;
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
        let active_file = &self.active_log_file;
        let entry = KeydirEntry {
            file_id: active_file.to_path_buf(),
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
                let log_file = self.open_active_log_file()?;

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
