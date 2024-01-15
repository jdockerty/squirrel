use glob::glob;
use serde::{Deserialize, Serialize};
use std::io::{prelude::*, SeekFrom};
use std::{
    collections::BTreeMap,
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

    #[error("Tried compacting the active file")]
    ActiveFileCompaction,

    #[error("Unable to serialize: {0}")]
    BincodeSerialization(#[from] bincode::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Operation {
    Set,
    Get,
    Remove,
}

/// An in-memory key-value store, backed by a [`BTreeMap`] from the standard library.
#[derive(Debug)]
pub struct KvStore {
    pub log_location: PathBuf,

    /// Keydir maps key entries in the log to their offset in the log file.
    /// The byte offset is used to seek to the correct position in the log file.
    keydir_location: PathBuf,

    active_log_file: PathBuf,

    /// Keydir is an in-memory hashmap of keys to their respective log file, which
    /// contains the offset to the entry in the file.
    ///
    /// This represents L1 data and it committed to disk.
    keydir: BTreeMap<String, KeydirEntry>,

    /// The maximum size of a log file in bytes.
    max_log_file_size: u64,
    /// The maximum number of log files before older versions are deleted.
    max_num_log_files: usize,

    /// Index used for the log file.
    file_index: usize,
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
    size: usize,

    timestamp: i64,
}

impl KvStore {
    /// Create a new KvStore.
    ///
    /// The store is created in memory and is not persisted to disk.
    fn new(max_log_file_size: u64, max_num_log_files: usize) -> KvStore {
        KvStore {
            active_log_file: PathBuf::default(),
            log_location: PathBuf::default(),
            keydir_location: PathBuf::default(),
            keydir: BTreeMap::new(),
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
        let mut store = KvStore::new(48000, 3);

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

        self.active_log_file = PathBuf::from(init_name.clone());
        Ok(log_file)
    }

    fn create_log_file(&mut self) -> Result<std::fs::File> {
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

        let no_compact = |p: &PathBuf| {
            !p.to_string_lossy().contains("compacted") || !p.to_string_lossy().contains("keydir")
        };

        let inactive_files = glob(&format!("{}/kvs.log*", self.log_location.display()))
            .unwrap()
            .map(|p| p.unwrap())
            .filter(no_compact)
            .collect::<Vec<_>>();
        if inactive_files.len() > self.max_num_log_files {
            self.compact(inactive_files)?;
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
    fn compact(&mut self, inactive_files: Vec<PathBuf>) -> Result<()> {
        println!("Inactive files: {:?}", inactive_files);
        if inactive_files.len() == 0 {
            return Ok(());
        }

        let compacted_filename = format!("{}-compacted", self.active_log_file.display());
        println!("Compacted filename: {:?}", compacted_filename);

        let mut compaction_file = std::fs::File::create(&compacted_filename).unwrap();
        for log_file in inactive_files {
            println!("Compacting file: {:?}", log_file);
            // Reads entire file into memory, not efficient but good enough for now.
            let file_data = std::fs::read(&log_file).expect("unable to read log file");
            for line in file_data.split(|&b| b == b'\n') {
                if let Ok(log_entry) = bincode::deserialize::<LogEntry>(line) {
                    if let Some(entry_value) = log_entry.value {
                        if let Some(keydir_entry) = self.keydir.get_mut(&log_entry.key) {
                            if keydir_entry.timestamp == log_entry.timestamp {
                                bincode::serialize_into(&mut compaction_file, &entry_value)
                                    .unwrap();
                                writeln!(compaction_file).unwrap();
                                self.write_keydir(
                                    compacted_filename.clone().into(),
                                    log_entry.key,
                                    compaction_file.metadata().unwrap().len(),
                                )
                                .unwrap();
                            }
                        }
                    }
                }
            }
            // Remove old log file after compaction.
            std::fs::remove_file(&log_file).unwrap();
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

        bincode::serialize_into(&mut log_file, &entry)
            .map_err(KvStoreError::BincodeSerialization)?;
        writeln!(&log_file).map_err(|e| KvStoreError::IoError {
            source: e,
            filename: self.active_log_file.as_path().to_string_lossy().to_string(),
        })?;

        // Returning the offset of the entry in the log file after it has been written.
        // This means that the next entry is written after this one.
        Ok(offset)
    }

    fn load_keydir(&mut self) -> Result<BTreeMap<String, KeydirEntry>> {
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
            return Ok(BTreeMap::new());
        }

        let keydir: BTreeMap<String, KeydirEntry> =
            bincode::deserialize_from(keydir_file).map_err(KvStoreError::BincodeSerialization)?;
        self.keydir = keydir.clone();
        Ok(keydir)
    }

    fn write_keydir(&mut self, file_id: PathBuf, key: String, offset: u64) -> Result<()> {
        let mut keydir_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(self.keydir_location.as_path())
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: self.keydir_location.display().to_string(),
            })?;

        let mut keydir = self.load_keydir()?;
        let entry = KeydirEntry {
            file_id,
            offset,
            size: key.len(),
            timestamp: chrono::Utc::now().timestamp(),
        };
        keydir.insert(key, entry);

        bincode::serialize_into(&keydir_file, &keydir)
            .map_err(KvStoreError::BincodeSerialization)?;
        writeln!(&mut keydir_file).map_err(|e| KvStoreError::IoError {
            source: e,
            filename: self.keydir_location.display().to_string(),
        })?;
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
        self.write_keydir(self.active_log_file.clone(), entry.key.clone(), offset)?;
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
                let entry: LogEntry = bincode::deserialize(&v)?;
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
                self.write_keydir(self.active_log_file.clone(), entry.key.clone(), offset)?;
                Ok(())
            }
            None => Err(KvStoreError::RemoveOperationWithNoKey),
        }
    }
}
