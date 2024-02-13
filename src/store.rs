use glob::glob;
use serde::{Deserialize, Serialize};
use std::io::{prelude::*, SeekFrom};
use std::{collections::BTreeMap, path::PathBuf};
use tracing::{self, debug, info};
use crate::KvStoreError;
use crate::Result;
use crate::{LOG_PREFIX, KEYDIR_NAME, MAX_LOG_FILE_SIZE, MAX_NUM_LOG_FILES};

#[derive(Debug, Serialize, Deserialize)]
pub enum Operation {
    Set,
    Get,
    Remove,
}

/// An in-memory key-value store, backed by a [`BTreeMap`] from the standard library.
pub struct KvStore {
    pub log_location: PathBuf,

    /// Keydir maps key entries in the log to their offset in the log file.
    /// The byte offset is used to seek to the correct position in the log file.
    keydir_location: PathBuf,

    active_log_file: PathBuf,

    /// Keydir is an in-memory (traditionally) hashmap of keys to their respective log file, which
    /// contains the offset to the entry in the file.
    ///
    /// This would traditionally be in-memory, but as we also have a CLI portion of the
    /// application, we persist the keydir to disk so that it can be loaded for various
    /// operations.
    keydir: BTreeMap<String, KeydirEntry>,

    /// The maximum size of a log file in bytes.
    max_log_file_size: u64,
    /// The maximum number of log files before older versions are deleted.
    max_num_log_files: usize,

    /// Index used for the log file.
    file_index: usize,

    tracing: tracing::subscriber::DefaultGuard,
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
    fn new(
        max_log_file_size: u64,
        max_num_log_files: usize,
        tracing_guard: tracing::subscriber::DefaultGuard,
    ) -> KvStore {
        KvStore {
            active_log_file: PathBuf::default(),
            log_location: PathBuf::default(),
            keydir_location: PathBuf::default(),
            keydir: BTreeMap::new(),
            max_log_file_size, // TODO: increase
            max_num_log_files,
            file_index: 0,
            tracing: tracing_guard,
        }
    }

    /// Open a KvStore at the given path.
    pub fn open<P>(path: P) -> Result<KvStore>
    where
        P: Into<PathBuf>,
    {
        let subscriber = tracing_subscriber::fmt().finish();
        let tracing_guard = tracing::subscriber::set_default(subscriber);
        let mut store = KvStore::new(MAX_LOG_FILE_SIZE, MAX_NUM_LOG_FILES, tracing_guard);

        let path = path.into();
        let keydir_path = path.join(KEYDIR_NAME);
        debug!("Using keydir at {}", keydir_path.display());

        store.log_location = path.clone();
        store.keydir_location = keydir_path;

        debug!("Creating initial log file");
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
        debug!("Created initial log file at {}", init_name);
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

        // Do not consider files which satisfy the below criterion as candidates for compaction.
        let no_compact = |p: &PathBuf| {
            !p.to_string_lossy().contains("compacted")
                || !p.to_string_lossy().contains("keydir")
                || *p != self.active_log_file // TODO: this doesn't quite work right
        };

        let inactive_files = glob(&format!("{}/kvs.log*", self.log_location.display()))
            .unwrap()
            .map(|p| p.unwrap())
            .filter(no_compact)
            .collect::<Vec<_>>();
        if inactive_files.len() > self.max_num_log_files {
            info!(
                "Compaction required, {inactive_count} inactive files > {max_num_log_files} max allowed: {inactive_files:?}",
                inactive_count = inactive_files.len(),
                max_num_log_files = self.max_num_log_files,
            );
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

    /// Perform compaction on the inactive log files.
    ///
    /// This works by reading the keydir (where our most recent values are stored) and then placing
    /// the values into a new "compacted" log file, updating our latest entries to point to this
    /// new file so that subsequent reads will be able to find the correct values.
    ///
    /// When a tombstone value is found ([`None`]), then the key is removed from the keydir. This
    /// is done implicitly by not writing the key into the new file.
    fn compact(&mut self, inactive_files: Vec<PathBuf>) -> Result<()> {
        // Using the current active file name with a compacted suffix to ensure uniqueness.
        let compacted_filename = format!("{}-compacted", self.active_log_file.display());
        debug!(
            "Compacting to {}, active file is {}",
            compacted_filename,
            self.active_log_file.display()
        );

        let mut compaction_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&compacted_filename)
            .unwrap();

        let keydir = self.load_keydir()?;
        for (_, v) in keydir.iter() {
            let mut file = std::fs::File::open(&v.file_id).expect("unable to read log file");
            file.seek(SeekFrom::Start(v.offset))
                .map_err(|e| KvStoreError::IoError {
                    source: e,
                    filename: self.active_log_file.as_path().to_string_lossy().to_string(),
                })?;
            let log_entry: LogEntry = bincode::deserialize_from(&mut file)?;
            let offset = compaction_file
                .metadata()
                .expect("file exists to check metadata")
                .len();
            bincode::serialize_into(&mut compaction_file, &log_entry)
                .map_err(KvStoreError::BincodeSerialization)?;
            self.write_keydir(
                compacted_filename.clone().into(),
                log_entry.key.clone(),
                offset,
            )?;
        }
        info!("Compaction complete, latest entries are available in {compacted_filename}");

        // Compaction process is complete, remove the inactive files.
        // NOTE: this is not entirely safe as the application could halt in the middle
        // of the above process and lingering files would be left on disk.
        for log_file in inactive_files {
            // TODO: this shouldn't be required as we filter out the active file above.
            if log_file == self.active_log_file {
                continue;
            }
            std::fs::remove_file(&log_file).unwrap();
            info!("Removed inactive log file {}", log_file.display());
        }
        Ok(())
    }

    /// Append a log into the active log file. This acts as a Write-ahead log (WAL)
    /// and is an operation which should be taken before other operations, such as
    /// updating the keydir.
    fn append_to_log(&mut self, entry: &LogEntry) -> Result<u64> {
        let mut log_file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&self.active_log_file.as_path())
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: self.active_log_file.as_path().to_string_lossy().to_string(),
            })?;

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
        // This will be used for compaction later.
        if offset > self.max_log_file_size {
            debug!(
                "Log file size exceeded threshold ({}), creating new log file",
                self.max_log_file_size
            );
            log_file = self.create_log_file()?;
            offset = 0;
        }

        bincode::serialize_into(&mut log_file, &entry)
            .map_err(KvStoreError::BincodeSerialization)?;

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

    /// Write a value into the keydir, this updates an entry within the mapping for where the
    /// key is stored in the passed log file.
    ///
    /// Typically, this is done right after a value is written into the WAL.
    fn write_keydir(&mut self, file_id: PathBuf, key: String, offset: u64) -> Result<()> {
        let keydir_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(self.keydir_location.as_path())
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: self.keydir_location.display().to_string(),
            })?;

        let entry = KeydirEntry {
            file_id,
            offset,
            size: key.len(),
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap(),
        };
        let mut keydir = self.load_keydir()?;
        keydir.insert(key, entry);

        bincode::serialize_into(&keydir_file, &keydir)
            .map_err(KvStoreError::BincodeSerialization)?;
        Ok(())
    }

    /// Set the value of a key by inserting the value into the store for the given key.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let entry = LogEntry {
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap(),
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
        // We load the keydir from disk here because we have a CLI aspect to the application
        // which means that the keydir is not always in memory, loading it from disk gets around
        // that.
        let keydir = self.load_keydir()?;
        match keydir.get(&key) {
            Some(entry) => {
                let mut log_file = std::fs::OpenOptions::new()
                    .read(true)
                    .append(true)
                    .create(true)
                    .open(entry.file_id.as_path())
                    .map_err(|e| KvStoreError::IoError {
                        source: e,
                        filename: entry.file_id.as_path().to_string_lossy().to_string(),
                    })?;

                let log_file_size = log_file
                    .metadata()
                    .map_err(|e| KvStoreError::IoError {
                        source: e,
                        filename: entry.file_id.as_path().to_string_lossy().to_string(),
                    })?
                    .len();

                if log_file_size == 0 {
                    // Edge case for the log file being empty, there is no value to return.
                    return Ok(None);
                }

                // Seek to the position provided by the keydir and serialize the entry.
                log_file.seek(SeekFrom::Start(entry.offset)).map_err(|e| {
                    KvStoreError::IoError {
                        source: e,
                        filename: entry.file_id.as_path().to_string_lossy().to_string(),
                    }
                })?;
                let log_entry: LogEntry = bincode::deserialize_from(log_file)?;
                match log_entry.value {
                    Some(value) => Ok(Some(value)),
                    // This is a tombstone value and equates to a deleted key and
                    // the "Key not found" scenario.
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
                    timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap(),
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
