use crate::engine::KvsEngine;
use crate::{KvStoreError, Result};
use crate::{KEYDIR_NAME, LOG_PREFIX, MAX_LOG_FILE_SIZE, MAX_NUM_LOG_FILES};
use glob::glob;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{prelude::*, SeekFrom};
use std::sync::Arc;
use std::{collections::BTreeMap, path::PathBuf};
use tracing::{self, debug, info};

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

    active_log_handle: Option<File>,

    /// Keydir is an in-memory (traditionally) hashmap of keys to their respective log file, which
    /// contains the offset to the entry in the file.
    ///
    /// This would traditionally be in-memory, but as we also have a CLI portion of the
    /// application, we persist the keydir to disk so that it can be loaded for various
    /// operations.
    keydir: BTreeMap<String, KeydirEntry>,

    keydir_handle: Option<Arc<File>>,

    /// The maximum size of a log file in bytes.
    max_log_file_size: u64,
    /// The maximum number of log files before older versions are deleted.
    max_num_log_files: usize,

    _tracing: Option<tracing::subscriber::DefaultGuard>,
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

impl KvsEngine for KvStore {
    /// Set the value of a key by inserting the value into the store for the given key.
    fn set(&mut self, key: String, value: String) -> Result<()> {
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
    fn get(&mut self, key: String) -> Result<Option<String>> {
        debug!("Getting key {}", key);
        debug!("Keydir: {:?}", self.keydir);
        match self.keydir.get(&key) {
            Some(entry) => {
                debug!(
                    key = key,
                    offset = entry.offset,
                    "entry exists in {}",
                    self.active_log_file.display(),
                );

                let mut entry_file = std::fs::File::open(entry.file_id.clone()).unwrap();
                entry_file.seek(SeekFrom::Start(entry.offset)).unwrap();
                let log_entry: LogEntry = bincode::deserialize_from(entry_file)?;
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
    fn remove(&mut self, key: String) -> Result<()> {
        match self.keydir.get(&key) {
            Some(_entry) => {
                let tombstone = LogEntry {
                    timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap(),
                    operation: Operation::Remove,
                    key_size: key.len(),
                    key,
                    value: None,
                    value_size: 0,
                };
                let offset = self.append_to_log(&tombstone)?;
                self.write_keydir(self.active_log_file.clone(), tombstone.key.clone(), offset)?;
                Ok(())
            }
            None => Err(KvStoreError::RemoveOperationWithNoKey),
        }
    }
}

impl KvStore {
    /// Create a new KvStore.
    ///
    /// The store is created in memory and is not persisted to disk.
    fn new(max_log_file_size: u64, max_num_log_files: usize) -> KvStore {
        KvStore {
            active_log_file: PathBuf::default(),
            active_log_handle: None,
            log_location: PathBuf::default(),
            keydir_location: PathBuf::default(),
            keydir: BTreeMap::new(),
            keydir_handle: None,
            max_log_file_size, // TODO: increase
            max_num_log_files,
            _tracing: None,
        }
    }

    /// Open a KvStore at the given path.
    pub fn open<P>(path: P) -> Result<KvStore>
    where
        P: Into<PathBuf>,
    {
        let mut store = KvStore::new(MAX_LOG_FILE_SIZE, MAX_NUM_LOG_FILES);

        let path = path.into();
        let keydir_path = path.join(KEYDIR_NAME);
        info!("Using keydir at {}", keydir_path.display());

        store.log_location = path.clone();
        store.keydir_location = keydir_path;

        debug!("Creating initial log file");
        store.active_log_handle = Some(store.create_log_file()?);
        store.set_keydir_handle()?;
        store.keydir = store.load_keydir()?;
        debug!("Loaded keydir: {:?}", store.keydir);

        Ok(store)
    }

    fn create_log_file(&mut self) -> Result<File> {
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
            debug!(
                "Compaction required, {inactive_count} inactive files > {max_num_log_files} max allowed: {inactive_files:?}",
                inactive_count = inactive_files.len(),
                max_num_log_files = self.max_num_log_files,
            );
            self.compact(inactive_files)?;
        }
        Ok(log_file)
    }

    fn next_log_file_name(&mut self) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        format!("{}{}", self.log_location.join(LOG_PREFIX).display(), now)
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

        let mut entries: Vec<(LogEntry, u64)> = Vec::new();

        for (_, v) in self.keydir.iter() {
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
            entries.push((log_entry, offset));
        }

        for (log_entry, offset) in entries {
            self.write_keydir(compacted_filename.clone().into(), log_entry.key, offset)?;
        }
        debug!("Compaction complete, latest entries are available in {compacted_filename}");

        // Compaction process is complete, remove the inactive files.
        // NOTE: this is not entirely safe as the application could halt in the middle
        // of the above process and lingering files would be left on disk.
        for log_file in inactive_files {
            // TODO: this shouldn't be required as we filter out the active file above.
            if log_file == self.active_log_file {
                continue;
            }
            std::fs::remove_file(&log_file).unwrap();
            debug!("Removed inactive log file {}", log_file.display());
        }
        Ok(())
    }

    /// Append a log into the active log file. This acts as a Write-ahead log (WAL)
    /// and is an operation which should be taken before other operations, such as
    /// updating the keydir.
    fn append_to_log(&mut self, entry: &LogEntry) -> Result<u64> {
        // The offset is where the last entry in the log file ends, this is because
        // the log file is an append-only log.
        let mut offset = self
            .active_log_handle
            .as_ref()
            .ok_or(KvStoreError::NoActiveLogFile)?
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
            self.active_log_handle = Some(self.create_log_file()?);
            offset = 0;
        }

        bincode::serialize_into(&mut self.active_log_handle.as_ref().unwrap(), &entry)
            .map_err(KvStoreError::BincodeSerialization)?;

        // Returning the offset of the entry in the log file after it has been written.
        // This means that the next entry is written after this one.
        Ok(offset)
    }

    fn set_keydir_handle(&mut self) -> Result<()> {
        let keydir_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true) // To enable creation in the case where the file doesn't exist.
            .create(true)
            .open(self.keydir_location.as_path())
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: self.keydir_location.display().to_string().clone(),
            })?;

        self.keydir_handle = Some(Arc::new(keydir_file));
        Ok(())
    }

    fn load_keydir(&mut self) -> Result<BTreeMap<String, KeydirEntry>> {
        debug!("Loading keydir");
        let keydir_file_size = &self
            .keydir_handle
            .as_ref()
            .ok_or(KvStoreError::NoKeydir)?
            .metadata()
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: self.keydir_location.display().to_string().clone(),
            })?
            .len();
        debug!(size = keydir_file_size, "keydir file size");

        if *keydir_file_size == 0 {
            info!("New keydir");
            return Ok(BTreeMap::new());
        }

        let keydir: BTreeMap<String, KeydirEntry> =
            bincode::deserialize_from(self.keydir_handle.as_ref().unwrap().clone())
                .map_err(KvStoreError::BincodeSerialization)?;
        Ok(keydir)
    }

    /// Write a value into the keydir, this updates an entry within the mapping for where the
    /// key is stored in the passed log file.
    ///
    /// Typically, this is done right after a value is written into the WAL.
    fn write_keydir(&mut self, file_id: PathBuf, key: String, offset: u64) -> Result<()> {
        debug!("Writing keydir entry for {key}");
        let entry = KeydirEntry {
            file_id,
            offset,
            size: key.len(),
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap(),
        };
        self.keydir.insert(key, entry);
        debug!("Keydir after insert: {:?}", self.keydir);

        self.keydir_handle
            .as_mut()
            .ok_or(KvStoreError::NoKeydir)?
            .seek(SeekFrom::Start(0))
            .unwrap();

        bincode::serialize_into(self.keydir_handle.as_mut().unwrap(), &self.keydir)
            .map_err(KvStoreError::BincodeSerialization)?;
        self.keydir_handle.as_mut().unwrap().flush().unwrap();
        debug!("Serialized keydir file {}", self.keydir_location.display());
        Ok(())
    }

    /// Detect the engine used to create the store.
    /// We must return an error if previously opened with another engine, as they are incompatible.
    pub fn engine_is_kvs(current_engine: String, engine_path: PathBuf) -> Result<()> {
        if engine_path.exists() {
            let engine_type = std::fs::read_to_string(engine_path)
                .unwrap()
                .trim()
                .to_string();

            if engine_type != current_engine {
                return Err(KvStoreError::IncorrectEngine {
                    current: current_engine,
                    previous: engine_type,
                });
            }
        } else {
            std::fs::write(engine_path, current_engine).unwrap();
        }
        Ok(())
    }

    pub fn set_tracing(&mut self, guard: tracing::subscriber::DefaultGuard) {
        self._tracing = Some(guard);
    }
}
