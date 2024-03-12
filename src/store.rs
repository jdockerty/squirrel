use crate::engine::KvsEngine;
use crate::{KvStoreError, Result};
use crate::{KEYDIR_NAME, LOG_PREFIX, MAX_LOG_FILE_SIZE};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use tracing::{self, debug};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Operation {
    Set,
    Get,
    Remove,
}

#[derive(Clone)]
pub struct StoreWriter {
    active_log_file: PathBuf,
    active_log_handle: Option<Arc<RwLock<File>>>,
    bytes_written: Arc<AtomicU64>,
}

impl StoreWriter {
    pub fn new() -> StoreWriter {
        StoreWriter {
            active_log_file: PathBuf::default(),
            active_log_handle: None,
            bytes_written: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Default for StoreWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// An in-memory key-value store inspired by Bitcask.
#[derive(Clone)]
pub struct KvStore {
    pub writer: RefCell<StoreWriter>,

    pub log_location: PathBuf,

    /// Keydir maps key entries in the log to their offset in the log file.
    /// The byte offset is used to seek to the correct position in the log file.
    keydir_location: PathBuf,

    /// Keydir is an in-memory map of keys to their respective log file, which
    /// contains the offset to the entry in the log file.
    ///
    /// This uses [`DashMap`] to allow for concurrent reads and writes.
    keydir: DashMap<String, KeydirEntry>,

    keydir_handle: Option<Arc<RwLock<File>>>,

    /// The maximum size of a log file in bytes.
    max_log_file_size: u64,

    _tracing: Option<Arc<tracing::subscriber::DefaultGuard>>,
}

/// A ['LogEntry'] is a single line entry in the log file.
/// Multiple entries can be used to rebuild the state of the store.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogEntry {
    /// The operation that was performed.
    operation: Operation,

    timestamp: i64,
    key: String,
    value: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct KeydirEntry {
    /// The file where the entry is stored.
    file_id: PathBuf,

    /// The offset of the entry in the log file.
    offset: u64,

    timestamp: i64,
}

impl Drop for KvStore {
    fn drop(&mut self) {
        // By persisting the keydir to disk on drop, this means that the keydir
        // is safely stored when the [`KvStore`] drops out of scope.
        self.commit_keydir()
            .expect("Unable to commit keydir to disk");
    }
}

impl KvsEngine for KvStore {
    /// Set the value of a key by inserting the value into the store for the given key.
    fn set(&self, key: String, value: String) -> Result<()> {
        debug!(key, value, "Setting key");
        let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        let entry = LogEntry {
            timestamp,
            operation: Operation::Set,
            key: key.clone(),
            value: Some(value),
        };

        let mut offset = self.append_to_log(&entry)?;
        debug!(offset, "Appended to log");

        let old = self
            .writer
            .borrow_mut()
            .bytes_written
            .fetch_add(offset, std::sync::atomic::Ordering::SeqCst);
        if old + offset > self.max_log_file_size {
            debug!(
                current_size = old + offset,
                max_log_file_size = self.max_log_file_size,
                active_file = self.writer.borrow().active_log_file.display().to_string(),
                "Compaction required"
            );
            self.compact()?;
            // Reset the offset because we have a new file after compaction.
            offset = 0;
        }

        debug!(active_file_set = self.writer.borrow().active_log_file.display().to_string());
        let entry = KeydirEntry {
            file_id: self.writer.borrow().active_log_file.clone(),
            offset,
            timestamp,
        };
        self.keydir.insert(key.clone(), entry);
        Ok(())
    }

    /// Retrieve the value of a key from the store.
    /// If the key does not exist, then [`None`] is returned.
    fn get(&self, key: String) -> Result<Option<String>> {
        debug!(key, "Getting key");
        match self.keydir.get(&key) {
            Some(entry) => {
                debug!(
                    key = key,
                    offset = entry.offset,
                    "entry exists in {}",
                    self.writer.borrow().active_log_file.display(),
                );

                let mut entry_file = std::fs::File::open(entry.file_id.clone())?;
                entry_file.seek(SeekFrom::Start(entry.offset))?;
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
    fn remove(&self, key: String) -> Result<()> {
        match self.keydir.remove(&key) {
            Some(_entry) => {
                let tombstone = LogEntry {
                    timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap(),
                    operation: Operation::Remove,
                    key: key.clone(),
                    value: None,
                };

                let offset = self.append_to_log(&tombstone)?;
                let old = self
                    .writer
                    .borrow_mut()
                    .bytes_written
                    .fetch_add(offset, std::sync::atomic::Ordering::SeqCst);

                if old + offset > self.max_log_file_size {
                    self.compact()?;
                }
                self.commit_keydir()?;
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
    fn new(max_log_file_size: u64) -> KvStore {
        KvStore {
            writer: RefCell::new(StoreWriter::default()),
            log_location: PathBuf::default(),
            keydir_location: PathBuf::default(),
            keydir: DashMap::new(),
            keydir_handle: None,
            max_log_file_size, // TODO: increase
            _tracing: None,
        }
    }

    /// Open a KvStore at the given path.
    pub fn open<P>(path: P) -> Result<KvStore>
    where
        P: Into<PathBuf>,
    {
        let mut store = KvStore::new(MAX_LOG_FILE_SIZE);

        let path = path.into();
        let keydir_path = path.join(KEYDIR_NAME);
        debug!("Using keydir at {}", keydir_path.display());

        store.log_location = path.clone();
        store.keydir_location = keydir_path;

        debug!("Creating initial log file");
        store.writer.borrow_mut().active_log_handle =
            Some(Arc::new(RwLock::new(store.create_log_file()?)));
        store.set_keydir_handle()?;
        store.keydir = store.load_keydir()?;
        debug!("Loaded keydir: {:?}", store.keydir);

        Ok(store)
    }

    /// Append a log into the active log file. This acts as a Write-ahead log (WAL)
    /// and is an operation which should be taken before other operations, such as
    /// updating the keydir.
    fn append_to_log(&self, entry: &LogEntry) -> Result<u64> {
        // The offset is where the last entry in the log file ends, this is because
        // the log file is an append-only log.
        let offset = self
            .writer
            .borrow_mut()
            .active_log_handle
            .as_ref()
            .ok_or(KvStoreError::NoActiveLogFile)?
            .read()
            .unwrap()
            .metadata()?
            .len();

        bincode::serialize_into(
            &*self
                .writer
                .borrow_mut()
                .active_log_handle
                .as_ref()
                .ok_or(KvStoreError::NoActiveLogFile)?
                .write()
                .unwrap(),
            &entry,
        )?;

        // Returning the offset of the entry in the log file after it has been written.
        // This means that the next entry is written after this one.
        Ok(offset)
    }

    fn create_log_file(&self) -> Result<File> {
        let next_log_file_name = self.next_log_file_name();
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&next_log_file_name)?;
        self.writer.borrow_mut().active_log_file = PathBuf::from(next_log_file_name.clone());
        debug!(active_file = next_log_file_name, "Created new log file");
        self.writer
            .borrow_mut()
            .bytes_written
            .store(0, std::sync::atomic::Ordering::SeqCst);
        Ok(log_file)
    }

    fn next_log_file_name(&self) -> String {
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
    fn compact(&self) -> Result<()> {
        // Using the current active file name with a compacted suffix to ensure uniqueness.
        let compacted_filename = format!(
            "{}-compacted",
            self.writer.borrow().active_log_file.display()
        );

        let mut compaction_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&compacted_filename)?;

        let mut entries: Vec<(LogEntry, u64)> = Vec::new();

        debug!(keydir_size = self.keydir.len());
        let mut counter = 0;
        for entry in self.keydir.iter() {
            let mut file = std::fs::File::open(&entry.file_id)?;
            file.seek(SeekFrom::Start(entry.offset))?;
            let log_entry: LogEntry = bincode::deserialize_from(&mut file)?;

            // Implicitly remove tombstone values by not adding them into the new file.
            if log_entry.operation != Operation::Remove {
                counter += 1;
                let offset = compaction_file
                    .metadata()
                    .expect("file exists to check metadata")
                    .len();
                bincode::serialize_into(&mut compaction_file, &log_entry)?;
                entries.push((log_entry, offset));
            }
        }

        for (log_entry, offset) in entries {
            let keydir_entry = KeydirEntry {
                file_id: PathBuf::from(&compacted_filename),
                offset,
                timestamp: log_entry.timestamp,
            };
            self.keydir.insert(log_entry.key, keydir_entry);
        }
        self.commit_keydir()?;

        std::fs::remove_file(&self.writer.borrow().active_log_file)?;
        self.set_active_log_handle()?;

        debug!(compacted_entries = counter);
        debug!("Compaction complete, latest entries are available in {compacted_filename}");
        Ok(())
    }

    fn set_active_log_handle(&self) -> Result<()> {
        self.writer.borrow_mut().active_log_handle =
            Some(Arc::new(RwLock::new(self.create_log_file()?)));
        Ok(())
    }

    fn set_keydir_handle(&mut self) -> Result<()> {
        let keydir_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true) // To enable creation in the case where the file doesn't exist.
            .create(true)
            .open(self.keydir_location.as_path())?;

        self.keydir_handle = Some(Arc::new(RwLock::new(keydir_file)));
        Ok(())
    }

    fn load_keydir(&self) -> Result<DashMap<String, KeydirEntry>> {
        debug!("Loading keydir");
        let handle = self.keydir_handle.clone();

        let keydir_file_size = handle
            .ok_or(KvStoreError::NoKeydir)?
            .read()
            .unwrap()
            .metadata()?
            .len();
        debug!(size = keydir_file_size, "keydir file size");

        if keydir_file_size == 0 {
            debug!("New keydir");
            return Ok(DashMap::new());
        }

        let keydir = bincode::deserialize_from(
            &*self
                .keydir_handle
                .clone()
                .ok_or(KvStoreError::NoKeydir)?
                .read()
                .unwrap(),
        )?;
        Ok(keydir)
    }

    /// Commit the keydir to disk to persist its state over crashes of the server.
    ///
    /// This is done by serializing the keydir to disk after important operations.
    ///
    /// Note that this is not entirely safe as the application could crash between
    /// operations.
    fn commit_keydir(&self) -> Result<()> {
        let _ = &self
            .keydir_handle
            .clone()
            .ok_or(KvStoreError::NoKeydir)?
            .write()
            .unwrap()
            .seek(SeekFrom::Start(0))?;

        bincode::serialize_into(
            &*self
                .keydir_handle
                .clone()
                .ok_or(KvStoreError::NoKeydir)?
                .read()
                .unwrap(),
            &self.keydir,
        )?;
        Ok(())
    }

    /// Detect the engine used to create the store.
    /// We must return an error if previously opened with another engine, as they are incompatible.
    pub fn engine_is_kvs(current_engine: String, engine_path: PathBuf) -> Result<()> {
        if engine_path.exists() {
            let engine_type = std::fs::read_to_string(engine_path)?.trim().to_string();

            if engine_type != current_engine {
                return Err(KvStoreError::IncorrectEngine {
                    current: current_engine,
                    previous: engine_type,
                });
            }
        } else {
            std::fs::write(engine_path, current_engine)?;
        }
        Ok(())
    }

    pub fn set_tracing(&mut self, guard: tracing::subscriber::DefaultGuard) {
        self._tracing = Some(Arc::new(guard));
    }
}
