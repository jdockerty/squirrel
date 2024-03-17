use crate::engine::KvsEngine;
use crate::{KvStoreError, Result};
use crate::{KEYDIR_NAME, LOG_PREFIX, MAX_LOG_FILE_SIZE};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{prelude::*, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use tracing::{self, debug};
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Operation {
    Set,
    Get,
    Remove,
}

#[derive(Clone, Debug)]
pub struct StoreWriter {
    active_log_file: Arc<RwLock<PathBuf>>,
    // TODO: can we hold BufWriter's here?
    active_log_handle: Option<Arc<RwLock<File>>>,
    position: Arc<AtomicUsize>,
}

impl StoreWriter {
    pub fn new() -> StoreWriter {
        StoreWriter {
            active_log_file: Arc::new(RwLock::new(PathBuf::default())),
            active_log_handle: None,
            position: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Default for StoreWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// An in-memory key-value store inspired by Bitcask.
#[derive(Clone, Debug)]
pub struct KvStore {
    pub writer: Arc<RwLock<StoreWriter>>,

    pub log_location: PathBuf,

    /// Keydir maps key entries in the log to their offset in the log file.
    /// The byte offset is used to seek to the correct position in the log file.
    keydir_location: PathBuf,

    /// Keydir is an in-memory map of keys to their respective log file, which
    /// contains the offset to the entry in the log file.
    ///
    /// This uses [`DashMap`] to allow for concurrent reads and writes.
    keydir: Arc<DashMap<String, KeydirEntry>>,

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
    offset: usize,

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
        let timestamp = chrono::Utc::now().timestamp();
        let entry = LogEntry {
            timestamp,
            operation: Operation::Set,
            key: key.clone(),
            value: Some(value),
        };

        let pos = self
            .writer
            .read()
            .unwrap()
            .position
            .load(std::sync::atomic::Ordering::SeqCst);
        self.append_to_log(&entry)?;

        let entry = KeydirEntry {
            file_id: self
                .writer
                .read()
                .unwrap()
                .active_log_file
                .read()
                .unwrap()
                .clone(),
            offset: pos,
            timestamp,
        };
        self.keydir.insert(key, entry);
        // Retrieve the new value without having to load the atomic value again.
        if pos as u64 > self.max_log_file_size {
            debug!(
                current_size = pos,
                max_log_file_size = self.max_log_file_size,
                active_file = self
                    .writer
                    .read()
                    .unwrap()
                    .active_log_file
                    .read()
                    .unwrap()
                    .display()
                    .to_string(),
                "Compaction required"
            );
            self.compact()?;
        }
        self.commit_keydir()?;
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
                    self.writer
                        .read()
                        .unwrap()
                        .active_log_file
                        .read()
                        .unwrap()
                        .display(),
                );

                let mut entry_file = std::fs::File::open(&entry.file_id)?;
                entry_file.seek(SeekFrom::Start(entry.offset as u64))?;
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
                    timestamp: chrono::Utc::now().timestamp(),
                    operation: Operation::Remove,
                    key: key.clone(),
                    value: None,
                };

                let pos = self
                    .writer
                    .read()
                    .unwrap()
                    .position
                    .load(std::sync::atomic::Ordering::SeqCst);
                self.append_to_log(&tombstone)?;

                if pos as u64 > self.max_log_file_size {
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
            writer: Arc::new(RwLock::new(StoreWriter::default())),
            log_location: PathBuf::default(),
            keydir_location: PathBuf::default(),
            keydir: Arc::new(DashMap::new()),
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
        let layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
        let subscriber = tracing_subscriber::registry()
            .with(tracing::level_filters::LevelFilter::DEBUG)
            .with(layer);
        let tracing_guard = tracing::subscriber::set_default(subscriber);
        let mut store = KvStore::new(MAX_LOG_FILE_SIZE.with(|f| *f));
        store.set_tracing(tracing_guard);

        let path = path.into();
        let keydir_path = path.join(KEYDIR_NAME);
        debug!("Using keydir at {}", keydir_path.display());

        store.log_location = path.clone();
        store.keydir_location = keydir_path;

        debug!("Creating initial log file");
        store.writer.write().unwrap().active_log_handle =
            Some(Arc::new(RwLock::new(store.create_log_file()?)));
        store.set_keydir_handle()?;
        store.keydir = Arc::new(store.load_keydir()?);
        debug!("Loaded keydir: {:?}", store.keydir);

        Ok(store)
    }

    /// Append a log into the active log file. This acts as a Write-ahead log (WAL)
    /// and is an operation which should be taken before other operations, such as
    /// updating the keydir.
    fn append_to_log(&self, entry: &LogEntry) -> Result<()> {
        let write_lock = self.writer.write().unwrap();
        let mut file_lock = write_lock
            .active_log_handle
            .as_ref()
            .ok_or(KvStoreError::NoActiveLogFile)?
            .write()
            .unwrap();
        let data = bincode::serialize(&entry)?;
        file_lock.write_all(&data)?;
        file_lock.flush()?;
        write_lock
            .position
            .fetch_add(data.len(), std::sync::atomic::Ordering::SeqCst);
        debug!(
            active_file_set = write_lock
                .active_log_file
                .read()
                .unwrap()
                .display()
                .to_string(),
            "Appended to log"
        );

        // Returning the offset of the entry in the log file after it has been written.
        // This means that the next entry is written after this one.
        Ok(())
    }

    fn create_log_file(&self) -> Result<File> {
        let next_log_file_name = self.next_log_file_name();
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&next_log_file_name)?;
        self.writer.write().unwrap().active_log_file =
            Arc::new(RwLock::new(PathBuf::from(next_log_file_name.clone())));
        debug!(active_file = next_log_file_name, "Created new log file");
        self.writer
            .write()
            .unwrap()
            .position
            .store(0, std::sync::atomic::Ordering::SeqCst);
        Ok(log_file)
    }

    fn next_log_file_name(&self) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        format!(
            "{}{}.log",
            self.log_location.join(LOG_PREFIX).display(),
            now
        )
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
        // Generate a new log file to write the compacted entries to.
        let compacted_filename = self.next_log_file_name();
        let compaction_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&compacted_filename)?;
        let mut buf_writer = std::io::BufWriter::new(&compaction_file);
        debug!(compaction_file = compacted_filename, "Compacting log files");

        let mut entries: Vec<(LogEntry, u64)> = Vec::new();
        debug!(keydir_size = self.keydir.len());

        // Maintain a map of handles to avoid opening a new file on every single
        // log entry.
        let file_handles = Arc::new(DashMap::new());
        // Build a map of active files, these are files which are still being referenced
        // in the keydir, which can include files from prior compaction.
        // Once they are no longer referenced, they can be safely removed.
        let active_files = Arc::new(DashMap::new());

        let mut compact_pos = 0;
        for entry in self.keydir.as_ref() {
            let mut file = file_handles
                .entry(entry.file_id.clone())
                .or_insert_with(|| std::fs::File::open(&entry.file_id).unwrap());

            file.seek(SeekFrom::Start(entry.offset as u64))?;
            let log_entry: LogEntry = bincode::deserialize_from(&mut *file)?;

            // Implicitly remove tombstone values by not adding them into the new file
            // and removing them from the keydir.
            if log_entry.operation == Operation::Remove {
                self.keydir.remove(entry.key());
                continue;
            }

            active_files
                .entry(entry.file_id.clone())
                .and_modify(|f: &mut u64| *f += 1)
                .or_default();
            let data = bincode::serialize(&log_entry)?;
            let written = buf_writer.write(&data)?;
            buf_writer.flush()?;

            entries.push((log_entry, compact_pos));
            compact_pos += written as u64;
        }

        for (log_entry, offset) in entries {
            self.keydir.entry(log_entry.key).and_modify(|e| {
                active_files.entry(e.file_id.clone()).and_modify(|f| {
                    if *f > 0 {
                        // Don't reduce below 0, this would be an underflow error
                        *f -= 1
                    }
                });
                e.file_id = PathBuf::from(&compacted_filename);
                e.offset = offset as usize;
            });
        }
        self.commit_keydir()?;

        self.set_active_log_handle()?;
        for file in active_files.as_ref() {
            if *file.value() == 0 {
                debug!(f = ?file.key(), "Removing file which has no entries");
                std::fs::remove_file(file.key())?;
            }
        }

        debug!("Compaction complete, latest entries are available in {compacted_filename}");
        Ok(())
    }

    fn set_active_log_handle(&self) -> Result<()> {
        self.writer.write().unwrap().active_log_handle =
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
            self.keydir.as_ref(),
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
