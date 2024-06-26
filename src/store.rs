use crate::engine::KvsEngine;
use crate::proto::GetResponse;
use crate::{KvStoreError, Result};
use crate::{LOG_PREFIX, MAX_LOG_FILE_SIZE};
use dashmap::DashMap;
use glob::glob;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{prelude::*, BufReader, BufWriter, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::usize;
use tracing::level_filters::LevelFilter;
use tracing::{self, debug, info, warn};
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Operation {
    Set,
    Get,
    Remove,
}

#[derive(Clone, Debug)]
pub struct StoreWriter {
    active_log_file: PathBuf,
    active_log_handle: Option<Arc<RwLock<File>>>,
    position: Arc<AtomicUsize>,
}

impl Default for StoreWriter {
    fn default() -> Self {
        StoreWriter {
            active_log_file: PathBuf::default(),
            active_log_handle: None,
            position: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// A key-value store inspired by Bitcask.
///
/// Writes are appended to a Write-Ahead Log (WAL) and then offset locations held
/// in memory using a keydir. In the event of a crash or [`Drop`], the keydir
/// index is rebuilt using any log files that are found within the given directory.
#[derive(Clone, Debug)]
pub struct KvStore {
    pub writer: Arc<RwLock<StoreWriter>>,

    /// Directory where the log files are stored.
    pub log_location: PathBuf,

    /// Keydir is an in-memory map of keys to their respective log file, which
    /// contains the offset to the entry in the log file.
    ///
    /// This uses [`DashMap`] to allow for concurrent reads and writes.
    keydir: Arc<DashMap<String, KeydirEntry>>,

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

    /// Timestamp of the entry.
    ///
    /// This is also used as a version when replication is in-use. This means
    /// the entry with the most recent timestamp wins.
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

    /// Timestamp of the entry.
    ///
    /// This is also used as a version when replication is in-use. This means
    /// the entry with the most recent timestamp wins.
    timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoreConfig {
    log_location: PathBuf,
    max_log_file_size: u64,
}

impl KvsEngine for KvStore {
    /// Set the value of a key by inserting the value into the store for the given key.
    async fn set(&self, key: String, value: String) -> Result<()> {
        debug!(key, value, "Setting key");
        let timestamp = chrono::Utc::now().timestamp();
        let entry = LogEntry {
            timestamp,
            operation: Operation::Set,
            key: key.clone(),
            value: Some(value),
        };

        let pos = self.append_to_log(&entry)?;
        debug!(
            position = pos,
            active_file = ?self.writer.read().unwrap().active_log_file.display(),
            "Appended to log"
        );

        self.keydir
            .entry(entry.key)
            .and_modify(|e| {
                e.file_id = self.writer.read().unwrap().active_log_file.clone();
                e.offset = pos;
                e.timestamp = timestamp;
            })
            .or_insert_with(|| KeydirEntry {
                file_id: self.writer.read().unwrap().active_log_file.clone(),
                offset: pos,
                timestamp,
            });
        debug!(key, "Updated keydir");
        if pos as u64 > self.max_log_file_size {
            debug!(
                current_size = pos,
                max_log_file_size = self.max_log_file_size,
                active_file = ?self.writer.read().unwrap().active_log_file.display(),
                "Compaction required"
            );
            self.compact()?;
        }
        Ok(())
    }

    /// Retrieve the value of a key from the store with a timestamp of its entry.
    /// If the key does not exist, then [`None`] is returned.
    ///
    /// The timestamp is typically used with replication, as the value acts as
    /// a version number and conflict resolution mechanism.
    async fn get(&self, key: String) -> Result<Option<GetResponse>> {
        debug!(key, "Getting key");
        match self.keydir.get(&key) {
            Some(entry) => {
                debug!(
                    key = key,
                    offset = entry.offset,
                    "entry exists in {}",
                    entry.file_id.display(),
                );

                let mut entry_file = std::fs::File::open(&entry.file_id)?;
                entry_file.seek(SeekFrom::Start(entry.offset as u64))?;
                let log_entry: LogEntry = bincode::deserialize_from(entry_file)?;
                match log_entry.value {
                    Some(value) => {
                        debug!(value, "Value exists");
                        Ok(Some(GetResponse {
                            value: Some(value),
                            timestamp: log_entry.timestamp,
                        }))
                    }
                    // This is a tombstone value and equates to a deleted key and
                    // the "Key not found" scenario.
                    None => {
                        debug!(key, "Tombstone record found");
                        Ok(None)
                    }
                }
            }
            None => Ok(None),
        }
    }

    /// Remove a key from the store.
    async fn remove(&self, key: String) -> Result<()> {
        debug!(key, "Removing key");
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
                debug!(
                    position = pos,
                    active_file = ?self.writer.read().unwrap().active_log_file.display(),
                    "Tombstone written"
                );

                if pos as u64 > self.max_log_file_size {
                    self.compact()?;
                    debug!(
                        current_size = pos,
                        max_log_file_size = self.max_log_file_size,
                        active_file = ?self.writer.read().unwrap().active_log_file.display(),
                        "Compaction required"
                    );
                }
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
    fn new(config: StoreConfig) -> KvStore {
        KvStore {
            writer: Arc::new(RwLock::new(StoreWriter::default())),
            log_location: config.log_location,
            keydir: Arc::new(DashMap::new()),
            max_log_file_size: config.max_log_file_size,
            _tracing: None,
        }
    }

    /// Open a KvStore at the given path.
    pub fn open<P>(path: P) -> Result<KvStore>
    where
        P: Into<PathBuf>,
    {
        let path = path.into();
        let store_config = StoreConfig {
            log_location: path.clone(),
            max_log_file_size: MAX_LOG_FILE_SIZE.with(|f| *f),
        };

        let mut store = KvStore::new(store_config.clone());
        let log_level = std::env::var("KVS_LOG").unwrap_or("info".to_string());
        store.setup_logging(log_level)?;
        info!("Initialising store");
        store.load()?;
        debug!(compaction_trigger = store_config.max_log_file_size);

        debug!("Creating initial log file");
        store.set_active_log_handle()?;
        Ok(store)
    }

    /// Load the keydir from any log files which are found in the log directory.
    fn load(&self) -> Result<()> {
        let dir = &format!("{}/{}*.log", self.log_location.display(), LOG_PREFIX);
        debug!(glob_pattern = dir, "Searching for log files");
        match glob(dir) {
            Ok(files) => {
                info!("Rebuilding keydir");
                files.for_each(|file| {
                    let file = file.expect("Glob pattern found log files");
                    debug!(file = ?file.display(), "Reading log file");
                    let f = File::open(&file).unwrap();
                    let file_size = f.metadata().unwrap().len();

                    if file_size == 0 {
                        info!(?file, "Skipping empty log file");
                        return;
                    }
                    let mut reader = BufReaderWithOffset::new(f);
                    loop {
                        let pos = reader.offset();
                        if file_size as usize == pos {
                            break;
                        }
                        let entry: LogEntry = bincode::deserialize_from(&mut reader).unwrap();
                        debug!(?entry, pos);
                        match entry.operation {
                            Operation::Set => {
                                let file = file.clone();
                                self.keydir
                                    .entry(entry.key)
                                    .and_modify(|current_entry| {
                                        // Only update the entry when the log file contains
                                        // a newer item than in the current index.
                                        if entry.timestamp >= current_entry.timestamp {
                                            current_entry.file_id = file.clone();
                                            current_entry.offset = pos;
                                            current_entry.timestamp = entry.timestamp;
                                        }
                                    })
                                    .or_insert_with(|| KeydirEntry {
                                        file_id: file,
                                        offset: pos,
                                        timestamp: entry.timestamp,
                                    });
                            }
                            Operation::Remove => {
                                self.keydir.remove(&entry.key);
                            }
                            Operation::Get => warn!(?file, "Get operation in log file, skipping"),
                        }
                    }
                });
            }
            Err(e) => {
                warn!("No log files found, keydir will be empty: {}", e);
                return Ok(());
            }
        };
        Ok(())
    }

    /// Append a log into the active log file. This acts as a Write-ahead log (WAL)
    /// and is an operation which should be taken before other operations, such as
    /// updating the keydir.
    fn append_to_log(&self, entry: &LogEntry) -> Result<usize> {
        let data = bincode::serialize(&entry)?;

        let write_lock = self.writer.write().unwrap();
        let pos = write_lock
            .position
            .load(std::sync::atomic::Ordering::SeqCst);
        let mut file_lock = write_lock
            .active_log_handle
            .as_ref()
            .ok_or(KvStoreError::NoActiveLogFile)?
            .write()
            .unwrap();
        file_lock.write_at(&data, pos as u64)?;
        file_lock.flush()?;
        write_lock
            .position
            .fetch_add(data.len(), std::sync::atomic::Ordering::SeqCst);
        // Returning the offset of the entry in the log file after it has been written.
        // This means that the next entry is written after this one.
        Ok(pos)
    }

    /// The next log file name to use.
    ///
    /// This is generated by utilising the current timestamp in milliseconds
    /// and appending it to the static log prefix.
    fn next_log_file_name(&self) -> String {
        let now = chrono::Utc::now().timestamp_micros();
        format!(
            "{}{}.log",
            self.log_location.join(LOG_PREFIX).display(),
            now
        )
    }

    /// Perform compaction on the log file which has reached the threshold.
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
        let mut compaction_buf = BufWriter::new(&compaction_file);
        debug!(compaction_file = compacted_filename, "Compacting log files");

        let mut entries: Vec<(LogEntry, u64)> = Vec::new();
        debug!(keydir_size = self.keydir.len());

        // Maintain a map of handles to avoid opening a new file on every single
        // log entry multiple times.
        let file_handles = Arc::new(DashMap::new());
        // Build a map of active files, these are files which are still being referenced
        // in the keydir, which can include files from prior compaction.
        // Once they are no longer referenced, they can be safely removed.
        let active_files = Arc::new(DashMap::new());

        let mut compact_pos = 0;
        for entry in self.keydir.as_ref() {
            let mut file = file_handles
                .entry(entry.file_id.clone())
                .or_insert_with(|| {
                    BufReader::new(
                        std::fs::File::open(&entry.file_id)
                            .expect("Log file should exist for compaction"),
                    )
                });

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
            let written = compaction_buf.write(&data)?;

            entries.push((log_entry, compact_pos));
            compact_pos += written as u64;
        }
        compaction_buf.flush()?;

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

    pub(crate) fn set_active_log_handle(&self) -> Result<()> {
        let mut writer = self.writer.write().unwrap();
        let next_log_file_name = self.next_log_file_name();
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&next_log_file_name)?;
        debug!(active_file = next_log_file_name, "Created new log file");
        writer.active_log_file = PathBuf::from(next_log_file_name.clone());
        writer
            .position
            .store(0, std::sync::atomic::Ordering::SeqCst);
        writer.active_log_handle = Some(Arc::new(RwLock::new(log_file)));
        Ok(())
    }

    /// Detect the engine used to create the store.
    /// We must return an error if previously opened with another engine, as they are incompatible.
    pub fn engine_is_sqrl(current_engine: String, engine_path: PathBuf) -> Result<()> {
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

    pub(crate) fn setup_logging(&mut self, level: String) -> anyhow::Result<()> {
        let level = LevelFilter::from_str(&level)?;
        let layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
        let subscriber = tracing_subscriber::registry().with(level).with(layer);
        let tracing_guard = tracing::subscriber::set_default(subscriber);
        self._tracing = Some(Arc::new(tracing_guard));
        Ok(())
    }
}

// A wrapper around BufReader that tracks the offset.
struct BufReaderWithOffset<R: Read> {
    reader: BufReader<R>,
    offset: usize,
}

impl<R: Read> BufReaderWithOffset<R> {
    fn new(reader: R) -> Self {
        BufReaderWithOffset {
            reader: BufReader::new(reader),
            offset: 0,
        }
    }

    // Method to get the current offset
    fn offset(&self) -> usize {
        self.offset
    }
}

impl<R: Read> Read for BufReaderWithOffset<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let result = self.reader.read(buf);
        if let Ok(size) = result {
            self.offset += size;
        }
        result
    }
}
