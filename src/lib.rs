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

enum Phase {
    Init,
    Standard,
}

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

    /// Index of the file in the log file list.
    index: usize,
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

fn create_log_file(store: &mut KvStore, phase: Phase) -> Result<std::fs::File> {
    // Handling file creation based on the phase, this largely handles the
    // special case where the initial log file is created.
    match phase {
        Phase::Init => {
            let init_name = format!("{}0", store.log_location.join(LOG_PREFIX).display());
            let log_file =
                std::fs::File::create(&init_name).map_err(|e| KvStoreError::IoError {
                    source: e,
                    filename: init_name.clone(),
                })?;
            let active_file = LogFile {
                active: true,
                name: init_name.clone(),
                index: store.file_index,
            };

            // Active file is always at the back of the queue.
            store.log_files.push_back(active_file);
            store.active_log_file = PathBuf::from(init_name.clone());
            Ok(log_file)
        }
        Phase::Standard => {
            let mut current_active_file = store.get_active_file();
            let active_index = current_active_file.index;
            current_active_file.active = false;
            store.log_files[active_index] = current_active_file;

            store.increment_file_index();
            // The file index is incremented before the new file is created.
            let next_log_file_name = store.current_log_file_name();
            let log_file =
                std::fs::File::create(&next_log_file_name).map_err(|e| KvStoreError::IoError {
                    source: e,
                    filename: next_log_file_name.clone(),
                })?;
            store.active_log_file = PathBuf::from(next_log_file_name.clone());

            let new_file = LogFile {
                active: true,
                name: next_log_file_name,
                index: store.file_index,
            };

            // Active file is always at the back of the queue.
            store.log_files.push_back(new_file);

            if store.log_files.len() > store.max_num_log_files as usize {
                store.compact()?;
            }
            Ok(log_file)
        }
    }
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
        let mut keydir_path = PathBuf::default();

        if path.is_dir() {
            keydir_path = path.join(KEYDIR_NAME);
        }

        store.log_location = path.clone();
        store.keydir_location = keydir_path;

        create_log_file(&mut store, Phase::Init)?;

        // TODO: the create_new_log_file logic is the same here, but this is used
        // for initialisation.
        // Should we change the create_new_log_file signature to accept a KvStore
        // instead of self.
        //let full_path = format!(
        //    "{}{}",
        //    store.log_location.join(LOG_PREFIX).display(),
        //    store.file_index
        //);

        //// Increment the file index after initialisation.
        //store.increment_file_index();

        //std::fs::OpenOptions::new()
        //    .write(true)
        //    .create(true)
        //    .open(PathBuf::from(full_path.clone()))
        //    .map_err(|e| KvStoreError::IoError {
        //        source: e,
        //        filename: full_path.clone(),
        //    })?;

        //store.log_files.push_back(LogFile {
        //    active: true,
        //    name: full_path.clone(),
        //    index: 0,
        //});

        //store.active_log_file = full_path.clone().into();

        //std::fs::OpenOptions::new()
        //    .write(true)
        //    .create(true)
        //    .open(store.keydir_location.as_path())
        //    .map_err(|e| KvStoreError::IoError {
        //        source: e,
        //        filename: store.keydir_location.display().to_string(),
        //    })?;
        Ok(store)
    }

    fn current_log_file_name(&self) -> String {
        format!(
            "{}{}",
            self.log_location.join(LOG_PREFIX).display(),
            self.get_active_file().index
        )
    }

    fn increment_file_index(&mut self) {
        self.file_index += 1;
    }

    fn get_active_file(&self) -> LogFile {
        self.log_files
            .back()
            .expect("No active file found")
            .to_owned()
    }

    //fn create_new_log_file(&mut store: KvStore) -> Result<()> {
    //    println!("Index: {}", store..file_index);
    //    let mut current_file = self.get_active_file();
    //    let current_name = current_file.name.clone();
    //    println!("Current log file: {}", current_name);

    //    // Back of the queue is the active file.
    //    //
    //    let active_index = current_file.index;
    //    current_file.active = false;
    //    self.log_files[active_index] = current_file;

    //    println!("Current file index: {}", self.file_index);
    //    self.increment_file_index();
    //    println!("New file index: {}", self.file_index);

    //    let next_log_file_name = self.current_log_file_name();
    //    println!("Creating new log file: {}", next_log_file_name);
    //    self.active_log_file = PathBuf::from(next_log_file_name.clone());

    //    std::fs::OpenOptions::new()
    //        .write(true)
    //        .create(true)
    //        .open(next_log_file_name.clone())
    //        .map_err(|e| KvStoreError::IoError {
    //            source: e,
    //            filename: current_name.clone(),
    //        })?;

    //    self.log_files.push_back(LogFile {
    //        active: true,
    //        name: next_log_file_name,
    //        index: self.file_index,
    //    });

    //    if self.log_files.len() > self.max_num_log_files as usize {
    //        self.compact()?;
    //    }

    //    Ok(())
    //}

    fn open_active_log_file(&mut self) -> Result<std::fs::File> {
        if std::path::Path::exists(self.active_log_file.as_path()) {
            println!("Opening file: {:?}", self.active_log_file);
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
            println!("Creating initial log file");
            // File needs to be created.
            Ok(create_log_file(self, Phase::Init)?)
        }
    }

    fn compact(&mut self) -> Result<()> {
        println!("Compacting");

        while self.log_files.len() > self.max_num_log_files as usize {
            println!("Files: {:?}", self.log_files);
            let file = self.log_files.pop_front().unwrap();
            println!("Removing file: {:?}", file);
            std::fs::remove_file(&file.name).map_err(|e| KvStoreError::IoError {
                source: e,
                filename: file.name.clone(),
            })?;
        }

        println!("Compaction complete: {:?}", self.log_files);

        self.file_index = self.get_active_file().index;

        Ok(())
    }

    fn append_to_log(&mut self, entry: &LogEntry) -> Result<u64> {
        let mut log_file = self.open_active_log_file()?;
        println!("Log file: {:?}, Entry: {:?}", log_file, entry);

        // The offset is where the last entry in the log file ends, this is because
        // the log file is an append-only log.
        let mut offset = log_file
            .metadata()
            .map_err(|e| KvStoreError::IoError {
                source: e,
                filename: self.active_log_file.as_path().to_string_lossy().to_string(),
            })?
            .len();

        if offset > self.max_log_file_size {
            println!("Log file is too large, creating new file");
            // Update to new file.
            log_file = create_log_file(self, Phase::Standard)?;
            // Reset offset to 0.
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
                println!("Log file: {:?}", log_file);

                let log_file_size = log_file
                    .metadata()
                    .map_err(|e| KvStoreError::IoError {
                        source: e,
                        filename: self.active_log_file.as_path().to_string_lossy().to_string(),
                    })?
                    .len();

                if log_file_size == 0 {
                    println!("Log file is empty");
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
                println!("Entry: {:?}", entry);
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
