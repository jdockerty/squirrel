use chrono;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};
use thiserror::Error;

pub const LOG_NAME: &str = "kvs.log";

pub type Result<T> = std::result::Result<T, KvStoreError>;

#[derive(Debug, Error)]
pub enum KvStoreError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Get operation was stored in the log")]
    GetOperationInLog,

    #[error("Cannot remove non-existent key")]
    RemoveOperationWithNoKey,
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

    /// Byte offset of the entry in the log file, this is used for lookups in
    /// the future.
    offset: u64,
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

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            log_location: PathBuf::from(""),
            offset: 0,
        }
    }

    pub fn open<P>(path: P) -> Result<KvStore>
    where
        P: Into<PathBuf>,
    {
        let mut store = KvStore::new();
        let mut path = path.into();
        if path.is_dir() {
            path = path.join(LOG_NAME);
        }
        store.log_location = path;
        Ok(store)
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

        self.append_to_log(entry)?;
        Ok(())
    }

    /// Retrieve the value of a key from the store.
    /// If the key does not exist, then [`None`] is returned.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        let raw_schema = r#"
        {
            "type": "record",
            "name": "LogEntry",
            "fields": [
                {"name": "operation", "type": {"type": "enum", "name": "Operation", "symbols": ["Set", "Get", "Remove"]}},
                {"name": "timestamp", "type": "long"},
                {"name": "key_size", "type": "long"},
                {"name": "value_size", "type": "long"},
                {"name": "key", "type": "string"},
                {"name": "value", "type": ["null", "string"]}
            ]
        }
"#;
        let log_file = std::fs::OpenOptions::new()
            .read(true)
            .open(self.log_location.as_path())?;

        // let reader = BufReader::new(log_file);

        let mut h: HashMap<String, Option<String>> = HashMap::new();
        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();
        // let avro_reader = apache_avro::Reader::with_schema(&schema, log_file).unwrap();
        let avro_reader = apache_avro::Reader::new(log_file).unwrap();

        for result in avro_reader {
            println!("{:?}", result);
            //match result {
            //    Ok(avro_value) => {
            //        let log_entry: LogEntry = apache_avro::from_value(&avro_value).unwrap();
            //        println!("{:?}", log_entry);
            //    }
            //    Err(e) => eprintln!("Error reading Avro record: {:?}", e),
            //}
        }

        // Rebuilt the state of the store from the log.
        //reader.lines().for_each(|line| {
        //    // On writing, we end an entry with a new line, so we know that this is valid.
        //    let line = line.unwrap();
        //    let entry: LogEntry = serde_json::from_str(&line).unwrap();
        //    apache_avro::from

        //    match entry.operation {
        //        Operation::Set => h.insert(entry.key.clone(), entry.value),
        //        Operation::Remove => h.remove(&entry.key),
        //        // Get entries are not written to the log, so this should not happen.
        //        Operation::Get => None,
        //    };
        //});

        match h.get(&key) {
            Some(Some(inserted_value)) => Ok(Some(inserted_value.to_string())),
            Some(&None) => Ok(None),
            None => Ok(None),
        }
    }

    fn append_to_log(&mut self, entry: LogEntry) -> Result<()> {
        let raw_schema = r#"
        {
            "type": "record",
            "name": "LogEntry",
            "fields": [
                {"name": "operation", "type": {"type": "enum", "name": "Operation", "symbols": ["Set", "Get", "Remove"]}},
                {"name": "timestamp", "type": "long"},
                {"name": "key_size", "type": "long"},
                {"name": "value_size", "type": "long"},
                {"name": "key", "type": "string"},
                {"name": "value", "type": ["null", "string"]}
            ]
        }
"#;

        // if the schema is not valid, this function will return an error
        let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();
        let log_file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(self.log_location.as_path())?;

        //        let mut writer =
        //           apache_avro::Writer::with_codec(&schema, &log_file, apache_avro::Codec::Snappy);
        let mut writer = apache_avro::Writer::new(&schema, &log_file);
        writer.append_ser(&entry).unwrap();
        let wrote = writer.into_inner().unwrap();
        println!("Wrote {:?} bytes", wrote);

        // TODO: This is always 0 currently
        let offset = log_file.metadata()?.len();
        self.offset = offset;
        Ok(())
    }

    /// Remove a key from the store.
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.get(key.clone())? {
            Some(_) => {
                let entry = LogEntry {
                    timestamp: chrono::Utc::now().timestamp(),
                    operation: Operation::Remove,
                    key_size: key.len(),
                    key,
                    value: None,
                    value_size: 0,
                };

                self.append_to_log(entry)?;

                Ok(())
            }
            None => Err(KvStoreError::RemoveOperationWithNoKey),
        }
    }
}
