use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug)]
pub enum KvStoreError {
    KeyNotFound,
    IoError(std::io::Error),
}

impl std::error::Error for KvStoreError {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        match *self {
            KvStoreError::KeyNotFound => None,
            KvStoreError::IoError(ref err) => Some(err),
        }
    }
}

impl std::fmt::Display for KvStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            KvStoreError::KeyNotFound => write!(f, "Key not found"),
            KvStoreError::IoError(ref err) => err.fmt(f),
        }
    }
}

pub type Result<T> = std::result::Result<T, KvStoreError>;

/// An in-memory key-value store, backed by a [`HashMap`] from the standard library.
pub struct KvStore {
    store: HashMap<String, String>,
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            store: HashMap::new(),
        }
    }

    pub fn open<F>(file: F) -> Result<KvStore>
    where
        F: Into<PathBuf>,
    {
        todo!()
    }

    /// Set the value of a key by inserting the value into the store for the given key.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.store.insert(key, value);
        Ok(())
    }

    /// Retrieve the value of a key from the store.
    /// If the key does not exist, then [`None`] is returned.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.store.get(&key).map(|val| val.to_string()))
    }

    /// Remove a key from the store.
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.store.remove(&key);
        Ok(())
    }
}
