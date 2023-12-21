use std::collections::HashMap;

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

    /// Set the value of a key by inserting the value into the store for the given key.
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    /// Retrieve the value of a key from the store.
    /// If the key does not exist, then [`None`] is returned.
    pub fn get(&self, key: String) -> Option<String> {
        Some(self.store.get(&key)?.to_string())
    }

    /// Remove a key from the store.
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key);
    }
}
