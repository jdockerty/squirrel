use std::future::Future;

use crate::{store::StoreValue, Result};

/// Generic trait implementation for pluggable storage engines outside of the one
/// implemented by this crate.
///
/// # Note
///
/// [`KvStore`] is the only current implementation of this trait within the crate.
/// However, it would be possible to implement other engines too, such as `sled`.
///
/// [`KvStore`]: crate::store::KvStore
pub trait KvsEngine: Clone + Send + Sync + 'static {
    fn set(&self, key: String, value: StoreValue) -> impl Future<Output = Result<()>>;
    fn get(&self, key: String) -> impl Future<Output = Result<Option<StoreValue>>>;
    fn remove(&self, key: String) -> impl Future<Output = Result<()>>;
}
