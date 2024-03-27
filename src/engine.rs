use std::future::Future;

use crate::Result;

/// Generic trait implementation for pluggable storage engines outside of the one
/// implemented by this crate.
///
/// # Note
///
/// [`KvStore`] is the only current implementation of this trait within the crate.
/// However, it would be possible to implement other engines too, such as `sled`.
///
/// [`KvStore`]: crate::store::KvStore
pub trait KvsEngine: Clone + Send + 'static {
    fn set(&self, key: String, value: String) -> impl Future<Output = Result<()>>;
    fn get(&self, key: String) -> impl Future<Output = Result<Option<String>>>;
    fn remove(&self, key: String) -> impl Future<Output = Result<()>>;
}
