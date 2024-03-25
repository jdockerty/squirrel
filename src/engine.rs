use std::future::Future;

use crate::Result;

pub trait KvsEngine: Clone + Send + 'static {
    fn set(&self, key: String, value: String) -> impl Future<Output = Result<()>>;
    fn get(&self, key: String) -> impl Future<Output = Result<Option<String>>>;
    fn remove(&self, key: String) -> impl Future<Output = Result<()>>;
}
