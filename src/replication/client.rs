use std::sync::Arc;
use tokio::sync::Mutex;

use crate::client::Client;
use crate::proto::Acknowledgement;
use crate::{KvStore, KvsEngine};

/// Implementation of a [`Client`] with an awareness of multiple remote cache
/// servers. These servers act as replication points and will be used to serve
/// requests based on a quorum read/write sequence.
#[derive(Clone, Debug)]
struct ReplicationClient<C> {
    local_store: Arc<Mutex<KvStore>>,
    remote_replicas: Arc<Mutex<Vec<C>>>,
}

#[tonic::async_trait]
impl<C> Client for ReplicationClient<C>
where
    C: Client + Send + Sync,
{
    async fn get(&mut self, key: String) -> anyhow::Result<Option<String>> {
        if let Some((local_value, local_timestamp)) = self
            .local_store
            .lock()
            .await
            .get_with_ts(key.clone())
            .await?
        {
            for r in self.remote_replicas.lock().await.iter_mut() {
                let remote_value = r.get(key.clone()).await?;
            }
            Ok(None)
        } else {
            Ok(None)
        }
    }

    async fn set(&mut self, key: String, value: String) -> anyhow::Result<Acknowledgement> {
        Ok(Acknowledgement { success: true })
    }

    async fn remove(&mut self, key: String) -> anyhow::Result<Acknowledgement> {
        Ok(Acknowledgement { success: true })
    }
}
