use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::client::Client;
use crate::proto::{Acknowledgement, GetResponse};
use crate::{KvServer, KvsEngine};

/// Implementation of a [`Client`] with an awareness of multiple remote cache
/// servers. These servers act as replication points and will be used to serve
/// requests based on a quorum read/write sequence.
#[derive(Clone)]
pub struct ReplicationClient<C> {
    local_store: Arc<Mutex<KvServer>>,
    remote_replicas: Arc<Mutex<Vec<C>>>,
}

impl<C> ReplicationClient<C>
where
    C: Client,
{
    pub fn new<P>(clients: Vec<C>, path: P, addr: SocketAddr) -> anyhow::Result<Self>
    where
        P: Into<PathBuf>,
    {
        Ok(Self {
            local_store: Arc::new(Mutex::new(KvServer::new(path, addr)?)),
            remote_replicas: Arc::new(Mutex::new(clients)),
        })
    }
}

#[tonic::async_trait]
impl<C> Client for ReplicationClient<C>
where
    C: Client + Send + Sync,
{
    async fn get(&mut self, key: String) -> anyhow::Result<Option<GetResponse>> {
        let guard = self.local_store.lock().await;
        match guard.store.get(key.clone()).await? {
            Some(local_result) => {
                let mut quorum_results = vec![];
                for r in self.remote_replicas.lock().await.iter_mut() {
                    match r.get(key.clone()).await? {
                        Some(remote_result) => {
                            if local_result.timestamp >= remote_result.timestamp {
                                r.set(key.clone(), local_result.value.clone().unwrap())
                                    .await?;
                            } else {
                                guard
                                    .store
                                    .set(key.clone(), remote_result.value.clone().unwrap())
                                    .await?;
                                quorum_results.push(remote_result.value);
                            }
                        }
                        None => {}
                    }
                }
            }
            None => {}
        }
        Ok(None)
    }

    async fn set(&mut self, key: String, value: String) -> anyhow::Result<Acknowledgement> {
        Ok(Acknowledgement { success: true })
    }

    async fn remove(&mut self, key: String) -> anyhow::Result<Acknowledgement> {
        Ok(Acknowledgement { success: true })
    }
}
