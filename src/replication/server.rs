use prost::bytes::BufMut;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::client::Client;
use crate::proto::action_client::ActionClient;
use crate::proto::{Acknowledgement, GetResponse};
use crate::{KvsEngine, StandaloneServer};

/// Wrapped implementation of a [`StandaloneServer`] with an awareness of multiple
/// remote stores. These servers act as replication points and will be used to serve
/// requests based on a quorum read and write sequence.
#[derive(Clone)]
pub struct ReplicatedServer<C> {
    local_store: Arc<Mutex<StandaloneServer>>,
    remote_replicas: Arc<Mutex<Vec<C>>>,
}

impl<C> ReplicatedServer<C>
where
    C: Client,
{
    pub fn new<P>(clients: Vec<C>, path: P, addr: SocketAddr) -> anyhow::Result<Self>
    where
        P: Into<PathBuf>,
    {
        Ok(Self {
            local_store: Arc::new(Mutex::new(StandaloneServer::new(path, addr)?)),
            remote_replicas: Arc::new(Mutex::new(clients)),
        })
    }
}

#[tonic::async_trait]
impl<C> Client for ReplicatedServer<C>
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

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use crate::client::{self, Client, RemoteNodeClient, ReplicatedServer};
    use tempfile::TempDir;

    async fn client_one() -> RemoteNodeClient {
        RemoteNodeClient::new("127.0.0.1:6000".to_string())
            .await
            .unwrap()
    }

    async fn client_two() -> RemoteNodeClient {
        RemoteNodeClient::new("127.0.0.1:6001".to_string())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn general_replication() {
        let node_one = ReplicatedServer::new(
            vec![client_two().await],
            TempDir::new().unwrap().into_path(),
            "127.0.0.1:6000".parse().unwrap(),
        );

        let node_one = ReplicatedServer::new(
            vec![client_one().await],
            TempDir::new().unwrap().into_path(),
            "127.0.0.1:6001".parse().unwrap(),
        );

        thread::sleep(Duration::from_secs(1));

        //let mut client = client_one().await;
        //client.set("key1".to_string(), "value1".to_string()).await.unwrap();

        //let mut client = client_two().await;
        //assert_eq!(client.get("key1".to_string()).await.unwrap().unwrap().value, Some("value1".to_string()));
    }
}
