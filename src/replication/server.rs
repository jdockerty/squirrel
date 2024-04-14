use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::client::Client;
use crate::proto::action_server::{Action, ActionServer};
use crate::proto::{Acknowledgement, GetRequest, GetResponse, RemoveRequest, SetRequest};
use crate::{KvsEngine, StandaloneServer};

/// Wrapped implementation of a [`StandaloneServer`] with an awareness of multiple
/// remote stores. These servers act as replication points and will be used to serve
/// requests based on a quorum read and write sequence.
#[derive(Clone)]
pub struct ReplicatedServer<C> {
    /// Identifier for the server.
    ///
    /// This is used for troubleshooting/debugging purposes.
    name: String,
    local_store: Arc<Mutex<StandaloneServer>>,
    addr: SocketAddr,
    remote_replicas: Arc<Mutex<Vec<C>>>,
}

impl<C> ReplicatedServer<C>
where
    C: Client + Clone + Send + Sync + 'static,
{
    pub fn new<P>(clients: Vec<C>, name: String, path: P, addr: SocketAddr) -> anyhow::Result<Self>
    where
        P: Into<PathBuf>,
    {
        Ok(Self {
            name,
            addr,
            local_store: Arc::new(Mutex::new(StandaloneServer::new(path, addr)?)),
            remote_replicas: Arc::new(Mutex::new(clients)),
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        info!("Listening on {}", self.addr);
        info!(
            "sqrl-server version: {}, engine: sqrl",
            env!("CARGO_PKG_VERSION"),
        );
        tonic::transport::Server::builder()
            .add_service(ActionServer::new(self.clone()))
            .serve(self.addr.clone())
            .await
            .unwrap();
        Ok(())
    }
}

#[tonic::async_trait]
impl<C> Action for ReplicatedServer<C>
where
    C: Client + Send + Sync + 'static,
{
    async fn get(
        &self,
        req: tonic::Request<GetRequest>,
    ) -> tonic::Result<tonic::Response<GetResponse>, tonic::Status> {
        info!("{} Replicated get", self.name);
        let req = req.into_inner();
        let key = req.key.clone();
        let guard = self.local_store.lock().await;
        let mut response = GetResponse {
            value: None,
            timestamp: 0,
        };
        match guard.store.get(key.clone()).await.unwrap() {
            Some(local_result) => {
                info!(
                    "{} has value locally, checking timestamps from replicas",
                    self.name
                );
                for r in self.remote_replicas.lock().await.iter_mut() {
                    match r.get(key.clone()).await.unwrap() {
                        Some(remote_result) => {
                            if remote_result.timestamp >= local_result.timestamp {
                                //r.set(key.clone(), local_result.value.clone().unwrap())
                                //    .await
                                //    .unwrap();
                                response = remote_result.clone();
                                guard
                                    .store
                                    .set(key.clone(), remote_result.value.unwrap())
                                    .await
                                    .unwrap();
                            }
                        }
                        None => response = local_result.clone(),
                    }
                }
                info!("{} respondind get {:?}", self.name, response);
                Ok(tonic::Response::new(response))
            }
            None => {
                info!(
                    "{} does not have {}, checking remotely",
                    self.name,
                    req.key.clone()
                );
                for r in self.remote_replicas.lock().await.iter_mut() {
                    match r.get(key.clone()).await.unwrap() {
                        Some(remote_result) => {
                            if let Some(v) = remote_result.clone().value {
                                if remote_result.timestamp > response.timestamp {
                                    response = remote_result;
                                    guard.store.set(req.key.clone(), v).await.unwrap();
                                }
                            }
                        }
                        None => {}
                    }
                }
                info!("{} respondind get {:?}", self.name, response);
                Ok(tonic::Response::new(response))
            }
        }
    }

    async fn set(
        &self,
        req: tonic::Request<SetRequest>,
    ) -> tonic::Result<tonic::Response<Acknowledgement>, tonic::Status> {
        info!("{} Replicated set", self.name);
        let req = req.into_inner();
        let local = self.local_store.lock().await;
        match local.store.get(req.key.clone()).await.unwrap() {
            Some(local_response) => {
                if req.timestamp > local_response.timestamp {
                    local
                        .store
                        .set(req.key.clone(), req.value.clone())
                        .await
                        .unwrap();
                    for r in self.remote_replicas.lock().await.iter_mut() {
                        r.set(req.key.clone(), req.value.clone()).await.unwrap();
                    }
                } else {
                    if let Some(value) = local_response.value {
                        for r in self.remote_replicas.lock().await.iter_mut() {
                            r.set(req.key.clone(), value.clone()).await.unwrap();
                        }
                    }
                }
            }
            None => local.store.set(req.key, req.value).await.unwrap(),
        }
        Ok(tonic::Response::new(Acknowledgement { success: true }))
    }

    async fn remove(
        &self,
        req: tonic::Request<RemoveRequest>,
    ) -> tonic::Result<tonic::Response<Acknowledgement>, tonic::Status> {
        info!("Replicated remove");
        Ok(tonic::Response::new(Acknowledgement { success: true }))
    }
}

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use crate::client::{Client, RemoteNodeClient, ReplicatedServer};
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
        // Binds to 6000 and connects to 6001 for replication
        let node_one = ReplicatedServer::new(
            vec![client_two().await],
            "node_one".to_string(),
            TempDir::new().unwrap().into_path(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();
        // Binds to 6001 and connects to 6000 for replication
        let node_two = ReplicatedServer::new(
            vec![client_one().await],
            "node_two".to_string(),
            TempDir::new().unwrap().into_path(),
            "127.0.0.1:6001".parse().unwrap(),
        )
        .unwrap();

        tokio::spawn(async move { node_one.run().await.unwrap() });
        tokio::spawn(async move { node_two.run().await.unwrap() });

        // Let the nodes startup
        thread::sleep(Duration::from_millis(1500));

        // Connect to node one
        let mut client = client_one().await;
        client
            .set("key1".to_string(), "value1".to_string())
            .await
            .unwrap();

        let mut client = client_two().await;
        assert_eq!(
            client.get("key1".to_string()).await.unwrap().unwrap().value,
            Some("value1".to_string()),
            "No replication from node one to node two"
        );
    }
}
