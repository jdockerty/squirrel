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
    local_store: Arc<Mutex<StandaloneServer>>,
    addr: SocketAddr,
    remote_replicas: Arc<Mutex<Vec<C>>>,
}

impl<C> ReplicatedServer<C>
where
    C: Client + Send,
{
    pub fn new<P>(clients: Vec<C>, path: P, addr: SocketAddr) -> anyhow::Result<Self>
    where
        P: Into<PathBuf>,
    {
        Ok(Self {
            addr,
            local_store: Arc::new(Mutex::new(StandaloneServer::new(path, addr)?)),
            remote_replicas: Arc::new(Mutex::new(clients)),
        })
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        info!(
            "Listening on {}\nsqrl-server version: {}, engine: sqrl",
            self.addr,
            env!("CARGO_PKG_VERSION"),
        );
        let local = self.local_store.clone();
        tonic::transport::Server::builder()
            .add_service(ActionServer::new(local.lock().await.clone()))
            .serve(self.addr)
            .await
            .unwrap();
        info!("Started gRPC server");
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
        info!("Replicated get");
        let req = req.into_inner();
        let key = req.key;
        let guard = self.local_store.lock().await;
        match guard.store.get(key.clone()).await.unwrap() {
            Some(local_result) => {
                let mut quorum_results = vec![];
                for r in self.remote_replicas.lock().await.iter_mut() {
                    match r.get(key.clone()).await.unwrap() {
                        Some(remote_result) => {
                            if local_result.timestamp >= remote_result.timestamp {
                                r.set(key.clone(), local_result.value.clone().unwrap())
                                    .await
                                    .unwrap();
                            } else {
                                guard
                                    .store
                                    .set(key.clone(), remote_result.value.clone().unwrap())
                                    .await
                                    .unwrap();
                                quorum_results.push(remote_result.value);
                            }
                        }
                        None => {}
                    }
                }
            }
            None => {}
        }
        Ok(tonic::Response::new(GetResponse {
            value: None,
            timestamp: 0,
        }))
    }

    async fn set(
        &self,
        req: tonic::Request<SetRequest>,
    ) -> tonic::Result<tonic::Response<Acknowledgement>, tonic::Status> {
        info!("Replicated set");
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
            TempDir::new().unwrap().into_path(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();
        tokio::spawn(async move { node_one.run().await.unwrap() });

        // Binds to 6001 and connects to 6000 for replication
        let node_two = ReplicatedServer::new(
            vec![client_one().await],
            TempDir::new().unwrap().into_path(),
            "127.0.0.1:6001".parse().unwrap(),
        )
        .unwrap();
        tokio::spawn(async move { node_two.run().await.unwrap() });

        // Let the nodes startup
        thread::sleep(Duration::from_secs(1));

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
