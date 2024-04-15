use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::client::{Client, RemoteNodeClient};
use crate::proto::action_server::{Action, ActionServer};
use crate::proto::{Acknowledgement, GetRequest, GetResponse, RemoveRequest, SetRequest};
use crate::{KvsEngine, StandaloneServer};

/// Wrapped implementation of a [`StandaloneServer`] with an awareness of multiple
/// remote stores. This server will replicate values to known servers through its
/// internally held [`RemoteNodeClient`]'s.
#[derive(Clone)]
pub struct ReplicatedServer {
    server: Arc<StandaloneServer>,
    addr: SocketAddr,

    /// [`Client`] implementations which provide access to remote replicas.
    ///
    /// # Notes
    /// This is locked at 2 replicas for simplicitiy in handling the replication
    /// logic of the stores.
    remote_replicas: Arc<[Mutex<RemoteNodeClient>; 2]>,
}

impl ReplicatedServer {
    pub fn new<P>(
        clients: [Mutex<RemoteNodeClient>; 2],
        path: P,
        addr: SocketAddr,
    ) -> anyhow::Result<Self>
    where
        P: Into<PathBuf>,
    {
        Ok(Self {
            addr,
            server: Arc::new(StandaloneServer::new(path, addr)?),
            remote_replicas: Arc::new(clients),
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
impl Action for ReplicatedServer {
    async fn get(
        &self,
        req: tonic::Request<GetRequest>,
    ) -> tonic::Result<tonic::Response<GetResponse>, tonic::Status> {
        let req = req.into_inner();
        let key = req.key.clone();
        let mut response = match self.server.store.get(key).await.unwrap() {
            Some(r) => r,
            None => GetResponse {
                value: None,
                timestamp: 0,
            },
        };

        let client_one = &self.remote_replicas[0];
        let client_two = &self.remote_replicas[1];

        if let Some(remote_response) = client_one.lock().await.get(req.key.clone()).await.unwrap() {
            if remote_response.timestamp > response.timestamp {
                response = remote_response.clone();
                self.server
                    .store
                    .set(req.key.clone(), remote_response.value.clone().unwrap())
                    .await
                    .unwrap();
                client_two
                    .lock()
                    .await
                    .set(req.key.clone(), remote_response.value.unwrap())
                    .await
                    .unwrap();
            }
        }
        if let Some(remote_response) = client_two.lock().await.get(req.key.clone()).await.unwrap() {
            if remote_response.timestamp > response.timestamp {
                response = remote_response.clone();
                self.server
                    .store
                    .set(req.key.clone(), remote_response.value.clone().unwrap())
                    .await
                    .unwrap();
                client_one
                    .lock()
                    .await
                    .set(req.key.clone(), remote_response.value.unwrap())
                    .await
                    .unwrap();
            }
        }
        Ok(tonic::Response::new(response))
    }

    async fn set(
        &self,
        req: tonic::Request<SetRequest>,
    ) -> tonic::Result<tonic::Response<Acknowledgement>, tonic::Status> {
        let req = req.into_inner();
        let local = match self.server.store.get(req.key.clone()).await.unwrap() {
            Some(r) => r,
            None => {
                self.server
                    .store
                    .set(req.key.clone(), req.value.clone())
                    .await
                    .unwrap();
                GetResponse {
                    value: None,
                    timestamp: 0,
                }
            }
        };
        let client_one = &self.remote_replicas[0];
        let client_two = &self.remote_replicas[1];

        if req.timestamp > local.timestamp {
            info!("Updating local node");
            self.server
                .store
                .set(req.key.clone(), req.value.clone())
                .await
                .unwrap();
            info!("Replicating request to known replicas");
            client_one
                .lock()
                .await
                .set(req.key.clone(), req.value.clone())
                .await
                .unwrap();
            client_two
                .lock()
                .await
                .set(req.key.clone(), req.value.clone())
                .await
                .unwrap();
        } else {
            info!("Replicating local value to known replicas");
            client_one
                .lock()
                .await
                .set(req.key.clone(), local.value.clone().unwrap())
                .await
                .unwrap();
            client_two
                .lock()
                .await
                .set(req.key.clone(), local.value.clone().unwrap())
                .await
                .unwrap();
        }
        Ok(tonic::Response::new(Acknowledgement { success: true }))
    }

    async fn remove(
        &self,
        req: tonic::Request<RemoveRequest>,
    ) -> tonic::Result<tonic::Response<Acknowledgement>, tonic::Status> {
        info!("Replicated remove");
        let req = req.into_inner();
        self.server.store.remove(req.key.clone()).await.unwrap();
        self.remote_replicas[0]
            .lock()
            .await
            .remove(req.key.clone())
            .await
            .unwrap();
        self.remote_replicas[1]
            .lock()
            .await
            .remove(req.key.clone())
            .await
            .unwrap();
        Ok(tonic::Response::new(Acknowledgement { success: true }))
    }
}

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use crate::{
        client::{Client, RemoteNodeClient, ReplicatedServer},
        StandaloneServer,
    };
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

    async fn client_three() -> RemoteNodeClient {
        RemoteNodeClient::new("127.0.0.1:6002".to_string())
            .await
            .unwrap()
    }

    async fn replication_node(addr: String) -> ReplicatedServer {
        ReplicatedServer::new(
            [client_two().await.into(), client_three().await.into()],
            TempDir::new().unwrap().into_path(),
            addr.parse().unwrap(),
        )
        .unwrap()
    }

    async fn follower_node(addr: String) -> StandaloneServer {
        StandaloneServer::new(TempDir::new().unwrap().into_path(), addr.parse().unwrap()).unwrap()
    }

    #[tokio::test]
    async fn replication() {
        // Binds to 6000 and connects to the follower nodes for replication.
        let replication_node = replication_node("127.0.0.1:6000".to_string()).await;

        let node_two = follower_node("127.0.0.1:6001".to_string()).await;
        let node_three = follower_node("127.0.0.1:6002".to_string()).await;

        tokio::spawn(async move { replication_node.run().await.unwrap() });
        tokio::spawn(async move { node_two.run().await.unwrap() });
        tokio::spawn(async move { node_three.run().await.unwrap() });

        // Connect to the replication node
        let mut replicated_client = client_one().await;
        let mut standalone_client_one = client_two().await;
        let mut standalone_client_two = client_three().await;

        let wait_for_replication = || {
            thread::sleep(Duration::from_secs(1));
        };

        // Let the nodes startup
        thread::sleep(Duration::from_millis(1500));

        replicated_client
            .set("key1".to_string(), "value1".to_string())
            .await
            .unwrap();

        wait_for_replication();

        assert_eq!(
            standalone_client_one
                .get("key1".to_string())
                .await
                .unwrap()
                .unwrap()
                .value,
            Some("value1".to_string()),
            "No replication for initial value"
        );
        assert_eq!(
            standalone_client_one.get("key2".to_string()).await.unwrap(),
            None
        );
        assert_eq!(
            standalone_client_two
                .get("key1".to_string())
                .await
                .unwrap()
                .unwrap()
                .value,
            Some("value1".to_string()),
            "No replication for initial value"
        );

        replicated_client
            .set("key1".to_string(), "overwritten".to_string())
            .await
            .unwrap();
        wait_for_replication();
        assert_eq!(
            standalone_client_one
                .get("key1".to_string())
                .await
                .unwrap()
                .unwrap()
                .value,
            Some("overwritten".to_string()),
            "No replication for overwritten value"
        );
        assert_eq!(
            standalone_client_two
                .get("key1".to_string())
                .await
                .unwrap()
                .unwrap()
                .value,
            Some("overwritten".to_string()),
            "No replication for overwritten value"
        );

        replicated_client.remove("key1".to_string()).await.unwrap();
        wait_for_replication();
        assert_eq!(
            standalone_client_one.get("key1".to_string()).await.unwrap(),
            None,
            "No replication for removal"
        );
        assert_eq!(
            standalone_client_two.get("key1".to_string()).await.unwrap(),
            None,
            "No replication for removal"
        );
    }
}
