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
/// remote stores. These servers act as replication points and will be used to serve
/// requests based on a quorum read and write sequence.
#[derive(Clone)]
pub struct ReplicatedServer {
    /// Identifier for the server.
    ///
    /// This is used for troubleshooting/debugging purposes.
    name: String,
    local_store: Arc<StandaloneServer>,
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
        name: String,
        path: P,
        addr: SocketAddr,
    ) -> anyhow::Result<Self>
    where
        P: Into<PathBuf>,
    {
        Ok(Self {
            name,
            addr,
            local_store: Arc::new(StandaloneServer::new(path, addr)?),
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
        info!("{} Replicated get", self.name);
        let req = req.into_inner();
        let key = req.key.clone();
        let mut response = match self.local_store.store.get(key).await.unwrap() {
            Some(r) => r,
            None => GetResponse {
                value: None,
                timestamp: 0,
            },
        };

        let client_one = &self.remote_replicas[0];
        let client_two = &self.remote_replicas[1];

        if let Some(remote_response) = client_one.lock().await.get(req.key.clone()).await.unwrap() {
            info!("{} response from c1 {:?}", self.name, remote_response);
            if remote_response.timestamp > response.timestamp {
                info!("{} Client one has greater timestamp", self.name);
                response = remote_response.clone();
                self.local_store
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
            info!("{} response from c2 {:?}", self.name, remote_response);
            if remote_response.timestamp > response.timestamp {
                info!("{} Client two has greater timestamp", self.name);
                response = remote_response.clone();
                self.local_store
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
        info!("{} Replicated set", self.name);
        let req = req.into_inner();
        let local = match self.local_store.store.get(req.key.clone()).await.unwrap() {
            Some(r) => r,
            None => {
                self.local_store
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
            self.local_store
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
            info!("Replicating local to known replicas");
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

    #[tokio::test]
    async fn general_replication() {
        // Binds to 6000 and connects to 6001 for replication
        let node_one = ReplicatedServer::new(
            [client_two().await.into(), client_three().await.into()],
            "node_one".to_string(),
            TempDir::new().unwrap().into_path(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();
        // Binds to 6001 and connects to 6000 for replication
        let node_two = StandaloneServer::new(
            TempDir::new().unwrap().into_path(),
            "127.0.0.1:6001".parse().unwrap(),
        )
        .unwrap();

        // Binds to 6002 and connects to 6000 for replication
        let node_three = StandaloneServer::new(
            TempDir::new().unwrap().into_path(),
            "127.0.0.1:6002".parse().unwrap(),
        )
        .unwrap();

        tokio::spawn(async move { node_one.run().await.unwrap() });
        tokio::spawn(async move { node_two.run().await.unwrap() });
        tokio::spawn(async move { node_three.run().await.unwrap() });

        // Let the nodes startup
        thread::sleep(Duration::from_millis(1500));

        // Connect to the replication node
        let mut replicated_client = client_one().await;
        replicated_client
            .set("key1".to_string(), "value1".to_string())
            .await
            .unwrap();

        // Wait for some replication to occur.
        thread::sleep(Duration::from_millis(250));

        let mut standalone_client = client_two().await;
        assert_eq!(
            standalone_client
                .get("key1".to_string())
                .await
                .unwrap()
                .unwrap()
                .value,
            Some("value1".to_string()),
            "No replication"
        );
    }
}
