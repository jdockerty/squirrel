use futures::future::Either;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
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
    local_store: Arc<StandaloneServer>,
    addr: SocketAddr,

    /// [`Client`] implementations which provide access to remote replicas.
    ///
    /// # Notes
    /// This is locked at 2 replicas for simplicitiy in handling the replication
    /// logic of the stores.
    remote_replicas: Arc<[C; 2]>,
}

impl<C> ReplicatedServer<C>
where
    C: Client + Clone + Send + Sync + 'static,
{
    pub fn new<P>(clients: [C; 2], name: String, path: P, addr: SocketAddr) -> anyhow::Result<Self>
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
        let guard = &self.local_store;
        let response = match guard.store.get(key).await.unwrap() {
            Some(r) => r,
            None => GetResponse {
                value: None,
                timestamp: 0,
            },
        };

        let client_one = &self.remote_replicas[0];
        let client_two = &self.remote_replicas[1];

        Ok(tonic::Response::new(response))
    }

    async fn set(
        &self,
        req: tonic::Request<SetRequest>,
    ) -> tonic::Result<tonic::Response<Acknowledgement>, tonic::Status> {
        info!("{} Replicated set", self.name);
        let req = req.into_inner();
        self.local_store
            .store
            .set(req.key.clone(), req.value.clone())
            .await
            .unwrap();
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

    async fn client_three() -> RemoteNodeClient {
        RemoteNodeClient::new("127.0.0.1:6002".to_string())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn general_replication() {
        // Binds to 6000 and connects to 6001 for replication
        let node_one = ReplicatedServer::new(
            [client_two().await, client_three().await],
            "node_one".to_string(),
            TempDir::new().unwrap().into_path(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();
        // Binds to 6001 and connects to 6000 for replication
        let node_two = ReplicatedServer::new(
            [client_one().await, client_three().await],
            "node_two".to_string(),
            TempDir::new().unwrap().into_path(),
            "127.0.0.1:6001".parse().unwrap(),
        )
        .unwrap();

        // Binds to 6001 and connects to 6000 for replication
        let node_three = ReplicatedServer::new(
            [client_one().await, client_two().await],
            "node_two".to_string(),
            TempDir::new().unwrap().into_path(),
            "127.0.0.1:6002".parse().unwrap(),
        )
        .unwrap();

        tokio::spawn(async move { node_one.run().await.unwrap() });
        tokio::spawn(async move { node_two.run().await.unwrap() });
        tokio::spawn(async move { node_three.run().await.unwrap() });

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
