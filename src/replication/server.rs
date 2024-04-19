use futures::StreamExt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::client::{Client, RemoteNodeClient};
use crate::proto::action_server::{Action, ActionServer};
use crate::proto::{Acknowledgement, GetRequest, GetResponse, RemoveRequest, SetRequest};
use crate::store::Value;
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
    remote_replicas: Arc<Mutex<Vec<RemoteNodeClient>>>,
}

impl ReplicatedServer {
    pub fn new<P>(
        clients: Mutex<Vec<RemoteNodeClient>>,
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
            .serve(self.addr)
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
        let response = match self.server.store.get(key).await.unwrap() {
            Some(r) => r,
            None => Value(None),
        };
        Ok(tonic::Response::new(GetResponse { value: response.0 }))
    }

    async fn set(
        &self,
        req: tonic::Request<SetRequest>,
    ) -> tonic::Result<tonic::Response<Acknowledgement>, tonic::Status> {
        let req = req.into_inner();
        debug!("Setting value to local store");
        self.server
            .store
            .set(req.key.clone(), Value(Some(req.value.clone())))
            .await
            .unwrap();

        debug!("Replicating to remote replicas");
        futures::stream::iter(self.remote_replicas.lock().await.iter_mut())
            .for_each(|r| async {
                r.set(req.key.clone(), Value(Some(req.value.clone())))
                    .await
                    .unwrap();
            })
            .await;

        Ok(tonic::Response::new(Acknowledgement { success: true }))
    }

    async fn remove(
        &self,
        req: tonic::Request<RemoveRequest>,
    ) -> tonic::Result<tonic::Response<Acknowledgement>, tonic::Status> {
        info!("Replicated remove");
        let req = req.into_inner();
        self.server.store.remove(req.key.clone()).await.unwrap();
        futures::stream::iter(self.remote_replicas.lock().await.iter_mut())
            .for_each(|r| async {
                r.remove(req.key.clone()).await.unwrap();
            })
            .await;
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
            Vec::from_iter([client_two().await, client_three().await]).into(),
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
            .set("key1".to_string(), "value1".into())
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
            Some("value1".into()),
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
            Some("value1".into()),
            "No replication for initial value"
        );

        replicated_client
            .set("key1".to_string(), "overwritten".into())
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
            Some("overwritten".into()),
            "No replication for overwritten value"
        );
        assert_eq!(
            standalone_client_two
                .get("key1".to_string())
                .await
                .unwrap()
                .unwrap()
                .value,
            Some("overwritten".into()),
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
