use tonic::transport::Channel;

use crate::proto::action_client::ActionClient;
use crate::proto::{Acknowledgement, RemoveRequest};
use crate::proto::{GetRequest, SetRequest};
use crate::Result;

/// A client used for interacting with the [`KvStore`] via gRPC requests.
#[tonic::async_trait]
pub trait Client {
    async fn get(&mut self, key: String) -> anyhow::Result<Option<String>>;

    async fn set(&mut self, key: String, value: String) -> anyhow::Result<Acknowledgement>;

    async fn remove(&mut self, key: String) -> anyhow::Result<Acknowledgement>;
}

/// A [`RemoteNodeClient`] is for interacting with a cache node over the network.
///
/// Owing to this, it can be used for both the user-facing interaction with the
/// service (client/server model) and when dealing with inter-node communication
/// for replication.
pub struct RemoteNodeClient {
    /// Inner gRPC client for actions that can be taken.
    inner: ActionClient<Channel>,

    /// Address of the cache server.
    pub addr: String,
}

impl RemoteNodeClient {
    pub async fn new(addr: String) -> Result<Self> {
        let inner = ActionClient::connect(format!("http://{}", addr)).await?;
        Ok(Self { inner, addr })
    }
}

#[tonic::async_trait]
impl Client for RemoteNodeClient {
    async fn set(&mut self, key: String, value: String) -> anyhow::Result<Acknowledgement> {
        let req = tonic::Request::new(SetRequest { key, value });
        let result = self.inner.set(req).await?;
        Ok(result.into_inner())
    }

    async fn get(&mut self, key: String) -> anyhow::Result<Option<String>> {
        let req = tonic::Request::new(GetRequest { key });
        Ok(self.inner.get(req).await?.into_inner().value)
    }

    async fn remove(&mut self, key: String) -> anyhow::Result<Acknowledgement> {
        let req = tonic::Request::new(RemoveRequest { key });
        let result = self.inner.remove(req).await?;
        Ok(result.into_inner())
    }
}
