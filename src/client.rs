use std::str::FromStr;
use tonic::transport::Channel;
use tracing::info;

use crate::proto::action_client::ActionClient;
use crate::proto::{Acknowledgement, GetResponse, RemoveRequest};
use crate::proto::{GetRequest, SetRequest};
pub use crate::replication::ReplicatedServer;
use crate::Result;

/// A client used for interacting with the [`KvStore`] via gRPC requests.
#[tonic::async_trait]
pub trait Client {
    async fn get(&mut self, key: String) -> anyhow::Result<Option<GetResponse>>;

    async fn set(&mut self, key: String, value: String) -> anyhow::Result<Acknowledgement>;

    async fn remove(&mut self, key: String) -> anyhow::Result<Acknowledgement>;
}

/// A [`RemoteNodeClient`] is for interacting with a cache node over the network.
///
/// Owing to this, it can be used for both the user-facing interaction with the
/// service (client/server model) and when dealing with inter-node communication
/// for replication.
#[derive(Clone)]
pub struct RemoteNodeClient {
    /// Inner gRPC client for actions that can be taken.
    inner: ActionClient<Channel>,

    /// Address of the cache server.
    pub addr: String,
}

impl RemoteNodeClient {
    /// Provides a new [`RemoteNodeClient`].
    ///
    /// The channel used for the connection is not utilised until first use.
    pub async fn new(addr: String) -> Result<Self> {
        let ep = tonic::transport::Endpoint::from_str(&format!("http://{}", addr))?;
        let inner = ActionClient::new(ep.connect_lazy());
        Ok(Self { inner, addr })
    }
}

#[tonic::async_trait]
impl Client for RemoteNodeClient {
    async fn set(&mut self, key: String, value: String) -> anyhow::Result<Acknowledgement> {
        info!("Set from RemoteNodeClient");
        let req = tonic::Request::new(SetRequest { key, value });
        let result = self.inner.set(req).await?;
        Ok(result.into_inner())
    }

    async fn get(&mut self, key: String) -> anyhow::Result<Option<GetResponse>> {
        let req = tonic::Request::new(GetRequest { key });
        let response = self.inner.get(req).await?.into_inner();
        if response.value.is_some() {
            Ok(Some(response))
        } else {
            Ok(None)
        }
    }

    async fn remove(&mut self, key: String) -> anyhow::Result<Acknowledgement> {
        let req = tonic::Request::new(RemoveRequest { key });
        let result = self.inner.remove(req).await?;
        Ok(result.into_inner())
    }
}
