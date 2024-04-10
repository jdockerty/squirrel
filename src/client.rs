use tonic::transport::Channel;

use crate::proto::action_client::ActionClient;
use crate::proto::{Acknowledgement, RemoveRequest};
use crate::proto::{GetRequest, SetRequest};
use crate::Result;

pub struct Client {
    /// Inner gRPC client for actions that can be taken.
    inner: ActionClient<Channel>,

    /// Address of the cache server.
    pub addr: String,
}

impl Client {
    pub async fn new(addr: String) -> Result<Self> {
        let inner = ActionClient::connect(format!("http://{}", addr)).await?;
        Ok(Self { inner, addr })
    }

    pub async fn set(&mut self, key: String, value: String) -> anyhow::Result<Acknowledgement> {
        let req = tonic::Request::new(SetRequest { key, value });
        let result = self.inner.set(req).await?;
        Ok(result.into_inner())
    }

    pub async fn get(&mut self, key: String) -> anyhow::Result<Option<String>> {
        let req = tonic::Request::new(GetRequest { key });
        Ok(self.inner.get(req).await?.into_inner().value)
    }

    pub async fn remove(&mut self, key: String) -> anyhow::Result<Acknowledgement> {
        let req = tonic::Request::new(RemoveRequest { key });
        let result = self.inner.remove(req).await?;
        Ok(result.into_inner())
    }
}
