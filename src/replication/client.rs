use crate::client::Client;
use crate::proto::action_client::ActionClient;
use crate::proto::{Acknowledgement, RemoveRequest};
use crate::proto::{GetRequest, SetRequest};
use crate::{KvsEngine, Result};

/// Implementation of a [`Client`] with an awareness of multiple remote cache
/// servers. These servers act as replication points and will be used to serve
/// requests based on a quorum read/write sequence.
struct ReplicationClient<S, C> {
    local_cache: S,
    remote_replicas: Vec<C>,
}

impl<S, C> Client for ReplicationClient<S, C> {
    async fn get(&mut self, key: String) -> anyhow::Result<Option<String>> {
        Ok(None)
    }

    async fn set(&mut self, key: String, value: String) -> anyhow::Result<Acknowledgement> {
        Ok(Acknowledgement { success: true })
    }

    async fn remove(&mut self, key: String) -> anyhow::Result<Acknowledgement> {
        Ok(Acknowledgement { success: true })
    }
}
