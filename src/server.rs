use crate::proto::{
    action_server::{Action, ActionServer},
    Acknowledgement, GetRequest, GetResponse, RemoveRequest, SetRequest,
};
use crate::KvStore;
use crate::KvsEngine;
use std::{net::SocketAddr, sync::Arc};
use tracing::info;

#[derive(Clone)]
pub struct KvServer {
    pub store: Arc<KvStore>,
    pub addr: SocketAddr,
}

impl KvServer {
    pub fn new<P>(path: P, addr: SocketAddr) -> anyhow::Result<Self>
    where
        P: Into<std::path::PathBuf>,
    {
        let store = Arc::new(KvStore::open(path)?);
        Ok(Self { store, addr })
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        info!(
            "Listening on {}\nsqrl-server version: {}, engine: sqrl",
            self.addr,
            env!("CARGO_PKG_VERSION"),
        );
        tonic::transport::Server::builder()
            .add_service(ActionServer::new(self.clone()))
            .serve(self.addr)
            .await?;
        Ok(())
    }
}

#[tonic::async_trait]
impl Action for KvServer {
    async fn get(
        &self,
        req: tonic::Request<GetRequest>,
    ) -> tonic::Result<tonic::Response<GetResponse>, tonic::Status> {
        let req = req.into_inner();
        match self.store.get(req.key).await.unwrap() {
            Some(v) => Ok(tonic::Response::new(GetResponse {
                value: v.value,
                timestamp: v.timestamp,
            })),
            None => Ok(tonic::Response::new(GetResponse {
                value: None,
                timestamp: 0,
            })),
        }
    }

    async fn set(
        &self,
        req: tonic::Request<SetRequest>,
    ) -> tonic::Result<tonic::Response<Acknowledgement>, tonic::Status> {
        let req = req.into_inner();
        self.store.set(req.key, req.value).await.unwrap();
        Ok(tonic::Response::new(Acknowledgement { success: true }))
    }

    async fn remove(
        &self,
        req: tonic::Request<RemoveRequest>,
    ) -> tonic::Result<tonic::Response<Acknowledgement>, tonic::Status> {
        let req = req.into_inner();
        match self.store.remove(req.key).await {
            Ok(_) => Ok(tonic::Response::new(Acknowledgement { success: true })),
            Err(_) => Ok(tonic::Response::new(Acknowledgement { success: false })),
        }
    }
}
