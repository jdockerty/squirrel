use clap::Parser;
use sqrl::actions;
use sqrl::actions::action_server::Action as ActionSrv;
use sqrl::actions::action_server::ActionServer;
use sqrl::KvStore;
use sqrl::KvsEngine;
use sqrl::ENGINE_FILE;
use std::sync::Arc;
use std::{ffi::OsString, path::PathBuf};
use std::{fmt::Display, net::SocketAddr};
use tracing::info;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct App {
    #[clap(long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    #[clap(name = "engine", short, long, default_value = "sqrl")]
    engine_name: Engine,

    #[clap(long, default_value = "info", env = "KVS_LOG")]
    log_level: tracing_subscriber::filter::LevelFilter,

    #[arg(long, global = true, default_value = default_log_location())]
    log_file: PathBuf,
}

fn default_log_location() -> OsString {
    std::env::current_dir()
        .expect("unable to find current directory")
        .into_os_string()
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Engine {
    Sqrl,
    // Not in use, but could be used to implement a different storage engine.
    Sled,
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqrl => write!(f, "sqrl"),
            Self::Sled => write!(f, "sled"),
        }
    }
}

#[derive(Clone)]
struct KvServer {
    pub store: Arc<KvStore>,
}

impl KvServer {
    pub fn new<P>(path: P) -> anyhow::Result<Self>
    where
        P: Into<std::path::PathBuf>,
    {
        let store = Arc::new(KvStore::open(path)?);
        Ok(Self { store })
    }
}

#[tonic::async_trait]
impl ActionSrv for KvServer {
    async fn get(
        &self,
        req: tonic::Request<actions::GetRequest>,
    ) -> tonic::Result<tonic::Response<actions::GetResponse>, tonic::Status> {
        let req = req.into_inner();
        let value = self.store.get(req.key).await.unwrap();
        Ok(tonic::Response::new(actions::GetResponse { value }))
    }
    async fn set(
        &self,
        req: tonic::Request<actions::SetRequest>,
    ) -> tonic::Result<tonic::Response<actions::Acknowledgement>, tonic::Status> {
        let req = req.into_inner();
        self.store.set(req.key, req.value).await.unwrap();
        Ok(tonic::Response::new(actions::Acknowledgement {
            success: true,
        }))
    }
    async fn remove(
        &self,
        req: tonic::Request<actions::RemoveRequest>,
    ) -> tonic::Result<tonic::Response<actions::Acknowledgement>, tonic::Status> {
        let req = req.into_inner();
        match self.store.remove(req.key).await {
            Ok(_) => Ok(tonic::Response::new(actions::Acknowledgement {
                success: true,
            })),
            Err(_) => Ok(tonic::Response::new(actions::Acknowledgement {
                success: false,
            })),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = App::parse();

    // We must error if the previous storage engine was not 'sqrl' as it is incompatible.
    KvStore::engine_is_sqrl(app.engine_name.to_string(), app.log_file.join(ENGINE_FILE))?;
    let srv = KvServer::new(app.log_file)?;

    info!(
        "sqrl-server version: {}, engine: {}",
        env!("CARGO_PKG_VERSION"),
        app.engine_name
    );

    info!("Listening on {}", app.addr);
    tonic::transport::Server::builder()
        .add_service(ActionServer::new(srv))
        .serve(app.addr)
        .await?;

    Ok(())
}
