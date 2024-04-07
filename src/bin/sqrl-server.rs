use actix_web::middleware;
use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::HttpServer;
use clap::Parser;
use sqrl::client::Action;
use sqrl::raft_api;
use sqrl::KvStore;
use sqrl::KvStoreError;
use sqrl::KvsEngine;
use sqrl::ENGINE_FILE;
use std::{ffi::OsString, path::PathBuf};
use std::{fmt::Display, net::SocketAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::signal::ctrl_c;
use tracing::{debug, error, info};

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct App {
    #[clap(long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    #[clap(long, default_value = "1")]
    node_id: u64,

    #[clap(long, value_delimiter = ',')]
    peers: Option<Vec<SocketAddr>>,

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

//#[tonic::async_trait]
//impl sqrl::proto::raft_server::Raft for SqrlServer {
//    async fn send(
//        &self,
//        msg: tonic::Request<raft::eraftpb::Message>,
//    ) -> Result<tonic::Response<sqrl::proto::MessageAck>, tonic::Status> {
//        let msg = msg.into_inner();
//        self.0.send(Msg::Raft(msg)).await.unwrap();
//        Ok(tonic::Response::new(sqrl::proto::MessageAck {
//            success: true,
//        }))
//    }
//}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = App::parse();

    // We must error if the previous storage engine was not 'sqrl' as it is incompatible.
    KvStore::engine_is_sqrl(app.engine_name.to_string(), app.log_file.join(ENGINE_FILE))?;
    tracing_subscriber::fmt()
        .with_max_level(app.log_level)
        .init();
    match app.peers {
        Some(_peers) => {
            info!(node_id = app.node_id, "Starting Raft node");
            let config = openraft::Config {
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            };

            let config = std::sync::Arc::new(config.validate().unwrap());
            let kv_store = std::sync::Arc::new(sqrl::raft::StateMachineStore::default());
            let log_store = sqrl::raft::LogStore::default();

            let network = sqrl::raft_network::Network {};

            let r = openraft::Raft::new(
                app.node_id,
                config.clone(),
                network,
                log_store.clone(),
                kv_store.clone(),
            )
            .await
            .unwrap();
            // Create an application that will store all the instances created above, this will
            // later be used on the actix-web services.
            let app_data = Data::new(sqrl::raft_network::AppData {
                id: app.node_id,
                addr: app.addr.to_string(),
                raft: r,
                log_store,
                state_machine_store: kv_store,
                config,
            });

            // Start the actix-web server.
            let server = HttpServer::new(move || {
                actix_web::App::new()
                    .wrap(Logger::default())
                    .wrap(Logger::new("%a %{User-Agent}i"))
                    .wrap(middleware::Compress::default())
                    .app_data(app_data.clone())
                    // raft internal RPC
                    .service(crate::raft_api::append)
                    .service(crate::raft_api::snapshot)
                    .service(crate::raft_api::vote)
                    // admin API
                    .service(crate::raft_api::init)
                    .service(crate::raft_api::add_learner)
                    .service(crate::raft_api::change_membership)
                    //.service(crate::raft_api::metrics)
                    // application API
                    .service(crate::raft_api::write)
                    .service(crate::raft_api::read)
                    .service(crate::raft_api::consistent_read)
            });

            let x = server.bind(app.addr)?;

            x.run().await?;
        }
        None => {
            info!("Starting single node store");

            let kv = std::sync::Arc::new(KvStore::open(app.log_file)?);

            info!(
                "sqrl-server version: {}, engine: {}",
                env!("CARGO_PKG_VERSION"),
                app.engine_name
            );

            let listener = TcpListener::bind(app.addr).await?;
            info!("k-v store server on {}", app.addr);

            // Handle incoming connections.
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    debug!("Connection established: {stream:?}");
                    handle_connection(stream, kv.clone()).await.unwrap();
                }
            });
        }
    };

    // TODO: add cancellation tokens
    match ctrl_c().await {
        Ok(_) => info!("Received shutdown signal"),
        Err(e) => error!("Error receiving Ctrl-C: {e}"),
    };

    Ok(())
}

async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    kv: std::sync::Arc<KvStore>,
) -> anyhow::Result<()> {
    // The client provides a size hint for how much data is incoming first.
    // This allows us to use read_exact.
    let size = stream.read_u64().await? as usize;
    let mut buf = vec![0; size];
    stream.read_exact(&mut buf).await?;
    let action: Action = bincode::deserialize_from(buf.as_slice()).unwrap();

    match &action {
        Action::Set { key, value } => {
            match kv.set(key.to_string(), value.to_string()).await {
                Ok(_) => debug!("{key} set to {value}"),
                Err(e) => error!("{}", e),
            };
        }
        Action::Get { key } => match kv.get(key.to_string()).await {
            Ok(Some(value)) => {
                debug!("{key} has value: {value}");
                stream.write_all(value.as_bytes()).await?;
                stream.flush().await?;
            }
            Ok(None) => {
                debug!("{key} not found");
                stream.write_all("Key not found".as_bytes()).await?;
                stream.flush().await?;
            }
            Err(e) => error!("{}", e),
        },
        Action::Remove { key } => match kv.remove(key.to_string()).await {
            Ok(_) => debug!("{key} removed"),
            Err(KvStoreError::RemoveOperationWithNoKey) => {
                debug!("{key} not found");
                stream.write_all("Key not found".as_bytes()).await?;
                stream.flush().await?;
            }
            Err(e) => error!("{}", e),
        },
    }

    Ok(())
}
