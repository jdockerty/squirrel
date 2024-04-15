use clap::Parser;
use sqrl::client::RemoteNodeClient;
use sqrl::replication;
use sqrl::replication::ReplicatedServer;
use sqrl::KvStore;
use sqrl::StandaloneServer;
use sqrl::ENGINE_FILE;
use std::{ffi::OsString, path::PathBuf};
use std::{fmt::Display, net::SocketAddr};
use tokio::sync::Mutex;

mod proto {
    tonic::include_proto!("actions");
}

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

    #[cfg(feature = "replication")]
    #[arg(long, default_value = "follower")]
    replication_mode: replication::Mode,

    #[cfg(feature = "replication")]
    #[arg(
        long,
        requires_if(replication::Mode::Leader, "replication_mode"),
        value_delimiter = ','
    )]
    followers: Vec<String>,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = App::parse();
    // We must error if the previous storage engine was not 'sqrl' as it is incompatible.
    KvStore::engine_is_sqrl(app.engine_name.to_string(), app.log_file.join(ENGINE_FILE))?;
    match app.replication_mode {
        replication::Mode::Leader => {
            assert_eq!(
                app.followers.len(),
                2,
                "Only 2 followers are configurable at present"
            );
            let clients = [
                Mutex::new(RemoteNodeClient::new(app.followers[0].clone()).await?),
                Mutex::new(RemoteNodeClient::new(app.followers[1].clone()).await?),
            ];
            ReplicatedServer::new(clients, app.log_file, app.addr)?
                .run()
                .await
        }
        replication::Mode::Follower => StandaloneServer::new(app.log_file, app.addr)?.run().await,
    }
}
