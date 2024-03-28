use clap::Parser;
use sqrl::client::Action;
use sqrl::KvStore;
use sqrl::KvStoreError;
use sqrl::KvsEngine;
use sqrl::ENGINE_FILE;
use std::{ffi::OsString, path::PathBuf};
use std::{fmt::Display, net::SocketAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, error, info};

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = App::parse();

    // We must error if the previous storage engine was not 'sqrl' as it is incompatible.
    KvStore::engine_is_sqrl(app.engine_name.to_string(), app.log_file.join(ENGINE_FILE))?;
    let kv = std::sync::Arc::new(KvStore::open(app.log_file)?);

    info!(
        "sqrl-server version: {}, engine: {}",
        env!("CARGO_PKG_VERSION"),
        app.engine_name
    );

    let listener = TcpListener::bind(app.addr).await?;
    info!("listening on {}", app.addr);

    while let Ok((stream, _)) = listener.accept().await {
        debug!("Connection established: {stream:?}");
        tokio::select! {
            _ = handle_connection(stream, kv.clone()) => {}
        }
    }
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
