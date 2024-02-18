use clap::Parser;
use kvs::client::Action;
use kvs::KvStore;
use kvs::KvsEngine;
use kvs::ENGINE_FILE;
use std::io::Write;
use std::{ffi::OsString, path::PathBuf};
use std::{
    fmt::Display,
    net::{SocketAddr, TcpListener},
};
use tracing::{debug, error, info};
use tracing_subscriber::prelude::*;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct App {
    #[clap(long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    #[clap(name = "engine", short, long, default_value = "kvs")]
    engine_name: Engine,

    #[clap(long, default_value = "info")]
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
    Kvs,
    Sled,
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kvs => write!(f, "kvs"),
            Self::Sled => write!(f, "sled"),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let app = App::parse();

    // We must error if the previous storage engine was not 'kvs' as it is incompatible.
    KvStore::engine_is_kvs(app.engine_name.to_string(), app.log_file.join(ENGINE_FILE))?;

    let mut kv = KvStore::open(app.log_file)?;
    let layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    let subscriber = tracing_subscriber::registry()
        .with(app.log_level)
        .with(layer);
    let _tracing_guard = tracing::subscriber::set_default(subscriber);

    info!(
        "kvs-server version: {}, engine: {}",
        env!("CARGO_PKG_VERSION"),
        app.engine_name
    );

    let listener = TcpListener::bind(app.addr)?;
    info!("listening on {}", app.addr);

    while let Ok((mut stream, _)) = listener.accept() {
        debug!("Connection established: {stream:?}");

        // We know the actions that a client can take, so we can encode and decode them
        // to know what action we should take.
        let action: Action = bincode::deserialize_from(&stream)?;
        debug!("Received action: {action:?}");

        match &action {
            Action::Set { key, value } => match kv.set(key.to_string(), value.to_string()) {
                Ok(_) => debug!("{key} set to {value}"),
                Err(e) => error!("{}", e),
            },
            Action::Get { key } => match kv.get(key.to_string()) {
                Ok(Some(value)) => {
                    debug!("{key} has value: {value}");
                    write!(stream, "{}", value)?;
                }
                Ok(None) => {
                    debug!("{key} not found");
                    write!(stream, "Key not found")?;
                }
                Err(e) => error!("{}", e),
            },
            Action::Remove { key } => match kv.remove(key.to_string()) {
                Ok(_) => debug!("{key} removed"),
                Err(kvs::KvStoreError::RemoveOperationWithNoKey) => {
                    debug!("{key} not found");
                    write!(stream, "Key not found")?;
                }
                Err(e) => error!("{}", e),
            },
        }
    }
    Ok(())
}
