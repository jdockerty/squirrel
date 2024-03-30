use crate::serialize_and_hint;
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::info;

/// Actions that can be performed by the client.
///
/// Subsequently, this means we can also serialize these actions to send them over
/// the network to the server, which can then deserialize them and perform the
/// corresponding action.
#[derive(Debug, Subcommand, Serialize, Deserialize)]
pub enum Action {
    /// Enter a key-value pair into the store.
    Set { key: String, value: String },

    /// Get a value from the store with the provided key.
    Get { key: String },

    /// Remove a value from the store with the provided key.
    #[clap(name = "rm")]
    Remove { key: String },
}

pub async fn set(stream: &mut TcpStream, key: String, value: String) -> anyhow::Result<()> {
    info!("Set");
    let action = Action::Set { key, value };
    serialize_and_hint!(stream, action);
    Ok(())
}

pub async fn get(stream: &mut TcpStream, key: String) -> anyhow::Result<String> {
    let action = Action::Get { key };
    serialize_and_hint!(stream, action);
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    Ok(response)
}

pub async fn remove(stream: &mut TcpStream, key: String) -> anyhow::Result<String> {
    let action = Action::Remove { key };
    serialize_and_hint!(stream, action);
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    Ok(response)
}

/// Serialize the provided action and provided a size hint to the server on
/// the provided [`TcpStream`].
///
/// [`TcpStream`]: tokio::net::TcpStream
#[macro_export]
macro_rules! serialize_and_hint {
    ($stream:expr, $action:expr) => {
        let data = bincode::serialize(&$action)?;
        $stream.write_u64(data.len() as u64).await?;
        $stream.write_all(&data).await?;
    };
}
