use clap::Subcommand;
use serde::{Deserialize, Serialize};

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
