use clap::Subcommand;
use serde::{Deserialize, Serialize};

/// Actions that can be performed by the client.
#[derive(Debug, Subcommand, Serialize, Deserialize)]
pub enum Action {
    /// Enter a key-value pair into the store.
    Set { key: String, value: String },

    /// Get a value from the store, providing a key.
    Get { key: String },

    /// Remove a value from the store, providing a key.
    #[clap(name = "rm")]
    Remove { key: String },
}
