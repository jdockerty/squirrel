//! Replication is achieved through a simplistic leader/follower model.
//!
//! The [`ReplicatedServer`] is the designated leader and will use its internally
//! configured [`RemoteNodeClient`]'s to update and retrieve values from remote
//! stores.

mod server;

pub use server::ReplicatedServer;

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum Mode {
    Leader,
    Follower,
}

impl From<Mode> for clap::builder::OsStr {
    fn from(value: Mode) -> Self {
        match value {
            Mode::Leader => "leader".into(),
            Mode::Follower => "follower".into(),
        }
    }
}
