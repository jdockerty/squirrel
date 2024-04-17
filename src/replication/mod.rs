//! Replication is achieved through a simplistic leader/follower model.
//!
//! The [`ReplicatedServer`] is the designated leader and will use its internally
//! configured [`RemoteNodeClient`]'s to replicate `set` and `remove` calls from
//! remote stores.
//!
//! The expectation is that the [`ReplicatedServer`] is the direct contact for
//! updates and whilst any node can serve a `get`, there is no guarantee that
//! the read is not stale.

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
