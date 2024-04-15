//! Replication is achieved through a simplistic leader/follower model.
//!
//! The [`ReplicatedServer`] is the designated leader and will use its internally
//! configured [`RemoteNodeClient`]'s to update and retrieve values from remote
//! stores.

mod server;

pub use server::ReplicatedServer;
