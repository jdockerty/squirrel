//! Replication is achieved through a simplistic replicator/follower model.
//!
//! The [`ReplicatedServer`] is the designated replicator and will use its internally
//! configured [`RemoteNodeClient`]'s to update/retrieve values from remote stores.

mod server;

pub use server::ReplicatedServer;
