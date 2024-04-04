use raft::eraftpb::Message;
use raft::eraftpb::Snapshot;
use raft::Config;
use raft::{storage::MemStorage, RawNode};

pub type ProposeCallback = Box<dyn Fn() + Send>;

pub enum Msg {
    Propose { id: u8, callback: ProposeCallback },
    Set { id: u8, key: String, value: String },
    Raft(Message),
}

pub struct Node(pub Option<RawNode<MemStorage>>);

impl Node {
    fn example_config() -> Config {
        Config {
            id: 1,
            election_tick: 10,
            heartbeat_tick: 3,
            ..Default::default()
        }
    }

    pub fn create_leader(node_id: u64) -> anyhow::Result<Self> {
        let mut cfg = Self::example_config();
        cfg.id = node_id;
        let drain = tracing_slog::TracingSlogDrain;
        let logger = slog::Logger::root(drain, slog::o!());
        let mut s = Snapshot::default();
        // Because we don't use the same configuration to initialize every node, so we use
        // a non-zero index to force new followers catch up logs by snapshot first, which will
        // bring all nodes to the same initial state.
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![1];
        let storage = MemStorage::new();
        storage.wl().apply_snapshot(s)?;
        let raft_group = Some(RawNode::new(&cfg, storage, &logger)?);
        Ok(Node(raft_group))
    }

    pub fn create_follower(node_id: u64) -> anyhow::Result<Self> {
        let mut cfg = Self::example_config();
        cfg.id = node_id;
        let drain = tracing_slog::TracingSlogDrain;
        let logger = slog::Logger::root(drain, slog::o!());
        let node = Some(RawNode::new(&cfg, MemStorage::new(), &logger)?);
        Ok(Node(node))
    }
}
