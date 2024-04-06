use clap::Parser;
use dashmap::DashMap;
use protobuf::Message as _;
use raft::eraftpb::ConfChange;
use raft::eraftpb::{Entry, EntryType, Message};
use raft::storage::MemStorage;
use raft::{prelude::*, StateRole};
use sqrl::client::{self, Action};
use sqrl::raft as sqrlraft;
use sqrl::Cluster;
use sqrl::KvStore;
use sqrl::KvStoreError;
use sqrl::KvsEngine;
use sqrl::ENGINE_FILE;
use sqrlraft::{Msg, Node, ProposeCallback};
use std::collections::VecDeque;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use std::{ffi::OsString, path::PathBuf};
use std::{fmt::Display, net::SocketAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::signal::ctrl_c;
use tokio::sync::Mutex;
use tracing::{debug, error, info};
use sqrl::proto::raft_server::RaftServer;


#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct App {
    #[clap(long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    #[clap(long, default_value = "1")]
    node_id: u64,

    #[clap(long, value_delimiter = ',')]
    peers: Option<Vec<SocketAddr>>,

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

struct SqrlServer(tokio::sync::mpsc::Sender<Msg>);


#[tonic::async_trait]
impl sqrl::proto::raft_server::Raft for SqrlServer {
    async fn send(
        &self,
        msg: tonic::Request<raft::eraftpb::Message>,
    ) -> Result<tonic::Response<sqrl::proto::MessageAck>, tonic::Status> {
        let msg = msg.into_inner();
        self.0.send(Msg::Raft(msg)).await.unwrap();
        Ok(tonic::Response::new(sqrl::proto::MessageAck {
            success: true,
        }))
    }
}

async fn run_server(c: Cluster) -> anyhow::Result<()> {
    Ok(())
}

async fn drive_raft(
    mut c: Cluster,
    mut rx: tokio::sync::mpsc::Receiver<Msg>,
) -> anyhow::Result<()> {
    // A global pending proposals queue. New proposals will be pushed back into the queue, and
    // after it's committed by the raft cluster, it will be poped from the queue.
    let proposals = Arc::new(Mutex::new(VecDeque::<Proposal>::new()));

    let mut interval = tokio::time::interval(Duration::from_millis(100));
    let mut cbs = DashMap::new();
    loop {
        interval.tick().await;

        let node = match c.node.0 {
            Some(ref mut c) => c,
            // Node not initialized
            None => continue,
        };

        match rx.recv().await {
            Some(msg) => match msg {
                Msg::Propose { id, callback } => {
                    cbs.insert(id, callback);
                    //let p = Proposal::normal(id, "test".to_string());
                    node.propose(vec![], vec![id]).unwrap();
                }
                Msg::Set { id, key, value } => {
                    match node.raft.state {
                        StateRole::Leader => {
                            info!("Leader sending to peers");
                            node.propose(vec![], vec![id]).unwrap();
                            for p in &c.peers.clone().unwrap() {
                                //info!("sending to {:?}", p);
                                //let mut proposals = proposals.lock().await;
                                //for p in proposals.iter_mut().skip_while(|p| p.proposed > 0)
                                //{
                                //    propose(node, p);
                                //}
                                let mut stream = TcpStream::connect(p).await.unwrap();
                                client::set(&mut stream, key.clone(), value.clone())
                                    .await
                                    .unwrap();
                            }
                        }
                        StateRole::Follower => {
                            info!("Follower receiving from leader");
                        }
                        _ => {}
                    }

                    //node.propose(vec![id], format!("{}={}", key, value).into_bytes())
                    //    .unwrap();
                }
                Msg::Raft(msg) => {
                    node.step(msg).unwrap();
                }
            },
            None => {} 
            //Err(RecvTimeoutError::Timeout) => (),
             //Err(RecvTimeoutError::Disconnected) => (),
        }
        node.tick();
        let peers = c.peers.clone();
        on_ready(&mut c.node, &mut cbs, peers.unwrap());
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = App::parse();

    // We must error if the previous storage engine was not 'sqrl' as it is incompatible.
    KvStore::engine_is_sqrl(app.engine_name.to_string(), app.log_file.join(ENGINE_FILE))?;
    tracing_subscriber::fmt()
        .with_max_level(app.log_level)
        .init();
    let (raft_tx, mut raft_rx) = tokio::sync::mpsc::channel(32);
    let kv = std::sync::Arc::new(KvStore::open(app.log_file)?.with_raft(raft_tx.clone()));
    let mut c = Cluster::new(app.node_id, app.peers.clone())?;

    info!(
        "sqrl-server version: {}, engine: {}",
        env!("CARGO_PKG_VERSION"),
        app.engine_name
    );

    let listener = TcpListener::bind(app.addr).await?;
    info!("k-v store server on {}", app.addr);

    // Handle incoming connections.
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            debug!("Connection established: {stream:?}");
            handle_connection(stream, kv.clone()).await.unwrap();
        }
    });

    match app.peers {
        Some(peers) => {
            info!(node_id = app.node_id, "Starting Raft node");
            let srv = SqrlServer(raft_tx);
            let s = tonic::transport::Server::builder()
                .add_service(RaftServer::new(srv));
            drive_raft(c, raft_rx).await?;
        }
        None => info!("Starting single node store"),
    };

    // TODO: add cancellation tokens
    match ctrl_c().await {
        Ok(_) => info!("Received shutdown signal"),
        Err(e) => error!("Error receiving Ctrl-C: {e}"),
    };

    Ok(())
}

fn on_ready(raft_group: &mut Node, cbs: &mut DashMap<u8, ProposeCallback>, peers: Vec<SocketAddr>) {
    if let Some(ref mut raft_group) = &mut raft_group.0 {
        if !raft_group.has_ready() {
            return;
        }
        let store = raft_group.raft.raft_log.store.clone();

        // Get the `Ready` with `RawNode::ready` interface.
        let mut ready = raft_group.ready();

        let handle_messages = |msgs: Vec<Message>| {
            for msg in msgs {
                // Send messages to other peers.
                for p in peers.clone() {
                    let mut stream = std::net::TcpStream::connect(p).unwrap();
                    stream.write_all(&msg.write_to_bytes().unwrap()).unwrap();
                }
            }
        };

        if !ready.messages().is_empty() {
            // Send out the messages come from the node.
            handle_messages(ready.take_messages());
        }

        if !ready.snapshot().is_empty() {
            // This is a snapshot, we need to apply the snapshot at first.
            store.wl().apply_snapshot(ready.snapshot().clone()).unwrap();
        }

        let mut _last_apply_index = 0;
        let handle_committed_entries =
            |rn: &mut RawNode<MemStorage>, committed_entries: Vec<Entry>| {
                for entry in committed_entries {
                    if entry.data.is_empty() {
                        // From new elected leaders.
                        continue;
                    }
                    if let EntryType::EntryConfChange = entry.get_entry_type() {
                        info!("ConfChange!");
                        // For conf change messages, make them effective.
                        let mut cc = ConfChange::default();
                        cc.merge_from_bytes(&entry.data).unwrap();
                        let cs = rn.apply_conf_change(&cc).unwrap();
                        store.wl().set_conf_state(cs);
                    } else {
                        // For normal proposals, extract the key-value pair and then
                        // insert them into the kv engine.
                    }
                    if rn.raft.state == StateRole::Leader {
                        info!("Leader proposing to new clients");
                        // The leader should response to the clients, tell them if their proposals
                        // succeeded or not.
                        // let proposal = proposals.lock().unwrap().pop_front().unwrap();
                        // proposal.propose_success.send(true).unwrap();
                    }
                }
            };
        handle_committed_entries(raft_group, ready.take_committed_entries());

        // Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
        // raft logs to the latest position.
        if let Err(e) = store.wl().append(ready.entries()) {
            error!("persist raft log fail: {:?}, need to retry or panic", e);
            return;
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            store.wl().set_hardstate(hs.clone());
        }

        if !ready.persisted_messages().is_empty() {
            // Send out the persisted messages come from the node.
            handle_messages(ready.take_persisted_messages());
        }

        // Advance the Raft.
        let mut light_rd = raft_group.advance(ready);
        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            store.wl().mut_hard_state().set_commit(commit);
        }
        // Send out the messages.
        handle_messages(light_rd.take_messages());
        // Apply all committed entries.
        handle_committed_entries(raft_group, light_rd.take_committed_entries());
        // Advance the apply index.
        raft_group.advance_apply();
    }
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

struct Proposal {
    normal: Option<(u16, String)>, // key is an u16 integer, and value is a string.
    conf_change: Option<ConfChange>, // conf change.
    transfer_leader: Option<u64>,
    // If it's proposed, it will be set to the index of the entry.
    proposed: u64,
}

impl Proposal {
    fn conf_change(cc: &ConfChange) -> Self {
        Proposal {
            normal: None,
            conf_change: Some(cc.clone()),
            transfer_leader: None,
            proposed: 0,
        }
    }
}

fn propose(raft_group: &mut RawNode<MemStorage>, proposal: &mut Proposal) {
    let last_index1 = raft_group.raft.raft_log.last_index() + 1;
    if let Some((ref key, ref value)) = proposal.normal {
        let data = format!("put {} {}", key, value).into_bytes();
        let _ = raft_group.propose(vec![], data);
    } else if let Some(ref cc) = proposal.conf_change {
        let _ = raft_group.propose_conf_change(vec![], cc.clone());
    } else if let Some(_transferee) = proposal.transfer_leader {
        // TODO: implement transfer leader.
        unimplemented!();
    }

    let last_index2 = raft_group.raft.raft_log.last_index() + 1;
    if last_index2 == last_index1 {
        // Propose failed, don't forget to respond to the client.
        // TODO: responding to other peers about this failure
        error!("Propsal failed!");
    } else {
        proposal.proposed = last_index1;
    }
}
