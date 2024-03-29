use clap::Parser;
use dashmap::DashMap;
use raft::prelude::*;
use raft::storage::MemStorage;
use sqrl::client::Action;
use sqrl::Cluster;
use sqrl::KvStore;
use sqrl::KvStoreError;
use sqrl::KvsEngine;
use sqrl::ENGINE_FILE;
use std::time::Duration;
use std::{ffi::OsString, path::PathBuf};
use std::{fmt::Display, net::SocketAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::signal::ctrl_c;
use tracing::{debug, error, info};

type ProposeCallback = Box<dyn Fn() + Send>;
enum Msg {
    Propose {
        id: u8,
        callback: Box<dyn Fn() + Send>,
    },
    Raft(Message),
}

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct App {
    #[clap(long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = App::parse();

    // We must error if the previous storage engine was not 'sqrl' as it is incompatible.
    KvStore::engine_is_sqrl(app.engine_name.to_string(), app.log_file.join(ENGINE_FILE))?;
    let kv = std::sync::Arc::new(KvStore::open(app.log_file)?);
    let mut c = Cluster::new()?;

    info!(
        "sqrl-server version: {}, engine: {}",
        env!("CARGO_PKG_VERSION"),
        app.engine_name
    );

    let listener = TcpListener::bind(app.addr).await?;
    info!("listening on {}", app.addr);

    // Handle incoming connections.
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            debug!("Connection established: {stream:?}");
            handle_connection(stream, kv.clone()).await.unwrap();
        }
    });

    use std::{
        sync::mpsc::{channel, RecvTimeoutError},
        time::Duration,
    };
    let (tx, rx) = channel();
    send_propose(tx);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        let mut cbs = DashMap::new();
        loop {
            interval.tick().await;

            match rx.recv_timeout(interval.period()) {
                Ok(Msg::Propose { id, callback }) => {
                    println!("Proposal {}", id);
                    cbs.insert(id, callback);
                    c.node.propose(vec![], vec![id]).unwrap();
                }
                Ok(Msg::Raft(m)) => c.node.step(m).unwrap(),
                Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => return,
            }
            c.node.tick();
            on_ready(&mut c.node, &mut cbs);
        }
    });

    // TODO: add cancellation tokens
    match ctrl_c().await {
        Ok(_) => info!("Received shutdown signal"),
        Err(e) => error!("Error receiving Ctrl-C: {e}"),
    };

    Ok(())
}
fn on_ready(raft_group: &mut RawNode<MemStorage>, cbs: &mut DashMap<u8, ProposeCallback>) {
    if !raft_group.has_ready() {
        return;
    }
    let store = raft_group.raft.raft_log.store.clone();

    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready = raft_group.ready();

    let handle_messages = |msgs: Vec<Message>| {
        for _msg in msgs {
            // Send messages to other peers.
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
    let mut handle_committed_entries = |committed_entries: Vec<Entry>| {
        for entry in committed_entries {
            // Mostly, you need to save the last apply index to resume applying
            // after restart. Here we just ignore this because we use a Memory storage.
            _last_apply_index = entry.index;

            if entry.data.is_empty() {
                // Empty entry, when the peer becomes Leader it will send an empty entry.
                continue;
            }

            if entry.get_entry_type() == EntryType::EntryNormal {
                if let Some((_, cb)) = cbs.remove(entry.data.first().unwrap()) {
                    cb();
                }
            }

            // TODO: handle EntryConfChange
        }
    };
    handle_committed_entries(ready.take_committed_entries());

    if !ready.entries().is_empty() {
        // Append entries to the Raft log.
        store.wl().append(ready.entries()).unwrap();
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
    handle_committed_entries(light_rd.take_committed_entries());
    // Advance the apply index.
    raft_group.advance_apply();
}

fn send_propose(sender: std::sync::mpsc::Sender<Msg>) {
    std::thread::spawn(move || {
        // Wait some time and send the request to the Raft.
        std::thread::sleep(Duration::from_secs(3));

        let (s1, r1) = std::sync::mpsc::channel::<u8>();

        println!("propose a request");

        // Send a command to the Raft, wait for the Raft to apply it
        // and get the result.
        sender
            .send(Msg::Propose {
                id: 1,
                callback: Box::new(move || {
                    s1.send(0).unwrap();
                }),
            })
            .unwrap();

        let n = r1.recv().unwrap();
        assert_eq!(n, 0);

        println!("receive the propose callback");
    });
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
