use clap::Parser;
use std::net::{SocketAddr, TcpListener};

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct App {
    #[clap(short, long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    #[clap(name = "engine", short, long, default_value = "kvs")]
    engine_name: Engine,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Engine {
    Kvs,
    Sled,
}

fn main() -> anyhow::Result<()> {
    let app = App::parse();

    let listener = TcpListener::bind(app.addr)?;

    while let Ok((stream, remote_peer)) = listener.accept() {
        println!("Connection established");
    }

    Ok(())
}
