use clap::Parser;
use kvs::client::Action;
use std::{io::prelude::*, net::TcpStream};

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct App {
    #[clap(short, long, default_value = "localhost:4000")]
    server: String,

    #[clap(subcommand)]
    subcmd: Action,
}

fn main() -> anyhow::Result<()> {
    let cli = App::parse();
    let mut response = String::from("");

    match cli.subcmd {
        Action::Set { key, value } => {
            let mut stream = TcpStream::connect(cli.server)?;
            bincode::serialize_into(&mut stream, &Action::Set { key, value })?;
            stream.read_to_string(&mut response)?;
            println!("{}", response);
        }
        Action::Get { key } => {
            let mut stream = TcpStream::connect(cli.server)?;
            bincode::serialize_into(&mut stream, &Action::Get { key })?;
            stream.read_to_string(&mut response)?;
            println!("{}", response);
        }
        Action::Remove { key } => {
            let mut stream = TcpStream::connect(cli.server)?;
            bincode::serialize_into(&mut stream, &Action::Remove { key })?;
            stream.read_to_string(&mut response)?;
            println!("{}", response);
        }
    }

    Ok(())
}
