use clap::Parser;
use kvs::client::Action;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct App {
    #[clap(name = "addr", global = true, long, default_value = "localhost:4000")]
    server: String,

    #[clap(subcommand)]
    subcmd: Action,
}

/// Serialize the provided action and provided a size hint to the server on
/// the provided [`TcpStream`].
///
/// [`TcpStream`]: tokio::net::TcpStream
macro_rules! serialize_and_hint {
    ($stream:expr, $action:expr) => {
        let data = bincode::serialize(&$action)?;
        $stream.write_u64(data.len() as u64).await?;
        $stream.write_all(&data).await?;
    };
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = App::parse();
    let mut response = String::new();

    match cli.subcmd {
        Action::Set { key, value } => {
            let mut stream = TcpStream::connect(cli.server).await?;
            serialize_and_hint!(stream, Action::Set { key, value });
        }
        Action::Get { key } => {
            let mut stream = TcpStream::connect(cli.server).await?;
            serialize_and_hint!(stream, Action::Get { key });
            stream.read_to_string(&mut response).await?;
            match response.as_str() {
                "Key not found" => println!("{}", response),
                _ => println!("{}", response),
            }
        }
        Action::Remove { key } => {
            let mut stream = TcpStream::connect(cli.server).await?;
            serialize_and_hint!(stream, Action::Remove { key });
            stream.read_to_string(&mut response).await?;
            if response.as_str() == "Key not found" {
                eprintln!("{}", response);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
