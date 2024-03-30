use clap::Parser;
use sqrl::client::{get, remove, set, Action};
use tokio::net::TcpStream;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct App {
    #[clap(name = "addr", global = true, long, default_value = "localhost:4000")]
    server: String,

    #[clap(subcommand)]
    subcmd: Action,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = App::parse();
    match cli.subcmd {
        Action::Set { key, value } => {
            set(&mut TcpStream::connect(cli.server).await?, key, value).await?;
        }
        Action::Get { key } => match get(&mut TcpStream::connect(cli.server).await?, key)
            .await?
            .as_str()
        {
            "Key not found" => println!("Key not found"),
            value => println!("{}", value),
        },
        Action::Remove { key } => {
            if remove(&mut TcpStream::connect(cli.server).await?, key)
                .await?
                .as_str()
                == "Key not found"
            {
                eprintln!("Key not found");
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
