use clap::Parser;

use sqrl::action::Action;
use sqrl::client::{Client, RemoteNodeClient};

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
    let mut client = RemoteNodeClient::new(cli.server).await?;

    match cli.subcmd {
        Action::Set { key, value } => {
            client.set(key, value).await?;
        }
        Action::Get { key } => {
            let response = client.get(key).await?;
            match response {
                Some(v) => println!("{}", v.value.unwrap()),
                None => println!("Key not found"),
            }
        }
        Action::Remove { key } => {
            let response = client.remove(key).await?;
            match response.success {
                true => {}
                false => {
                    eprintln!("Key not found");
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}
