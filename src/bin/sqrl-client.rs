use clap::Parser;
use proto::action_client::ActionClient;
use proto::RemoveRequest;
use proto::{GetRequest, SetRequest};
use sqrl::action::Action;

mod proto {
    tonic::include_proto!("actions");
}

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
    let mut client = ActionClient::connect(format!("http://{}", cli.server)).await?;

    match cli.subcmd {
        Action::Set { key, value } => {
            client
                .set(tonic::Request::new(SetRequest { key, value }))
                .await?;
        }
        Action::Get { key } => {
            let response = client.get(tonic::Request::new(GetRequest { key })).await?;
            match response.into_inner().value {
                Some(v) => println!("{}", v),
                None => println!("Key not found"),
            }
        }
        Action::Remove { key } => {
            let response = client
                .remove(tonic::Request::new(RemoveRequest { key }))
                .await?;
            match response.into_inner().success {
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
