use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct App {
    #[clap(subcommand)]
    subcmd: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Set {
        key: String,
        value: String,
    },
    Get {
        key: String,
    },

    #[clap(name = "rm")]
    Remove {
        key: String,
    },
}

fn main() {
    let cli = App::parse();

    let mut kv = kvs::KvStore::new();

    match cli.subcmd {
        Some(Commands::Set { key, value }) => {
            kv.set(key, value).unwrap();
        }
        Some(Commands::Get { key }) => {
            kv.get(key).unwrap();
        }
        Some(Commands::Remove { key }) => {
            kv.remove(key).unwrap();
        }
        None => {
            std::process::exit(1);
        }
    }
}
