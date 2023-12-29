use std::path::PathBuf;

use clap::{Parser, Subcommand};
use kvs::Result;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct App {
    #[arg(short, long, global = true, default_value = default_log_location().into_os_string())]
    log_file: PathBuf,

    #[clap(subcommand)]
    subcmd: Option<Commands>,
}

fn default_log_location() -> PathBuf {
    std::env::current_dir()
        .expect("unable to find current directory")
        .join(kvs::LOG_NAME)
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

fn main() -> Result<()> {
    let cli = App::parse();

    let mut kv = kvs::KvStore::new();
    kv.log_location = cli.log_file;

    match cli.subcmd {
        Some(Commands::Set { key, value }) => {
            kv.set(key, value)?;
        }
        Some(Commands::Get { key }) => match kv.get(key)? {
            Some(v) => {
                println!("{}", v);
            }
            None => {
                println!("Key not found");
            }
        },
        Some(Commands::Remove { key }) => {
            if let Err(kvs::KvStoreError::RemoveOperationWithNoKey) = kv.remove(key) {
                println!("Key not found");
                std::process::exit(1);
            }
        }
        None => {
            std::process::exit(1);
        }
    }

    Ok(())
}
