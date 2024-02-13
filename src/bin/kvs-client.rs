use std::{ffi::OsString, path::PathBuf};

use clap::{Parser, Subcommand};
use kvs::Result;
use kvs::store::KvStore;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct App {
    #[arg(short, long, global = true, default_value = default_log_location())]
    log_file: PathBuf,

    #[clap(subcommand)]
    subcmd: Option<Commands>,
}

fn default_log_location() -> OsString {
    std::env::current_dir()
        .expect("unable to find current directory")
        .into_os_string()
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Enter a key-value pair into the store.
    Set { key: String, value: String },

    /// Get a value from the store, providing a key.
    Get { key: String },

    /// Remove a value from the store, providing a key.
    #[clap(name = "rm")]
    Remove { key: String },
}

fn main() -> Result<()> {
    let cli = App::parse();

    let mut kv = KvStore::open(cli.log_file)?;

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
