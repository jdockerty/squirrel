use std::path::PathBuf;

use clap::{Parser, Subcommand};
use kvs::Result;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct App {
    #[arg(short, long, global = true, default_value = "~/.kvs_log", default_value = default_log_location().into_os_string())]
    log_file: PathBuf,

    #[clap(subcommand)]
    subcmd: Option<Commands>,
}

fn default_log_location() -> PathBuf {
    dirs::home_dir()
        .expect("unable to find home directory")
        .join(".kvs_log")
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
        Some(Commands::Get { key }) => {
            kv.get(key)?;
        }
        Some(Commands::Remove { key }) => {
            kv.remove(key)?;
        }
        None => {
            std::process::exit(1);
        }
    }

    Ok(())
}
