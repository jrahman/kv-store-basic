use std::io::Result;
use std::io::Write;
use std::path::Path;

use clap::{Parser, Subcommand};
use kvs::engines::KvsEngine;
use slog::o;
use slog::Drain;

#[derive(Debug, Parser)] // requires `derive` feature
#[command(name = env!("CARGO_PKG_NAME"), about = env!("CARGO_PKG_DESCRIPTION"), author = env!("CARGO_PKG_AUTHORS"), version = env!("CARGO_PKG_VERSION"))]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Get {
        #[arg(value_name = "KEY")]
        key: String,
    },
    Set {
        #[arg(value_name = "KEY")]
        key: String,
        #[arg(value_name = "VALUE")]
        value: String,
    },
    Rm {
        #[arg(value_name = "KEY")]
        key: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!("module" => "Log"));

    let path = Path::new("./log");
    let kvs = kvs::engines::KvStore::open(Some(logger), path.to_path_buf())?;

    writeln!(std::io::stdout(), "Finished opening kvstore")?;

    match cli.command {
        Commands::Get { key } => match kvs.get(key.to_string())? {
            Some(value) => {
                writeln!(std::io::stdout(), "Found {} => {}", key, value)?;
            }
            None => {
                writeln!(std::io::stdout(), "Not found")?;
            }
        },
        Commands::Set { key, value } => {
            writeln!(std::io::stdout(), "Storing {} => {}", key, value)?;
            kvs.set(key.to_string(), value.to_string())?;
            writeln!(std::io::stdout(), "Stored {} => {}", key, value)?;
        }
        Commands::Rm { key } => {
            kvs.remove(key.to_string())?;
            writeln!(std::io::stdout(), "Removed {}", key)?;
        }
    }
    Ok(())
}
