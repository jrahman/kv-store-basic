use std::io::Result;

use clap::{Parser, Subcommand};
use slog::{Drain, o};

use kvs::client::KvsClient;

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

        #[arg(long = "addr", default_value = "127.0.0.1:4000")]
        addr: String
    },
    Set {
        #[arg(value_name = "KEY")]
        key: String,

        #[arg(value_name = "VALUE")]
        value: String,

        #[arg(long = "addr", default_value = "127.0.0.1:4000")]
        addr: String
    },
    Rm {
        #[arg(value_name = "KEY")]
        key: String,

        #[arg(long = "addr", default_value = "127.0.0.1:4000")]
        addr: String
    },
}

fn main() -> Result<()> {
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!("module" => "client"));
    
    let cli = Cli::parse();

    match cli.command {
        Commands::Get { key, addr } => {
            let mut client = KvsClient::new(logger, addr)?;
            match client.get(key)? {
                Some(value) => {
                    println!("Found value: {}", value);
                },
                None => {
                    println!("Missing value");
                }
            }
        },
        Commands::Set { key, value, addr } => {
            let mut client = KvsClient::new(logger, addr)?;
            client.set(key.clone(), value.clone())?;
            println!("Set {} => {}", key, value);
        }
        Commands::Rm { key, addr } => {
            let mut client = KvsClient::new(logger, addr)?;
            client.rm(key.clone())?;
            println!("Removed {}", key);
        }
    };
    Ok(())
}