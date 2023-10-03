

use clap::{Parser, Subcommand};
use slog::{Drain, o};

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

        #[arg(long = "addr")]
        addr: String
    },
    Set {
        #[arg(value_name = "KEY")]
        key: String,
        #[arg(value_name = "VALUE")]
        value: String,

        #[arg(long = "addr")]
        addr: String
    },
    Rm {
        #[arg(value_name = "KEY")]
        key: String,

        #[arg(long = "addr")]
        addr: String
    },
}

fn main() {
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!("module" => "client"));
    
    let cli = Cli::parse();

    match cli.command {
        Commands::Get { key, addr } => {

        },
        Commands::Set { key, value, addr } => {

        }
        Commands::Rm { key, addr } => {

        }
    }
}