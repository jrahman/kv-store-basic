use std::net::{TcpListener, TcpStream};

use clap::Parser;
use slog::{o, Drain, Logger, info};
use std::io::Result;

#[derive(Parser)] // requires `derive` feature
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(default_value = "127.0.0.1:4000")]
    addr: String,

    #[arg(long = "engine")]
    engine: Option<String>,
}

fn main() -> Result<()> {
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!("module" => "server"));

    let cli = Cli::parse();

    let mut server = kvs::server::KvsServer::new(cli.addr, logger);
    server.run()
}
