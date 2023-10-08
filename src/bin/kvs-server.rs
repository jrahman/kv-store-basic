use std::path::PathBuf;

use clap::Parser;
use kvs::{engines::KvStore, server::KvsServer};
use slog::{o, Drain};
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

    let kv_store = KvStore::open(Some(logger.clone()), PathBuf::from("./log"))?;

    let mut server = KvsServer::new(cli.addr, logger, kv_store);
    server.run()
}
