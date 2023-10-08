use std::{
    io::Error,
    path::{Path, PathBuf},
};

use clap::Parser;
use kvs::{
    engines::{KvStore, SledKvStore},
    server::KvsServer,
};
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

    match cli.engine.unwrap().as_str() {
        "kvs" => Ok(KvsServer::new(
            cli.addr,
            logger.clone(),
            KvStore::open(Some(logger), PathBuf::from("./log"))?,
        )
        .run()?),
        "sled" => Ok(
            KvsServer::new(cli.addr, logger, SledKvStore::open(PathBuf::from("./log"))?).run()?,
        ),
        _ => Err(Error::other("Unknown storage engine")),
    }
}
