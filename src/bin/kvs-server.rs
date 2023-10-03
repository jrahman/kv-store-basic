use clap::Parser;
use slog::{Drain, o};


#[derive(Parser)] // requires `derive` feature
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(default_value="127.0.0.1:4000")]
    addr: String,

    #[arg(long = "engine")]
    engine: Option<String>,
}


fn main() {
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!("module" => "server"));

    let cli = Cli::parse();

}