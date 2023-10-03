use clap::Parser;


#[derive(Parser)] // requires `derive` feature
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(default_value="127.0.0.1:4000")]
    addr: String,

    #[arg(long = "engine")]
    engine: Option<String>,
}


fn main() {
    let cli = Cli::parse();

}