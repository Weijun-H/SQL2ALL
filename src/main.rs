use std::path::PathBuf;

use clap::Parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Database url to connect to
    #[arg(short, long)]
    url: Option<String>,

    /// Output filename
    #[arg(short, long)]
    output: PathBuf,

    /// SQL query to execute
    #[arg()]
    query: String,
}

fn main() {
    let args = Args::parse();
}
