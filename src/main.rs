use std::{path::PathBuf, str::FromStr};

use clap::Parser;
use sql2all::Database;

use anyhow::Result;

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
    #[arg(short, long)]
    query: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let db = Database::from_str(args.url.as_deref().unwrap_or("").to_string().as_str())?;

    db.query(&args.query, &args.output).await?;
    Ok(())
}
