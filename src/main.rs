use anyhow::Result;
use clap::{Parser, Subcommand};
use minisearch::coordinator::{self, DocsResponse};
use minisearch::index::read_dir;
use minisearch::shard;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "minisearch")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Shard {
        #[arg(long)]
        port: u16,
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long, default_value_t = 1000)]
        snapshot_every: usize,
    },
    Coordinator {
        #[arg(long)]
        port: u16,
        #[arg(long, value_delimiter = ',', required = true)]
        shards: Vec<String>,
    },
    Load {
        #[arg(long)]
        coordinator: String,
        #[arg(long)]
        dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    match Cli::parse().command {
        Command::Shard {
            port,
            data_dir,
            snapshot_every,
        } => shard::run(port, data_dir, snapshot_every).await,
        Command::Coordinator { port, shards } => coordinator::run(port, shards).await,
        Command::Load { coordinator, dir } => load(&coordinator, &dir).await,
    }
}

async fn load(coordinator: &str, dir: &Path) -> Result<()> {
    let docs = read_dir(dir)?;
    let url = format!("{}/docs", coordinator.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let mut total = DocsResponse::default();
    for batch in docs.chunks(100) {
        let resp = client.post(&url).json(batch).send().await?;
        let response: DocsResponse = if resp.status() == reqwest::StatusCode::BAD_GATEWAY {
            resp.json().await?
        } else {
            resp.error_for_status()?.json().await?
        };
        total.indexed += response.indexed;
        total.skipped += response.skipped;
        total.failed_shards.extend(response.failed_shards);
    }
    total.failed_shards.sort();
    total.failed_shards.dedup();
    println!("{}", serde_json::to_string(&total)?);
    Ok(())
}
