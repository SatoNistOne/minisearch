use anyhow::Result;
use clap::{Parser, Subcommand};
use minisearch::coordinator::{self, DocsResponse, EXTRACT_ERROR_CODE};
use minisearch::extract::{MAX_FILE_BYTES, extract};
use minisearch::index::read_dir;
use minisearch::shard;
use std::io::{Read, Write};
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
        #[arg(long, value_delimiter = ',')]
        drain: Vec<String>,
    },
    Load {
        #[arg(long)]
        coordinator: String,
        #[arg(long)]
        dir: PathBuf,
    },
    #[command(hide = true)]
    Extract {
        #[arg(long)]
        name: String,
    },
}

const EXTRACT_MEMORY: u64 = 1024 * 1024 * 1024;
const EXTRACT_CPU_SECONDS: u64 = 60;

fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Command::Extract { name } = &cli.command {
        std::process::exit(extract_main(name));
    }
    tracing_subscriber::fmt::init();
    tokio::runtime::Runtime::new()?.block_on(serve(cli.command))
}

fn extract_main(name: &str) -> i32 {
    #[cfg(unix)]
    {
        rlimit::Resource::AS
            .set(EXTRACT_MEMORY, EXTRACT_MEMORY)
            .ok();
        rlimit::Resource::CPU
            .set(EXTRACT_CPU_SECONDS, EXTRACT_CPU_SECONDS)
            .ok();
    }
    let mut bytes = Vec::new();
    let limit = MAX_FILE_BYTES as u64 + 1;
    if let Err(e) = std::io::stdin().take(limit).read_to_end(&mut bytes) {
        eprintln!("не удалось прочитать файл: {e}");
        return EXTRACT_ERROR_CODE;
    }
    let doc = match extract(name, &bytes) {
        Ok(doc) => doc,
        Err(e) => {
            eprintln!("{e:#}");
            return EXTRACT_ERROR_CODE;
        }
    };
    match serde_json::to_vec(&doc) {
        Ok(json) if std::io::stdout().write_all(&json).is_ok() => 0,
        _ => EXTRACT_ERROR_CODE,
    }
}

async fn serve(command: Command) -> Result<()> {
    match command {
        Command::Shard {
            port,
            data_dir,
            snapshot_every,
        } => shard::run(port, data_dir, snapshot_every).await,
        Command::Coordinator {
            port,
            shards,
            drain,
        } => coordinator::run(port, shards, drain).await,
        Command::Load { coordinator, dir } => load(&coordinator, &dir).await,
        Command::Extract { .. } => Ok(()),
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
