use crate::index::Doc;
use crate::shard::{SyncApply, SyncResult};
use crate::storage::{Digest, SyncEntry};
use anyhow::{Context, Result};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

pub const SYNC_EVERY: Duration = Duration::from_secs(5);
pub const SYNC_TIMEOUT: Duration = Duration::from_secs(60);
pub const SYNC_BATCH: usize = 200;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncReport {
    pub copied: usize,
    pub deleted: usize,
    pub files: usize,
    pub errors: Vec<String>,
}

impl SyncReport {
    fn merge(&mut self, other: SyncReport) {
        self.copied += other.copied;
        self.deleted += other.deleted;
        self.files += other.files;
        self.errors.extend(other.errors);
    }

    pub fn is_empty(&self) -> bool {
        self.copied == 0 && self.deleted == 0 && self.files == 0 && self.errors.is_empty()
    }
}

type Entries = HashMap<String, (u64, bool)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Best {
    version: u64,
    deleted: bool,
    source: usize,
}

fn newer(a: (u64, bool), b: (u64, bool)) -> bool {
    a.0 > b.0 || (a.0 == b.0 && a.1 && !b.1)
}

async fn get_json<T: serde::de::DeserializeOwned>(client: &reqwest::Client, url: String) -> Result<T> {
    Ok(client
        .get(url)
        .timeout(SYNC_TIMEOUT)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn post_json<Req: Serialize, Resp: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: String,
    body: &Req,
) -> Result<Resp> {
    Ok(client
        .post(url)
        .timeout(SYNC_TIMEOUT)
        .json(body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

fn file_url(addr: &str, id: &str) -> Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(addr)?;
    url.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("bad shard address {addr}"))?
        .pop_if_empty()
        .extend(["files", id]);
    Ok(url)
}

async fn copy_file(client: &reqwest::Client, from: &str, to: &str, id: &str) -> Result<bool> {
    let resp = client
        .get(file_url(from, id)?)
        .timeout(SYNC_TIMEOUT)
        .send()
        .await?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(false);
    }
    let bytes = resp.error_for_status()?.bytes().await?;
    client
        .put(file_url(to, id)?)
        .timeout(SYNC_TIMEOUT)
        .body(bytes)
        .send()
        .await?
        .error_for_status()?;
    Ok(true)
}

pub async fn sync_group(client: &reqwest::Client, group: &[String]) -> SyncReport {
    let mut report = SyncReport::default();
    if group.len() < 2 {
        return report;
    }
    let digests = join_all(
        group
            .iter()
            .map(|addr| get_json::<Digest>(client, format!("{addr}/sync/digest"))),
    )
    .await;
    let up: Vec<(usize, Digest)> = digests
        .into_iter()
        .enumerate()
        .filter_map(|(i, d)| d.ok().map(|d| (i, d)))
        .collect();
    if up.len() < 2 || up.iter().all(|(_, d)| *d == up[0].1) {
        return report;
    }
    let lists = join_all(
        up.iter()
            .map(|(i, _)| get_json::<Vec<SyncEntry>>(client, format!("{}/sync", group[*i]))),
    )
    .await;
    let mut replicas: Vec<(usize, Entries)> = Vec::new();
    for ((i, _), list) in up.iter().zip(lists) {
        match list {
            Ok(list) => replicas.push((
                *i,
                list.into_iter()
                    .map(|e| (e.id, (e.version, e.deleted)))
                    .collect(),
            )),
            Err(e) => report.errors.push(format!("{}: {e:#}", group[*i])),
        }
    }
    let mut best: HashMap<&str, Best> = HashMap::new();
    for (i, entries) in &replicas {
        for (id, &(version, deleted)) in entries {
            let candidate = Best {
                version,
                deleted,
                source: *i,
            };
            match best.get(id.as_str()) {
                Some(b) if !newer((version, deleted), (b.version, b.deleted)) => {}
                _ => {
                    best.insert(id.as_str(), candidate);
                }
            }
        }
    }
    for (target, entries) in &replicas {
        let mut deletes = Vec::new();
        let mut copies: HashMap<usize, Vec<(String, u64)>> = HashMap::new();
        for (id, b) in &best {
            let behind = match entries.get(*id) {
                Some(&local) => newer((b.version, b.deleted), local),
                None => true,
            };
            if b.source == *target || !behind {
                continue;
            }
            if b.deleted {
                deletes.push(SyncApply {
                    id: id.to_string(),
                    version: b.version,
                    doc: None,
                });
            } else {
                copies
                    .entry(b.source)
                    .or_default()
                    .push((id.to_string(), b.version));
            }
        }
        let to = &group[*target];
        for chunk in deletes.chunks(SYNC_BATCH) {
            match post_json::<_, SyncResult>(client, format!("{to}/sync"), &chunk).await {
                Ok(r) => report.deleted += r.applied,
                Err(e) => report.errors.push(format!("{to}: {e:#}")),
            }
        }
        for (source, items) in copies {
            let from = &group[source];
            for chunk in items.chunks(SYNC_BATCH) {
                match copy_docs(client, from, to, chunk).await {
                    Ok(r) => report.merge(r),
                    Err(e) => report.errors.push(format!("{from} -> {to}: {e:#}")),
                }
            }
        }
    }
    report
}

async fn copy_docs(
    client: &reqwest::Client,
    from: &str,
    to: &str,
    items: &[(String, u64)],
) -> Result<SyncReport> {
    let ids: Vec<&String> = items.iter().map(|(id, _)| id).collect();
    let docs: Vec<Doc> = post_json(client, format!("{from}/sync/fetch"), &ids)
        .await
        .context("fetch docs")?;
    let versions: HashMap<&str, u64> = items.iter().map(|(id, v)| (id.as_str(), *v)).collect();
    let applies: Vec<SyncApply> = docs
        .iter()
        .filter_map(|doc| {
            versions.get(doc.id.as_str()).map(|&version| SyncApply {
                id: doc.id.clone(),
                version,
                doc: Some(doc.clone()),
            })
        })
        .collect();
    let result: SyncResult = post_json(client, format!("{to}/sync"), &applies)
        .await
        .context("apply docs")?;
    let mut report = SyncReport {
        copied: result.applied,
        ..SyncReport::default()
    };
    for doc in docs.iter().filter(|doc| doc.meta.file) {
        match copy_file(client, from, to, &doc.id).await {
            Ok(true) => report.files += 1,
            Ok(false) => {}
            Err(e) => report.errors.push(format!("file {}: {e:#}", doc.id)),
        }
    }
    Ok(report)
}

pub async fn sync_all(client: &reqwest::Client, groups: &[Vec<String>]) -> SyncReport {
    let mut report = SyncReport::default();
    for r in join_all(groups.iter().map(|g| sync_group(client, g))).await {
        report.merge(r);
    }
    report
}

pub async fn run(groups: Vec<Vec<String>>) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let mut ticker = tokio::time::interval(SYNC_EVERY);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let report = sync_all(&client, &groups).await;
        if !report.is_empty() {
            tracing::info!(
                copied = report.copied,
                deleted = report.deleted,
                files = report.files,
                errors = ?report.errors,
                "replicas synced"
            );
        }
    }
}
