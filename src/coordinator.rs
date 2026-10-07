use crate::fuzzy::{
    ExpandRequest, ExpandResponse, MIN_PREFIX, Suggestion, merge_expansions, merge_suggestions,
};
use crate::analyzer::analyze;
use crate::extract::extract;
use crate::index::Doc;
use crate::query::{Segment, highlight, last_prefix, parse_query};
use crate::shard::{
    stamp, DocBody, DocInfo, DocList, Health, Hit, IndexResult, ListParams, SearchRequest, SearchResult,
    Stats, StatsRequest, SuggestParams, SuggestResponse, cursor, info_key, now, order,
};
use crate::synonyms::synonyms;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use anyhow::{Context, Result};
use axum::extract::{DefaultBodyLimit, Multipart, Path, Query, State};
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::future::join_all;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use std::time::{Duration, Instant};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Clone)]
pub enum Extractor {
    Thread,
    Process { exe: PathBuf, timeout: Duration },
}

#[derive(Clone)]
struct AppState {
    groups: Arc<Vec<Vec<String>>>,
    client: reqwest::Client,
    extractor: Extractor,
    slots: Arc<Semaphore>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardHit {
    pub id: String,
    pub title: String,
    pub score: f64,
    pub shard: String,
    pub snippet: Vec<Segment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub took_ms: u64,
    pub total: u64,
    pub from: usize,
    pub size: usize,
    pub failed_shards: Vec<String>,
    pub hits: Vec<ShardHit>,
    #[serde(default)]
    pub terms: Vec<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DocsResponse {
    pub indexed: usize,
    pub skipped: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_shards: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SearchParams {
    #[serde(default)]
    q: String,
    k: Option<usize>,
    from: Option<usize>,
    size: Option<usize>,
    operator: Option<String>,
    #[serde(default)]
    fuzzy: bool,
    #[serde(default = "yes")]
    proximity: bool,
    #[serde(default = "yes")]
    synonyms: bool,
    #[serde(default)]
    prefix: bool,
}

fn yes() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct AnalyzeParams {
    #[serde(default)]
    text: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalyzeResponse {
    pub tokens: Vec<String>,
}

pub fn analyze_text(text: &str) -> AnalyzeResponse {
    AnalyzeResponse {
        tokens: analyze(text),
    }
}

async fn analyze_handler(Query(params): Query<AnalyzeParams>) -> Json<AnalyzeResponse> {
    Json(analyze_text(&params.text))
}

pub const MAX_WINDOW: usize = 1000;

pub fn shard_for(id: &str, shards: usize) -> usize {
    (xxh3_64(id.as_bytes()) % shards as u64) as usize
}

pub fn query_terms(q: &str) -> Vec<String> {
    parse_query(q).terms
}

pub fn merge_stats(parts: &[Stats]) -> Stats {
    let mut total = Stats::default();
    for part in parts {
        total.n += part.n;
        total.total_len += part.total_len;
        for (term, df) in &part.df {
            *total.df.entry(term.clone()).or_default() += df;
        }
    }
    total
}

pub fn search_request(terms: Vec<String>, k: usize, stats: &Stats) -> SearchRequest {
    let avgdl = if stats.n == 0 {
        0.0
    } else {
        stats.total_len as f64 / stats.n as f64
    };
    SearchRequest {
        terms,
        phrases: Vec::new(),
        all: false,
        variants: HashMap::new(),
        proximity: false,
        k,
        n: stats.n,
        avgdl,
        df: stats.df.clone(),
    }
}

pub fn add_variants(
    variants: &mut HashMap<String, Vec<String>>,
    terms: &[String],
    term: &str,
    extra: impl IntoIterator<Item = String>,
) {
    let mut set: BTreeSet<String> = variants.remove(term).unwrap_or_default().into_iter().collect();
    set.extend(extra.into_iter().filter(|v| !terms.contains(v)));
    if !set.is_empty() {
        variants.insert(term.to_string(), set.into_iter().collect());
    }
}

pub fn with_variants(terms: &[String], variants: &HashMap<String, Vec<String>>) -> Vec<String> {
    let mut all: Vec<String> = terms
        .iter()
        .chain(variants.values().flatten())
        .cloned()
        .collect();
    all.sort();
    all.dedup();
    all
}

fn mark_failed(failed: &mut Vec<String>, group: &[String]) {
    for addr in group {
        if !failed.iter().any(|f| f == addr) {
            failed.push(addr.clone());
        }
    }
}

pub fn parse_groups(shards: &[String]) -> Result<Vec<Vec<String>>> {
    let groups: Vec<Vec<String>> = shards
        .iter()
        .map(|group| {
            group
                .split('|')
                .map(|s| s.trim().trim_end_matches('/').to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|group| !group.is_empty())
        .collect();
    anyhow::ensure!(!groups.is_empty(), "no shards given");
    Ok(groups)
}

fn prefer(group: &[String], first: &str) -> Vec<String> {
    std::iter::once(first.to_string())
        .chain(group.iter().filter(|a| *a != first).cloned())
        .collect()
}

async fn read_any<T, F, Fut>(group: &[String], what: &str, call: F) -> Option<(String, T)>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    for addr in group {
        match call(addr.clone()).await {
            Ok(value) => return Some((addr.clone(), value)),
            Err(e) => tracing::warn!(shard = %addr, error = %e, "{what} request failed"),
        }
    }
    None
}

pub fn merge_hits(parts: Vec<(String, Vec<Hit>)>, k: usize) -> Vec<ShardHit> {
    let mut hits: Vec<ShardHit> = parts
        .into_iter()
        .flat_map(|(shard, hits)| {
            hits.into_iter().map(move |hit| ShardHit {
                id: hit.id,
                title: hit.title,
                score: hit.score,
                shard: shard.clone(),
                snippet: hit.snippet,
            })
        })
        .collect();
    hits.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
    hits.truncate(k);
    hits
}

async fn post_json<Req: Serialize, Resp: DeserializeOwned>(
    client: &reqwest::Client,
    url: String,
    body: &Req,
) -> Result<Resp> {
    Ok(client
        .post(url)
        .json(body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

fn first_ok<T>(group: &[String], what: &str, results: Vec<Result<T>>) -> (Option<T>, Vec<String>) {
    let mut first = None;
    let mut failed = Vec::new();
    for (addr, result) in group.iter().zip(results) {
        match result {
            Ok(value) => {
                first.get_or_insert(value);
            }
            Err(e) => {
                tracing::warn!(shard = %addr, error = %e, "{what} request failed");
                failed.push(addr.clone());
            }
        }
    }
    (first, failed)
}

async fn docs_handler(
    State(st): State<AppState>,
    Json(mut docs): Json<Vec<Doc>>,
) -> (StatusCode, Json<DocsResponse>) {
    let created = now();
    for doc in &mut docs {
        if doc.meta.created == 0 {
            doc.meta.created = created;
        }
        if doc.meta.format.is_empty() {
            doc.meta.format = "txt".to_string();
        }
        if doc.meta.size == 0 {
            doc.meta.size = (doc.title.len() + 1 + doc.body.len()) as u64;
        }
    }
    let response = index_in_groups(&st, docs).await;
    let status = if response.failed_shards.is_empty() {
        StatusCode::OK
    } else {
        StatusCode::BAD_GATEWAY
    };
    (status, Json(response))
}

pub const MAX_UPLOAD_FILES: usize = 20;
pub const UPLOAD_BODY_LIMIT: usize = 50 * 1024 * 1024;
pub const JSON_BODY_LIMIT: usize = 64 * 1024 * 1024;
pub const EXTRACT_TIMEOUT: Duration = Duration::from_secs(30);
pub const FILE_TIMEOUT: Duration = Duration::from_secs(60);

pub const EXTRACT_ERROR_CODE: i32 = 2;

async fn extract_file(st: &AppState, file: String, bytes: axum::body::Bytes) -> Result<Doc> {
    let _slot = st.slots.acquire().await?;
    match &st.extractor {
        Extractor::Thread => {
            let task = tokio::task::spawn_blocking(move || extract(&file, &bytes));
            match tokio::time::timeout(EXTRACT_TIMEOUT, task).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => anyhow::bail!("не удалось прочитать файл"),
                Err(_) => anyhow::bail!("файл обрабатывается слишком долго"),
            }
        }
        Extractor::Process { exe, timeout } => extract_in_process(exe, *timeout, file, bytes).await,
    }
}

async fn extract_in_process(
    exe: &std::path::Path,
    timeout: Duration,
    file: String,
    bytes: axum::body::Bytes,
) -> Result<Doc> {
    let mut child = tokio::process::Command::new(exe)
        .arg("extract")
        .arg(format!("--name={file}"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("не удалось запустить разбор файла")?;
    let mut stdin = child.stdin.take().context("не удалось запустить разбор файла")?;
    let feed = tokio::spawn(async move {
        stdin.write_all(&bytes).await.ok();
    });
    let output = match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(output) => output.context("не удалось прочитать файл")?,
        Err(_) => anyhow::bail!("файл обрабатывается слишком долго"),
    };
    feed.abort();
    if output.status.success() {
        return serde_json::from_slice(&output.stdout).context("не удалось прочитать файл");
    }
    if output.status.code() == Some(EXTRACT_ERROR_CODE) {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let message = stderr.lines().rev().find(|l| !l.trim().is_empty()).unwrap_or("").trim().to_string();
        anyhow::bail!(if message.is_empty() {
            "не удалось прочитать файл".to_string()
        } else {
            message
        });
    }
    tracing::warn!(file, status = %output.status, "extractor crashed");
    anyhow::bail!("файл повреждён или слишком сложный для разбора")
}

async fn store_file(st: &AppState, id: &str, bytes: &axum::body::Bytes) -> Vec<String> {
    let group = &st.groups[shard_for(id, st.groups.len())];
    let results = join_all(group.iter().map(|addr| async move {
        let mut url = reqwest::Url::parse(addr)?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("bad shard address {addr}"))?
            .pop_if_empty()
            .extend(["files", id]);
        st.client
            .put(url)
            .timeout(FILE_TIMEOUT)
            .body(bytes.clone())
            .send()
            .await?
            .error_for_status()?;
        anyhow::Ok(())
    }))
    .await;
    first_ok(group, "file", results).1
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UploadResult {
    pub file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UploadResponse {
    pub results: Vec<UploadResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_shards: Vec<String>,
}

fn upload_error(file: String, id: Option<String>, error: String) -> UploadResult {
    UploadResult {
        file,
        id,
        status: "error".to_string(),
        error: Some(error),
    }
}

async fn upload_handler(
    State(st): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, (StatusCode, String)> {
    let mut files = Vec::new();
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| (e.status(), e.body_text()))?
    {
        let Some(name) = field.file_name().map(str::to_string) else {
            continue;
        };
        if files.len() == MAX_UPLOAD_FILES {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("не больше {MAX_UPLOAD_FILES} файлов за раз"),
            ));
        }
        let bytes = field.bytes().await.map_err(|e| (e.status(), e.body_text()))?;
        files.push((name, bytes));
    }
    if files.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "нет файлов".to_string()));
    }
    let st = &st;
    let outcomes = join_all(files.into_iter().map(|(file, bytes)| async move {
        let mut doc = match extract_file(st, file.clone(), bytes.clone()).await {
            Ok(doc) => doc,
            Err(e) => return (upload_error(file, None, format!("{e:#}")), Vec::new()),
        };
        doc.meta.created = now();
        doc.meta.file = true;
        let id = doc.id.clone();
        let mut response = index_in_groups(st, vec![doc]).await;
        if response.indexed > 0 {
            response.failed_shards.extend(store_file(st, &id, &bytes).await);
        }
        let result = if response.indexed > 0 {
            UploadResult {
                file,
                id: Some(id),
                status: "indexed".to_string(),
                error: None,
            }
        } else if response.skipped > 0 {
            UploadResult {
                file,
                id: Some(id),
                status: "exists".to_string(),
                error: None,
            }
        } else {
            upload_error(file, Some(id), "хранилище недоступно".to_string())
        };
        (result, response.failed_shards)
    }))
    .await;
    let mut response = UploadResponse::default();
    for (result, failed) in outcomes {
        response.results.push(result);
        for addr in failed {
            if !response.failed_shards.contains(&addr) {
                response.failed_shards.push(addr);
            }
        }
    }
    Ok(Json(response))
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DocListResponse {
    pub total: u64,
    pub docs: Vec<DocInfo>,
    pub next: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_shards: Vec<String>,
}

async fn get_list(client: &reqwest::Client, addr: &str, params: &ListParams) -> Result<DocList> {
    let mut url = reqwest::Url::parse(&format!("{addr}/docs"))?;
    url.query_pairs_mut()
        .append_pair("after", &params.after)
        .append_pair("size", &params.size().to_string())
        .append_pair("sort", params.sort.as_str())
        .append_pair("desc", if params.desc { "true" } else { "false" })
        .append_pair("format", &params.format);
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

pub fn merge_lists(parts: Vec<DocList>, params: &ListParams) -> (u64, Vec<DocInfo>, Option<String>) {
    let size = params.size();
    let total = parts.iter().map(|p| p.total).sum();
    let mut more = parts.iter().any(|p| p.more);
    let mut docs: Vec<DocInfo> = parts.into_iter().flat_map(|p| p.docs).collect();
    docs.sort_by(|a, b| {
        order(
            params.desc,
            &info_key(params.sort, a),
            &info_key(params.sort, b),
        )
    });
    docs.dedup_by(|a, b| a.id == b.id);
    if docs.len() > size {
        docs.truncate(size);
        more = true;
    }
    let next = if more {
        docs.last().map(|d| cursor(params.sort, d))
    } else {
        None
    };
    (total, docs, next)
}

async fn list_handler(
    State(st): State<AppState>,
    Query(params): Query<ListParams>,
) -> Json<DocListResponse> {
    let client = &st.client;
    let params = &params;
    let results = join_all(st.groups.iter().map(|group| {
        read_any(group, "list", move |s| async move {
            get_list(client, &s, params).await
        })
    }))
    .await;
    let mut parts = Vec::new();
    let mut failed = Vec::new();
    for (group, result) in st.groups.iter().zip(results) {
        match result {
            Some((_, part)) => parts.push(part),
            None => mark_failed(&mut failed, group),
        }
    }
    let (total, docs, next) = merge_lists(parts, params);
    Json(DocListResponse {
        total,
        docs,
        next,
        failed_shards: failed,
    })
}

async fn get_doc(client: &reqwest::Client, addr: &str, id: &str) -> Result<Option<Doc>> {
    let resp = client.get(doc_url(addr, id)?).send().await?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    Ok(Some(resp.error_for_status()?.json().await?))
}

#[derive(Debug, Default, Deserialize)]
pub struct GetParams {
    #[serde(default)]
    pub hl: String,
}

pub fn doc_with_highlight(doc: &Doc, hl: &str) -> serde_json::Value {
    let mut value = serde_json::json!(doc);
    let terms: HashSet<&str> = hl.split_whitespace().collect();
    if !terms.is_empty() {
        let segments = highlight(&doc.body, &terms);
        let matches = segments.iter().filter(|s| s.hl).count();
        value["highlight"] = serde_json::json!(segments);
        value["matches"] = serde_json::json!(matches);
    }
    value
}

async fn find_doc(st: &AppState, id: &str) -> Option<Option<Doc>> {
    let group = &st.groups[shard_for(id, st.groups.len())];
    let client = &st.client;
    read_any(group, "get", move |s| async move { get_doc(client, &s, id).await })
        .await
        .map(|(_, doc)| doc)
}

async fn get_handler(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<GetParams>,
) -> (StatusCode, Json<serde_json::Value>) {
    let group = &st.groups[shard_for(&id, st.groups.len())];
    match find_doc(&st, &id).await {
        Some(Some(doc)) => (StatusCode::OK, Json(doc_with_highlight(&doc, &params.hl))),
        Some(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "id": id, "result": "not_found" })),
        ),
        None => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "id": id, "failed_shards": group })),
        ),
    }
}

pub fn content_type(format: &str) -> &'static str {
    match format {
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "pdf" => "application/pdf",
        _ => "text/plain; charset=utf-8",
    }
}

pub fn content_disposition(name: &str) -> String {
    let ascii: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || ".-_ ".contains(c) { c } else { '_' })
        .collect();
    let mut encoded = String::new();
    for byte in name.bytes() {
        if byte.is_ascii_alphanumeric() || b".-_~".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    format!("attachment; filename=\"{ascii}\"; filename*=UTF-8''{encoded}")
}

async fn fetch_file(st: &AppState, id: &str) -> Option<Vec<u8>> {
    let group = &st.groups[shard_for(id, st.groups.len())];
    let client = &st.client;
    for addr in group {
        let Ok(mut url) = reqwest::Url::parse(addr) else {
            continue;
        };
        if let Ok(mut segments) = url.path_segments_mut() {
            segments.pop_if_empty().extend(["files", id]);
        }
        let resp = client.get(url).timeout(FILE_TIMEOUT).send().await;
        match resp {
            Ok(resp) if resp.status().is_success() => match resp.bytes().await {
                Ok(bytes) => return Some(bytes.to_vec()),
                Err(e) => tracing::warn!(shard = %addr, error = %e, "file request failed"),
            },
            Ok(resp) => tracing::warn!(shard = %addr, status = %resp.status(), "file not found"),
            Err(e) => tracing::warn!(shard = %addr, error = %e, "file request failed"),
        }
    }
    None
}

async fn file_handler(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    let doc = match find_doc(&st, &id).await {
        Some(Some(doc)) => doc,
        Some(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "id": id, "result": "not_found" })),
            )
                .into_response();
        }
        None => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "id": id, "result": "unavailable" })),
            )
                .into_response();
        }
    };
    let original = if doc.meta.file {
        fetch_file(&st, &id).await
    } else {
        None
    };
    let (bytes, format) = match original {
        Some(bytes) => (bytes, doc.meta.format.as_str()),
        None => (format!("{}\n\n{}\n", doc.title, doc.body).into_bytes(), "txt"),
    };
    let name = format!("{id}.{format}");
    (
        [
            (header::CONTENT_TYPE, content_type(format).to_string()),
            (header::CONTENT_DISPOSITION, content_disposition(&name)),
        ],
        bytes,
    )
        .into_response()
}

async fn index_in_groups(st: &AppState, docs: Vec<Doc>) -> DocsResponse {
    let mut batches: Vec<Vec<Doc>> = vec![Vec::new(); st.groups.len()];
    for doc in docs {
        let shard = shard_for(&doc.id, st.groups.len());
        batches[shard].push(doc);
    }
    let client = &st.client;
    let version = stamp();
    let requests = st
        .groups
        .iter()
        .zip(&batches)
        .filter(|(_, batch)| !batch.is_empty())
        .map(|(group, batch)| async move {
            let results = join_all(group.iter().map(|addr| {
                post_json::<_, IndexResult>(client, format!("{addr}/docs?version={version}"), batch)
            }))
            .await;
            first_ok(group, "docs", results)
        });
    let mut response = DocsResponse::default();
    for (first, failed) in join_all(requests).await {
        if let Some(r) = first {
            response.indexed += r.indexed;
            response.skipped += r.skipped;
        }
        response.failed_shards.extend(failed);
    }
    response
}

fn doc_url(addr: &str, id: &str) -> Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(addr)?;
    url.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("bad shard address {addr}"))?
        .pop_if_empty()
        .extend(["docs", id]);
    Ok(url)
}

async fn forward_write(
    st: &AppState,
    id: &str,
    method: reqwest::Method,
    doc: Option<&DocBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let group = &st.groups[shard_for(id, st.groups.len())];
    let method = &method;
    let version = stamp();
    let results = join_all(group.iter().map(|addr| async move {
        let mut url = doc_url(addr, id)?;
        if doc.is_none() {
            url.query_pairs_mut()
                .append_pair("version", &version.to_string());
        }
        let mut request = st.client.request(method.clone(), url);
        if let Some(doc) = doc {
            request = request.json(doc);
        }
        let resp = request.send().await?;
        let status = StatusCode::from_u16(resp.status().as_u16())?;
        anyhow::ensure!(!status.is_server_error(), "shard returned {status}");
        let body: serde_json::Value = resp.json().await?;
        anyhow::Ok((status, body))
    }))
    .await;
    match first_ok(group, "write", results) {
        (Some((status, body)), failed) if failed.is_empty() => (status, Json(body)),
        (_, failed) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "id": id, "failed_shards": failed })),
        ),
    }
}

async fn delete_handler(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    forward_write(&st, &id, reqwest::Method::DELETE, None).await
}

async fn put_handler(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(mut doc): Json<DocBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    if doc.updated == 0 {
        doc.updated = now();
    }
    doc.version = stamp();
    forward_write(&st, &id, reqwest::Method::PUT, Some(&doc)).await
}

async fn search_handler(
    State(st): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<SearchResponse>, (StatusCode, String)> {
    let start = Instant::now();
    let from = params.from.unwrap_or(0);
    let size = params.size.or(params.k).unwrap_or(10);
    if from.saturating_add(size) > MAX_WINDOW {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("from + size must be at most {MAX_WINDOW}"),
        ));
    }
    let all = match params.operator.as_deref() {
        None | Some("or") => false,
        Some("and") => true,
        Some(other) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("unknown operator {other}, expected or/and"),
            ));
        }
    };
    let k = from + size;
    let parsed = parse_query(&params.q);
    let terms = parsed.terms;
    let mut failed = Vec::new();
    let mut hits = Vec::new();
    let mut total = 0;
    let mut variants = HashMap::new();
    let client = &st.client;
    if params.synonyms {
        for term in &terms {
            add_variants(&mut variants, &terms, term, synonyms(term));
        }
    }
    let prefix = last_prefix(&params.q).filter(|p| params.prefix && terms.contains(&p.term));
    if !terms.is_empty() && (params.fuzzy || prefix.is_some()) {
        let expand_req = &ExpandRequest {
            terms: if params.fuzzy { terms.clone() } else { Vec::new() },
            prefix: prefix.as_ref().map(|p| p.word.clone()),
        };
        let results = join_all(st.groups.iter().map(|group| {
            read_any(group, "expand", move |s| {
                post_json::<_, ExpandResponse>(client, format!("{s}/expand"), expand_req)
            })
        }))
        .await;
        let mut parts = Vec::new();
        for (group, result) in st.groups.iter().zip(results) {
            match result {
                Some((_, part)) => parts.push(part),
                None => mark_failed(&mut failed, group),
            }
        }
        for (term, found) in merge_expansions(&terms, &parts) {
            add_variants(&mut variants, &terms, &term, found);
        }
        if let Some(prefix) = &prefix {
            let completions: Vec<String> =
                parts.iter().flat_map(|p| p.completions.clone()).collect();
            add_variants(&mut variants, &terms, &prefix.term, completions);
        }
    }
    let all_terms = with_variants(&terms, &variants);
    if !terms.is_empty() {
        let stats_req = &StatsRequest {
            terms: with_variants(&terms, &variants),
        };
        let results = join_all(st.groups.iter().map(|group| {
            read_any(group, "stats", move |s| {
                post_json::<_, Stats>(client, format!("{s}/stats"), stats_req)
            })
        }))
        .await;
        let mut alive = Vec::new();
        let mut parts = Vec::new();
        for (group, result) in st.groups.iter().zip(results) {
            match result {
                Some((addr, stats)) => {
                    alive.push((group, prefer(group, &addr)));
                    parts.push(stats);
                }
                None => mark_failed(&mut failed, group),
            }
        }
        let global = merge_stats(&parts);
        if global.n > 0 {
            let mut search_req = search_request(terms, k, &global);
            search_req.phrases = parsed.phrases;
            search_req.all = all;
            search_req.variants = variants;
            search_req.proximity = params.proximity;
            let search_req = &search_req;
            let results = join_all(alive.iter().map(|(_, order)| {
                read_any(order, "search", move |s| {
                    post_json::<_, SearchResult>(client, format!("{s}/search"), search_req)
                })
            }))
            .await;
            let mut ok = Vec::new();
            for ((group, _), result) in alive.iter().zip(results) {
                match result {
                    Some((addr, result)) => {
                        total += result.total;
                        ok.push((addr, result.hits));
                    }
                    None => mark_failed(&mut failed, group),
                }
            }
            hits = merge_hits(ok, k).into_iter().skip(from).collect();
        }
    }
    Ok(Json(SearchResponse {
        took_ms: start.elapsed().as_millis() as u64,
        total,
        from,
        size,
        failed_shards: failed,
        hits,
        terms: all_terms,
    }))
}

async fn get_suggest(client: &reqwest::Client, addr: &str, prefix: &str) -> Result<Vec<Suggestion>> {
    let mut url = reqwest::Url::parse(&format!("{addr}/suggest"))?;
    url.query_pairs_mut().append_pair("prefix", prefix);
    let resp: SuggestResponse = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(resp.suggestions)
}

async fn suggest_handler(
    State(st): State<AppState>,
    Query(params): Query<SuggestParams>,
) -> Json<SuggestResponse> {
    let prefix = params.prefix.trim();
    if prefix.chars().count() < MIN_PREFIX {
        return Json(SuggestResponse::default());
    }
    let client = &st.client;
    let results = join_all(st.groups.iter().map(|group| {
        read_any(group, "suggest", move |s| async move {
            get_suggest(client, &s, prefix).await
        })
    }))
    .await;
    let mut parts = Vec::new();
    let mut failed = Vec::new();
    for (group, result) in st.groups.iter().zip(results) {
        match result {
            Some((_, part)) => parts.push(part),
            None => mark_failed(&mut failed, group),
        }
    }
    Json(SuggestResponse {
        suggestions: merge_suggestions(parts),
        failed_shards: failed,
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardStatus {
    pub group: usize,
    pub addr: String,
    pub up: bool,
    pub docs: u64,
    pub terms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStats {
    pub shards: Vec<ShardStatus>,
}

const INDEX_HTML: &str = include_str!("../web/index.html");

async fn index_handler() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn health_handler(State(st): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "shards": st.groups.len(),
        "replicas": st.groups.iter().map(Vec::len).sum::<usize>(),
    }))
}

async fn get_health(client: &reqwest::Client, addr: &str) -> Result<Health> {
    Ok(client
        .get(format!("{addr}/health"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn stats_handler(State(st): State<AppState>) -> Json<ClusterStats> {
    let replicas: Vec<(usize, &String)> = st
        .groups
        .iter()
        .enumerate()
        .flat_map(|(g, group)| group.iter().map(move |addr| (g, addr)))
        .collect();
    let results = join_all(replicas.iter().map(|(_, s)| get_health(&st.client, s))).await;
    let shards = replicas
        .into_iter()
        .zip(results)
        .map(|((group, addr), result)| match result {
            Ok(h) => ShardStatus {
                group,
                addr: addr.clone(),
                up: true,
                docs: h.docs,
                terms: h.terms,
            },
            Err(e) => {
                tracing::warn!(shard = %addr, error = %e, "health request failed");
                ShardStatus {
                    group,
                    addr: addr.clone(),
                    up: false,
                    docs: 0,
                    terms: 0,
                }
            }
        })
        .collect();
    Json(ClusterStats { shards })
}

pub fn router(groups: Vec<Vec<String>>) -> Result<Router> {
    router_with(groups, Extractor::Thread)
}

pub fn router_with(groups: Vec<Vec<String>>, extractor: Extractor) -> Result<Router> {
    anyhow::ensure!(groups.iter().all(|g| !g.is_empty()), "empty replica group");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let workers = std::thread::available_parallelism().map_or(2, |n| n.get());
    let state = AppState {
        groups: Arc::new(groups),
        client,
        extractor,
        slots: Arc::new(Semaphore::new(workers)),
    };
    Ok(Router::new()
        .route(
            "/docs",
            get(list_handler)
                .post(docs_handler)
                .layer(DefaultBodyLimit::max(JSON_BODY_LIMIT)),
        )
        .route(
            "/docs/{id}",
            get(get_handler)
                .put(put_handler)
                .delete(delete_handler)
                .layer(DefaultBodyLimit::max(JSON_BODY_LIMIT)),
        )
        .route("/docs/{id}/file", get(file_handler))
        .route(
            "/upload",
            post(upload_handler).layer(DefaultBodyLimit::max(UPLOAD_BODY_LIMIT)),
        )
        .route("/search", get(search_handler))
        .route("/stats", get(stats_handler))
        .route("/suggest", get(suggest_handler))
        .route("/analyze", get(analyze_handler))
        .route("/health", get(health_handler))
        .route("/", get(index_handler))
        .with_state(state))
}

pub async fn run(port: u16, shards: Vec<String>) -> Result<()> {
    let groups = parse_groups(&shards)?;
    let extractor = Extractor::Process {
        exe: std::env::current_exe().context("current exe")?,
        timeout: EXTRACT_TIMEOUT,
    };
    let app = router_with(groups.clone(), extractor)?;
    tokio::spawn(async move {
        if let Err(e) = crate::sync::run(groups).await {
            tracing::error!(error = %e, "replica sync stopped");
        }
    });
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "coordinator listening");
    axum::serve(listener, app).await?;
    Ok(())
}
