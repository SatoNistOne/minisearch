use crate::fuzzy::{self, ExpandRequest, ExpandResponse, FUZZY_WEIGHT, Suggestion};
use crate::index::{Doc, Index, Meta};
use crate::query::{Segment, snippet};
use crate::scoring;
use crate::storage::{SNAPSHOT_FILE, Store};
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use unicode_segmentation::UnicodeSegmentation;

pub type SharedStore = Arc<RwLock<Store>>;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexResult {
    pub indexed: usize,
    pub skipped: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsRequest {
    pub terms: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Stats {
    pub n: u64,
    pub total_len: u64,
    pub df: HashMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    pub terms: Vec<String>,
    #[serde(default)]
    pub phrases: Vec<Vec<String>>,
    #[serde(default)]
    pub all: bool,
    #[serde(default)]
    pub variants: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub proximity: bool,
    pub k: usize,
    pub n: u64,
    pub avgdl: f64,
    pub df: HashMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hit {
    pub id: String,
    pub title: String,
    pub score: f64,
    #[serde(default)]
    pub snippet: Vec<Segment>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    pub total: u64,
    pub hits: Vec<Hit>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Health {
    pub status: String,
    pub docs: u64,
    pub terms: u64,
}

pub fn health(index: &Index) -> Health {
    Health {
        status: "ok".to_string(),
        docs: index.len() as u64,
        terms: index.postings.len() as u64,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocBody {
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub updated: u64,
}

pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriteResult {
    pub id: String,
    pub result: String,
}

pub fn write_result(id: String, result: &str) -> Json<WriteResult> {
    Json(WriteResult {
        id,
        result: result.to_string(),
    })
}

pub fn index_docs(index: &mut Index, docs: Vec<Doc>) -> anyhow::Result<IndexResult> {
    let mut result = IndexResult::default();
    for doc in docs {
        if index.add(doc)? {
            result.indexed += 1;
        } else {
            result.skipped += 1;
        }
    }
    Ok(result)
}

pub fn stats(index: &Index, terms: &[String]) -> Stats {
    let df = terms
        .iter()
        .map(|term| {
            let count = index.postings.get(term).map_or(0, |p| p.len() as u64);
            (term.clone(), count)
        })
        .collect();
    Stats {
        n: index.len() as u64,
        total_len: index.total_len,
        df,
    }
}

fn has_phrase(index: &Index, doc: u32, phrase: &[String]) -> bool {
    let mut lists = Vec::with_capacity(phrase.len());
    for term in phrase {
        let Some(postings) = index.postings.get(term) else {
            return false;
        };
        let Some(posting) = postings
            .binary_search_by_key(&doc, |p| p.doc)
            .ok()
            .and_then(|i| postings.get(i))
        else {
            return false;
        };
        lists.push(&posting.positions);
    }
    let Some((first, rest)) = lists.split_first() else {
        return true;
    };
    first.iter().any(|&start| {
        rest.iter().zip(1u32..).all(|(positions, offset)| {
            start
                .checked_add(offset)
                .is_some_and(|pos| positions.binary_search(&pos).is_ok())
        })
    })
}

pub const PROXIMITY_WEIGHT: f64 = 1.0;

fn positions_of(index: &Index, doc: u32, members: &[&str]) -> Vec<u32> {
    let mut out = Vec::new();
    for term in members {
        let posting = index.postings.get(*term).and_then(|list| {
            list.binary_search_by_key(&doc, |p| p.doc)
                .ok()
                .and_then(|i| list.get(i))
        });
        if let Some(posting) = posting {
            out.extend_from_slice(&posting.positions);
        }
    }
    out
}

pub fn proximity(groups: &[Vec<u32>]) -> f64 {
    let lists: Vec<&Vec<u32>> = groups.iter().filter(|g| !g.is_empty()).collect();
    let m = lists.len();
    if m < 2 {
        return 0.0;
    }
    let mut events: Vec<(u32, usize)> = lists
        .iter()
        .enumerate()
        .flat_map(|(g, list)| list.iter().map(move |&pos| (pos, g)))
        .collect();
    events.sort_unstable();
    let mut counts = vec![0usize; m];
    let mut covered = 0;
    let mut best = u32::MAX;
    let mut left = 0;
    for &(pos, g) in &events {
        if counts[g] == 0 {
            covered += 1;
        }
        counts[g] += 1;
        while covered == m {
            let Some(&(start, first)) = events.get(left) else {
                break;
            };
            best = best.min(pos - start + 1);
            counts[first] -= 1;
            if counts[first] == 0 {
                covered -= 1;
            }
            left += 1;
        }
    }
    let gap = (best as usize + 1).saturating_sub(m).max(1);
    PROXIMITY_WEIGHT * (m - 1) as f64 / gap as f64
}

pub fn search(index: &Index, req: &SearchRequest) -> SearchResult {
    if req.n == 0 || req.avgdl <= 0.0 {
        return SearchResult::default();
    }
    let mut seen = HashSet::new();
    let unique: Vec<&str> = req
        .terms
        .iter()
        .map(String::as_str)
        .filter(|t| seen.insert(*t))
        .collect();
    let mut weighted: Vec<(&str, f64)> = unique.iter().map(|&t| (t, 1.0)).collect();
    let mut extra: Vec<&str> = req
        .variants
        .values()
        .flatten()
        .map(String::as_str)
        .filter(|t| seen.insert(*t))
        .collect();
    extra.sort_unstable();
    weighted.extend(extra.into_iter().map(|t| (t, FUZZY_WEIGHT)));
    let mut matches: HashMap<u32, Vec<(&str, u32, u64, f64)>> = HashMap::new();
    for &(term, weight) in &weighted {
        let df = req.df.get(term).copied().unwrap_or(0);
        if df == 0 {
            continue;
        }
        let Some(postings) = index.postings.get(term) else {
            continue;
        };
        for posting in postings {
            matches
                .entry(posting.doc)
                .or_default()
                .push((term, posting.tf(), df, weight));
        }
    }
    let covers = |terms: &[(&str, u32, u64, f64)]| {
        unique.iter().all(|&t| {
            let variants = req.variants.get(t).map(Vec::as_slice).unwrap_or_default();
            terms
                .iter()
                .any(|&(m, ..)| m == t || variants.iter().any(|v| v == m))
        })
    };
    let groups: Vec<Vec<&str>> = unique
        .iter()
        .map(|&t| {
            std::iter::once(t)
                .chain(req.variants.get(t).into_iter().flatten().map(String::as_str))
                .collect()
        })
        .collect();
    let bonus = |doc: u32| {
        if !req.proximity || groups.len() < 2 {
            return 0.0;
        }
        let positions: Vec<Vec<u32>> = groups
            .iter()
            .map(|members| positions_of(index, doc, members))
            .collect();
        proximity(&positions)
    };
    let mut scored: Vec<(f64, &Doc)> = matches
        .into_iter()
        .filter(|(doc, terms)| {
            (!req.all || covers(terms)) && req.phrases.iter().all(|p| has_phrase(index, *doc, p))
        })
        .filter_map(|(doc, terms)| {
            let len = *index.lengths.get(&doc)?;
            let stored = index.docs.get(&doc)?;
            let score = terms
                .iter()
                .map(|&(_, tf, df, w)| {
                    w * scoring::term_score(scoring::idf(req.n, df), tf, len, req.avgdl)
                })
                .sum::<f64>()
                + bonus(doc);
            Some((score, stored))
        })
        .collect();
    let total = scored.len() as u64;
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.id.cmp(&b.1.id)));
    scored.truncate(req.k);
    let terms: HashSet<&str> = weighted.into_iter().map(|(t, _)| t).collect();
    let hits = scored
        .into_iter()
        .map(|(score, stored)| Hit {
            id: stored.id.clone(),
            title: stored.title.clone(),
            score,
            snippet: snippet(&stored.body, &terms),
        })
        .collect();
    SearchResult { total, hits }
}

pub const SHARD_BODY_LIMIT: usize = 256 * 1024 * 1024;
pub const LIST_DEFAULT: usize = 50;
pub const LIST_MAX: usize = 200;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocInfo {
    pub id: String,
    pub title: String,
    pub words: u64,
    #[serde(default)]
    pub format: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub created: u64,
    #[serde(default)]
    pub updated: u64,
    #[serde(default)]
    pub file: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocList {
    pub total: u64,
    pub docs: Vec<DocInfo>,
    pub more: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Sort {
    #[default]
    Id,
    Title,
    Created,
    Size,
}

impl Sort {
    pub fn as_str(self) -> &'static str {
        match self {
            Sort::Id => "id",
            Sort::Title => "title",
            Sort::Created => "created",
            Sort::Size => "size",
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
pub struct ListParams {
    #[serde(default)]
    pub after: String,
    pub size: Option<usize>,
    #[serde(default)]
    pub sort: Sort,
    #[serde(default)]
    pub desc: bool,
    #[serde(default)]
    pub format: String,
}

impl ListParams {
    pub fn size(&self) -> usize {
        self.size.unwrap_or(LIST_DEFAULT).clamp(1, LIST_MAX)
    }

    pub fn first(size: usize) -> Self {
        Self {
            size: Some(size),
            ..Self::default()
        }
    }
}

pub const CURSOR_SEP: char = '\u{1f}';

pub fn sort_key(sort: Sort, id: &str, title: &str, meta: &Meta) -> String {
    match sort {
        Sort::Id => id.to_string(),
        Sort::Title => title.to_lowercase(),
        Sort::Created => format!("{:020}", meta.created),
        Sort::Size => format!("{:020}", meta.size),
    }
}

pub fn info_key(sort: Sort, info: &DocInfo) -> (String, String) {
    let meta = Meta {
        format: String::new(),
        size: info.size,
        created: info.created,
        updated: info.updated,
        file: info.file,
    };
    (sort_key(sort, &info.id, &info.title, &meta), info.id.clone())
}

pub fn cursor(sort: Sort, info: &DocInfo) -> String {
    match sort {
        Sort::Id => info.id.clone(),
        _ => {
            let (key, id) = info_key(sort, info);
            format!("{key}{CURSOR_SEP}{id}")
        }
    }
}

pub fn parse_cursor(after: &str) -> Option<(String, String)> {
    if after.is_empty() {
        return None;
    }
    Some(match after.split_once(CURSOR_SEP) {
        Some((key, id)) => (key.to_string(), id.to_string()),
        None => (after.to_string(), after.to_string()),
    })
}

pub fn order(desc: bool, a: &(String, String), b: &(String, String)) -> std::cmp::Ordering {
    if desc { b.cmp(a) } else { a.cmp(b) }
}

pub fn doc_info(doc: &Doc) -> DocInfo {
    DocInfo {
        words: doc.body.unicode_words().count() as u64,
        id: doc.id.clone(),
        title: doc.title.clone(),
        format: doc.meta.format.clone(),
        size: doc.meta.size,
        created: doc.meta.created,
        updated: doc.meta.updated,
        file: doc.meta.file,
    }
}

pub fn list_docs(index: &Index, params: &ListParams) -> DocList {
    let size = params.size();
    let bound = parse_cursor(&params.after);
    let mut total = 0u64;
    let mut keyed: Vec<((String, String), &Doc)> = index
        .docs
        .values()
        .filter(|doc| params.format.is_empty() || doc.meta.format == params.format)
        .inspect(|_| total += 1)
        .map(|doc| {
            let key = sort_key(params.sort, &doc.id, &doc.title, &doc.meta);
            ((key, doc.id.clone()), doc)
        })
        .filter(|(key, _)| {
            bound
                .as_ref()
                .is_none_or(|b| order(params.desc, key, b) == std::cmp::Ordering::Greater)
        })
        .collect();
    keyed.sort_by(|a, b| order(params.desc, &a.0, &b.0));
    let more = keyed.len() > size;
    let docs = keyed
        .into_iter()
        .take(size)
        .map(|(_, doc)| doc_info(doc))
        .collect();
    DocList { total, docs, more }
}

pub fn get_doc<'a>(index: &'a Index, id: &str) -> Option<&'a Doc> {
    index.docs.get(index.ids.get(id)?)
}

async fn list_handler(
    State(store): State<SharedStore>,
    Query(params): Query<ListParams>,
) -> Json<DocList> {
    Json(list_docs(&store.read().await.index, &params))
}

async fn get_handler(
    State(store): State<SharedStore>,
    Path(id): Path<String>,
) -> Result<Json<Doc>, (StatusCode, Json<WriteResult>)> {
    match get_doc(&store.read().await.index, &id) {
        Some(doc) => Ok(Json(doc.clone())),
        None => Err((StatusCode::NOT_FOUND, write_result(id, "not_found"))),
    }
}

fn internal(e: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
}

async fn docs_handler(
    State(store): State<SharedStore>,
    Json(docs): Json<Vec<Doc>>,
) -> Result<Json<IndexResult>, (StatusCode, String)> {
    store.write().await.add(docs).map(Json).map_err(internal)
}

async fn delete_handler(
    State(store): State<SharedStore>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<WriteResult>), (StatusCode, String)> {
    if store.write().await.delete(&id).map_err(internal)? {
        Ok((StatusCode::OK, write_result(id, "deleted")))
    } else {
        Ok((StatusCode::NOT_FOUND, write_result(id, "not_found")))
    }
}

async fn put_handler(
    State(store): State<SharedStore>,
    Path(id): Path<String>,
    Json(doc): Json<DocBody>,
) -> Result<(StatusCode, Json<WriteResult>), (StatusCode, String)> {
    let mut store = store.write().await;
    let old = get_doc(&store.index, &id).map(|d| d.meta.clone());
    let updated = if doc.updated > 0 { doc.updated } else { now() };
    let mut meta = old.clone().unwrap_or_else(|| Meta {
        format: "txt".to_string(),
        created: updated,
        ..Meta::default()
    });
    if old.is_some() {
        meta.updated = updated;
    }
    if !meta.file {
        meta.size = (doc.title.len() + 1 + doc.body.len()) as u64;
    }
    let doc = Doc {
        id: id.clone(),
        title: doc.title,
        body: doc.body,
        meta,
    };
    let existed = store.update(doc).map_err(internal)?;
    if existed {
        Ok((StatusCode::OK, write_result(id, "updated")))
    } else {
        Ok((StatusCode::CREATED, write_result(id, "created")))
    }
}

async fn get_file_handler(
    State(store): State<SharedStore>,
    Path(id): Path<String>,
) -> Result<Vec<u8>, (StatusCode, String)> {
    match store.read().await.get_file(&id).map_err(internal)? {
        Some(bytes) => Ok(bytes),
        None => Err((StatusCode::NOT_FOUND, "not_found".to_string())),
    }
}

async fn put_file_handler(
    State(store): State<SharedStore>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<(StatusCode, Json<WriteResult>), (StatusCode, String)> {
    if store
        .write()
        .await
        .put_file(&id, body.to_vec())
        .map_err(internal)?
    {
        Ok((StatusCode::OK, write_result(id, "stored")))
    } else {
        Ok((StatusCode::NOT_FOUND, write_result(id, "not_found")))
    }
}

async fn snapshot_handler(
    State(store): State<SharedStore>,
) -> Result<Json<WriteResult>, (StatusCode, String)> {
    store
        .write()
        .await
        .snapshot()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("{e:#}")))?;
    Ok(write_result(SNAPSHOT_FILE.to_string(), "saved"))
}

async fn health_handler(State(store): State<SharedStore>) -> Json<Health> {
    Json(health(&store.read().await.index))
}

async fn stats_handler(
    State(store): State<SharedStore>,
    Json(req): Json<StatsRequest>,
) -> Json<Stats> {
    Json(stats(&store.read().await.index, &req.terms))
}

async fn search_handler(
    State(store): State<SharedStore>,
    Json(req): Json<SearchRequest>,
) -> Json<SearchResult> {
    Json(search(&store.read().await.index, &req))
}

#[derive(Debug, Deserialize)]
pub struct SuggestParams {
    #[serde(default)]
    pub prefix: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SuggestResponse {
    pub suggestions: Vec<Suggestion>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_shards: Vec<String>,
}

async fn suggest_handler(
    State(store): State<SharedStore>,
    Query(params): Query<SuggestParams>,
) -> Json<SuggestResponse> {
    Json(SuggestResponse {
        suggestions: fuzzy::suggest(&store.read().await.index, &params.prefix),
        failed_shards: Vec::new(),
    })
}

async fn expand_handler(
    State(store): State<SharedStore>,
    Json(req): Json<ExpandRequest>,
) -> Json<ExpandResponse> {
    let store = store.read().await;
    let mut response = fuzzy::expand(&store.index, &req.terms);
    if let Some(prefix) = &req.prefix {
        response.completions = fuzzy::complete(&store.index, prefix);
    }
    Json(response)
}

pub fn router(store: SharedStore) -> Router {
    Router::new()
        .route("/suggest", get(suggest_handler))
        .route("/expand", post(expand_handler))
        .route("/docs", get(list_handler).post(docs_handler))
        .route(
            "/docs/{id}",
            get(get_handler).put(put_handler).delete(delete_handler),
        )
        .route("/files/{id}", get(get_file_handler).put(put_file_handler))
        .route("/stats", post(stats_handler))
        .route("/search", post(search_handler))
        .route("/snapshot", post(snapshot_handler))
        .route("/health", get(health_handler))
        .layer(DefaultBodyLimit::max(SHARD_BODY_LIMIT))
        .with_state(store)
}

pub async fn run(port: u16, data_dir: Option<PathBuf>, snapshot_every: usize) -> anyhow::Result<()> {
    let store = match &data_dir {
        Some(dir) => Store::open(dir, snapshot_every)?,
        None => Store::memory(),
    };
    tracing::info!(docs = store.index.len(), wal_ops = store.wal_ops(), "shard loaded");
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "shard listening");
    axum::serve(listener, router(Arc::new(RwLock::new(store)))).await?;
    Ok(())
}
