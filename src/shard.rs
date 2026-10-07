use crate::fuzzy::{self, ExpandRequest, ExpandResponse, FUZZY_WEIGHT, Suggestion};
use crate::index::{Doc, Index};
use crate::query::{Segment, snippet};
use crate::scoring;
use crate::storage::{SNAPSHOT_FILE, Store};
use axum::extract::{Path, Query, State};
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
                .sum();
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

pub const LIST_DEFAULT: usize = 50;
pub const LIST_MAX: usize = 200;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocInfo {
    pub id: String,
    pub title: String,
    pub words: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocList {
    pub total: u64,
    pub docs: Vec<DocInfo>,
    pub more: bool,
}

#[derive(Debug, Deserialize)]
pub struct ListParams {
    #[serde(default)]
    pub after: String,
    pub size: Option<usize>,
}

impl ListParams {
    pub fn size(&self) -> usize {
        self.size.unwrap_or(LIST_DEFAULT).clamp(1, LIST_MAX)
    }
}

pub fn list_docs(index: &Index, after: &str, size: usize) -> DocList {
    let mut ids: Vec<&String> = index
        .ids
        .keys()
        .filter(|id| id.as_str() > after)
        .collect();
    ids.sort_unstable();
    let more = ids.len() > size;
    let docs = ids
        .into_iter()
        .take(size)
        .filter_map(|id| get_doc(index, id))
        .map(|doc| DocInfo {
            words: doc.body.unicode_words().count() as u64,
            id: doc.id.clone(),
            title: doc.title.clone(),
        })
        .collect();
    DocList {
        total: index.len() as u64,
        docs,
        more,
    }
}

pub fn get_doc<'a>(index: &'a Index, id: &str) -> Option<&'a Doc> {
    index.docs.get(index.ids.get(id)?)
}

async fn list_handler(
    State(store): State<SharedStore>,
    Query(params): Query<ListParams>,
) -> Json<DocList> {
    Json(list_docs(&store.read().await.index, &params.after, params.size()))
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
    let doc = Doc {
        id: id.clone(),
        title: doc.title,
        body: doc.body,
    };
    let existed = store.write().await.update(doc).map_err(internal)?;
    if existed {
        Ok((StatusCode::OK, write_result(id, "updated")))
    } else {
        Ok((StatusCode::CREATED, write_result(id, "created")))
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
    Json(fuzzy::expand(&store.read().await.index, &req.terms))
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
        .route("/stats", post(stats_handler))
        .route("/search", post(search_handler))
        .route("/snapshot", post(snapshot_handler))
        .route("/health", get(health_handler))
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
