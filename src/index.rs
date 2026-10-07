use crate::analyzer::{analyze, words};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Doc {
    pub id: String,
    pub title: String,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Posting {
    pub doc: u32,
    pub positions: Vec<u32>,
    pub title_tf: u32,
}

impl Posting {
    pub fn tf(&self) -> u32 {
        self.positions.len() as u32 + self.title_tf
    }
}

pub const BODY_GAP: u32 = 100;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Index {
    pub postings: HashMap<String, Vec<Posting>>,
    pub lengths: HashMap<u32, u32>,
    pub docs: HashMap<u32, Doc>,
    pub forward: HashMap<u32, Vec<String>>,
    pub ids: HashMap<String, u32>,
    pub words: BTreeMap<String, u32>,
    pub doc_words: HashMap<u32, Vec<String>>,
    pub total_len: u64,
    pub next_doc: u32,
}

impl Index {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.docs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    pub fn contains(&self, id: &str) -> bool {
        self.ids.contains_key(id)
    }

    pub fn delete(&mut self, id: &str) -> bool {
        let Some(internal) = self.ids.remove(id) else {
            return false;
        };
        for term in self.forward.remove(&internal).unwrap_or_default() {
            if let Some(list) = self.postings.get_mut(&term) {
                if let Ok(i) = list.binary_search_by_key(&internal, |p| p.doc) {
                    list.remove(i);
                }
                if list.is_empty() {
                    self.postings.remove(&term);
                }
            }
        }
        for word in self.doc_words.remove(&internal).unwrap_or_default() {
            if let Some(count) = self.words.get_mut(&word) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.words.remove(&word);
                }
            }
        }
        if let Some(len) = self.lengths.remove(&internal) {
            self.total_len -= u64::from(len);
        }
        self.docs.remove(&internal);
        true
    }

    pub fn update(&mut self, doc: Doc) -> Result<bool> {
        let existed = self.delete(&doc.id);
        self.add(doc)?;
        Ok(existed)
    }

    pub fn add(&mut self, doc: Doc) -> Result<bool> {
        if self.ids.contains_key(&doc.id) {
            return Ok(false);
        }
        let internal = self.next_doc;
        let next = internal.checked_add(1).context("shard is full")?;
        let title = analyze(&doc.title);
        let body = analyze(&doc.body);
        let title_len = u32::try_from(title.len()).context("title is too long")?;
        let body_len = u32::try_from(body.len()).context("body is too long")?;
        let body_start = title_len
            .checked_add(BODY_GAP)
            .context("title is too long")?;
        let mut terms: HashMap<String, Posting> = HashMap::new();
        let positioned = title
            .into_iter()
            .zip(0..)
            .chain(body.into_iter().zip(body_start..));
        for (term, pos) in positioned {
            let posting = terms.entry(term).or_insert_with(|| Posting {
                doc: internal,
                positions: Vec::new(),
                title_tf: 0,
            });
            posting.positions.push(pos);
            if pos < title_len {
                posting.title_tf += 1;
            }
        }
        let len = title_len
            .checked_mul(2)
            .and_then(|t| t.checked_add(body_len))
            .context("document is too long")?;
        let mut unique = Vec::with_capacity(terms.len());
        for (term, posting) in terms {
            self.postings.entry(term.clone()).or_default().push(posting);
            unique.push(term);
        }
        self.forward.insert(internal, unique);
        let doc_words: HashSet<String> = words(&doc.title)
            .into_iter()
            .chain(words(&doc.body))
            .collect();
        for word in &doc_words {
            *self.words.entry(word.clone()).or_default() += 1;
        }
        self.doc_words.insert(internal, doc_words.into_iter().collect());
        self.lengths.insert(internal, len);
        self.total_len += u64::from(len);
        self.ids.insert(doc.id.clone(), internal);
        self.docs.insert(internal, doc);
        self.next_doc = next;
        Ok(true)
    }
}

pub fn read_dir(dir: &Path) -> Result<Vec<Doc>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("read dir {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "txt") {
            paths.push(path);
        }
    }
    paths.sort();
    paths.iter().map(|path| parse_file(path)).collect()
}

fn parse_file(path: &Path) -> Result<Doc> {
    let id = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .with_context(|| format!("bad file name {}", path.display()))?
        .to_string();
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let (title, body) = text.split_once('\n').unwrap_or((text.as_str(), ""));
    Ok(Doc {
        id,
        title: title.trim().to_string(),
        body: body.trim().to_string(),
    })
}
