use crate::analyzer::{normalize, stem};
use crate::index::Index;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub const FUZZY_WEIGHT: f64 = 0.5;
pub const SUGGEST_LIMIT: usize = 10;
pub const MIN_PREFIX: usize = 2;
pub const COMPLETE_LIMIT: usize = 50;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Suggestion {
    pub word: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpandRequest {
    pub terms: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExpandResponse {
    pub expansions: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub completions: Vec<String>,
}

pub fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        cur[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            cur[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(cur[j] + 1);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

pub fn max_distance(len: usize) -> usize {
    match len {
        0..=4 => 0,
        5..=8 => 1,
        _ => 2,
    }
}

pub fn expand(index: &Index, terms: &[String]) -> ExpandResponse {
    let mut expansions = BTreeMap::new();
    for term in terms {
        let len = term.chars().count();
        let max = max_distance(len);
        let mut found: Vec<String> = index
            .postings
            .keys()
            .filter(|candidate| {
                if max == 0 {
                    return *candidate == term;
                }
                candidate.chars().count().abs_diff(len) <= max && levenshtein(term, candidate) <= max
            })
            .cloned()
            .collect();
        found.sort();
        expansions.insert(term.clone(), found);
    }
    ExpandResponse {
        expansions,
        completions: Vec::new(),
    }
}

pub fn complete(index: &Index, prefix: &str) -> Vec<String> {
    let prefix = normalize(prefix.trim());
    if prefix.chars().count() < MIN_PREFIX {
        return Vec::new();
    }
    let mut words: Vec<(&String, u32)> = index
        .words
        .range(prefix.clone()..)
        .take_while(|(word, _)| word.starts_with(&prefix))
        .map(|(word, &count)| (word, count))
        .collect();
    words.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    let terms: BTreeSet<String> = words
        .into_iter()
        .take(COMPLETE_LIMIT)
        .map(|(word, _)| stem(word.clone()))
        .collect();
    terms.into_iter().collect()
}

pub fn merge_expansions(
    terms: &[String],
    parts: &[ExpandResponse],
) -> HashMap<String, Vec<String>> {
    let mut merged: HashMap<String, Vec<String>> = HashMap::new();
    for term in terms {
        let variants: BTreeSet<&String> = parts
            .iter()
            .filter_map(|part| part.expansions.get(term))
            .flatten()
            .filter(|variant| !terms.contains(variant))
            .collect();
        if !variants.is_empty() {
            merged.insert(term.clone(), variants.into_iter().cloned().collect());
        }
    }
    merged
}

fn top(mut list: Vec<Suggestion>) -> Vec<Suggestion> {
    list.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.word.cmp(&b.word)));
    list.truncate(SUGGEST_LIMIT);
    list
}

pub fn suggest(index: &Index, prefix: &str) -> Vec<Suggestion> {
    let prefix = normalize(prefix.trim());
    if prefix.is_empty() {
        return Vec::new();
    }
    let list = index
        .words
        .range(prefix.clone()..)
        .take_while(|(word, _)| word.starts_with(&prefix))
        .map(|(word, &count)| Suggestion {
            word: word.clone(),
            count: u64::from(count),
        })
        .collect();
    top(list)
}

pub fn merge_suggestions(parts: Vec<Vec<Suggestion>>) -> Vec<Suggestion> {
    let mut counts: HashMap<String, u64> = HashMap::new();
    for suggestion in parts.into_iter().flatten() {
        *counts.entry(suggestion.word).or_default() += suggestion.count;
    }
    top(counts
        .into_iter()
        .map(|(word, count)| Suggestion { word, count })
        .collect())
}
