use crate::analyzer::{analyze, is_stop_word, normalize, stem};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use unicode_segmentation::UnicodeSegmentation;

pub const SNIPPET_WORDS: usize = 30;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParsedQuery {
    pub terms: Vec<String>,
    pub phrases: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Segment {
    pub text: String,
    pub hl: bool,
}

pub const QUOTES: [char; 6] = ['"', '\u{201c}', '\u{201d}', '\u{201e}', '\u{ab}', '\u{bb}'];
pub const MIN_PREFIX_CHARS: usize = 2;

#[derive(Debug, Clone, PartialEq)]
pub struct Prefix {
    pub word: String,
    pub term: String,
}

pub fn last_prefix(q: &str) -> Option<Prefix> {
    let last = q.chars().last()?;
    if !last.is_alphanumeric() {
        return None;
    }
    let word = normalize(q.unicode_words().next_back()?);
    if word.chars().count() < MIN_PREFIX_CHARS || is_stop_word(&word) {
        return None;
    }
    let term = stem(word.clone());
    Some(Prefix { word, term })
}

pub fn parse_query(q: &str) -> ParsedQuery {
    let mut parsed = ParsedQuery::default();
    for (i, part) in q.split(QUOTES).enumerate() {
        let tokens = analyze(part);
        if i % 2 == 1 && !tokens.is_empty() {
            parsed.phrases.push(tokens.clone());
        }
        parsed.terms.extend(tokens);
    }
    parsed.terms.sort();
    parsed.terms.dedup();
    parsed
}

pub fn snippet(body: &str, terms: &HashSet<&str>) -> Vec<Segment> {
    let words: Vec<(usize, &str)> = body.unicode_word_indices().collect();
    let Some(&(first_offset, _)) = words.first() else {
        return Vec::new();
    };
    let matched: Vec<bool> = words
        .iter()
        .map(|(_, word)| analyze(word).iter().any(|t| terms.contains(t.as_str())))
        .collect();
    let first = matched.iter().position(|&m| m).unwrap_or(0);
    let end = (first.saturating_sub(SNIPPET_WORDS / 2) + SNIPPET_WORDS).min(words.len());
    let start = end.saturating_sub(SNIPPET_WORDS);
    let window = words.get(start..end).unwrap_or_default();
    let mut segments = Vec::new();
    let mut cursor = window.first().map_or(first_offset, |&(offset, _)| offset);
    let mut stop = cursor;
    for (&(offset, word), &hl) in window.iter().zip(matched.get(start..end).unwrap_or_default()) {
        if hl {
            push(&mut segments, body.get(cursor..offset).unwrap_or_default(), false);
            push(&mut segments, word, true);
            cursor = offset + word.len();
        }
        stop = offset + word.len();
    }
    push(&mut segments, body.get(cursor..stop).unwrap_or_default(), false);
    segments
}

pub fn highlight(text: &str, terms: &HashSet<&str>) -> Vec<Segment> {
    let mut segments = Vec::new();
    let mut cursor = 0;
    for (offset, word) in text.unicode_word_indices() {
        if analyze(word).iter().any(|t| terms.contains(t.as_str())) {
            push(&mut segments, text.get(cursor..offset).unwrap_or_default(), false);
            push(&mut segments, word, true);
            cursor = offset + word.len();
        }
    }
    push(&mut segments, text.get(cursor..).unwrap_or_default(), false);
    segments
}

fn push(segments: &mut Vec<Segment>, text: &str, hl: bool) {
    if text.is_empty() {
        return;
    }
    match segments.last_mut() {
        Some(last) if last.hl == hl => last.text.push_str(text),
        _ => segments.push(Segment {
            text: text.to_string(),
            hl,
        }),
    }
}
