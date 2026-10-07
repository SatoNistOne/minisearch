use rust_stemmers::{Algorithm, Stemmer};
use std::sync::LazyLock;
use unicode_segmentation::UnicodeSegmentation;

static RU: LazyLock<Stemmer> = LazyLock::new(|| Stemmer::create(Algorithm::Russian));
static EN: LazyLock<Stemmer> = LazyLock::new(|| Stemmer::create(Algorithm::English));

const STOP_RU: &[&str] = &[
    "и", "в", "во", "не", "что", "он", "на", "я", "с", "со", "как", "а", "то", "все", "она",
    "так", "его", "но", "да", "ты", "к", "у", "же", "вы", "за", "бы", "по", "ее", "от", "о",
    "из", "это", "для", "при", "или",
];

const STOP_EN: &[&str] = &[
    "a", "an", "the", "and", "or", "but", "if", "of", "at", "by", "for", "with", "about", "to",
    "from", "in", "on", "is", "are", "was", "were", "be", "been", "it", "its", "this", "that",
    "these", "those", "as", "not", "no",
];

enum Script {
    Cyrillic,
    Latin,
    Other,
}

fn script(word: &str) -> Script {
    let mut cyrillic = false;
    let mut latin = false;
    for c in word.chars() {
        if matches!(c, 'а'..='я' | 'ё') {
            cyrillic = true;
        } else if c.is_ascii_lowercase() {
            latin = true;
        } else {
            return Script::Other;
        }
    }
    match (cyrillic, latin) {
        (true, false) => Script::Cyrillic,
        (false, true) => Script::Latin,
        _ => Script::Other,
    }
}

pub fn is_stop_word(word: &str) -> bool {
    STOP_RU.contains(&word) || STOP_EN.contains(&word)
}

pub fn normalize(word: &str) -> String {
    word.to_lowercase().replace('ё', "е")
}

pub fn words(text: &str) -> Vec<String> {
    text.unicode_words()
        .map(normalize)
        .filter(|word| !is_stop_word(word))
        .collect()
}

pub fn stem(word: String) -> String {
    match script(&word) {
        Script::Cyrillic => RU.stem(&word).into_owned(),
        Script::Latin => EN.stem(&word).into_owned(),
        Script::Other => word,
    }
}

pub fn analyze(text: &str) -> Vec<String> {
    words(text).into_iter().map(stem).collect()
}
