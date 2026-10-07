use crate::analyzer::analyze;
use std::collections::{BTreeSet, HashMap};
use std::sync::LazyLock;

pub const GROUPS: &[&[&str]] = &[
    &["хлеб", "батон", "буханка", "булка"],
    &["bread", "loaf"],
    &["закваска", "опара"],
    &["sourdough", "leaven"],
    &["пекарня", "булочная"],
    &["машина", "автомобиль"],
    &["car", "automobile"],
    &["компьютер", "ПК"],
    &["computer", "PC"],
    &["программа", "приложение"],
    &["program", "application", "app"],
    &["ошибка", "баг"],
    &["error", "bug", "fault"],
    &["быстрый", "скорый"],
    &["fast", "rapid", "speedy"],
    &["большой", "крупный"],
    &["big", "large", "huge"],
    &["документ", "файл"],
    &["document", "file"],
    &["космос", "вселенная"],
    &["space", "cosmos"],
    &["ракета", "корабль"],
    &["rocket", "spacecraft"],
];

static MAP: LazyLock<HashMap<String, BTreeSet<String>>> = LazyLock::new(|| {
    let mut map: HashMap<String, BTreeSet<String>> = HashMap::new();
    for group in GROUPS {
        let terms: Vec<String> = group
            .iter()
            .filter_map(|word| match analyze(word).as_slice() {
                [term] => Some(term.clone()),
                _ => None,
            })
            .collect();
        for term in &terms {
            map.entry(term.clone())
                .or_default()
                .extend(terms.iter().filter(|other| *other != term).cloned());
        }
    }
    map
});

pub fn synonyms(term: &str) -> Vec<String> {
    MAP.get(term)
        .map(|set| set.iter().cloned().collect())
        .unwrap_or_default()
}
