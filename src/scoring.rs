pub const K1: f64 = 1.2;
pub const B: f64 = 0.75;

pub fn idf(n: u64, df: u64) -> f64 {
    let (n, df) = (n as f64, df as f64);
    (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
}

pub fn term_score(idf: f64, tf: u32, len: u32, avgdl: f64) -> f64 {
    let tf = f64::from(tf);
    idf * tf * (K1 + 1.0) / (tf + K1 * (1.0 - B + B * f64::from(len) / avgdl))
}

pub fn bm25(terms: &[(u32, u64)], len: u32, n: u64, avgdl: f64) -> f64 {
    terms
        .iter()
        .filter(|(tf, df)| *tf > 0 && *df > 0)
        .map(|&(tf, df)| term_score(idf(n, df), tf, len, avgdl))
        .sum()
}
