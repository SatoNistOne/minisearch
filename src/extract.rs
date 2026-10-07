use crate::index::Doc;
use anyhow::{Context, Result, bail, ensure};
use quick_xml::Reader;
use quick_xml::events::Event;
use std::io::{Cursor, Read};

pub const MAX_FILE_BYTES: usize = 10 * 1024 * 1024;
pub const MAX_XML_BYTES: u64 = 50 * 1024 * 1024;
pub const MAX_TITLE_CHARS: usize = 200;
pub const EXTENSIONS: [&str; 2] = ["txt", "docx"];

pub fn split_name(file_name: &str) -> Result<(String, String)> {
    let base = file_name
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or_default()
        .trim();
    let (stem, ext) = base
        .rsplit_once('.')
        .with_context(|| format!("у файла {base} нет расширения, нужен .txt или .docx"))?;
    let stem = stem.trim();
    ensure!(!stem.is_empty(), "пустое имя файла {base}");
    Ok((stem.to_string(), ext.to_lowercase()))
}

pub fn extract(file_name: &str, bytes: &[u8]) -> Result<Doc> {
    let (id, ext) = split_name(file_name)?;
    ensure!(
        bytes.len() <= MAX_FILE_BYTES,
        "файл больше {} МБ",
        MAX_FILE_BYTES / 1024 / 1024
    );
    let text = match ext.as_str() {
        "txt" => std::str::from_utf8(bytes)
            .context("файл не в кодировке UTF-8")?
            .trim_start_matches('\u{feff}')
            .to_string(),
        "docx" => docx_text(bytes)?,
        _ => bail!("формат .{ext} не поддерживается, нужен .txt или .docx"),
    };
    let (title, body) = split_title(&text, &id);
    ensure!(!text.trim().is_empty(), "в файле нет текста");
    Ok(Doc { id, title, body })
}

fn split_title(text: &str, fallback: &str) -> (String, String) {
    let text = text.trim();
    let (first, rest) = text.split_once('\n').unwrap_or((text, ""));
    let first = first.trim();
    if first.chars().count() > MAX_TITLE_CHARS {
        (fallback.to_string(), text.to_string())
    } else {
        (first.to_string(), rest.trim().to_string())
    }
}

pub fn docx_text(bytes: &[u8]) -> Result<String> {
    let mut archive =
        zip::ZipArchive::new(Cursor::new(bytes)).context("файл .docx повреждён")?;
    let entry = archive
        .by_name("word/document.xml")
        .context("в файле .docx нет текста документа")?;
    let mut xml = Vec::new();
    entry
        .take(MAX_XML_BYTES + 1)
        .read_to_end(&mut xml)
        .context("файл .docx повреждён")?;
    ensure!(
        xml.len() as u64 <= MAX_XML_BYTES,
        "текст документа больше {} МБ",
        MAX_XML_BYTES / 1024 / 1024
    );
    let mut reader = Reader::from_reader(xml.as_slice());
    let mut out = String::new();
    let mut in_text = false;
    loop {
        match reader.read_event().context("файл .docx повреждён")? {
            Event::Start(e) if e.local_name().as_ref() == "t" => in_text = true,
            Event::End(e) => match e.local_name().as_ref() {
                "t" => in_text = false,
                "p" => out.push('\n'),
                _ => {}
            },
            Event::Empty(e) => match e.local_name().as_ref() {
                "tab" => out.push('\t'),
                "br" | "cr" | "p" => out.push('\n'),
                _ => {}
            },
            Event::Text(t) if in_text => out.push_str(&t.xml10_content()),
            Event::GeneralRef(r) if in_text => {
                let resolved = match r.resolve_char_ref().context("файл .docx повреждён")? {
                    Some(c) => Some(c),
                    None => match &*r {
                        "amp" => Some('&'),
                        "lt" => Some('<'),
                        "gt" => Some('>'),
                        "quot" => Some('"'),
                        "apos" => Some('\''),
                        _ => None,
                    },
                };
                out.extend(resolved);
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(out)
}
