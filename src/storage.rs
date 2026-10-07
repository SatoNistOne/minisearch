use crate::index::{Doc, Index, Posting};
use crate::shard::{IndexResult, index_docs, stamp};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use xxhash_rust::xxh3::{xxh3_64, xxh3_128};

pub const SNAPSHOT_FILE: &str = "shard.snap";
pub const WAL_FILE: &str = "wal.log";
pub const FILES_DIR: &str = "files";
pub const SNAPSHOT_MAGIC: &[u8] = b"MSNAP3\n";
pub const SNAPSHOT_V2_MAGIC: &[u8] = b"MSNAP2\n";
pub const WAL_MAGIC: &[u8] = b"MSWAL2\n";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Op {
    Add(Vec<Doc>),
    Delete(String),
    Update(Doc),
    AddAt(Vec<Doc>, u64),
    PutAt(Doc, u64),
    DeleteAt(String, u64),
    Evict(String),
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Versions {
    pub docs: HashMap<String, u64>,
    pub deleted: HashMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncEntry {
    pub id: String,
    pub version: u64,
    pub deleted: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Digest {
    pub count: u64,
    pub hash: u64,
}

#[derive(Debug, Deserialize)]
struct LegacyDoc {
    id: String,
    title: String,
    body: String,
}

#[derive(Debug, Deserialize)]
enum LegacyOp {
    Add(Vec<LegacyDoc>),
    Delete(String),
    Update(LegacyDoc),
}

#[derive(Debug, Deserialize)]
struct LegacyIndex {
    _postings: HashMap<String, Vec<Posting>>,
    _lengths: HashMap<u32, u32>,
    docs: HashMap<u32, LegacyDoc>,
    _forward: HashMap<u32, Vec<String>>,
    _ids: HashMap<String, u32>,
    _words: BTreeMap<String, u32>,
    _doc_words: HashMap<u32, Vec<String>>,
    _total_len: u64,
    _next_doc: u32,
}

impl From<LegacyDoc> for Doc {
    fn from(doc: LegacyDoc) -> Self {
        Doc::new(doc.id, doc.title, doc.body)
    }
}

impl From<LegacyOp> for Op {
    fn from(op: LegacyOp) -> Self {
        match op {
            LegacyOp::Add(docs) => Op::Add(docs.into_iter().map(Doc::from).collect()),
            LegacyOp::Delete(id) => Op::Delete(id),
            LegacyOp::Update(doc) => Op::Update(doc.into()),
        }
    }
}

fn legacy_index(bytes: &[u8]) -> Result<Index> {
    let legacy: LegacyIndex = postcard::from_bytes(bytes)?;
    let mut docs: Vec<(u32, LegacyDoc)> = legacy.docs.into_iter().collect();
    docs.sort_by_key(|(internal, _)| *internal);
    let mut index = Index::new();
    index_docs(&mut index, docs.into_iter().map(|(_, doc)| doc.into()).collect())?;
    Ok(index)
}

#[derive(Debug)]
struct Disk {
    dir: PathBuf,
    wal: File,
    snapshot_every: usize,
}

#[derive(Debug, Default)]
pub struct Store {
    pub index: Index,
    pub versions: Versions,
    disk: Option<Disk>,
    files: HashMap<String, Vec<u8>>,
    wal_ops: usize,
}

impl Store {
    pub fn memory() -> Self {
        Self::default()
    }

    pub fn open(dir: &Path, snapshot_every: usize) -> Result<Self> {
        std::fs::create_dir_all(dir.join(FILES_DIR))
            .with_context(|| format!("create {}", dir.display()))?;
        let snap_path = dir.join(SNAPSHOT_FILE);
        let mut legacy = false;
        let (mut index, mut versions) = if snap_path.exists() {
            let bytes = std::fs::read(&snap_path)
                .with_context(|| format!("read {}", snap_path.display()))?;
            if let Some(rest) = bytes.strip_prefix(SNAPSHOT_MAGIC) {
                postcard::from_bytes(rest)
                    .with_context(|| format!("decode {}", snap_path.display()))?
            } else if let Some(rest) = bytes.strip_prefix(SNAPSHOT_V2_MAGIC) {
                let index = postcard::from_bytes(rest)
                    .with_context(|| format!("decode {}", snap_path.display()))?;
                (index, Versions::default())
            } else {
                legacy = true;
                tracing::warn!("migrating legacy snapshot");
                let index = legacy_index(&bytes)
                    .with_context(|| format!("decode {}", snap_path.display()))?;
                (index, Versions::default())
            }
        } else {
            (Index::new(), Versions::default())
        };
        let wal_path = dir.join(WAL_FILE);
        let (ops, valid, legacy_wal) = read_wal_any(&wal_path)?;
        legacy |= legacy_wal;
        let wal_ops = ops.len();
        for op in ops {
            if let Err(e) = apply(&mut index, &mut versions, op) {
                tracing::warn!(error = %e, "wal replay");
            }
        }
        let wal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .with_context(|| format!("open {}", wal_path.display()))?;
        if wal.metadata()?.len() > valid {
            tracing::warn!(valid, "truncating torn wal tail");
            wal.set_len(valid)?;
            wal.sync_data()?;
        }
        let mut store = Self {
            index,
            versions,
            disk: Some(Disk {
                dir: dir.to_path_buf(),
                wal,
                snapshot_every,
            }),
            files: HashMap::new(),
            wal_ops,
        };
        if legacy {
            store.snapshot()?;
        } else {
            store.maybe_snapshot()?;
        }
        Ok(store)
    }

    pub fn wal_ops(&self) -> usize {
        self.wal_ops
    }

    pub fn version(&self, id: &str) -> u64 {
        let doc = match self.versions.docs.get(id) {
            Some(v) => *v,
            None => self
                .index
                .ids
                .get(id)
                .and_then(|internal| self.index.docs.get(internal))
                .map_or(0, |doc| doc.meta.updated.max(doc.meta.created) * 1000),
        };
        doc.max(self.versions.deleted.get(id).copied().unwrap_or(0))
    }

    fn next_version(&self, id: &str) -> u64 {
        stamp().max(self.version(id) + 1)
    }

    pub fn add(&mut self, docs: Vec<Doc>) -> Result<IndexResult> {
        let version = docs
            .iter()
            .map(|doc| self.version(&doc.id) + 1)
            .fold(stamp(), u64::max);
        self.add_at(docs, version)
    }

    pub fn add_at(&mut self, docs: Vec<Doc>, version: u64) -> Result<IndexResult> {
        let total = docs.len();
        let mut seen = HashSet::new();
        let fresh: Vec<Doc> = docs
            .into_iter()
            .filter(|doc| {
                !self.index.contains(&doc.id)
                    && self.version(&doc.id) < version
                    && seen.insert(doc.id.clone())
            })
            .collect();
        if fresh.is_empty() {
            return Ok(IndexResult {
                indexed: 0,
                skipped: total,
            });
        }
        let added = fresh.len();
        self.log_apply(Op::AddAt(fresh, version))?;
        self.maybe_snapshot()?;
        Ok(IndexResult {
            indexed: added,
            skipped: total - added,
        })
    }

    pub fn delete(&mut self, id: &str) -> Result<bool> {
        if !self.index.contains(id) {
            return Ok(false);
        }
        let version = self.next_version(id);
        self.delete_at(id, version)
    }

    pub fn delete_at(&mut self, id: &str, version: u64) -> Result<bool> {
        if version <= self.version(id) {
            return Ok(false);
        }
        let existed = self.index.contains(id);
        self.log_apply(Op::DeleteAt(id.to_string(), version))?;
        self.remove_file(id)?;
        self.maybe_snapshot()?;
        Ok(existed)
    }

    pub fn update(&mut self, doc: Doc) -> Result<bool> {
        let version = self.next_version(&doc.id);
        Ok(self.put_at(doc, version)?.unwrap_or(true))
    }

    pub fn known(&self, id: &str) -> bool {
        self.index.contains(id) || self.versions.deleted.contains_key(id)
    }

    pub fn put_at(&mut self, doc: Doc, version: u64) -> Result<Option<bool>> {
        if self.known(&doc.id) && version <= self.version(&doc.id) {
            return Ok(None);
        }
        let existed = self.index.contains(&doc.id);
        self.log_apply(Op::PutAt(doc, version))?;
        self.maybe_snapshot()?;
        Ok(Some(existed))
    }

    pub fn evict(&mut self, id: &str, version: u64) -> Result<bool> {
        if !self.index.contains(id) || self.version(id) > version {
            return Ok(false);
        }
        self.log_apply(Op::Evict(id.to_string()))?;
        self.remove_file(id)?;
        self.maybe_snapshot()?;
        Ok(true)
    }

    fn log_apply(&mut self, op: Op) -> Result<()> {
        self.log(&op)?;
        apply(&mut self.index, &mut self.versions, op)
    }

    pub fn entries(&self) -> Vec<SyncEntry> {
        let mut entries: Vec<SyncEntry> = self
            .index
            .ids
            .keys()
            .map(|id| SyncEntry {
                id: id.clone(),
                version: self.version(id),
                deleted: false,
            })
            .chain(
                self.versions
                    .deleted
                    .iter()
                    .filter(|(id, _)| !self.index.contains(id))
                    .map(|(id, version)| SyncEntry {
                        id: id.clone(),
                        version: *version,
                        deleted: true,
                    }),
            )
            .collect();
        entries.sort_by(|a, b| a.id.cmp(&b.id));
        entries
    }

    pub fn digest(&self) -> Digest {
        let entries = self.entries();
        let hash = entries.iter().fold(0u64, |acc, e| {
            let key = format!("{}\0{}\0{}", e.id, e.version, e.deleted);
            acc.wrapping_add(xxh3_64(key.as_bytes()))
        });
        Digest {
            count: entries.len() as u64,
            hash,
        }
    }

    fn file_path(&self, id: &str) -> Option<PathBuf> {
        let disk = self.disk.as_ref()?;
        Some(
            disk.dir
                .join(FILES_DIR)
                .join(format!("{:032x}", xxh3_128(id.as_bytes()))),
        )
    }

    pub fn put_file(&mut self, id: &str, bytes: Vec<u8>) -> Result<bool> {
        if !self.index.contains(id) {
            return Ok(false);
        }
        match self.file_path(id) {
            Some(path) => {
                let tmp = path.with_extension("tmp");
                let mut file =
                    File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
                file.write_all(&bytes)?;
                file.sync_all()?;
                drop(file);
                std::fs::rename(&tmp, &path).context("rename file")?;
            }
            None => {
                self.files.insert(id.to_string(), bytes);
            }
        }
        Ok(true)
    }

    pub fn get_file(&self, id: &str) -> Result<Option<Vec<u8>>> {
        if !self.index.contains(id) {
            return Ok(None);
        }
        match self.file_path(id) {
            Some(path) => match std::fs::read(&path) {
                Ok(bytes) => Ok(Some(bytes)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
            },
            None => Ok(self.files.get(id).cloned()),
        }
    }

    fn remove_file(&mut self, id: &str) -> Result<()> {
        match self.file_path(id) {
            Some(path) => match std::fs::remove_file(&path) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                    Err(e).with_context(|| format!("remove {}", path.display()))
                }
                _ => Ok(()),
            },
            None => {
                self.files.remove(id);
                Ok(())
            }
        }
    }

    pub fn snapshot(&mut self) -> Result<()> {
        let Some(disk) = &self.disk else {
            bail!("shard has no data dir");
        };
        let mut bytes = SNAPSHOT_MAGIC.to_vec();
        bytes.extend(
            postcard::to_allocvec(&(&self.index, &self.versions)).context("encode snapshot")?,
        );
        let tmp = disk.dir.join(format!("{SNAPSHOT_FILE}.tmp"));
        let mut file = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, disk.dir.join(SNAPSHOT_FILE)).context("rename snapshot")?;
        if let Ok(dir) = File::open(&disk.dir) {
            dir.sync_all().ok();
        }
        disk.wal.set_len(0)?;
        disk.wal.sync_data()?;
        self.wal_ops = 0;
        Ok(())
    }

    fn log(&mut self, op: &Op) -> Result<()> {
        let Some(disk) = &mut self.disk else {
            return Ok(());
        };
        let payload = postcard::to_allocvec(op).context("encode wal record")?;
        let len = u32::try_from(payload.len()).context("wal record is too large")?;
        let mut record = Vec::with_capacity(payload.len() + WAL_MAGIC.len() + 4);
        if disk.wal.metadata()?.len() == 0 {
            record.extend_from_slice(WAL_MAGIC);
        }
        record.extend_from_slice(&len.to_le_bytes());
        record.extend_from_slice(&payload);
        disk.wal.write_all(&record).context("write wal")?;
        disk.wal.sync_data().context("sync wal")?;
        self.wal_ops += 1;
        Ok(())
    }

    fn maybe_snapshot(&mut self) -> Result<()> {
        match &self.disk {
            Some(disk) if self.wal_ops > disk.snapshot_every => self.snapshot(),
            _ => Ok(()),
        }
    }
}

fn apply(index: &mut Index, versions: &mut Versions, op: Op) -> Result<()> {
    match op {
        Op::Add(docs) => index_docs(index, docs).map(|_| ()),
        Op::Delete(id) => {
            index.delete(&id);
            Ok(())
        }
        Op::Update(doc) => index.update(doc).map(|_| ()),
        Op::AddAt(docs, version) => {
            for doc in docs {
                let id = doc.id.clone();
                if index.add(doc)? {
                    versions.docs.insert(id.clone(), version);
                    versions.deleted.remove(&id);
                }
            }
            Ok(())
        }
        Op::PutAt(doc, version) => {
            let id = doc.id.clone();
            index.update(doc)?;
            versions.docs.insert(id.clone(), version);
            versions.deleted.remove(&id);
            Ok(())
        }
        Op::DeleteAt(id, version) => {
            index.delete(&id);
            versions.docs.remove(&id);
            versions.deleted.insert(id, version);
            Ok(())
        }
        Op::Evict(id) => {
            index.delete(&id);
            versions.docs.remove(&id);
            versions.deleted.remove(&id);
            Ok(())
        }
    }
}

fn read_records<T: serde::de::DeserializeOwned>(bytes: &[u8], start: usize) -> (Vec<T>, u64) {
    let mut ops = Vec::new();
    let mut offset = start;
    while let Some(header) = bytes.get(offset..offset + 4) {
        let mut len = [0u8; 4];
        len.copy_from_slice(header);
        let begin = offset + 4;
        let end = begin + u32::from_le_bytes(len) as usize;
        let Some(payload) = bytes.get(begin..end) else {
            break;
        };
        let Ok(op) = postcard::from_bytes(payload) else {
            break;
        };
        ops.push(op);
        offset = end;
    }
    (ops, offset as u64)
}

fn read_wal_any(path: &Path) -> Result<(Vec<Op>, u64, bool)> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0, false)),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    if bytes.is_empty() {
        return Ok((Vec::new(), 0, false));
    }
    if bytes.starts_with(WAL_MAGIC) {
        let (ops, valid) = read_records(&bytes, WAL_MAGIC.len());
        return Ok((ops, valid, false));
    }
    tracing::warn!("migrating legacy wal");
    let (ops, valid) = read_records::<LegacyOp>(&bytes, 0);
    Ok((ops.into_iter().map(Op::from).collect(), valid, true))
}

pub fn read_wal(path: &Path) -> Result<(Vec<Op>, u64)> {
    let (ops, valid, _) = read_wal_any(path)?;
    Ok((ops, valid))
}
