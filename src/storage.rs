use crate::index::{Doc, Index};
use crate::shard::{IndexResult, index_docs};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub const SNAPSHOT_FILE: &str = "shard.snap";
pub const WAL_FILE: &str = "wal.log";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Op {
    Add(Vec<Doc>),
    Delete(String),
    Update(Doc),
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
    disk: Option<Disk>,
    wal_ops: usize,
}

impl Store {
    pub fn memory() -> Self {
        Self::default()
    }

    pub fn open(dir: &Path, snapshot_every: usize) -> Result<Self> {
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let snap_path = dir.join(SNAPSHOT_FILE);
        let mut index = if snap_path.exists() {
            let bytes = std::fs::read(&snap_path)
                .with_context(|| format!("read {}", snap_path.display()))?;
            postcard::from_bytes(&bytes)
                .with_context(|| format!("decode {}", snap_path.display()))?
        } else {
            Index::new()
        };
        let wal_path = dir.join(WAL_FILE);
        let (ops, valid) = read_wal(&wal_path)?;
        let wal_ops = ops.len();
        for op in ops {
            if let Err(e) = apply(&mut index, op) {
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
            disk: Some(Disk {
                dir: dir.to_path_buf(),
                wal,
                snapshot_every,
            }),
            wal_ops,
        };
        store.maybe_snapshot()?;
        Ok(store)
    }

    pub fn wal_ops(&self) -> usize {
        self.wal_ops
    }

    pub fn add(&mut self, docs: Vec<Doc>) -> Result<IndexResult> {
        if docs.is_empty() {
            return Ok(IndexResult::default());
        }
        let op = Op::Add(docs);
        self.log(&op)?;
        let Op::Add(docs) = op else {
            bail!("unexpected op");
        };
        let result = index_docs(&mut self.index, docs);
        self.maybe_snapshot()?;
        result
    }

    pub fn delete(&mut self, id: &str) -> Result<bool> {
        if !self.index.contains(id) {
            return Ok(false);
        }
        self.log(&Op::Delete(id.to_string()))?;
        let deleted = self.index.delete(id);
        self.maybe_snapshot()?;
        Ok(deleted)
    }

    pub fn update(&mut self, doc: Doc) -> Result<bool> {
        let op = Op::Update(doc);
        self.log(&op)?;
        let Op::Update(doc) = op else {
            bail!("unexpected op");
        };
        let existed = self.index.update(doc);
        self.maybe_snapshot()?;
        existed
    }

    pub fn snapshot(&mut self) -> Result<()> {
        let Some(disk) = &self.disk else {
            bail!("shard has no data dir");
        };
        let bytes = postcard::to_allocvec(&self.index).context("encode snapshot")?;
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
        let mut record = Vec::with_capacity(payload.len() + 4);
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

fn apply(index: &mut Index, op: Op) -> Result<()> {
    match op {
        Op::Add(docs) => index_docs(index, docs).map(|_| ()),
        Op::Delete(id) => {
            index.delete(&id);
            Ok(())
        }
        Op::Update(doc) => index.update(doc).map(|_| ()),
    }
}

pub fn read_wal(path: &Path) -> Result<(Vec<Op>, u64)> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0)),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    let mut ops = Vec::new();
    let mut offset = 0usize;
    while let Some(header) = bytes.get(offset..offset + 4) {
        let mut len = [0u8; 4];
        len.copy_from_slice(header);
        let start = offset + 4;
        let end = start + u32::from_le_bytes(len) as usize;
        let Some(payload) = bytes.get(start..end) else {
            break;
        };
        let Ok(op) = postcard::from_bytes(payload) else {
            break;
        };
        ops.push(op);
        offset = end;
    }
    Ok((ops, offset as u64))
}
