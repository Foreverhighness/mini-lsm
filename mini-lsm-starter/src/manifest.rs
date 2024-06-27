#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality
#![allow(clippy::needless_pass_by_value)] // TODO(fh): remove clippy allow

use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

#[derive(Debug)]
pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(path)?;
        let file = Arc::new(Mutex::new(file));

        Ok(Self { file })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let path = path.as_ref();
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        let buffer_file = BufReader::new(file);
        let records = serde_json::from_reader(buffer_file)?;

        let file = std::fs::OpenOptions::new().append(true).open(path)?;
        let file = Arc::new(Mutex::new(file));

        Ok((Self { file }, records))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let json = serde_json::to_vec(&record)?;

        file.write_all(&json)?;
        file.sync_all()?;

        Ok(())
    }
}
