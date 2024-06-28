use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::Result;
use bytes::Buf;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;
use crate::table::{validate_checksum, SIZE_CHECKSUM};

#[derive(Debug)]
pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
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
        let buf = std::fs::read(path)?;

        let mut records = Vec::new();

        let mut buf = &buf[..];
        while !buf.is_empty() {
            let len = buf.get_u64().try_into().unwrap();

            validate_checksum(&buf[..len + SIZE_CHECKSUM])?;

            let record = serde_json::from_slice::<ManifestRecord>(&buf[..len])?;
            records.push(record);

            buf.advance(len + SIZE_CHECKSUM);
        }

        let file = std::fs::OpenOptions::new().append(true).open(path)?;
        let file = Arc::new(Mutex::new(file));

        Ok((Self { file }, records))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: &ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: &ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let json = serde_json::to_vec(record)?;

        let len: u64 = json.len().try_into().unwrap();
        file.write_all(&len.to_be_bytes())?;

        file.write_all(&json)?;

        let checksum = crc32fast::hash(&json);
        file.write_all(&checksum.to_be_bytes())?;

        file.sync_all()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_serde() {
        let record = ManifestRecord::Flush(1);
        let json = serde_json::to_vec(&record).unwrap();
        let back = serde_json::from_slice(&json).unwrap();
        assert_eq!(record, back);
    }

    #[test]
    fn test_two_record() {
        let record1 = ManifestRecord::Flush(1);
        let record2 = ManifestRecord::Flush(2);
        let mut json1 = serde_json::to_vec(&record1).unwrap();
        let json2 = serde_json::to_vec(&record2).unwrap();
        json1.extend(json2);

        let mut two_record = Vec::with_capacity(2);
        let de = serde_json::Deserializer::from_slice(&json1);
        for record in de.into_iter() {
            let record: ManifestRecord = record.unwrap();
            two_record.push(record);
        }
        assert_eq!(two_record, vec![record1, record2]);
    }
}
