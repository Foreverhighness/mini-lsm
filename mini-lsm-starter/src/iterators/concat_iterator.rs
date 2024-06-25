use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    fn next_sst(&mut self) -> Result<()> {
        debug_assert!(self.current.is_none() || !self.current.as_ref().unwrap().is_valid());
        self.current = None;

        let start = self.next_sst_idx;
        let len = self.sstables.len();
        let tables = &self.sstables[start..];
        for (idx, sst) in (start..len).zip(tables) {
            let iter = SsTableIterator::create_and_seek_to_first(Arc::clone(sst))?;
            debug_assert!(iter.is_valid());
            if iter.is_valid() {
                self.current = Some(iter);
                self.next_sst_idx = idx + 1;
                break;
            }
        }
        Ok(())
    }

    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let mut iter = SstConcatIterator {
            current: None,
            next_sst_idx: 0,
            sstables,
        };
        iter.next_sst()?;
        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(SstConcatIterator {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }

        let sst_idx = sstables
            .partition_point(|sst| sst.first_key().as_key_slice() <= key)
            .saturating_sub(1);

        let iter = SsTableIterator::create_and_seek_to_key(Arc::clone(&sstables[sst_idx]), key)?;
        let need_next_sst = !iter.is_valid();
        let mut iter = SstConcatIterator {
            current: Some(iter),
            next_sst_idx: sst_idx + 1,
            sstables,
        };

        if need_next_sst {
            iter.next_sst()?;
        }

        Ok(iter)
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        debug_assert!(self.is_valid());
        debug_assert!(self.current.as_ref().unwrap().is_valid());

        let current = self.current.as_mut().unwrap();
        current.next()?;
        if !current.is_valid() {
            self.next_sst()?;
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.current
            .as_ref()
            .map(StorageIterator::num_active_iterators)
            .unwrap_or(0)
    }
}
