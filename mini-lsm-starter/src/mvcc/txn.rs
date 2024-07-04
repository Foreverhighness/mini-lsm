#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod
#![allow(clippy::future_not_send)] // TODO(fh): remove clippy allow
#![allow(clippy::mem_forget)] // TODO(fh): remove clippy allow

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{
        atomic::{AtomicBool, Ordering::Relaxed},
        Arc,
    },
};

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
    mem_table::map_bound,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Read set and write set
    pub(crate) rw_set: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    fn assume_running(&self) {
        let committed = self.committed.load(Relaxed);
        assert!(!committed, "txn already committed");
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.assume_running();

        if let Some(ref rw_set) = self.rw_set {
            let (ref mut read_set, _) = *rw_set.lock();
            let hash = farmhash::hash32(key);
            read_set.insert(hash);
        }

        if let Some(v) = self.local_storage.get(key) {
            let value = Bytes::clone(v.value());
            return Ok(Some(value).filter(|v| !v.is_empty()));
        }

        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.assume_running();

        let lsm_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let mut local_iter = TxnLocalIteratorBuilder {
            map: Arc::clone(&self.local_storage),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: Default::default(),
        }
        .build();
        local_iter.next().unwrap();

        let txn_lsm_iter = TwoMergeIterator::create(local_iter, lsm_iter)?;
        TxnIterator::create(Arc::clone(self), txn_lsm_iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.assume_running();

        if let Some(ref rw_set) = self.rw_set {
            let (_, ref mut write_set) = *rw_set.lock();
            let hash = farmhash::hash32(key);
            write_set.insert(hash);
        }

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));

        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
    }

    fn serializable_validate(&self) -> bool {
        let Some(ref rw_set) = self.rw_set else {
            return true;
        };

        let (ref read_set, ref write_set) = *rw_set.lock();
        let is_read_only_txn = write_set.is_empty();
        if is_read_only_txn {
            return true;
        }

        let mvcc = self.inner.mvcc();
        let conflict_txns = mvcc.committed_txns.lock().range((self.read_ts + 1)..);

        true
    }

    pub fn commit(&self) -> Result<()> {
        self.assume_running();

        let mvcc = self.inner.mvcc();
        let _guard = mvcc.commit_lock.lock();

        self.committed
            .compare_exchange(false, true, Relaxed, Relaxed)
            .map_err(|_| anyhow!("commit twice?"))?;

        if !self.serializable_validate() {
            bail!("violate serializable, abort");
        }

        let batch = self
            .local_storage
            .iter()
            .map(|entry| {
                WriteBatchRecord::Put(Bytes::clone(entry.key()), Bytes::clone(entry.value()))
            })
            .collect::<Vec<_>>();

        self.inner.write_batch(&batch)?;

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().remove_read_ts(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0
    }

    fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|this| {
            *this.item = this
                .iter
                .next()
                .map(|entry| (Bytes::clone(entry.key()), Bytes::clone(entry.value())))
                .unwrap_or_default();
        });

        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self { txn, iter };
        iter.next_valid_value()?;

        Ok(iter)
    }

    fn next_valid_value(&mut self) -> Result<()> {
        while self.is_valid() && self.value().is_empty() {
            self.iter.next()?;
        }

        if self.is_valid() {
            let key = self.key();
            if let Some(ref rw_set) = self.txn.rw_set {
                let (ref mut read_set, _) = *rw_set.lock();
                let hash = farmhash::hash32(key);
                read_set.insert(hash);
            }
        }

        Ok(())
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.next_valid_value()
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
