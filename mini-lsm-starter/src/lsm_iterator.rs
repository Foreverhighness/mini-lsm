use std::ops::Bound;

use anyhow::Result;
use bytes::Bytes;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    key::TimeStamp,
    mem_table::{map_bound, MemTableIterator},
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper: Bound<Bytes>,
    read_ts: TimeStamp,
    prev_key: Vec<u8>,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        upper: Bound<&[u8]>,
        read_ts: TimeStamp,
    ) -> Result<Self> {
        let upper = map_bound(upper);
        let mut lsm_iter = Self {
            inner: iter,
            upper,
            read_ts,
            prev_key: Vec::new(),
        };
        lsm_iter.next_valid_value()?;
        Ok(lsm_iter)
    }

    fn next_valid_value(&mut self) -> Result<()> {
        while self.is_valid() {
            let (key, ts) = {
                let key = self.inner.key();
                (key.key_ref(), key.ts())
            };

            let same_key = key == self.prev_key;
            let key_from_future = ts > self.read_ts;
            if same_key || key_from_future {
                self.inner.next()?;
                continue;
            }
            key.clone_into(&mut self.prev_key);

            let tombstone = self.value().is_empty();
            if tombstone {
                self.inner.next()?;
                continue;
            }

            break;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
            && match self.upper {
                Bound::Included(ref right) if right < self.key() => false,
                Bound::Excluded(ref right) if right <= self.key() => false,
                _ => true,
            }
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        debug_assert!(self.is_valid());

        self.inner.next()?;

        self.next_valid_value()?;

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            return Err(anyhow::anyhow!("Invalid FusedIterator::next call"));
        }

        if self.iter.is_valid() {
            if let e @ Err(_) = self.iter.next() {
                self.has_errored = true;
                return e;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
