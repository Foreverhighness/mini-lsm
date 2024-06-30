#![allow(clippy::future_not_send)]
#![allow(clippy::mem_forget)]

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

pub(crate) type UserKey = KeyBytes;
pub(crate) type UserKeyRef<'a> = KeySlice<'a>;
pub(crate) type UserValue = Bytes;
pub(crate) type KvStore = SkipMap<UserKey, UserValue>;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<KvStore>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

impl std::fmt::Debug for MemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemTable")
            .field("map", &self.map.iter().collect::<Vec<_>>())
            .finish()
    }
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Create a bound of `UserKey` from a bound of `UserKeyRef`.
pub(crate) fn map_key_bound(bound: Bound<UserKeyRef>) -> Bound<UserKey> {
    match bound {
        Bound::Included(x) => Bound::Included(UserKey::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(UserKey::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(KvStore::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::default()),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(path)?;

        Ok(Self {
            map: Arc::default(),
            wal: Some(wal),
            id,
            approximate_size: Arc::default(),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let kv_store = KvStore::new();
        let wal = Wal::recover(path, &kv_store)?;

        Ok(Self {
            map: Arc::new(kv_store),
            wal: Some(wal),
            id,
            approximate_size: Arc::default(),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(UserKeyRef::from_slice_ts(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(UserKeyRef::from_slice_ts(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        let map = |b| UserKeyRef::from_slice_ts(b, TS_DEFAULT);
        self.scan(lower.map(map), upper.map(map))
    }

    /// Get a value by key.
    pub fn get(&self, key: UserKeyRef) -> Option<Bytes> {
        self.map
            .get(key.as_key_bytes().as_ref())
            .map(|v| Bytes::clone(v.value()))
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    pub fn put(&self, key: UserKeyRef, value: &[u8]) -> Result<()> {
        let size_delta = key.raw_len() + value.len();
        self.approximate_size
            .fetch_add(size_delta, std::sync::atomic::Ordering::Relaxed);

        if let Some(ref wal) = self.wal {
            wal.put(key, value)?;
        }

        self.map
            .insert(UserKey::copy_from_slice(key), Bytes::copy_from_slice(value));
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<UserKeyRef>, upper: Bound<UserKeyRef>) -> MemTableIterator {
        let mut iter = MemTableIteratorBuilder {
            map: Arc::clone(&self.map),
            iter_builder: |map| map.range((map_key_bound(lower), map_key_bound(upper))),
            item: Default::default(),
        }
        .build();
        iter.next().unwrap();
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        debug_assert!(
            {
                let mut keys = self
                    .map
                    .iter()
                    .map(|entry| entry.key().clone())
                    .collect::<Vec<_>>();
                keys.sort();
                self.map
                    .iter()
                    .zip(keys.into_iter())
                    .all(|(entry, key)| entry.key() == &key)
            },
            "Assume map iterator is sorted order"
        );

        for entry in self.map.iter() {
            let key = entry.key();
            let key = UserKeyRef::from_slice_ts(key.key_ref(), key.ts());
            let value = entry.value();
            builder.add(key, value);
        }

        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    UserKey,
    (Bound<UserKey>, Bound<UserKey>),
    UserKey,
    UserValue,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<UserKey, UserValue>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (UserKey, UserValue),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = UserKeyRef<'a>;

    fn value(&self) -> &[u8] {
        let value = &self.borrow_item().1;
        value
    }

    fn key(&self) -> UserKeyRef {
        let key = &self.borrow_item().0;
        key.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        let key = &self.borrow_item().0;
        !key.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|this| {
            *this.item = this
                .iter
                .next()
                .map(|entry| (entry.key().clone(), Bytes::clone(entry.value())))
                .unwrap_or_default();
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::key::{Key, TS_RANGE_BEGIN, TS_RANGE_END};

    use super::*;

    fn key_in_range(key: KeySlice, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> bool {
        let (key, ts) = key.into_inner();
        match lower.map(Key::into_inner) {
            Bound::Included((left, read_ts)) if key < left || ts > read_ts => return false,
            Bound::Excluded((left, read_ts)) if key <= left || ts > read_ts => return false,
            _ => (),
        }
        match upper.map(Key::into_inner) {
            Bound::Included((right, read_ts)) if right < key || ts > read_ts => return false,
            Bound::Excluded((right, read_ts)) if right <= key || ts > read_ts => return false,
            _ => (),
        }
        true
    }

    #[test]
    fn test_map_key_bound() {
        let lower = UserKeyRef::from_slice_ts(b"2", TS_RANGE_BEGIN);
        let upper = UserKeyRef::from_slice_ts(b"3", TS_RANGE_END);

        let v1 = UserKeyRef::from_slice_ts(b"1", TS_DEFAULT);
        let v2 = UserKeyRef::from_slice_ts(b"2", TS_DEFAULT);
        let v3 = UserKeyRef::from_slice_ts(b"3", TS_DEFAULT);
        let v4 = UserKeyRef::from_slice_ts(b"4", TS_DEFAULT);

        {
            let lower = Bound::Unbounded;
            let upper = Bound::Excluded(upper);
            assert!(key_in_range(v1, lower, upper));
            assert!(key_in_range(v2, lower, upper));
            assert!(!key_in_range(v3, lower, upper));
            assert!(!key_in_range(v4, lower, upper));
        }

        {
            let lower = Bound::Unbounded;
            let upper = Bound::Included(upper);
            assert!(key_in_range(v1, lower, upper));
            assert!(key_in_range(v2, lower, upper));
            assert!(key_in_range(v3, lower, upper));
            assert!(!key_in_range(v4, lower, upper));
        }

        {
            let lower = Bound::Excluded(lower);
            let upper = Bound::Unbounded;
            assert!(!key_in_range(v1, lower, upper));
            assert!(!key_in_range(v2, lower, upper));
            assert!(key_in_range(v3, lower, upper));
            assert!(key_in_range(v4, lower, upper));
        }

        {
            let lower = Bound::Included(lower);
            let upper = Bound::Unbounded;
            assert!(!key_in_range(v1, lower, upper));
            assert!(key_in_range(v2, lower, upper));
            assert!(key_in_range(v3, lower, upper));
            assert!(key_in_range(v4, lower, upper));
        }

        {
            let lower = Bound::Included(lower);
            let upper = Bound::Included(upper);
            assert!(!key_in_range(v1, lower, upper));
            assert!(key_in_range(v2, lower, upper));
            assert!(key_in_range(v3, lower, upper));
            assert!(!key_in_range(v4, lower, upper));
        }

        {
            let lower = Bound::Excluded(lower);
            let upper = Bound::Excluded(upper);
            assert!(!key_in_range(v1, lower, upper));
            assert!(!key_in_range(v2, lower, upper));
            assert!(!key_in_range(v3, lower, upper));
            assert!(!key_in_range(v4, lower, upper));
        }

        {
            let lower = Bound::Included(lower);
            let upper = Bound::Excluded(upper);
            assert!(!key_in_range(v1, lower, upper));
            assert!(key_in_range(v2, lower, upper));
            assert!(!key_in_range(v3, lower, upper));
            assert!(!key_in_range(v4, lower, upper));
        }

        {
            let lower = Bound::Excluded(lower);
            let upper = Bound::Included(upper);
            assert!(!key_in_range(v1, lower, upper));
            assert!(!key_in_range(v2, lower, upper));
            assert!(key_in_range(v3, lower, upper));
            assert!(!key_in_range(v4, lower, upper));
        }
    }
}
