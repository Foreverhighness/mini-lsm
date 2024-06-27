#![allow(clippy::pattern_type_mismatch)]
#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality
#![allow(clippy::unused_self)] // TODO(fh): remove clippy allow
#![allow(clippy::unnecessary_wraps)] // TODO(fh): remove clippy allow

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone, Debug)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: HashMap::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

impl std::fmt::Debug for LsmStorageInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmStorageInner")
            .field("state", &self.state)
            .finish()
    }
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
#[derive(Debug)]
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(())?;
        self.compaction_notifier.send(())?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter);
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);
        let mut next_sst_id = 1;
        let block_cache = Arc::new(BlockCache::new(1024));

        let compaction_controller = match options.compaction_options {
            CompactionOptions::Leveled(ref options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(ref options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(ref options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let manifest_path = &path.join("MANIFEST");
        let manifest = if let Ok(manifest) = Manifest::create(manifest_path) {
            manifest
        } else {
            let (manifest, records) = Manifest::recover(manifest_path)?;
            let mut sst_ids = HashSet::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(id) => {
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, id);
                        } else {
                            state.levels.insert(0, (id, vec![id]));
                        }

                        let is_new_id = sst_ids.insert(id);
                        debug_assert!(is_new_id);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, deleted_sst_ids) =
                            compaction_controller.apply_compaction_result(&state, &task, &output);
                        state = new_state;

                        for id in &deleted_sst_ids {
                            let found = sst_ids.remove(id);
                            debug_assert!(found);
                        }

                        let expected_new_len = sst_ids.len() + output.len();
                        sst_ids.extend(output);
                        debug_assert_eq!(sst_ids.len(), expected_new_len);
                    }
                    ManifestRecord::NewMemtable(_) => todo!(),
                }
            }

            debug_assert!(state.sstables.is_empty());
            debug_assert!(!sst_ids.is_empty());
            next_sst_id = sst_ids.iter().max().unwrap() + 1;

            for id in sst_ids {
                let file = FileObject::open(&Self::path_of_sst_static(path, id))?;
                let block_cache = Some(Arc::clone(&block_cache));
                let sst = SsTable::open(id, block_cache, file)?;
                state.sstables.insert(id, Arc::new(sst));
            }
            manifest
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        if let Some(v) = snapshot.memtable.get(key) {
            return Ok(Some(v).filter(|v| !v.is_empty()));
        }

        // I'm wondering here why not use Arc<Vec<Arc<MemTable>>> (week 1 day 1)
        // So that I can use `let imm_memtables = Arc::clone(&state.imm_memtables)` then immediately release the lock
        // Answered at week 1 day 6: because we are using Arc<LsmStorageState>, there are already an Arc wrapper.
        for memtable in &snapshot.imm_memtables {
            if let Some(v) = memtable.get(key) {
                return Ok(Some(v).filter(|v| !v.is_empty()));
            }
        }

        let key_within_sst = move |sst: &Arc<SsTable>| {
            let first_key = sst.first_key().raw_ref();
            let last_key = sst.last_key().raw_ref();
            first_key <= key && key <= last_key
        };

        let key_in_bloom = move |sst: &Arc<SsTable>| {
            sst.bloom.as_ref().map_or(true, |bloom| {
                bloom.may_contain(farmhash::fingerprint32(key))
            })
        };

        let key = KeySlice::from_slice(key);
        let l0_iters = snapshot
            .l0_sstables
            .iter()
            .filter_map(|sst_id| {
                let sst = &snapshot.sstables[sst_id];
                (key_within_sst(sst) && key_in_bloom(sst)).then(|| {
                    let table = Arc::clone(sst);
                    let iter = SsTableIterator::create_and_seek_to_key(table, key);
                    iter.map(Box::new)
                })
            })
            .collect::<Result<_>>()?;
        let l0_iter = MergeIterator::create(l0_iters);
        if l0_iter.is_valid() && l0_iter.key() == key {
            return Ok(Some(Bytes::copy_from_slice(l0_iter.value())).filter(|v| !v.is_empty()));
        }

        let level_iters = snapshot
            .levels
            .iter()
            .filter_map(|(_, sst_ids)| {
                let sstables = sst_ids
                    .iter()
                    .filter_map(|sst_id| {
                        let sst = &snapshot.sstables[sst_id];
                        (key_within_sst(sst) && key_in_bloom(sst)).then(move || Arc::clone(sst))
                    })
                    .collect::<Vec<_>>();
                (!sstables.is_empty())
                    .then(|| SstConcatIterator::create_and_seek_to_key(sstables, key).map(Box::new))
            })
            .collect::<Result<_>>()?;
        let level_iter = MergeIterator::create(level_iters);
        if level_iter.is_valid() && level_iter.key() == key {
            return Ok(Some(Bytes::copy_from_slice(level_iter.value())).filter(|v| !v.is_empty()));
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let guard_arc_state = self.state.read();
        guard_arc_state.memtable.put(key, value)?;
        let size = guard_arc_state.memtable.approximate_size();

        drop(guard_arc_state);

        let memtable_reaches_capacity_on_put = size >= self.options.target_sst_size;
        if memtable_reaches_capacity_on_put {
            let state_lock = self.state_lock.lock();
            let current_memtable_reaches_capacity =
                self.state.read().memtable.approximate_size() >= self.options.target_sst_size;
            if current_memtable_reaches_capacity {
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{id:05}.sst"))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{id:05}.wal"))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        Ok(std::fs::File::open(self.path.as_path())?.sync_all()?)
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_arc_memtable = Arc::new(MemTable::create(self.next_sst_id()));

        let mut guard_arc_state = self.state.write();

        // I'm wondering why not just use Arc<RwLock<LsmStorageState>>> but Arc<RwLock<Arc<LsmStorageState>>>> (week 1 day 1)
        // So we need to clone all state to just update one field
        let old_state = guard_arc_state.as_ref();
        let mut new_state = old_state.clone();

        let old_arc_memtable = std::mem::replace(&mut new_state.memtable, new_arc_memtable);
        new_state.imm_memtables.insert(0, old_arc_memtable);

        *guard_arc_state = Arc::new(new_state);
        drop(guard_arc_state);

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        if let Some(memtable) = snapshot.imm_memtables.last() {
            let _guard = self.state_lock.lock();

            let memtable_not_changed =
                self.state.read().imm_memtables.last().map(|t| t.id()) == Some(memtable.id());
            if memtable_not_changed {
                let mut builder = SsTableBuilder::new(self.options.block_size);
                memtable.flush(&mut builder)?;
                let id = self.next_sst_id();
                let block_cache = Arc::clone(&self.block_cache);
                let sst = builder.build(id, Some(block_cache), self.path_of_sst(id))?;

                self.sync_dir()?;

                let mut guard_arc_state = self.state.write();

                let old_state = guard_arc_state.as_ref();
                let mut new_state = old_state.clone();

                let last_memtable = new_state.imm_memtables.pop().unwrap();
                debug_assert_eq!(last_memtable.id(), memtable.id());

                if self.compaction_controller.flush_to_l0() {
                    new_state.l0_sstables.insert(0, id);
                } else {
                    new_state.levels.insert(0, (id, vec![id]));
                }
                let is_new_id = new_state.sstables.insert(id, Arc::new(sst)).is_none();
                debug_assert!(is_new_id);

                *guard_arc_state = Arc::new(new_state);
                drop(guard_arc_state);

                let record = ManifestRecord::Flush(id);
                if let Some(ref manifest) = self.manifest {
                    manifest.add_record(&_guard, record)?;
                }
            }
        } else {
            return Err(anyhow::anyhow!("there is no immutable memtable"));
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let snapshot = self.state.read();
            Arc::clone(&snapshot)
        };
        let memtable_iters = std::iter::once(&snapshot.memtable)
            .chain(snapshot.imm_memtables.iter())
            .map(|memtable| Box::new(memtable.scan(lower, upper)))
            .collect();
        let memtable_iter = MergeIterator::create(memtable_iters);

        let range_overlap_with_sst = move |sst: &Arc<SsTable>| {
            let first_key = sst.first_key().raw_ref();
            let last_key = sst.last_key().raw_ref();
            match lower {
                Bound::Included(left) if last_key < left => return false,
                Bound::Excluded(left) if last_key <= left => return false,
                _ => (),
            }
            match upper {
                Bound::Included(right) if right < first_key => return false,
                Bound::Excluded(right) if right <= first_key => return false,
                _ => (),
            }
            true
        };

        let l0_iters = snapshot
            .l0_sstables
            .iter()
            .filter_map(|sst_id| {
                let sst = &snapshot.sstables[sst_id];
                range_overlap_with_sst(sst).then(move || {
                    let table = Arc::clone(sst);
                    let iter = match lower.map(KeySlice::from_slice) {
                        Bound::Included(key) => SsTableIterator::create_and_seek_to_key(table, key),
                        Bound::Excluded(key) => SsTableIterator::create_and_seek_to_key(table, key)
                            .and_then(|mut iter| {
                                if iter.is_valid() && iter.key() == key {
                                    iter.next()?;
                                }
                                Ok(iter)
                            }),
                        Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table),
                    };
                    iter.map(Box::new)
                })
            })
            .collect::<Result<_>>()?;
        let l0_iter = MergeIterator::create(l0_iters);

        let level_iters = snapshot
            .levels
            .iter()
            .filter_map(|(_, sst_ids)| {
                let sstables = sst_ids
                    .iter()
                    .filter_map(|sst_id| {
                        let sst = &snapshot.sstables[sst_id];
                        range_overlap_with_sst(sst).then(move || Arc::clone(sst))
                    })
                    .collect::<Vec<_>>();
                (!sstables.is_empty())
                    .then(|| SstConcatIterator::create_and_seek_to_first(sstables).map(Box::new))
            })
            .collect::<Result<_>>()?;
        let level_iter = MergeIterator::create(level_iters);

        let memtable_l0_iter = TwoMergeIterator::create(memtable_iter, l0_iter)?;

        let iter = TwoMergeIterator::create(memtable_l0_iter, level_iter)?;
        let lsm_iter = LsmIterator::new(iter, upper)?;
        Ok(FusedIterator::new(lsm_iter))
    }
}
