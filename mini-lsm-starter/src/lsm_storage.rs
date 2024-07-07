#![allow(clippy::pattern_type_mismatch)]

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Weak};

use anyhow::{anyhow, Result};
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
use crate::key::{TimeStamp, TS_DEFAULT, TS_ENABLED, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{MemTable, UserKeyRef};
use crate::mvcc::txn::{Transaction, TxnIterator};
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

#[derive(Debug, Clone, Copy)]
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

    /// return (sst_ids, mmt_ids)
    fn replay_records<I>(
        &mut self,
        records: I,
        ctrl: &CompactionController,
    ) -> (HashSet<usize>, VecDeque<usize>)
    where
        I: IntoIterator<Item = ManifestRecord>,
    {
        let mut sst_ids = HashSet::new();
        let mut mmt_ids = VecDeque::new();
        for record in records {
            match record {
                ManifestRecord::Flush(sst_id) => {
                    if ctrl.flush_to_l0() {
                        self.l0_sstables.insert(0, sst_id);
                    } else {
                        self.levels.insert(0, (sst_id, vec![sst_id]));
                    }

                    let mmt_id = mmt_ids.pop_back();
                    debug_assert_eq!(Some(sst_id), mmt_id, "{sst_ids:?} {mmt_ids:?}");

                    let is_new_id = sst_ids.insert(sst_id);
                    debug_assert!(is_new_id);
                }
                ManifestRecord::Compaction(task, output) => {
                    let (new_state, deleted_sst_ids) =
                        ctrl.apply_compaction_result(self, &task, &output, true);
                    *self = new_state;

                    for id in &deleted_sst_ids {
                        let found = sst_ids.remove(id);
                        debug_assert!(found);
                    }

                    let expected_new_len = sst_ids.len() + output.len();
                    sst_ids.extend(output);
                    debug_assert_eq!(sst_ids.len(), expected_new_len);
                }
                ManifestRecord::NewMemtable(mmt_id) => {
                    mmt_ids.push_front(mmt_id);
                }
            }
        }
        (sst_ids, mmt_ids)
    }

    fn rebuild_sstables_from_sst_ids<I>(
        &mut self,
        sst_ids: I,
        path: &Path,
        block_cache: &Arc<BlockCache>,
    ) -> Result<()>
    where
        I: IntoIterator<Item = usize>,
    {
        for id in sst_ids {
            let path = LsmStorageInner::path_of_sst_static(path, id);
            let file = FileObject::open(&path)?;
            let block_cache = Some(Arc::clone(block_cache));
            let sst = SsTable::open(id, block_cache, file)?;

            self.sstables.insert(id, Arc::new(sst));
        }
        Ok(())
    }

    fn rebuild_imm_memtables_from_mmt_ids<I>(&mut self, mmt_ids: I, path: &Path) -> Result<()>
    where
        I: IntoIterator<Item = usize>,
    {
        for id in mmt_ids {
            let path = LsmStorageInner::path_of_wal_static(path, id);
            let memtable = MemTable::recover_from_wal(id, path)?;

            if !memtable.is_empty() {
                self.imm_memtables.push(Arc::new(memtable));
            }
        }
        Ok(())
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

impl LsmStorageInner {
    fn flush_all_memtables(&self) -> Result<()> {
        let guard = self.state_lock.lock();
        let mut guard_arc_state = self.state.write();
        let old_state = guard_arc_state.as_ref();
        let mut new_state = old_state.clone();

        for memtable in new_state
            .imm_memtables
            .iter()
            .rev()
            .chain(Some(&new_state.memtable).filter(|&t| !t.is_empty()))
        {
            let sst = self.create_l0_sst_from_memtable(memtable)?;
            let id = sst.sst_id();

            let record = ManifestRecord::Flush(id);
            if let Some(ref manifest) = self.manifest {
                manifest.add_record(&guard, &record)?;
            }
        }
        self.sync_dir()?;

        // Is there really need to update state? just for consistency?
        new_state.memtable = Arc::new(MemTable::create(0));
        new_state.imm_memtables.clear();

        *guard_arc_state = Arc::new(new_state);

        Ok(())
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(())?;
        self.compaction_notifier.send(())?;

        if let Some(thread) = self.flush_thread.lock().take() {
            let ok = thread.join().is_ok();
            debug_assert!(ok);
        }
        if let Some(thread) = self.compaction_thread.lock().take() {
            let ok = thread.join().is_ok();
            debug_assert!(ok);
        }

        if self.inner.options.enable_wal {
            self.inner.sync()
        } else {
            self.inner.flush_all_memtables()
        }
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = LsmStorageInner::open(path, options)?;
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

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
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

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
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

    pub fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);
        let mut next_sst_id = 0;
        let mut initial_ts = None;
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
            let (sst_ids, mmt_ids) = state.replay_records(records, &compaction_controller);

            debug_assert!(state.sstables.is_empty());
            debug_assert!(!mmt_ids.is_empty());

            next_sst_id = sst_ids.iter().chain(mmt_ids.iter()).max().unwrap() + 1;

            state.rebuild_sstables_from_sst_ids(sst_ids, path, &block_cache)?;

            initial_ts = initial_ts.max(
                state
                    .sstables
                    .values()
                    .map(|sst| SsTable::max_ts(sst))
                    .max(),
            );

            if options.enable_wal {
                state.rebuild_imm_memtables_from_mmt_ids(mmt_ids, path)?;

                initial_ts = initial_ts.max(
                    state
                        .imm_memtables
                        .iter()
                        .map(|mmt| mmt.max_ts().unwrap())
                        .max(),
                );
            }

            manifest
        };

        let memtable = if options.enable_wal {
            let wal_path = Self::path_of_wal_static(path, next_sst_id);
            MemTable::create_with_wal(next_sst_id, wal_path)?
        } else {
            MemTable::create(next_sst_id)
        };
        state.memtable = Arc::new(memtable);

        let record = ManifestRecord::NewMemtable(next_sst_id);
        manifest.add_record_when_init(&record)?;

        next_sst_id += 1;

        let storage = if TS_ENABLED {
            let initial_ts = initial_ts.unwrap_or(TS_DEFAULT);
            Arc::new_cyclic(|weak| {
                let mvcc = LsmMvccInner::new(initial_ts, Weak::clone(weak));

                Self {
                    state: Arc::new(RwLock::new(Arc::new(state))),
                    state_lock: Mutex::new(()),
                    path: path.to_path_buf(),
                    block_cache,
                    next_sst_id: AtomicUsize::new(next_sst_id),
                    compaction_controller,
                    manifest: Some(manifest),
                    options: options.into(),
                    mvcc: Some(mvcc),
                    compaction_filters: Arc::new(Mutex::new(Vec::new())),
                }
            })
        } else {
            Arc::new(Self {
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
            })
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.new_txn()?;
        txn.get(key)
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get_with_ts(&self, key: &[u8], read_ts: TimeStamp) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let key_with_ts = UserKeyRef::from_slice_ts(key, read_ts);
        if let Some(v) = snapshot.memtable.get_with_ts(key_with_ts) {
            return Ok(Some(v).filter(|v| !v.is_empty()));
        }

        // I'm wondering here why not use Arc<Vec<Arc<MemTable>>> (week 1 day 1)
        // So that I can use `let imm_memtables = Arc::clone(&state.imm_memtables)` then immediately release the lock
        // Answered at week 1 day 6: because we are using Arc<LsmStorageState>, there are already an Arc wrapper.
        for memtable in &snapshot.imm_memtables {
            if let Some(v) = memtable.get_with_ts(key_with_ts) {
                return Ok(Some(v).filter(|v| !v.is_empty()));
            }
        }

        let key_within_sst = move |sst: &Arc<SsTable>| {
            let first_key = sst.first_key().key_ref();
            let last_key = sst.last_key().key_ref();
            first_key <= key && key <= last_key
        };

        let key_in_bloom = move |sst: &Arc<SsTable>| {
            sst.bloom.as_ref().map_or(true, |bloom| {
                bloom.may_contain(farmhash::fingerprint32(key))
            })
        };

        let l0_iters = snapshot
            .l0_sstables
            .iter()
            .filter_map(|sst_id| {
                let sst = &snapshot.sstables[sst_id];
                (key_within_sst(sst) && key_in_bloom(sst)).then(|| {
                    let table = Arc::clone(sst);
                    let iter = SsTableIterator::create_and_seek_to_key(table, key_with_ts);
                    iter.map(Box::new)
                })
            })
            .collect::<Result<_>>()?;
        let l0_iter = MergeIterator::create(l0_iters);
        if l0_iter.is_valid() && l0_iter.key().key_ref() == key {
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
                (!sstables.is_empty()).then(|| {
                    SstConcatIterator::create_and_seek_to_key(sstables, key_with_ts).map(Box::new)
                })
            })
            .collect::<Result<_>>()?;
        let level_iter = MergeIterator::create(level_iters);
        if level_iter.is_valid() && level_iter.key().key_ref() == key {
            return Ok(Some(Bytes::copy_from_slice(level_iter.value())).filter(|v| !v.is_empty()));
        }

        Ok(None)
    }

    pub(crate) fn write_batch_inner<T, R, I>(&self, batch: I) -> Result<TimeStamp>
    where
        T: AsRef<[u8]>,
        R: Borrow<WriteBatchRecord<T>>,
        I: IntoIterator<Item = R>,
    {
        let mvcc = self.mvcc();
        let _guard = mvcc.write_lock.lock();
        let ts = mvcc.latest_commit_ts() + 1;

        let guard_arc_state = self.state.read();
        let memtable = &guard_arc_state.memtable;
        for record in batch {
            match *record.borrow() {
                WriteBatchRecord::Put(ref key, ref value) => {
                    let key = UserKeyRef::from_slice_ts(key.as_ref(), ts);
                    memtable.put(key, value.as_ref())?;
                }
                WriteBatchRecord::Del(ref key) => {
                    let key = UserKeyRef::from_slice_ts(key.as_ref(), ts);
                    memtable.put(key, &[])?;
                }
            }
        }
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

        mvcc.update_commit_ts(ts);

        Ok(ts)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let mvcc = self.mvcc();
        let txn = mvcc.new_txn(self.options.serializable);

        for record in batch {
            match *record {
                WriteBatchRecord::Put(ref key, ref value) => txn.put(key.as_ref(), value.as_ref()),
                WriteBatchRecord::Del(ref key) => txn.delete(key.as_ref()),
            }
        }

        txn.commit()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let ts = if let Some(mvcc) = self.mvcc.as_ref() {
            let ts = mvcc.latest_commit_ts() + 1;
            mvcc.update_commit_ts(ts);
            ts
        } else {
            TS_DEFAULT
        };

        let key = UserKeyRef::from_slice_ts(key, ts);

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
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let id = self.next_sst_id();

        let memtable = if self.options.enable_wal {
            MemTable::create_with_wal(id, self.path_of_wal(id))?
        } else {
            MemTable::create(id)
        };
        let new_arc_memtable = Arc::new(memtable);

        let mut guard_arc_state = self.state.write();

        // I'm wondering why not just use Arc<RwLock<LsmStorageState>>> but Arc<RwLock<Arc<LsmStorageState>>>> (week 1 day 1)
        // So we need to clone all state to just update one field
        let old_state = guard_arc_state.as_ref();
        let mut new_state = old_state.clone();

        let old_arc_memtable = std::mem::replace(&mut new_state.memtable, new_arc_memtable);
        new_state.imm_memtables.insert(0, old_arc_memtable);

        *guard_arc_state = Arc::new(new_state);
        drop(guard_arc_state);

        let record = ManifestRecord::NewMemtable(id);
        if let Some(ref manifest) = self.manifest {
            manifest.add_record(state_lock_observer, &record)?;
        }

        Ok(())
    }

    fn create_l0_sst_from_memtable(&self, memtable: &MemTable) -> Result<SsTable> {
        let mut builder = SsTableBuilder::new(self.options.block_size);
        memtable.flush(&mut builder)?;

        let id = memtable.id();
        let block_cache = Arc::clone(&self.block_cache);
        builder.build(id, Some(block_cache), self.path_of_sst(id))
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        if let Some(memtable) = snapshot.imm_memtables.last() {
            let guard = self.state_lock.lock();

            let memtable_not_changed =
                self.state.read().imm_memtables.last().map(|t| t.id()) == Some(memtable.id());
            if memtable_not_changed {
                let sst = self.create_l0_sst_from_memtable(memtable)?;
                self.sync_dir()?;

                let id = sst.sst_id();
                debug_assert_eq!(id, memtable.id());

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
                    manifest.add_record(&guard, &record)?;
                }
            }
        } else {
            return Err(anyhow::anyhow!("there is no immutable memtable"));
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.mvcc
            .as_ref()
            .map(|mvcc| mvcc.new_txn(self.options.serializable))
            .ok_or(anyhow!("not support mvcc"))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let txn = self.new_txn()?;
        txn.scan(lower, upper)
    }

    /// Create an iterator over a range of keys.
    pub fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: TimeStamp,
    ) -> Result<FusedIterator<LsmIterator>> {
        let lower_with_ts = match lower {
            Bound::Included(x) => Bound::Included(UserKeyRef::from_slice_ts(x, TS_RANGE_BEGIN)),
            Bound::Excluded(x) => Bound::Excluded(UserKeyRef::from_slice_ts(x, TS_RANGE_END)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper_with_ts = match upper {
            Bound::Included(x) => Bound::Included(UserKeyRef::from_slice_ts(x, TS_RANGE_END)),
            Bound::Excluded(x) => Bound::Excluded(UserKeyRef::from_slice_ts(x, TS_RANGE_BEGIN)),
            Bound::Unbounded => Bound::Unbounded,
        };

        let snapshot = {
            let snapshot = self.state.read();
            Arc::clone(&snapshot)
        };

        let memtable_iters = std::iter::once(&snapshot.memtable)
            .chain(snapshot.imm_memtables.iter())
            .map(|memtable| Box::new(memtable.scan(lower_with_ts, upper_with_ts)))
            .collect();
        let memtable_iter = MergeIterator::create(memtable_iters);

        let range_overlap_with_sst = move |sst: &Arc<SsTable>| {
            let first_key = sst.first_key().key_ref();
            let last_key = sst.last_key().key_ref();
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
                    let iter = match lower_with_ts {
                        Bound::Included(key) => SsTableIterator::create_and_seek_to_key(table, key),
                        Bound::Excluded(key) => SsTableIterator::create_and_seek_to_key(table, key)
                            .and_then(|mut iter| {
                                if iter.is_valid() && iter.key().key_ref() == key.key_ref() {
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
        let lsm_iter = LsmIterator::new(iter, upper, read_ts)?;
        Ok(FusedIterator::new(lsm_iter))
    }
}
