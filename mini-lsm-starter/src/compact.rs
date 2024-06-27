#![allow(clippy::pattern_type_mismatch)]
#![allow(clippy::unnecessary_wraps)]

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match *self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(ref task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(ref task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(ref task) => task.bottom_tier_included,
        }
    }
}

#[derive(Debug)]
pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match *self {
            CompactionController::Leveled(ref ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ref ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ref ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            *self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn build_sst(&self, builder: SsTableBuilder) -> Result<Arc<SsTable>> {
        let id = self.next_sst_id();
        let block_cache = Arc::clone(&self.block_cache);
        let block_cache = Some(block_cache);
        let path = self.path_of_sst(id);
        let sst = builder.build(id, block_cache, path)?;
        Ok(Arc::new(sst))
    }

    fn l0_l1_compaction(
        &self,
        snapshot: &LsmStorageState,
        l0_sst_ids: &[usize],
        l1_sst_ids: &[usize],
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let l0_iters = l0_sst_ids
            .iter()
            .map(|sst_id| {
                let table = Arc::clone(&snapshot.sstables[sst_id]);
                let iter = SsTableIterator::create_and_seek_to_first(table);
                iter.map(Box::new)
            })
            .collect::<Result<_>>()?;
        let l0_iter = MergeIterator::create(l0_iters);

        let l1_sstables = l1_sst_ids
            .iter()
            .map(|id| Arc::clone(&snapshot.sstables[id]))
            .collect();
        let l1_iter = SstConcatIterator::create_and_seek_to_first(l1_sstables)?;

        let iter = TwoMergeIterator::create(l0_iter, l1_iter)?;
        self.build_sstables_from_iter(iter, compact_to_bottom_level)
    }

    fn two_level_compaction(
        &self,
        snapshot: &LsmStorageState,
        upper_level_sst_ids: &[usize],
        lower_level_sst_ids: &[usize],
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let upper_level_sstables = upper_level_sst_ids
            .iter()
            .map(|id| Arc::clone(&snapshot.sstables[id]))
            .collect();
        let upper_level_iter = SstConcatIterator::create_and_seek_to_first(upper_level_sstables)?;

        let lower_level_sstables = lower_level_sst_ids
            .iter()
            .map(|id| Arc::clone(&snapshot.sstables[id]))
            .collect();
        let lower_level_iter = SstConcatIterator::create_and_seek_to_first(lower_level_sstables)?;

        let iter = TwoMergeIterator::create(upper_level_iter, lower_level_iter)?;
        self.build_sstables_from_iter(iter, compact_to_bottom_level)
    }

    fn tiered_compaction(
        &self,
        snapshot: &LsmStorageState,
        tiers: &[(usize, Vec<usize>)],
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        debug_assert!(tiers.len() >= 2);
        if tiers.len() == 2 {
            self.two_level_compaction(snapshot, &tiers[0].1, &tiers[1].1, compact_to_bottom_level)
        } else {
            let level_iters = tiers
                .iter()
                .map(|(_, sst_ids)| {
                    let sstables = sst_ids
                        .iter()
                        .map(|sst_id| Arc::clone(&snapshot.sstables[sst_id]))
                        .collect::<Vec<_>>();
                    let iter = SstConcatIterator::create_and_seek_to_first(sstables);
                    iter.map(Box::new)
                })
                .collect::<Result<_>>()?;
            let iter = MergeIterator::create(level_iters);
            self.build_sstables_from_iter(iter, compact_to_bottom_level)
        }
    }

    fn build_sstables_from_iter<I>(
        &self,
        mut iter: I,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>>
    where
        I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    {
        let mut builder = None;
        let mut tables = Vec::new();
        while iter.is_valid() {
            let key = iter.key();
            let value = iter.value();
            if !compact_to_bottom_level || !value.is_empty() {
                let builder_mut =
                    builder.get_or_insert_with(|| SsTableBuilder::new(self.options.block_size));
                builder_mut.add(key, value);
                if builder_mut.estimated_size() >= self.options.target_sst_size {
                    let builder = builder.take().unwrap();
                    tables.push(self.build_sst(builder)?);
                }
            }
            iter.next()?;
        }

        if let Some(builder) = builder {
            tables.push(self.build_sst(builder)?);
        }

        Ok(tables)
    }

    fn do_simple_compaction(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        if task.upper_level.is_some() {
            self.two_level_compaction(
                snapshot,
                &task.upper_level_sst_ids,
                &task.lower_level_sst_ids,
                compact_to_bottom_level,
            )
        } else {
            self.l0_l1_compaction(
                snapshot,
                &task.upper_level_sst_ids,
                &task.lower_level_sst_ids,
                compact_to_bottom_level,
            )
        }
    }

    fn do_tiered_compaction(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        self.tiered_compaction(snapshot, &task.tiers, compact_to_bottom_level)
    }

    fn do_level_compaction(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        if task.upper_level.is_some() {
            self.two_level_compaction(
                snapshot,
                &task.upper_level_sst_ids,
                &task.lower_level_sst_ids,
                compact_to_bottom_level,
            )
        } else {
            self.l0_l1_compaction(
                snapshot,
                &task.upper_level_sst_ids,
                &task.lower_level_sst_ids,
                compact_to_bottom_level,
            )
        }
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            &Arc::clone(&guard)
        };

        let compact_to_bottom_level = task.compact_to_bottom_level();
        match *task {
            CompactionTask::ForceFullCompaction {
                ref l0_sstables,
                ref l1_sstables,
            } => self.l0_l1_compaction(snapshot, l0_sstables, l1_sstables, compact_to_bottom_level),
            CompactionTask::Simple(ref task) => {
                self.do_simple_compaction(snapshot, task, compact_to_bottom_level)
            }
            CompactionTask::Tiered(ref task) => {
                self.do_tiered_compaction(snapshot, task, compact_to_bottom_level)
            }
            CompactionTask::Leveled(ref task) => {
                self.do_level_compaction(snapshot, task, compact_to_bottom_level)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let l0_sstables = &snapshot.l0_sstables;
        let l1_sstables = &snapshot.levels[0].1;
        // drop(snapshot); // Is it cheaper to clone l0|l1_sstables than holding an Arc?

        if l0_sstables.is_empty() {
            return Ok(());
        }

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        let new_l1_sstables = self.compact(&task)?;
        let l1_table_ids = new_l1_sstables
            .iter()
            .map(|sst| sst.sst_id())
            .collect::<Vec<_>>();
        {
            let _guard = self.state_lock.lock();
            let mut guard_arc_state = self.state.write();

            let old_state = guard_arc_state.as_ref();
            let mut new_state = old_state.clone();

            let new_l0_len = new_state.l0_sstables.len() - l0_sstables.len();
            debug_assert_eq!(&l0_sstables[..], &new_state.l0_sstables[new_l0_len..]);
            new_state.l0_sstables.truncate(new_l0_len);

            debug_assert_eq!(new_state.levels[0].0, 1);
            debug_assert_eq!(&new_state.levels[0].1, l1_sstables);
            new_state.levels[0] = (1, l1_table_ids.clone());

            for id in l0_sstables.iter().chain(l1_sstables.iter()) {
                let found = new_state.sstables.remove(id).is_some();
                debug_assert!(found);
            }
            let expected_new_len = new_state.sstables.len() + new_l1_sstables.len();
            new_state
                .sstables
                .extend(new_l1_sstables.into_iter().map(|sst| (sst.sst_id(), sst)));
            debug_assert_eq!(new_state.sstables.len(), expected_new_len);

            *guard_arc_state = Arc::new(new_state);

            let record = ManifestRecord::Compaction(task, l1_table_ids);
            if let Some(ref manifest) = self.manifest {
                manifest.add_record(&_guard, record)?;
            }
        }

        for &id in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(id))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            &Arc::clone(&guard)
        };

        let ctrl = &self.compaction_controller;
        // 1. generate a compaction task, if no task needs to be scheduled, return ok.
        if let Some(task) = ctrl.generate_compaction_task(snapshot) {
            // 2. run the compaction and get a list of new SSTs.
            let new_lower_level_sstables = self.compact(&task)?;
            let output = new_lower_level_sstables
                .iter()
                .map(|sst| sst.sst_id())
                .collect::<Vec<_>>();

            // 3. update the LSM state.
            let deleted_ids = {
                let _guard = self.state_lock.lock();
                let mut guard_arc_state = self.state.write();

                let (mut new_state, deleted_ids) =
                    ctrl.apply_compaction_result(guard_arc_state.as_ref(), &task, &output);

                for id in &deleted_ids {
                    let found = new_state.sstables.remove(id).is_some();
                    debug_assert!(found);
                }
                let expected_new_len = new_state.sstables.len() + output.len();
                new_state
                    .sstables
                    .extend(output.iter().copied().zip(new_lower_level_sstables));
                debug_assert_eq!(new_state.sstables.len(), expected_new_len);

                *guard_arc_state = Arc::new(new_state);

                let record = ManifestRecord::Compaction(task, output);
                if let Some(ref manifest) = self.manifest {
                    manifest.add_record(&_guard, record)?;
                }

                deleted_ids
            };

            for id in deleted_ids {
                std::fs::remove_file(self.path_of_sst(id))?;
            }
        }
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = Arc::clone(self);
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {e}");
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let imm_memtable_len_exceeds_limit =
            self.state.read().imm_memtables.len() >= self.options.num_memtable_limit;

        if imm_memtable_len_exceeds_limit {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = Arc::clone(self);
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {e}");
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
