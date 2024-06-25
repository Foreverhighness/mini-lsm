#![allow(clippy::must_use_candidate)] // TODO(fh): remove clippy allow
#![allow(clippy::missing_const_for_fn)] // TODO(fh): remove clippy allow

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug)]
pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        debug_assert!(self.options.max_levels >= 2);
        debug_assert_eq!(snapshot.levels.len(), self.options.max_levels);

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let upper_level_sst_ids = &snapshot.l0_sstables;
            let lower_level_sst_ids = &snapshot.levels[0].1;
            if 100 * lower_level_sst_ids.len()
                < self.options.size_ratio_percent * upper_level_sst_ids.len()
            {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: None,
                    upper_level_sst_ids: upper_level_sst_ids.clone(),
                    lower_level: 1,
                    lower_level_sst_ids: lower_level_sst_ids.clone(),
                    is_lower_level_bottom_level: 1 == self.options.max_levels,
                });
            }
        }

        for idx in 1..snapshot.levels.len() {
            let (upper_level, ref upper_level_sst_ids) = snapshot.levels[idx - 1];
            let (lower_level, ref lower_level_sst_ids) = snapshot.levels[idx];
            debug_assert_eq!(upper_level, idx);
            debug_assert_eq!(lower_level, idx + 1);

            if 100 * lower_level_sst_ids.len()
                < self.options.size_ratio_percent * upper_level_sst_ids.len()
            {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(upper_level),
                    upper_level_sst_ids: upper_level_sst_ids.clone(),
                    lower_level,
                    lower_level_sst_ids: lower_level_sst_ids.clone(),
                    is_lower_level_bottom_level: lower_level == self.options.max_levels,
                });
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = snapshot.clone();
        let SimpleLeveledCompactionTask {
            upper_level,
            ref upper_level_sst_ids,
            lower_level,
            ref lower_level_sst_ids,
            is_lower_level_bottom_level: _,
        } = *task;

        let deleted_sst_ids = upper_level_sst_ids
            .iter()
            .chain(lower_level_sst_ids.iter())
            .copied()
            .collect();

        if let Some(upper_level) = upper_level {
            debug_assert_eq!(new_state.levels[upper_level - 1].0, upper_level);
            debug_assert_eq!(&new_state.levels[upper_level - 1].1, upper_level_sst_ids);
            new_state.levels[upper_level - 1].1.clear();
        } else {
            let new_l0_len = new_state.l0_sstables.len() - upper_level_sst_ids.len();
            debug_assert_eq!(&new_state.l0_sstables[new_l0_len..], upper_level_sst_ids);
            new_state.l0_sstables.truncate(new_l0_len);
        }

        debug_assert_eq!(new_state.levels[lower_level - 1].0, lower_level);
        debug_assert_eq!(&new_state.levels[lower_level - 1].1, lower_level_sst_ids);
        output.clone_into(&mut new_state.levels[lower_level - 1].1);

        (new_state, deleted_sst_ids)
    }
}
