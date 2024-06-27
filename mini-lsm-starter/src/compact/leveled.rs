#![allow(clippy::must_use_candidate)] // TODO(fh): remove clippy allow
#![allow(clippy::missing_const_for_fn)] // TODO(fh): remove clippy allow
#![allow(clippy::unused_self)] // TODO(fh): remove clippy allow

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

const MB: usize = 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

#[derive(Debug)]
pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        unimplemented!()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let len = snapshot.levels.len();
        let level_size = snapshot
            .levels
            .iter()
            .map(|(_, sst_ids)| {
                let size = sst_ids
                    .iter()
                    .map(|id| snapshot.sstables[id].table_size())
                    .sum::<u64>();
                usize::try_from(size).unwrap()
            })
            .collect::<Vec<_>>();

        let base_level_size = self.options.base_level_size_mb * MB;
        let mut target_size = vec![0; len];
        target_size[len - 1] = level_size[len - 1].max(base_level_size);

        for idx in (1..len).rev() {
            if target_size[idx] <= base_level_size {
                break;
            }
            target_size[idx - 1] = target_size[idx] / self.options.level_size_multiplier;
        }
        let target_level_idx = target_size.partition_point(|&x| x == 0);
        let target_level = target_level_idx + 1;
        eprintln!("base_level_size: {}MiB", base_level_size / MB);
        eprintln!(
            "level_size: {:?}",
            level_size.iter().map(|s| s / MB).collect::<Vec<_>>()
        );
        eprintln!(
            "target_size: {:?}",
            target_size.iter().map(|s| s / MB).collect::<Vec<_>>()
        );
        eprintln!("target_level: {target_level_idx:?}");

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let upper_level_sst_ids = &snapshot.l0_sstables;
            let lower_level_sst_ids = &snapshot.levels[target_level_idx].1;
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: upper_level_sst_ids.clone(),
                lower_level: target_level,
                lower_level_sst_ids: lower_level_sst_ids.clone(),
                is_lower_level_bottom_level: target_level == len,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = snapshot.clone();
        let LeveledCompactionTask {
            upper_level,
            ref upper_level_sst_ids,
            lower_level,
            ref lower_level_sst_ids,
            ..
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
