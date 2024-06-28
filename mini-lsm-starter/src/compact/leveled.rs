use std::ops::Bound;

use serde::{Deserialize, Serialize};

use crate::{lsm_storage::LsmStorageState, table::SsTable};

const MB: usize = 1024 * 1024;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
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

    fn lower_upper_key<'r, 's: 'r>(
        snapshot: &'s LsmStorageState,
        sst_ids: &[usize],
    ) -> (&'r [u8], &'r [u8]) {
        sst_ids
            .iter()
            .map(|id| {
                let sst = &snapshot.sstables[id];
                (sst.first_key().raw_ref(), sst.last_key().raw_ref())
            })
            .reduce(|(lower, upper), (lo, up)| (lower.min(lo), upper.max(up)))
            .unwrap()
    }

    fn find_overlapping_ssts(
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        debug_assert!(!sst_ids.is_empty());
        let (lower_key, upper_key) = Self::lower_upper_key(snapshot, sst_ids);
        let (lower, upper) = (Bound::Included(lower_key), Bound::Included(upper_key));

        let level = &snapshot.levels[in_level - 1].1;

        let range_overlap_with_sst = move |sst: &SsTable| {
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

        let res = level
            .iter()
            .filter(|&id| range_overlap_with_sst(&snapshot.sstables[id]))
            .copied()
            .collect::<Vec<_>>();

        // TODO(fh): use partition_point to optimize O(n) -> O(nlogn)
        let _res = {
            let mut left = level
                .partition_point(|id| snapshot.sstables[id].first_key().raw_ref() <= lower_key)
                .saturating_sub(1);
            if let Some(id) = level.get(left) {
                if snapshot.sstables[id].last_key().raw_ref() < lower_key {
                    left += 1;
                }
            }
            if !res.is_empty() {
                debug_assert_eq!(level[left], res[0]);
            }
            // TODO(fh): get right right
            let right =
                level.partition_point(|id| snapshot.sstables[id].last_key().raw_ref() <= upper_key);
            level[left..right].to_vec()
        };

        res
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let len = snapshot.levels.len();
        let mut level_size = snapshot
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
            let lower_level_sst_ids =
                Self::find_overlapping_ssts(snapshot, upper_level_sst_ids, target_level);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: upper_level_sst_ids.clone(),
                lower_level: target_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level: target_level == len,
            });
        }

        let mut multiplier = 1;
        for idx in (target_level_idx..len).rev() {
            level_size[idx] *= multiplier;
            multiplier *= self.options.level_size_multiplier;
        }

        debug_assert!(level_size[len - 1] <= target_size[len - 1]);
        if let Some((upper_level_idx, _)) = level_size
            .iter()
            .enumerate()
            .filter(|&(_, &size)| size > target_size[len - 1])
            .max_by_key(|(_, &v)| v)
        {
            let upper_level = upper_level_idx + 1;
            let lower_level_idx = upper_level_idx + 1;
            let lower_level = lower_level_idx + 1;

            debug_assert_ne!(upper_level, len);

            eprintln!("{upper_level} compact to {lower_level}");

            let upper_level_sst_ids = snapshot.levels[upper_level_idx]
                .1
                .iter()
                .min()
                .copied()
                .unwrap();
            let upper_level_sst_ids = vec![upper_level_sst_ids];
            let lower_level_sst_ids =
                Self::find_overlapping_ssts(snapshot, &upper_level_sst_ids, lower_level);
            return Some(LeveledCompactionTask {
                upper_level: Some(upper_level),
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level: lower_level == len,
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
            debug_assert_eq!(upper_level_sst_ids.len(), 1);

            let upper_level = &mut new_state.levels[upper_level - 1].1;
            upper_level.remove(
                upper_level
                    .iter()
                    .enumerate()
                    .find(|&(_, &v)| v == upper_level_sst_ids[0])
                    .unwrap()
                    .0,
            );
        } else {
            let new_l0_len = new_state.l0_sstables.len() - upper_level_sst_ids.len();
            debug_assert_eq!(&new_state.l0_sstables[new_l0_len..], upper_level_sst_ids);
            new_state.l0_sstables.truncate(new_l0_len);
        }

        debug_assert_eq!(new_state.levels[lower_level - 1].0, lower_level);

        let lower_level = &mut new_state.levels[lower_level - 1].1;

        eprintln!("old_lower_level: {lower_level:?}");
        eprintln!("lower_level_sst_ids: {lower_level_sst_ids:?}");
        let start = {
            let (lower_key, _) = Self::lower_upper_key(snapshot, upper_level_sst_ids);
            let mut left = lower_level
                .partition_point(|id| snapshot.sstables[id].first_key().raw_ref() <= lower_key)
                .saturating_sub(1);
            if let Some(id) = lower_level.get(left) {
                if snapshot.sstables[id].last_key().raw_ref() < lower_key {
                    left += 1;
                }
            }
            left
        };
        eprintln!("new_lower_level: {lower_level:?}");
        let end = start + lower_level_sst_ids.len();
        let old = lower_level.splice(start..end, output.to_vec());
        let old = old.collect::<Vec<_>>();
        debug_assert_eq!(&old, lower_level_sst_ids);
        eprintln!("new_lower_level: {lower_level:?}");

        (new_state, deleted_sst_ids)
    }
}
