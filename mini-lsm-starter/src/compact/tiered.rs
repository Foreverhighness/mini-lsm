#![allow(clippy::missing_const_for_fn)] // TODO(fh): remove clippy allow
#![allow(clippy::must_use_candidate)] // TODO(fh): remove clippy allow

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

#[derive(Debug)]
pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        let tiers = &snapshot.levels;
        let len = tiers.len();
        if len < self.options.num_tiers {
            return None;
        }

        let all_levels_size: usize = tiers.iter().map(|(_, sst_ids)| sst_ids.len()).sum();
        let last_level_size = tiers[len - 1].1.len();
        let all_levels_except_last_level_size = all_levels_size - last_level_size;

        let trigger_space_amplification_ratio = 100 * all_levels_except_last_level_size
            >= self.options.max_size_amplification_percent * last_level_size;
        if trigger_space_amplification_ratio {
            return Some(TieredCompactionTask {
                tiers: tiers.clone(),
                bottom_tier_included: true,
            });
        }

        let mut size_of_all_previous_tiers = 0;
        for (idx, (_, tier)) in (1..=len).zip(tiers) {
            let size_of_this_tier = tier.len();

            let trigger_size_ratio = 100 * size_of_all_previous_tiers
                >= (100 + self.options.size_ratio) * size_of_this_tier;
            if idx >= self.options.min_merge_width && trigger_size_ratio {
                return Some(TieredCompactionTask {
                    tiers: tiers[..idx].to_vec(),
                    bottom_tier_included: idx == len,
                });
            }
            size_of_all_previous_tiers += size_of_this_tier;
        }

        let end = len - self.options.num_tiers + 2;
        Some(TieredCompactionTask {
            tiers: tiers[..end].to_vec(),
            bottom_tier_included: false,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = snapshot.clone();
        let TieredCompactionTask {
            ref tiers,
            bottom_tier_included,
        } = *task;

        let deleted_sst_ids = tiers
            .iter()
            .flat_map(|(_, tier)| tier.iter())
            .copied()
            .collect::<Vec<_>>();
        debug_assert_eq!(output.len(), deleted_sst_ids.len());

        let new_tier_id = output[0];

        if bottom_tier_included {
            let new_tier_len = new_state.levels.len() - tiers.len();
            assert_eq!(&snapshot.levels[new_tier_len..], tiers);

            // Correct way is truncate then push
            // new_state.levels.truncate(new_tier_len);
            // new_state.levels.push((new_tier_id, output.to_vec()));
            new_state.levels.truncate(new_tier_len + 1);
            let new_tier = &mut new_state.levels[new_tier_len];
            new_tier.0 = new_tier_id;
            output.clone_into(&mut new_tier.1);
        } else {
            let first_tier_id = tiers[0].0;
            let start = new_state
                .levels
                .iter()
                .position(|&(tier_id, _)| tier_id == first_tier_id)
                .unwrap();

            // let end = start + tiers.len();
            // let old_tiers = new_state.levels.drain(start..end);
            // let old_tiers = old_tiers.collect::<Vec<_>>();
            // debug_assert_eq!(&old_tiers, tiers);
            // new_state
            //     .levels
            //     .insert(start, (new_tier_id, output.to_vec()));
            let end = start + tiers.len() - 1;
            let end_element = new_state.levels[end].clone();
            let old_tiers = new_state.levels.drain(start..end);
            let old_tiers = old_tiers
                .chain(std::iter::once(end_element))
                .collect::<Vec<_>>();
            debug_assert_eq!(&old_tiers, tiers);
            let new_tier = &mut new_state.levels[start];
            new_tier.0 = new_tier_id;
            output.clone_into(&mut new_tier.1);
        }

        (new_state, deleted_sst_ids)
    }
}
