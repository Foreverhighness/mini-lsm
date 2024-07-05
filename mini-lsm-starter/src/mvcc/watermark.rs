use std::collections::{btree_map::Entry, BTreeMap};

#[derive(Debug, Default)]
pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        *self.readers.entry(ts).or_default() += 1;
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let Entry::Occupied(entry) = self.readers.entry(ts).and_modify(|e| *e -= 1) else {
            panic!("entry not found");
        };

        if *entry.get() == 0 {
            entry.remove();
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|(&ts, _)| ts)
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}
