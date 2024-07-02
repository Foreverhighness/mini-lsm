use std::collections::BTreeMap;

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
        debug_assert!(self.readers.contains_key(&ts));

        let cnt = self.readers.get_mut(&ts).unwrap();
        *cnt -= 1;
        if *cnt == 0 {
            self.readers.remove(&ts);
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|(&ts, _)| ts)
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}
