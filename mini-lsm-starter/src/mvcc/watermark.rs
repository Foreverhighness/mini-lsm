#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod
#![allow(clippy::unused_self)] // TODO(fh): remove clippy allow
#![allow(clippy::unnecessary_wraps)] // TODO(fh): remove clippy allow
#![allow(clippy::missing_const_for_fn)] // TODO(fh): remove clippy allow

use std::collections::BTreeMap;

#[derive(Debug)]
pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {}

    pub fn remove_reader(&mut self, ts: u64) {}

    pub fn watermark(&self) -> Option<u64> {
        Some(0)
    }
}
