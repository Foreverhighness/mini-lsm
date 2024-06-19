#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod
#![allow(clippy::needless_pass_by_value)] // TODO(fh): remove clippy allow
#![allow(clippy::must_use_candidate)] // TODO(fh): remove clippy allow

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.1.key().eq(&other.1.key()) && self.0.eq(&other.0)
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then_with(|| self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters: BinaryHeap<_> = iters
            .into_iter()
            .filter(|iter| iter.is_valid())
            .enumerate()
            .map(|(idx, iter)| HeapWrapper(idx, iter))
            .collect();
        let current = iters.pop();
        Self { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .map(|heap| heap.1.key())
            .unwrap_or_default()
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .map(|heap| heap.1.value())
            .unwrap_or_default()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let mut old_heap = self.current.take().unwrap();
        debug_assert!(!self.is_valid());

        let key = old_heap.1.key();
        while let Some(mut heap) = self.iters.pop() {
            if key != heap.1.key() {
                debug_assert!(heap.1.is_valid());
                self.iters.push(heap);
                break;
            }

            heap.1.next()?;
            if heap.1.is_valid() {
                self.iters.push(heap);
            }
        }

        old_heap.1.next()?;
        if old_heap.1.is_valid() {
            self.iters.push(old_heap);
        }

        self.current = self.iters.pop();
        Ok(())
    }
}
