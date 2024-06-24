use std::sync::Arc;

use bytes::Buf;

use crate::{
    block::{SIZE_KEY_LEN, SIZE_VALUE_LEN},
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        debug_assert!(!block.offsets.is_empty(), "number of element is zero");
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        debug_assert!(self.is_valid());
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(self.is_valid());
        let range = self.value_range.0..self.value_range.1;
        &self.block.data[range]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        (self.key, self.value_range) = self.get_key_value_range_by_offset(0);
        self.idx = 0;
        self.first_key = self.key.clone();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        debug_assert!(self.is_valid());

        self.idx += 1;
        if let Some(&offset) = self.block.offsets.get(self.idx) {
            (self.key, self.value_range) = self.get_key_value_range_by_offset(offset as usize);
        } else {
            self.key = KeyVec::new();
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();

        while self.key.as_key_slice() < key {
            if !self.is_valid() {
                return;
            }

            self.next();
        }
    }

    /// Get key value from offset
    fn get_key_value_range_by_offset(&self, offset: usize) -> (KeyVec, (usize, usize)) {
        let mut data: &[u8] = &self.block.data[offset..];
        let overlap_len: usize = data.get_u16().into();
        let rest_key_len: usize = data.get_u16().into();
        let mut key = Vec::with_capacity(overlap_len + rest_key_len);

        key.extend_from_slice(&self.first_key.raw_ref()[..overlap_len]);
        key.extend_from_slice(&data[..rest_key_len]);
        data.advance(rest_key_len);

        let key = KeyVec::from_vec(key);

        let value_len: usize = data.get_u16().into();
        let value_start = offset + SIZE_KEY_LEN + rest_key_len + SIZE_VALUE_LEN;
        (key, (value_start, value_start + value_len))
    }
}
