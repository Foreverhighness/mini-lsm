use bytes::BufMut;

use crate::{
    block::{SIZE_NUM_OF_ELEMENT, SIZE_OF_OFFSET_ELEMENT, SIZE_VALUE_LEN},
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
    /// Current size
    size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::default(),
            size: 0,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        debug_assert!(!key.is_empty());

        let first_key = self.first_key.key_ref();
        let data_size = key.occupy_size_overlap(first_key) + SIZE_VALUE_LEN + value.len();
        let exceeds_block_size =
            self.size + data_size + SIZE_OF_OFFSET_ELEMENT + SIZE_NUM_OF_ELEMENT > self.block_size;
        if exceeds_block_size && !self.is_empty() {
            return false;
        }

        let offset = self.data.len();
        // encode key
        key.encode_overlap(&mut self.data, first_key);

        if self.is_empty() {
            self.first_key = key.to_key_vec();
        }

        // encode value
        let value_len: u16 = value.len().try_into().unwrap();
        self.data.put_u16(value_len);
        self.data.put_slice(value);

        debug_assert_eq!(self.data.len() - offset, data_size);
        self.offsets.push(offset.try_into().unwrap());

        self.size += data_size + SIZE_OF_OFFSET_ELEMENT;

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        debug_assert!(!self.is_empty());

        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
