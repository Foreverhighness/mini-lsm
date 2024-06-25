use bytes::BufMut;

use crate::{
    block::{SIZE_KEY_LEN, SIZE_NUM_OF_ELEMENT, SIZE_OF_OFFSET_ELEMENT, SIZE_VALUE_LEN},
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
    /// Current offset
    offset: usize,
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
            offset: 0,
            size: 0,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        debug_assert!(!key.is_empty());

        let key_overlap_len = key
            .raw_ref()
            .iter()
            .zip(self.first_key.raw_ref().iter())
            .take_while(|&(&a, &b)| a == b)
            .count();
        let rest_key_len = key.len() - key_overlap_len;
        let value_len: u16 = value.len().try_into().unwrap();
        let offset: u16 = self.offset.try_into().unwrap();

        let data_size = SIZE_KEY_LEN + rest_key_len + SIZE_VALUE_LEN + value.len();

        let exceeds_block_size =
            self.size + data_size + SIZE_OF_OFFSET_ELEMENT + SIZE_NUM_OF_ELEMENT > self.block_size;
        if exceeds_block_size && !self.is_empty() {
            return false;
        }
        if self.is_empty() {
            self.first_key = key.to_key_vec();
        }

        self.data.put_u16(key_overlap_len.try_into().unwrap());
        self.data.put_u16(rest_key_len.try_into().unwrap());
        self.data.put_slice(&key.raw_ref()[key_overlap_len..]);
        self.data.put_u16(value_len);
        self.data.put_slice(value);

        self.offsets.push(offset);
        self.offset += data_size;

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
