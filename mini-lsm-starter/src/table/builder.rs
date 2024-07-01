use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{
    bloom::{Bloom, BLOOM_DEFAULT_FPR},
    BlockMeta, FileObject, SsTable,
};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice, KeyVec, TimeStamp, TS_MIN},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
    max_ts: TimeStamp,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
            max_ts: TS_MIN,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        self.max_ts = self.max_ts.max(key.ts());

        // prepare bloom filter
        let hash = farmhash::fingerprint32(key.key_ref());
        self.key_hashes.push(hash);

        let not_full = self.builder.add(key, value);
        if not_full {
            self.last_key.set_from_slice(key);
            return;
        }

        // block is full
        self.flush_block();

        let success = self.builder.add(key, value);
        debug_assert!(success);

        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
    }

    fn flush_block(&mut self) {
        let block = {
            let full_block_builder =
                std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            full_block_builder.build()
        };

        let meta = {
            let offset = self.data.len();
            let first_key = std::mem::take(&mut self.first_key).into_key_bytes();
            let last_key = std::mem::take(&mut self.last_key).into_key_bytes();
            BlockMeta {
                offset,
                first_key,
                last_key,
            }
        };

        let buf = block.encode();
        self.data.extend_from_slice(&buf);
        let checksum = crc32fast::hash(&buf);
        self.data.put_u32(checksum);
        self.meta.push(meta);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.flush_block();

        let bloom = {
            let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), BLOOM_DEFAULT_FPR);
            Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key)
        };

        let block_meta_offset = self.data.len();

        let file = {
            BlockMeta::encode_block_meta(&self.meta, &mut self.data, self.max_ts);
            self.data.put_u32(block_meta_offset.try_into().unwrap());

            let bloom_filter_offset = self.data.len();
            bloom.encode(&mut self.data);
            self.data.put_u32(bloom_filter_offset.try_into().unwrap());

            FileObject::create(path.as_ref(), &self.data).unwrap()
        };

        let block_meta = self.meta;
        let first_key = KeyBytes::clone(&block_meta.first().unwrap().first_key);
        let last_key = KeyBytes::clone(&block_meta.last().unwrap().last_key);

        let bloom = Some(bloom);

        let max_ts = self.max_ts;

        Ok(SsTable {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom,
            max_ts,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
