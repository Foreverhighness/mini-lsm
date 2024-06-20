#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod
#![allow(clippy::must_use_candidate)] // TODO(fh): remove clippy allow

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            key.raw_ref().clone_into(&mut self.first_key);
        }

        let not_full = self.builder.add(key, value);
        if not_full {
            eprintln!("not full on {:?} {:?}", self.first_key, key);
            key.raw_ref().clone_into(&mut self.last_key);
            return;
        }
        eprintln!("full on {:?} {:?}", self.first_key, key);

        // block is full
        self.flush_block();

        let success = self.builder.add(key, value);
        debug_assert!(success);

        key.raw_ref().clone_into(&mut self.first_key);
        key.raw_ref().clone_into(&mut self.last_key);
    }

    fn flush_block(&mut self) {
        let block = {
            let full_block_builder =
                std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            full_block_builder.build()
        };

        let meta = {
            let offset = self.data.len();
            let first_key = KeyBytes::from_bytes(Bytes::copy_from_slice(&self.first_key));
            let last_key = KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key));
            BlockMeta {
                offset,
                first_key,
                last_key,
            }
        };

        self.data.extend_from_slice(&block.encode());
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

        let block_meta_offset = self.data.len();
        let first_key = {
            let mut first_key_entry = &self.data[..];
            let first_key_len = first_key_entry.get_u16().into();
            let first_key = first_key_entry.copy_to_bytes(first_key_len);
            KeyBytes::from_bytes(first_key)
        };

        let file = {
            BlockMeta::encode_block_meta(&self.meta, &mut self.data);
            self.data.put_u32(block_meta_offset.try_into().unwrap());

            FileObject::create(path.as_ref(), self.data).unwrap()
        };

        let block_meta = self.meta;
        let last_key = KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key));

        Ok(SsTable {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Default::default(),
            max_ts: Default::default(),
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
