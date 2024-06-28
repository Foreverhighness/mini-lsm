pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

const SIZE_BLOOM_FILTER_OFFSET: u64 = std::mem::size_of::<u32>() as u64;
const SIZE_META_BLOCK_OFFSET: u64 = std::mem::size_of::<u32>() as u64;
const SIZE_CHECKSUM: usize = std::mem::size_of::<u32>();

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        buf.put_u16(block_meta.len().try_into().unwrap());
        for &BlockMeta {
            offset,
            ref first_key,
            ref last_key,
        } in block_meta
        {
            buf.put_u32(offset.try_into().unwrap());
            buf.put_u16(first_key.len().try_into().unwrap());
            buf.put_slice(first_key.raw_ref());
            buf.put_u16(last_key.len().try_into().unwrap());
            buf.put_slice(last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let len = buf.get_u16().into();
        let mut vec_block_meta = Vec::with_capacity(len);
        for _ in 0..len {
            let offset = buf.get_u32().try_into().unwrap();
            let first_key_len = buf.get_u16().into();
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len));
            let last_key_len = buf.get_u16().into();
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len));
            vec_block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        vec_block_meta
    }
}

/// A file object.
#[derive(Debug)]
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len.try_into().unwrap()];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: &[u8]) -> Result<Self> {
        std::fs::write(path, data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
#[derive(Debug)]
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let mut len = file.1;

        let bloom = {
            let bloom_filter_offset =
                file.read(len - SIZE_BLOOM_FILTER_OFFSET, SIZE_BLOOM_FILTER_OFFSET)?;
            len -= SIZE_BLOOM_FILTER_OFFSET;

            let mut bloom_filter_offset = &bloom_filter_offset[..];
            let bloom_filter_offset = bloom_filter_offset.get_u32().into();

            let bloom_filter_len = len - bloom_filter_offset;
            len = bloom_filter_offset;
            Bloom::decode(&file.read(bloom_filter_offset, bloom_filter_len)?)?
        };

        let block_meta_offset = {
            let block_meta_offset =
                file.read(len - SIZE_META_BLOCK_OFFSET, SIZE_META_BLOCK_OFFSET)?;
            len -= SIZE_META_BLOCK_OFFSET;

            let mut block_meta_offset = &block_meta_offset[..];
            block_meta_offset.get_u32().into()
        };
        let block_meta_len = len - block_meta_offset;
        let block_meta =
            BlockMeta::decode_block_meta(&file.read(block_meta_offset, block_meta_len)?[..]);

        let first_key = KeyBytes::clone(&block_meta.first().unwrap().first_key);
        let last_key = KeyBytes::clone(&block_meta.last().unwrap().last_key);

        let block_meta_offset = block_meta_offset.try_into().unwrap();

        let bloom = Some(bloom);

        Ok(SsTable {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom,
            max_ts: Default::default(),
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let data = {
            let meta = &self.block_meta[block_idx];
            let offset = meta.offset;

            let next_offset = self
                .block_meta
                .get(block_idx + 1)
                .map(|meta| meta.offset)
                .unwrap_or(self.block_meta_offset);
            let len = next_offset - offset;

            let data = {
                let offset = offset.try_into().unwrap();
                let len = len.try_into().unwrap();
                self.file.read(offset, len)?
            };

            debug_assert!(data.len() == len);

            debug_assert!(len > SIZE_CHECKSUM);
            let mut block_sum = &data[len - SIZE_CHECKSUM..];
            let block_sum = block_sum.get_u32();
            let checksum = crc32fast::hash(&data[..len - SIZE_CHECKSUM]);

            if block_sum != checksum {
                return Err(anyhow!("Checksum validation failed"));
            }

            let mut data = data;
            data.truncate(len - SIZE_CHECKSUM);

            data
        };
        let block = Block::decode(&data[..]);

        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        let read_block = || self.read_block(block_idx);
        self.block_cache
            .as_ref()
            .map(|cache| {
                cache
                    .try_get_with((self.sst_id(), block_idx), read_block)
                    .map_err(|e| anyhow!("{}", e))
            })
            .unwrap_or_else(read_block)
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let len = self.num_of_blocks();
        let meta = &self.block_meta;
        let check = move |idx| {
            let meta: &BlockMeta = &meta[idx];
            let first_key = meta.first_key.as_key_slice();
            first_key < key
        };

        let mut left = 0;
        let mut right = len;
        while right > left + 1 {
            let mid = (left + right) / 2;

            if check(mid) {
                left = mid;
            } else {
                right = mid;
            }
        }
        left
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
