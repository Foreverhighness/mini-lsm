use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::table::{validate_checksum, SIZE_CHECKSUM};

const SIZE_KEY_LEN: usize = std::mem::size_of::<u16>();
const SIZE_VALUE_LEN: usize = std::mem::size_of::<u16>();

#[derive(Debug)]
pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(path)?;
        let file = Arc::new(Mutex::new(BufWriter::new(file)));

        Ok(Wal { file })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let buf = fs::read(path)?;

        let mut buf = &buf[..];
        while !buf.is_empty() {
            let start = buf;

            let key_len = buf.get_u16() as usize;
            let key = Bytes::copy_from_slice(&buf[..key_len]);
            buf.advance(key_len);

            let value_len = buf.get_u16() as usize;
            let value = Bytes::copy_from_slice(&buf[..value_len]);
            buf.advance(value_len);

            buf.advance(SIZE_CHECKSUM);

            let len = SIZE_KEY_LEN + key_len + SIZE_VALUE_LEN + value_len + SIZE_CHECKSUM;
            validate_checksum(&start[..len])?;

            debug_assert!(!key.is_empty());
            skiplist.insert(key, value);
        }

        let file = OpenOptions::new().append(true).open(path)?;
        let file = Arc::new(Mutex::new(BufWriter::new(file)));
        Ok(Wal { file })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let len = SIZE_KEY_LEN + key.len() + SIZE_VALUE_LEN + value.len() + SIZE_CHECKSUM;
        let mut buf = Vec::with_capacity(len);

        let key_len = key.len().try_into().unwrap();
        let value_len = value.len().try_into().unwrap();

        buf.put_u16(key_len);
        buf.put_slice(key);
        buf.put_u16(value_len);
        buf.put_slice(value);

        let checksum = crc32fast::hash(&buf);
        buf.put_u32(checksum);

        let mut file = self.file.lock();
        file.write_all(&buf)?;

        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_ref().sync_all()?;

        Ok(())
    }
}
