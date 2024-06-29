use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use parking_lot::Mutex;

use crate::mem_table::{KvStore, UserKey, UserKeyRef};
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

    pub fn recover(path: impl AsRef<Path>, kv_store: &KvStore) -> Result<Self> {
        let path = path.as_ref();
        let buf = fs::read(path)?;

        let mut buf = &buf[..];
        while !buf.is_empty() {
            let start = buf;

            let key = UserKey::decode_from_raw(&mut buf);

            let value_len = buf.get_u16() as usize;
            let value = Bytes::copy_from_slice(&buf[..value_len]);
            buf.advance(value_len);

            buf.advance(SIZE_CHECKSUM);

            let len = SIZE_KEY_LEN + key.raw_len() + SIZE_VALUE_LEN + value_len + SIZE_CHECKSUM;
            validate_checksum(&start[..len])?;

            debug_assert!(!key.is_empty());
            kv_store.insert(key, value);
        }

        let file = OpenOptions::new().append(true).open(path)?;
        let file = Arc::new(Mutex::new(BufWriter::new(file)));
        Ok(Wal { file })
    }

    pub fn put(&self, key: UserKeyRef, value: &[u8]) -> Result<()> {
        let len = SIZE_KEY_LEN + key.raw_len() + SIZE_VALUE_LEN + value.len() + SIZE_CHECKSUM;
        let mut buf = Vec::with_capacity(len);

        // put key
        key.encode_raw(&mut buf);

        let value_len = value.len().try_into().unwrap();
        buf.put_u16(value_len);
        buf.put_slice(value);

        let checksum = crc32fast::hash(&buf);
        buf.put_u32(checksum);

        debug_assert_eq!(buf.len(), len);

        let mut file = self.file.lock();
        file.write_all(&buf)?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_ref().sync_all()?;

        Ok(())
    }
}
