#![allow(dead_code)]

use std::{cmp::Reverse, fmt::Debug, marker::PhantomData};

use bytes::{Buf, BufMut, Bytes};

pub const TS_ENABLED: bool = true;

pub type TimeStamp = u64;

pub struct TimeStampedKey<T: AsRef<[u8]>>(T, TimeStamp);

pub type Key<T> = TimeStampedKey<T>;
pub type KeySlice<'a> = Key<&'a [u8]>;
pub type KeyVec = Key<Vec<u8>>;
pub type KeyBytes = Key<Bytes>;

/// Temporary, should remove after implementing full week 3 day 1 + 2.
pub const TS_DEFAULT: TimeStamp = 0;

pub const TS_MAX: TimeStamp = u64::MAX;
pub const TS_MIN: TimeStamp = u64::MIN;
pub const TS_RANGE_BEGIN: TimeStamp = u64::MAX;
pub const TS_RANGE_END: TimeStamp = u64::MIN;

impl<T: AsRef<[u8]>> Key<T> {
    pub fn into_inner(self) -> T {
        self.0
    }

    pub fn key_len(&self) -> usize {
        self.0.as_ref().len()
    }

    pub fn raw_len(&self) -> usize {
        self.0.as_ref().len() + std::mem::size_of::<TimeStamp>()
    }

    pub fn is_empty(&self) -> bool {
        self.0.as_ref().is_empty()
    }

    pub fn for_testing_ts(self) -> TimeStamp {
        self.1
    }
}

impl Key<Vec<u8>> {
    pub fn new() -> Self {
        Self(Vec::new(), TS_DEFAULT)
    }

    /// Create a `KeyVec` from a `Vec<u8>` and a ts. Will be removed in week 3.
    pub fn from_vec_with_ts(key: Vec<u8>, ts: TimeStamp) -> Self {
        Self(key, ts)
    }

    /// Clears the key and set ts to 0.
    pub fn clear(&mut self) {
        self.0.clear()
    }

    /// Append a slice to the end of the key
    pub fn append(&mut self, data: &[u8]) {
        self.0.extend(data)
    }

    pub fn set_ts(&mut self, ts: TimeStamp) {
        self.1 = ts;
    }

    /// Set the key from a slice without re-allocating.
    pub fn set_from_slice(&mut self, key_slice: KeySlice) {
        self.0.clear();
        self.0.extend(key_slice.0);
        self.1 = key_slice.1;
    }

    pub fn as_key_slice(&self) -> KeySlice {
        TimeStampedKey(self.0.as_slice(), self.1)
    }

    pub fn into_key_bytes(self) -> KeyBytes {
        TimeStampedKey(self.0.into(), self.1)
    }

    pub fn key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn ts(&self) -> TimeStamp {
        self.1
    }

    pub fn for_testing_key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn for_testing_from_vec_no_ts(key: Vec<u8>) -> Self {
        Self(key, TS_DEFAULT)
    }
}

impl Key<Bytes> {
    pub fn new() -> Self {
        Self(Bytes::new(), TS_DEFAULT)
    }

    /// recover one record from wal
    pub fn recover_from_wal(buf: &mut &[u8], key_len: usize) -> Self {
        let key = Bytes::copy_from_slice(&buf[..key_len]);
        buf.advance(key_len);
        let ts = buf.get_u64();

        TimeStampedKey(key, ts)
    }

    pub fn copy_from_slice(key: KeySlice) -> Self {
        Self(Bytes::copy_from_slice(key.0), key.1)
    }

    pub fn as_key_slice(&self) -> KeySlice {
        TimeStampedKey(&self.0, self.1)
    }

    /// Create a `KeyBytes` from a `Bytes` and a ts.
    pub fn from_bytes_with_ts(bytes: Bytes, ts: TimeStamp) -> KeyBytes {
        TimeStampedKey(bytes, ts)
    }

    pub fn key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn ts(&self) -> TimeStamp {
        self.1
    }

    pub fn for_testing_from_bytes_no_ts(bytes: Bytes) -> KeyBytes {
        TimeStampedKey(bytes, TS_DEFAULT)
    }

    pub fn for_testing_key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

pub struct KeyBytesGuard<'a>(KeyBytes, PhantomData<&'a [u8]>);

impl AsRef<KeyBytes> for KeyBytesGuard<'_> {
    fn as_ref(&self) -> &KeyBytes {
        &self.0
    }
}

impl<'a> Key<&'a [u8]> {
    pub fn to_key_vec(self) -> KeyVec {
        TimeStampedKey(self.0.to_vec(), self.1)
    }

    /// Create a key slice from a slice. Will be removed in week 3.
    pub fn from_slice(slice: &'a [u8], ts: TimeStamp) -> Self {
        Self(slice, ts)
    }

    /// put self into wal record
    pub fn put_into_wal(&self, buf: &mut Vec<u8>) {
        let key_len = self.key_len().try_into().unwrap();
        buf.put_u16(key_len);
        buf.put_slice(self.key_ref());
        buf.put_u64(self.ts());
    }

    /// # Examples
    ///
    /// ```compile_fail
    /// let key_bytes = KeyBytes::new();
    /// let guard = {
    ///     let key_slice = key_bytes.as_key_slice();
    ///     key_slice.as_key_bytes()
    /// };
    /// ```
    pub fn as_key_bytes(self) -> KeyBytesGuard<'a> {
        // This is safe because the guard lifetime no longer than self
        let bytes = unsafe { std::mem::transmute::<&'_ [u8], &'static [u8]>(self.0) };
        let bytes = Bytes::from_static(bytes);
        let ts = self.1;

        KeyBytesGuard(KeyBytes::from_bytes_with_ts(bytes, ts), PhantomData)
    }

    pub fn key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn ts(&self) -> TimeStamp {
        self.1
    }

    pub fn for_testing_key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn for_testing_from_slice_no_ts(slice: &'a [u8]) -> Self {
        Self(slice, TS_DEFAULT)
    }

    pub fn for_testing_from_slice_with_ts(slice: &'a [u8], ts: TimeStamp) -> Self {
        Self(slice, ts)
    }
}

impl<T: AsRef<[u8]> + Debug> Debug for Key<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: AsRef<[u8]> + Default> Default for Key<T> {
    fn default() -> Self {
        Self(T::default(), TS_DEFAULT)
    }
}

impl<T: AsRef<[u8]> + PartialEq> PartialEq for Key<T> {
    fn eq(&self, other: &Self) -> bool {
        (self.0.as_ref(), self.1).eq(&(other.0.as_ref(), other.1))
    }
}

impl<T: AsRef<[u8]> + Eq> Eq for Key<T> {}

impl<T: AsRef<[u8]> + Clone> Clone for Key<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1)
    }
}

impl<T: AsRef<[u8]> + Copy> Copy for Key<T> {}

impl<T: AsRef<[u8]> + PartialOrd> PartialOrd for Key<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.0.as_ref(), Reverse(self.1)).partial_cmp(&(other.0.as_ref(), Reverse(other.1)))
    }
}

impl<T: AsRef<[u8]> + Ord> Ord for Key<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.0.as_ref(), Reverse(self.1)).cmp(&(other.0.as_ref(), Reverse(other.1)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_as_bytes_guard() {
        let key_bytes = KeyBytes::from_bytes_with_ts(Bytes::from_static(b"233"), 666);
        let guard = {
            let key_slice = key_bytes.as_key_slice();
            key_slice.as_key_bytes()
        };

        assert_eq!(&key_bytes, guard.as_ref())
    }
}
