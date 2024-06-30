#![allow(dead_code)]
#![allow(clippy::unused_self)]

use std::{fmt::Debug, marker::PhantomData};

use bytes::{Buf, BufMut, Bytes};

pub const TS_ENABLED: bool = false;

#[derive(Clone, Copy)]
pub struct RawKey<T: AsRef<[u8]>>(T);

pub type Key<T> = RawKey<T>;
pub type KeySlice<'a> = RawKey<&'a [u8]>;
pub type KeyVec = RawKey<Vec<u8>>;
pub type KeyBytes = RawKey<Bytes>;

impl<T: AsRef<[u8]>> Key<T> {
    pub fn into_inner(self) -> T {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.as_ref().len()
    }

    pub fn key_len(&self) -> usize {
        self.len()
    }

    pub fn raw_len(&self) -> usize {
        self.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.as_ref().is_empty()
    }

    pub fn for_testing_ts(self) -> u64 {
        0
    }

    /// encode key without consider overlap
    pub fn encode_raw(&self, buf: &mut Vec<u8>) {
        let key = self.0.as_ref();
        let key_len = key.len().try_into().unwrap();
        buf.put_u16(key_len);
        buf.put_slice(key);
    }

    /// calculate occupied size when consider overlap
    pub fn occupy_size_overlap(&self, first_key: &[u8]) -> usize {
        const SIZE_KEY_OVERLAP_LEN: usize = std::mem::size_of::<u16>();
        const SIZE_REST_KEY_LEN: usize = std::mem::size_of::<u16>();

        let key = self.0.as_ref();
        let key_overlap_len = key
            .as_ref()
            .iter()
            .zip(first_key.iter())
            .take_while(|&(&a, &b)| a == b)
            .count();

        SIZE_KEY_OVERLAP_LEN + SIZE_REST_KEY_LEN + key.len() - key_overlap_len
    }

    /// encode key with consider overlap
    pub fn encode_overlap(&self, buf: &mut Vec<u8>, first_key: &[u8]) {
        let key = self.0.as_ref();
        let key_overlap_len = key
            .as_ref()
            .iter()
            .zip(first_key.iter())
            .take_while(|&(&a, &b)| a == b)
            .count();
        let rest_key_len = key.len() - key_overlap_len;

        // encode key
        buf.put_u16(key_overlap_len.try_into().unwrap());
        buf.put_u16(rest_key_len.try_into().unwrap());
        buf.put_slice(&key[key_overlap_len..]);
    }
}

impl Key<Vec<u8>> {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Create a `KeyVec` from a `Vec<u8>`. Will be removed in week 3.
    pub fn from_vec(key: Vec<u8>) -> Self {
        Self(key)
    }

    /// Clears the key and set ts to 0.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Append a slice to the end of the key
    pub fn append(&mut self, data: &[u8]) {
        self.0.extend(data);
    }

    /// Set the key from a slice without re-allocating. The signature will change in week 3.
    pub fn set_from_slice(&mut self, key_slice: KeySlice) {
        self.0.clear();
        self.0.extend(key_slice.0);
    }

    pub fn as_key_slice(&self) -> KeySlice {
        RawKey(self.0.as_slice())
    }

    pub fn into_key_bytes(self) -> KeyBytes {
        RawKey(self.0.into())
    }

    /// Always use `raw_ref` to access the key in week 1 + 2. This function will be removed in week 3.
    pub fn raw_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn key_ref(&self) -> &[u8] {
        self.raw_ref()
    }

    /// decode from buffer, consider overlap
    pub fn decode_from_overlap(buf: &mut &[u8], first_key: &[u8]) -> Self {
        let overlap_len: usize = buf.get_u16().into();
        let rest_key_len: usize = buf.get_u16().into();
        let mut key = Vec::with_capacity(overlap_len + rest_key_len);

        key.extend_from_slice(&first_key[..overlap_len]);
        key.extend_from_slice(&buf[..rest_key_len]);
        buf.advance(rest_key_len);

        Self(key)
    }

    pub fn for_testing_key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn for_testing_from_vec_no_ts(key: Vec<u8>) -> Self {
        Self(key)
    }
}

impl Key<Bytes> {
    pub fn as_key_slice(&self) -> KeySlice {
        RawKey(&self.0)
    }

    /// Create a `KeyBytes` from a `Bytes`. Will be removed in week 3.
    pub fn from_bytes(bytes: Bytes) -> KeyBytes {
        RawKey(bytes)
    }

    /// Always use `raw_ref` to access the key in week 1 + 2. This function will be removed in week 3.
    pub fn raw_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn key_ref(&self) -> &[u8] {
        self.raw_ref()
    }

    pub fn copy_from_slice(slice: KeySlice) -> Self {
        RawKey(Bytes::copy_from_slice(slice.raw_ref()))
    }

    /// decode from buffer, do not consider overlap
    pub fn decode_from_raw(buf: &mut &[u8]) -> Self {
        let key_len = buf.get_u16().into();
        let key = Bytes::copy_from_slice(&buf[..key_len]);
        buf.advance(key_len);

        RawKey(key)
    }

    pub fn for_testing_from_bytes_no_ts(bytes: Bytes) -> KeyBytes {
        RawKey(bytes)
    }

    pub fn for_testing_key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<'a> Key<&'a [u8]> {
    pub fn to_key_vec(self) -> KeyVec {
        RawKey(self.0.to_vec())
    }

    /// Create a key slice from a slice. Will be removed in week 3.
    pub fn from_slice(slice: &'a [u8]) -> Self {
        Self(slice)
    }

    /// Always use `raw_ref` to access the key in week 1 + 2. This function will be removed in week 3.
    pub fn raw_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn key_ref(self) -> &'a [u8] {
        self.raw_ref()
    }

    pub fn for_testing_key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn for_testing_from_slice_no_ts(slice: &'a [u8]) -> Self {
        Self(slice)
    }

    pub fn for_testing_from_slice_with_ts(slice: &'a [u8], _ts: u64) -> Self {
        Self(slice)
    }
}

pub struct KeyBytesGuard<'a>(KeyBytes, PhantomData<&'a [u8]>);
impl AsRef<KeyBytes> for KeyBytesGuard<'_> {
    fn as_ref(&self) -> &KeyBytes {
        &self.0
    }
}
impl<'a> KeySlice<'a> {
    /// # Examples
    ///
    /// ```
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

        KeyBytesGuard(KeyBytes::from_bytes(bytes), PhantomData)
    }
}

impl<T: AsRef<[u8]> + Debug> Debug for Key<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: AsRef<[u8]> + Default> Default for Key<T> {
    fn default() -> Self {
        Self(T::default())
    }
}

impl<T: AsRef<[u8]> + PartialEq> PartialEq for Key<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<T: AsRef<[u8]> + Eq> Eq for Key<T> {}

impl<T: AsRef<[u8]> + PartialOrd> PartialOrd for Key<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl<T: AsRef<[u8]> + Ord> Ord for Key<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}
