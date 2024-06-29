mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

const SIZE_NUM_OF_ELEMENT: usize = std::mem::size_of::<u16>();
const SIZE_OF_DATA_ELEMENT: usize = std::mem::size_of::<u8>();
const SIZE_OF_OFFSET_ELEMENT: usize = std::mem::size_of::<u16>();
const SIZE_KEY_OVERLAP_LEN: usize = std::mem::size_of::<u16>();
const SIZE_REST_KEY_LEN: usize = std::mem::size_of::<u16>();
const SIZE_TIMESTAMP: usize = std::mem::size_of::<u64>();
const SIZE_KEY_LEN: usize = SIZE_KEY_OVERLAP_LEN + SIZE_REST_KEY_LEN + SIZE_TIMESTAMP;
const SIZE_VALUE_LEN: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
#[derive(Debug)]
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        debug_assert!(!self.data.is_empty());

        let num_of_elements: u16 = self.offsets.len().try_into().unwrap();

        let capacity = self.data.len() * SIZE_OF_DATA_ELEMENT
            + self.offsets.len() * SIZE_OF_OFFSET_ELEMENT
            + SIZE_NUM_OF_ELEMENT;

        let mut buf = BytesMut::with_capacity(capacity);
        buf.put_slice(&self.data);
        self.offsets.iter().for_each(|&v| buf.put_u16(v));
        buf.put_u16(num_of_elements);

        assert_eq!(buf.len(), capacity);
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let n = data.len();

        let num_of_elements = {
            let mut bytes_num_of_elements = &data[n - SIZE_NUM_OF_ELEMENT..];
            bytes_num_of_elements.get_u16() as usize
        };

        let offsets_len = num_of_elements * SIZE_OF_OFFSET_ELEMENT;
        let data_len = n - SIZE_NUM_OF_ELEMENT - offsets_len;

        let offsets = {
            let mut bytes_offsets = &data[data_len..n - 2];
            (0..num_of_elements)
                .map(|_| bytes_offsets.get_u16())
                .collect()
        };
        let data = data[..data_len].to_vec();

        Block { data, offsets }
    }
}
