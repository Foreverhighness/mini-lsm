use super::raw_key::RawKey;
use super::TS_ENABLED;

pub type TimeStamp = u64;

/// Temporary, should remove after implementing full week 3 day 1 + 2.
pub const TS_DEFAULT: TimeStamp = 0;

pub const TS_MAX: TimeStamp = if TS_ENABLED { u64::MAX } else { TS_DEFAULT };
pub const TS_MIN: TimeStamp = if TS_ENABLED { u64::MIN } else { TS_DEFAULT };
pub const TS_RANGE_BEGIN: TimeStamp = if TS_ENABLED { u64::MAX } else { TS_DEFAULT };
pub const TS_RANGE_END: TimeStamp = if TS_ENABLED { u64::MIN } else { TS_DEFAULT };

#[allow(dead_code)]
impl<'a> RawKey<&'a [u8]> {
    pub fn from_slice_ts(slice: &'a [u8], _: TimeStamp) -> Self {
        Self::from_slice(slice)
    }
}

#[allow(dead_code)]
impl<T: AsRef<[u8]>> RawKey<T> {
    pub fn ts(&self) -> TimeStamp {
        TS_DEFAULT
    }
}
