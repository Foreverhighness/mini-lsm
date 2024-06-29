mod raw_key;
mod timestamped_key;

// use raw_key as key;
use timestamped_key as key;

pub const TS_ENABLED: bool = key::TS_ENABLED;

pub type Key<T> = key::Key<T>;
pub type KeySlice<'a> = key::KeySlice<'a>;
pub type KeyVec = key::KeyVec;
pub type KeyBytes = key::KeyBytes;
