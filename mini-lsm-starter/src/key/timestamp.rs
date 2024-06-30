use super::TS_ENABLED;

pub type TimeStamp = u64;

/// Temporary, should remove after implementing full week 3 day 1 + 2.
pub const TS_DEFAULT: TimeStamp = 0;

pub const TS_MAX: TimeStamp = if TS_ENABLED { u64::MAX } else { TS_DEFAULT };
pub const TS_MIN: TimeStamp = if TS_ENABLED { u64::MIN } else { TS_DEFAULT };
pub const TS_RANGE_BEGIN: TimeStamp = if TS_ENABLED { u64::MAX } else { TS_DEFAULT };
pub const TS_RANGE_END: TimeStamp = if TS_ENABLED { u64::MIN } else { TS_DEFAULT };
