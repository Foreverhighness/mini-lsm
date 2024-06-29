pub type TimeStamp = u64;

/// Temporary, should remove after implementing full week 3 day 1 + 2.
pub const TS_DEFAULT: TimeStamp = 0;

pub const TS_MAX: TimeStamp = u64::MAX;
pub const TS_MIN: TimeStamp = u64::MIN;
pub const TS_RANGE_BEGIN: TimeStamp = u64::MAX;
pub const TS_RANGE_END: TimeStamp = u64::MIN;
