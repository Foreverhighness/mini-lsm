pub mod txn;
pub mod watermark;

use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Weak},
};

use parking_lot::Mutex;

use crate::{key::TimeStamp, lsm_storage::LsmStorageInner};

use self::{txn::Transaction, watermark::Watermark};

#[derive(Debug)]
pub(crate) struct CommittedTxnData {
    pub(crate) key_hashes: HashSet<u32>,
    #[allow(dead_code)]
    pub(crate) read_ts: TimeStamp,
    #[allow(dead_code)]
    pub(crate) commit_ts: TimeStamp,
}

#[derive(Debug)]
pub(crate) struct LsmMvccInner {
    pub(crate) write_lock: Mutex<()>,
    pub(crate) commit_lock: Mutex<()>,
    pub(crate) ts: Arc<Mutex<(TimeStamp, Watermark)>>,
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<TimeStamp, CommittedTxnData>>>,
    weak: Weak<LsmStorageInner>,
}

impl LsmMvccInner {
    pub fn new(initial_ts: TimeStamp, weak: Weak<LsmStorageInner>) -> Self {
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            ts: Arc::new(Mutex::new((initial_ts, Watermark::new()))),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
            weak,
        }
    }

    pub fn latest_commit_ts(&self) -> TimeStamp {
        self.ts.lock().0
    }

    pub fn update_commit_ts(&self, ts: TimeStamp) {
        self.ts.lock().0 = ts;
    }

    /// All ts (strictly) below this ts can be garbage collected.
    pub fn watermark(&self) -> TimeStamp {
        let ts = self.ts.lock();
        ts.1.watermark().unwrap_or(ts.0)
    }

    pub fn remove_read_ts(&self, read_ts: TimeStamp) {
        let (_, ref mut watermark) = *self.ts.lock();
        watermark.remove_reader(read_ts);
    }

    pub fn new_txn(&self, serializable: bool) -> Arc<Transaction> {
        let inner = self.weak.upgrade().unwrap();

        let (read_ts, ref mut watermark) = *self.ts.lock();
        watermark.add_reader(read_ts);

        let rw_set = serializable.then(Default::default);

        Arc::new(Transaction {
            read_ts,
            inner,
            local_storage: Arc::default(),
            committed: Arc::default(),
            rw_set,
        })
    }

    fn vacuum(&self) {
        let watermark = self.watermark();
        self.committed_txns.lock().retain(|&ts, _| ts > watermark);
    }
}
