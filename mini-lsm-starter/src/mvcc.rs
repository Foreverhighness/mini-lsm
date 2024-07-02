#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod
#![allow(clippy::needless_pass_by_value)] // TODO(fh): remove clippy allow
#![allow(clippy::unused_self)] // TODO(fh): remove clippy allow

pub mod txn;
pub mod watermark;

use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Weak},
};

use parking_lot::Mutex;

use crate::lsm_storage::LsmStorageInner;

use self::{txn::Transaction, watermark::Watermark};

#[derive(Debug)]
pub(crate) struct CommittedTxnData {
    pub(crate) key_hashes: HashSet<u32>,
    #[allow(dead_code)]
    pub(crate) read_ts: u64,
    #[allow(dead_code)]
    pub(crate) commit_ts: u64,
}

#[derive(Debug)]
pub(crate) struct LsmMvccInner {
    pub(crate) write_lock: Mutex<()>,
    pub(crate) commit_lock: Mutex<()>,
    pub(crate) ts: Arc<Mutex<(u64, Watermark)>>,
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
    weak: Weak<LsmStorageInner>,
}

impl LsmMvccInner {
    pub fn new(initial_ts: u64, weak: Weak<LsmStorageInner>) -> Self {
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            ts: Arc::new(Mutex::new((initial_ts, Watermark::new()))),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
            weak,
        }
    }

    pub fn latest_commit_ts(&self) -> u64 {
        self.ts.lock().0
    }

    pub fn update_commit_ts(&self, ts: u64) {
        self.ts.lock().0 = ts;
    }

    /// All ts (strictly) below this ts can be garbage collected.
    pub fn watermark(&self) -> u64 {
        let ts = self.ts.lock();
        ts.1.watermark().unwrap_or(ts.0)
    }

    pub fn new_txn(&self, serializable: bool) -> Arc<Transaction> {
        let inner = self.weak.upgrade().unwrap();
        // TODO(fh): clear default
        Arc::new(Transaction {
            read_ts: self.latest_commit_ts(),
            inner,
            local_storage: Default::default(),
            committed: Default::default(),
            key_hashes: Default::default(),
        })
    }
}
