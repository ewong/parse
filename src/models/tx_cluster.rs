use std::collections::{HashMap, HashSet};

use csv::ByteRecord;

pub trait TxClusterData: Send + Sync + 'static {
    fn block(&self) -> usize;
    fn client_txns(&self) -> &HashMap<u16, Vec<ByteRecord>>;
    fn client_conflicts(&self) -> &HashMap<u16, HashSet<u32>>;
}

pub struct TxCluster {
    block: usize,
    client_txns: HashMap<u16, Vec<ByteRecord>>,
    client_conflicts: HashMap<u16, HashSet<u32>>,
}

impl TxCluster {
    pub fn new(block: usize) -> Self {
        Self {
            block,
            client_txns: HashMap::new(),
            client_conflicts: HashMap::new(),
        }
    }

    pub fn add_tx(&mut self, client_id: &u16, byte_record: &ByteRecord) {
        if self.client_txns.contains_key(client_id) {
            self.client_txns.entry(client_id.clone()).and_modify(|e| {
                e.push(byte_record.clone());
            });
            return;
        }
        let mut v = Vec::new();
        v.push(byte_record.clone());
        self.client_txns.insert(client_id.clone(), v);
    }

    pub fn add_conflict(&mut self, client_id: &u16, tx_id: &u32) {
        if self.client_conflicts.contains_key(client_id) {
            self.client_conflicts
                .entry(client_id.clone())
                .and_modify(|e| {
                    e.insert(tx_id.clone());
                });
            return;
        }
        let mut set = HashSet::new();
        set.insert(tx_id.clone());
        self.client_conflicts.insert(client_id.clone(), set);
    }
}

impl TxClusterData for TxCluster {
    fn block(&self) -> usize {
        self.block
    }

    fn client_txns(&self) -> &HashMap<u16, Vec<ByteRecord>> {
        &self.client_txns
    }

    fn client_conflicts(&self) -> &HashMap<u16, HashSet<u32>> {
        &self.client_conflicts
    }
}
