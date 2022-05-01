use std::collections::HashMap;

use csv::ByteRecord;

pub trait TxClusterData: Send + Sync + 'static {
    fn block(&self) -> usize;
    fn tx_map(&self) -> &HashMap<u16, Vec<ByteRecord>>;
    fn conflict_map(&self) -> &HashMap<u16, Vec<u32>>;
}

pub struct TxCluster {
    block: usize,
    tx_map: HashMap<u16, Vec<ByteRecord>>,
    conflict_map: HashMap<u16, Vec<u32>>,
}

impl TxCluster {
    pub fn new(block: usize) -> Self {
        Self {
            block,
            tx_map: HashMap::new(),
            conflict_map: HashMap::new(),
        }
    }

    pub fn add_tx(&mut self, client_id: &u16, byte_record: &ByteRecord) {
        if self.tx_map.contains_key(client_id) {
            self.tx_map.entry(client_id.clone()).and_modify(|e| {
                e.push(byte_record.clone());
            });
            return;
        }
        let mut v = Vec::new();
        v.push(byte_record.clone());
        self.tx_map.insert(client_id.clone(), v);
    }

    pub fn add_conflict(&mut self, client_id: &u16, tx_id: &Option<u32>) {
        if let Some(tid) = tx_id {
            if self.conflict_map.contains_key(client_id) {
                self.conflict_map.entry(client_id.clone()).and_modify(|e| {
                    e.push(tid.clone());
                });
                return;
            }
            let mut v = Vec::new();
            v.push(tid.clone());
            self.conflict_map.insert(client_id.clone(), v);
        }
    }
}

impl TxClusterData for TxCluster {
    fn block(&self) -> usize {
        self.block
    }

    fn tx_map(&self) -> &HashMap<u16, Vec<ByteRecord>> {
        &self.tx_map
    }

    fn conflict_map(&self) -> &HashMap<u16, Vec<u32>> {
        &self.conflict_map
    }
}
