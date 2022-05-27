use super::tx_record::TxRow;
use std::collections::HashMap;

pub struct TxCluster {
    pub tx_row_map: HashMap<u16, Vec<TxRow>>,
}

impl TxCluster {
    pub fn new() -> Self {
        Self {
            tx_row_map: HashMap::new(),
        }
    }

    pub fn add(&mut self, tx_row: TxRow) {
        if self.tx_row_map.contains_key(&tx_row.client_id) {
            self.tx_row_map
                .entry(tx_row.client_id.clone())
                .and_modify(|e| {
                    e.push(tx_row);
                });
        } else {
            let client_id = tx_row.client_id.clone();
            let mut v = Vec::new();
            v.push(tx_row);
            self.tx_row_map.insert(client_id, v);
        }
    }
}