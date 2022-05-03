use csv::ByteRecord;
use std::collections::{HashMap, HashSet};
use std::fs;

use crate::lib::error::AppError;
use super::tx_record::TxRecordType;

const PATH: &str = "model/tx_cluster";

pub trait TxClusterData: Send + Sync + 'static {
    fn block(&self) -> usize;
    fn tx_map(&self) -> &HashMap<u16, Vec<ByteRecord>>;
    fn tx_deposit_withdraw_map(&self) -> &HashMap<u16, HashMap<u32, usize>>;
    fn tx_conflict_map(&self) -> &HashMap<u16, HashSet<u32>>;
}

pub struct TxCluster {
    block: usize,
    tx_map: HashMap<u16, Vec<ByteRecord>>,
    tx_deposit_withdraw_map: HashMap<u16, HashMap<u32, usize>>,
    tx_conflict_map: HashMap<u16, HashSet<u32>>,
}

impl TxCluster {
    pub fn new(block: usize) -> Self {
        Self {
            block,
            tx_map: HashMap::new(),
            tx_deposit_withdraw_map: HashMap::new(),
            tx_conflict_map: HashMap::new(),
        }
    }

    pub fn add_tx(
        &mut self,
        tx_type: &TxRecordType,
        client_id: &u16,
        tx_id: &u32,
        byte_record: &ByteRecord,
    ) {
        if self.tx_map.contains_key(client_id) {
            self.tx_map.entry(client_id.clone()).and_modify(|e| {
                if tx_type.conflict_type() {
                    if self.tx_deposit_withdraw_map.contains_key(client_id) {
                        self.tx_deposit_withdraw_map
                            .entry(client_id.clone())
                            .and_modify(|e| {
                                e.insert(tx_id.clone(), e.len());
                            });
                    } else {
                        let mut map = HashMap::new();
                        map.insert(tx_id.clone(), e.len());
                        self.tx_deposit_withdraw_map.insert(client_id.clone(), map);
                    }
                }
                e.push(byte_record.clone());
            });
        } else {
            let mut v = Vec::new();
            v.push(byte_record.clone());
            self.tx_map.insert(client_id.clone(), v);
            if tx_type.conflict_type() {
                let mut map = HashMap::new();
                map.insert(tx_id.clone(), 0);
                self.tx_deposit_withdraw_map.insert(client_id.clone(), map);
            }
        }
    }

    pub fn add_conflict(&mut self, tx_type: &TxRecordType, client_id: &u16, tx_id: &u32) {
        if !tx_type.conflict_type() {
            return;
        }

        if self.tx_conflict_map.contains_key(client_id) {
            self.tx_conflict_map
                .entry(client_id.clone())
                .and_modify(|e| {
                    e.insert(tx_id.clone());
                });
            return;
        }
        let mut set = HashSet::new();
        set.insert(tx_id.clone());
        self.tx_conflict_map.insert(client_id.clone(), set);
    }
}

impl TxClusterData for TxCluster {
    fn block(&self) -> usize {
        self.block
    }

    fn tx_map(&self) -> &HashMap<u16, Vec<ByteRecord>> {
        &self.tx_map
    }

    fn tx_deposit_withdraw_map(&self) -> &HashMap<u16, HashMap<u32, usize>> {
        &self.tx_deposit_withdraw_map
    }

    fn tx_conflict_map(&self) -> &HashMap<u16, HashSet<u32>> {
        &self.tx_conflict_map
    }
}

pub trait TxClusterPathData: Send + Sync + 'static {
    fn client_id(&self) -> u16;
    fn dir_path(&self) -> &str;
}

pub struct TxClusterPath {
    client_id: u16,
    dir_path: String,
}

impl TxClusterPath {
    pub fn paths(cluster_dir: &str) -> Result<Vec<TxClusterPath>, AppError> {
        let paths = fs::read_dir(cluster_dir)
            .map_err(|e| AppError::new(PATH, "paths", "00", &e.to_string()))?;

        let paths: Vec<TxClusterPath> = paths
            .map(|e| {
                if let Ok(path) = e {
                    if path.path().is_dir() {
                        let dir_path = path.path().display().to_string();
                        let pos = dir_path.rfind("/").unwrap();
                        let client_id: u16 = dir_path[(pos + 1)..].parse::<u16>().unwrap();
                        return TxClusterPath {
                            client_id,
                            dir_path,
                        };
                    }
                }
                TxClusterPath {
                    client_id: 0,
                    dir_path: "".to_string(),
                }
            })
            .filter(|s| s.dir_path.len() > 0)
            .collect();

        Ok(paths)
    }
}

impl TxClusterPathData for TxClusterPath {
    fn client_id(&self) -> u16 {
        self.client_id
    }

    fn dir_path(&self) -> &str {
        &self.dir_path
    }
}
