use rust_decimal::prelude::*;
use std::io::Read;
use std::{collections::HashMap, fs};

use super::tx_record::TxRecordType;

#[derive(Debug, Clone)]
pub struct TxConflictState {
    pub state: TxRecordType,
    pub tx_type: TxRecordType,
    pub amount: Decimal,
}

#[derive(Debug, Clone)]
pub struct TxConflict {
    pub map: HashMap<u32, TxConflictState>,
}

impl TxConflict {
    pub fn new(tx_dir: &str) -> Option<Self> {
        let conflict_dir = [tx_dir, "conflicts"].join("/");
        let paths = fs::read_dir(&conflict_dir);

        if paths.is_err() {
            return None;
        }

        let conflict_paths: Vec<String> = paths
            .unwrap()
            .map(|e| {
                if e.is_err() {
                    return "".to_string();
                }

                let path = e.unwrap();
                if path.path().file_name().is_none() {
                    return "".to_string();
                }

                if !path.path().is_file() {
                    return "".to_string();
                }

                path.path().display().to_string()
            })
            .filter(|s| s.len() > 0)
            .collect();

        if conflict_paths.len() == 0 {
            return None;
        }

        let mut map: HashMap<u32, TxConflictState> = HashMap::new();
        for path in conflict_paths {
            let result = fs::File::open(&path);
            if result.is_err() {
                continue;
            }

            let mut f = result.unwrap();
            let mut s = String::new();
            let result = f.read_to_string(&mut s);

            if result.is_ok() {
                let list = s.replace("{", "").replace("}", "").replace(" ", "");
                for x in list.split(",") {
                    let tx_id = x.to_string().parse::<u32>().unwrap();
                    if !map.contains_key(&tx_id) {
                        map.insert(
                            tx_id,
                            TxConflictState {
                                state: TxRecordType::NONE,
                                tx_type: TxRecordType::NONE,
                                amount: Decimal::new(0, 0),
                            },
                        );
                    }
                }
            }
        }

        if map.len() == 0 {
            return None;
        }

        Some(Self { map })
    }

    pub fn update_conflicts(&mut self, tx_id: &u32, tx_type: &TxRecordType, amount: &Decimal) {
        if tx_type.conflict_type() {
            return;
        }

        if !self.map.contains_key(&tx_id) {
            return;
        }

        if let Some(conflict) = self.map.get(&tx_id) {
            if conflict.tx_type != TxRecordType::NONE {
                return;
            }
        }

        self.map.entry(tx_id.clone()).and_modify(|e| {
            e.tx_type = tx_type.clone();
            e.amount = *amount;
        });

        // println!(
        //     "client conflict updated --> type: {}, client: {}, tx: {}, amount: {:?}",
        //     tx_type.name(),
        //     self.client_id,
        //     tx_id,
        //     amount
        // );
    }
}
