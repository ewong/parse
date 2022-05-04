use rust_decimal::prelude::*;
use std::io::Read;
use std::{collections::HashMap, fs};

use crate::lib::constants::ACCOUNT_DIR;

use super::tx_record::TxRecordType;
use super::tx_reader::TxReader;

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
    pub fn new(client_id: &u16, tx_dir: &str) -> Self {
        let mut s = Self {
            map: HashMap::new(),
        };
        s.load_txt_map(tx_dir);
        s.load_csv_map(client_id, tx_dir);
        s
    }

    fn load_txt_map(&mut self, tx_dir: &str) {
        let paths = Self::conflict_paths(tx_dir, "txt");
        if paths.is_none() {
            return;
        }

        for path in paths.unwrap() {
            let result = fs::File::open(&path);
            if result.is_err() {
                continue;
            }

            let mut f = result.unwrap();
            let mut s = String::new();
            let result = f.read_to_string(&mut s);

            if result.is_ok() {
                let list = s.replace("[", "").replace("]", "").replace(" ", "");
                for x in list.split(",") {
                    let tx_id = x.to_string().parse::<u32>().unwrap();
                    if !self.map.contains_key(&tx_id) {
                        self.map.insert(
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
    }

    fn load_csv_map(&mut self, client_id: &u16, tx_dir: &str) {
        let opt = Self::conflict_paths(tx_dir, "csv");
        if opt.is_none() {
            return;
        }

        let paths = opt.unwrap();
        let result = TxReader::new(&paths.get(0).unwrap());
        if result.is_err() {
            return;
        }
        let mut tx_reader = result.unwrap();

        // first load all txt
        for (tx_id, conflict) in &mut self.map {
            let file_path = &[
                ACCOUNT_DIR,
                "/",
                &client_id.to_string(),
                "/conflicts/",
                &tx_id.to_string(),
                ".csv",
            ]
            .join("");
            let result = tx_reader.set_reader(&file_path);
            if result.is_err() {
                continue;
            }
            if tx_reader.next_record() {
                conflict.tx_type = tx_reader.tx_record_type().clone();
                conflict.amount = tx_reader.tx_record_amount().clone();
            }
        }

        // then load csv
        let mut initial_loop = true;
        for path in paths {
            if initial_loop {
                let result = tx_reader.set_reader(&path);
                if result.is_err() {
                    return;
                }
                initial_loop = false;
            }

            while tx_reader.next_record() {
                if let Some(_e) = tx_reader.error() {
                    return;
                }
                if !self.map.contains_key(tx_reader.tx_record_tx()) {
                    self.map.insert(
                        tx_reader.tx_record_tx().clone(),
                        TxConflictState {
                            state: TxRecordType::NONE,
                            tx_type: tx_reader.tx_record_type().clone(),
                            amount: tx_reader.tx_record_amount().clone(),
                        },
                    );
                }
            }
        }
    }

    fn conflict_paths(tx_dir: &str, ext: &str) -> Option<Vec<String>> {
        let conflict_txt_dir = [tx_dir, ext].join("/");
        let paths = fs::read_dir(&conflict_txt_dir);

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

        Some(conflict_paths)
    }

    // pub fn update_conflicts(&mut self, tx_id: &u32, tx_type: &TxRecordType, amount: &Decimal) {
    //     if tx_type.conflict_type() {
    //         return;
    //     }

    //     if !self.map.contains_key(&tx_id) {
    //         return;
    //     }

    //     if let Some(conflict) = self.map.get(&tx_id) {
    //         if conflict.tx_type != TxRecordType::NONE {
    //             return;
    //         }
    //     }

    //     self.map.entry(tx_id.clone()).and_modify(|e| {
    //         e.tx_type = tx_type.clone();
    //         e.amount = *amount;
    //     });

    //     // println!(
    //     //     "client conflict updated --> type: {}, client: {}, tx: {}, amount: {:?}",
    //     //     tx_type.name(),
    //     //     self.client_id,
    //     //     tx_id,
    //     //     amount
    //     // );
    // }
}
