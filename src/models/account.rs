use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::{collections::HashMap, fs};

use super::tx_record::TxRecordType;
use crate::lib::error::AppError;

#[derive(Debug, Clone)]
struct TxConflict {
    state: TxRecordType,
    tx_type: TxRecordType,
    amount: Decimal,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Account {
    #[serde(rename(deserialize = "client", serialize = "client"))]
    client_id: u16,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,

    #[serde(skip)]
    tx_conflict_map: Option<HashMap<u32, TxConflict>>,
}

impl Account {
    // todo: check to see the client has an existing account
    pub fn new(client_id: u16, tx_dir: &str) -> Result<Self, AppError> {
        let tx_conflict_map = Self::load_tx_id_conflict_map(tx_dir);
        Ok(Self {
            client_id,
            available: Decimal::new(0, 0),
            held: Decimal::new(0, 0),
            total: Decimal::new(0, 0),
            locked: false,
            tx_conflict_map,
        })
    }

    pub fn show(&self) {
        println!(
            "completed --> client {}, available: {}, held: {}, total: {}, locked: {}",
            self.client_id, self.available, self.held, self.total, self.locked
        );
    }

    pub fn handle_tx(&mut self, tx_type: &TxRecordType, tx_id: &u32, amount: &Decimal) {
        match *tx_type {
            TxRecordType::DEPOSIT => {
                if self.locked {
                    return;
                }
                self.available += *amount;
                self.total += *amount;
                self.update_conflicts(tx_id, tx_type, amount);

                // println!(
                //     "deposit --> client {}, available: {}, held: {}, total: {}, locked: {}",
                //     self.client_id, self.available, self.held, self.total, self.locked
                // );
            }
            TxRecordType::WITHDRAW => {
                if self.locked {
                    return;
                }
                if self.available >= *amount {
                    self.available -= *amount;
                    self.total -= *amount;
                    // println!(
                    //     "withdraw --> client {}, available: {}, held: {}, total: {}, locked: {}",
                    //     self.client_id, self.available, self.held, self.total, self.locked
                    // );
                }
                self.update_conflicts(tx_id, tx_type, amount);
            }
            TxRecordType::DISPUTE => {
                if self.locked {
                    return;
                }

                if let Some(map) = &mut self.tx_conflict_map {
                    if map.contains_key(tx_id) {
                        map.entry(tx_id.clone()).and_modify(|e| {
                            if e.state == TxRecordType::NONE {
                                e.state = TxRecordType::DISPUTE;
                                self.held += e.amount;
                                self.available -= e.amount;
                                println!(
                                    "dispute --> tx_id: {}, client {}, available: {}, held: {}, total: {}, locked: {}",
                                    tx_id, self.client_id, self.available, self.held, self.total, self.locked
                                );
                            }
                        });
                    }
                }
            }
            TxRecordType::RESOLVE => {
                if self.locked {
                    return;
                }

                if let Some(map) = &mut self.tx_conflict_map {
                    if map.contains_key(tx_id) {
                        map.entry(tx_id.clone()).and_modify(|e| {
                            if e.state == TxRecordType::DISPUTE {
                                self.held -= e.amount;
                                self.available += e.amount;
                                e.state = TxRecordType::NONE;
                                println!(
                                    "resolve --> tx_id: {}, client {}, available: {}, held: {}, total: {}, locked: {}",
                                    tx_id, self.client_id, self.available, self.held, self.total, self.locked
                                );
                            }
                        });
                    }
                }
            }
            TxRecordType::CHARGEBACK => {
                if self.locked {
                    return;
                }

                if let Some(map) = &mut self.tx_conflict_map {
                    if map.contains_key(tx_id) {
                        map.entry(tx_id.clone()).and_modify(|e| {
                            if e.state == TxRecordType::DISPUTE {
                                self.held -= e.amount;
                                self.total -= e.amount;
                                self.locked = true;
                                e.state = TxRecordType::CHARGEBACK;
                                // println!(
                                //     "chargeback --> tx_id: {}, client {}, available: {}, held: {}, total: {}, locked: {}",
                                //     tx_id, self.client_id, self.available, self.held, self.total, self.locked
                                // );
                            }
                        });
                    }
                }
            }
            _ => {}
        }
    }

    fn load_tx_id_conflict_map(tx_dir: &str) -> Option<HashMap<u32, TxConflict>> {
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

        let mut map: HashMap<u32, TxConflict> = HashMap::new();
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
                            TxConflict {
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

        Some(map)
    }

    fn update_conflicts(&mut self, tx_id: &u32, tx_type: &TxRecordType, amount: &Decimal) {
        if let Some(map) = &mut self.tx_conflict_map {
            if tx_type.conflict_type() {
                return;
            }

            if !map.contains_key(&tx_id) {
                return;
            }

            if let Some(conflict) = map.get(&tx_id) {
                if conflict.tx_type != TxRecordType::NONE {
                    return;
                }
            }

            map.entry(tx_id.clone()).and_modify(|e| {
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
}
