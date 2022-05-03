use csv::ByteRecord;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::{collections::HashMap, fs};

use super::tx_record::TxRecordType;
use crate::lib::constants::FN_NEW;
use crate::lib::error::AppError;

const PATH: &str = "model/account";
const FN_WRITE_CSV: &str = "write_csv";

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
    pub fn new(client_id: u16, account_dir: &str) -> Result<Self, AppError> {
        let result = fs::File::open(&[account_dir, "/", &client_id.to_string(), ".csv"].join(""));
        if result.is_err() {
            return Ok(Self::default_instance(client_id));
        }

        let f = result.unwrap();
        let mut reader = csv::Reader::from_reader(f);
        let headers = reader
            .byte_headers()
            .map_err(|e| AppError::new(PATH, FN_NEW, "01", &e.to_string()))?
            .clone();

        let mut byte_record = ByteRecord::new();
        let result = reader.read_byte_record(&mut byte_record);
        if result.is_err() {
            return Ok(Self::default_instance(client_id));
        }

        if byte_record.len() == 0 {
            return Ok(Self::default_instance(client_id));
        }

        let result = byte_record.deserialize::<Account>(Some(&headers));

        if result.is_err() {
            return Ok(Self::default_instance(client_id));
        }

        Ok(result.unwrap())
    }

    fn default_instance(client_id: u16) -> Self {
        Self {
            client_id,
            available: Decimal::new(0, 0),
            held: Decimal::new(0, 0),
            total: Decimal::new(0, 0),
            locked: false,
            tx_conflict_map: None,
        }
    }

    pub fn load_tx_conflict_map(&mut self, tx_dir: &str) {
        let conflict_dir = [tx_dir, "conflicts"].join("/");
        let paths = fs::read_dir(&conflict_dir);

        if paths.is_err() {
            return;
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
            return;
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
            return;
        }

        self.tx_conflict_map = Some(map);
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

    pub fn write_to_csv(&mut self, account_dir: &str) -> Result<(), AppError> {
        fs::create_dir_all(account_dir)
            .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "00", &e.to_string()))?;
        let file_path = [account_dir, "/", &self.client_id.to_string(), ".csv"].join("");

        let mut writer = csv::Writer::from_path(&file_path)
            .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "01", &e.to_string()))?;

        writer
            .serialize(self)
            .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "03", &e.to_string()))?;

        writer
            .flush()
            .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "04", &e.to_string()))?;

        Ok(())
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
