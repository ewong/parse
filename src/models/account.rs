use csv::ByteRecord;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs;

use super::tx_history::TxHistory;
use super::tx_record::TxRecordType;
use crate::lib::constants::FN_NEW;
use crate::lib::error::AppError;

const PATH: &str = "model/account";
const FN_WRITE_CSV: &str = "write_csv";

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Account {
    #[serde(rename(deserialize = "client", serialize = "client"))]
    pub client_id: u16,
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
}

impl Account {
    pub fn new(client_id: u16, account_dir: &str) -> Self {
        let user_opt = Self::load_user_from_file(client_id, account_dir);

        if user_opt.is_none() {
            return Self {
                client_id,
                available: Decimal::new(0, 0),
                held: Decimal::new(0, 0),
                total: Decimal::new(0, 0),
                locked: false,
            };
        }
        user_opt.unwrap()
    }

    fn load_user_from_file(client_id: u16, account_dir: &str) -> Option<Account> {
        let result = fs::File::open(&[account_dir, "/", &client_id.to_string(), ".csv"].join(""));
        if result.is_err() {
            return None;
        }

        let f = result.unwrap();
        let mut reader = csv::Reader::from_reader(f);
        let result = reader
            .byte_headers()
            .map_err(|e| AppError::new(PATH, FN_NEW, "01", &e.to_string()));

        if result.is_err() {
            return None;
        }

        let headers = result.unwrap().clone();
        let mut byte_record = ByteRecord::new();
        let result = reader.read_byte_record(&mut byte_record);
        if result.is_err() {
            return None;
        }

        if byte_record.len() == 0 {
            return None;
        }

        let result = byte_record.deserialize::<Account>(Some(&headers));

        if result.is_err() {
            return None;
        }

        Some(result.unwrap())
    }

    pub fn handle_tx(
        &mut self,
        tx_type: &TxRecordType,
        tx_id: &u32,
        amount: &Decimal,
        tx_history: &mut TxHistory,
    ) {
        match *tx_type {
            TxRecordType::DEPOSIT => {
                if self.locked {
                    return;
                }
                self.available += *amount;
                self.total += *amount;

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
            }
            TxRecordType::DISPUTE => {
                if self.locked {
                    return;
                }

                if let Some(tx) = &mut tx_history.get_tx(tx_id) {
                    if tx.conflict_type_id != TxRecordType::NONE {
                        return;
                    }

                    if tx.type_id == TxRecordType::DEPOSIT {
                        self.held += tx.amount;
                        self.available -= tx.amount;
                    }
                    // if the dispute is on a withdrawal we do nothing.
                    // withdrawal chargebacks are handled like debit card/atm chargebacks.
                    // debit card/atm chargebacks only revert transaction on the chargeback.

                    tx.conflict_type_id = TxRecordType::DISPUTE;
                    tx_history.set_tx(&tx.type_id, &tx.client_id, &tx.tx_id, &tx.amount);
                    // println!(
                    //     "dispute --> tx_id: {}, client {}, available: {}, held: {}, total: {}, locked: {}",
                    //     tx_id, self.client_id, self.available, self.held, self.total, self.locked
                    // );
                }
            }
            TxRecordType::RESOLVE => {
                if self.locked {
                    return;
                }

                if let Some(tx) = &mut tx_history.get_tx(tx_id) {
                    if tx.conflict_type_id != TxRecordType::DISPUTE {
                        return;
                    }

                    if tx.type_id == TxRecordType::DEPOSIT {
                        self.held -= tx.amount;
                        self.available += tx.amount;
                    }
                    // if the resolve is on a withdrawal we do nothing.
                    // withdrawal chargebacks are handled like debit card/atm chargebacks.
                    // debit card/atm chargebacks only revert transaction on the chargeback.

                    tx.conflict_type_id = TxRecordType::RESOLVE;
                    tx_history.set_tx(&tx.type_id, &tx.client_id, &tx.tx_id, &tx.amount);

                    // println!(
                    //     "resolve --> tx_id: {}, client {}, available: {}, held: {}, total: {}, locked: {}",
                    //     tx_id, self.client_id, self.available, self.held, self.total, self.locked
                    // );
                }
            }
            TxRecordType::CHARGEBACK => {
                if self.locked {
                    return;
                }

                if let Some(tx) = &mut tx_history.get_tx(tx_id) {
                    if tx.conflict_type_id != TxRecordType::DISPUTE {
                        return;
                    }

                    if tx.type_id == TxRecordType::DEPOSIT {
                        self.held -= tx.amount;
                        self.total -= tx.amount;
                    } else if tx.type_id == TxRecordType::WITHDRAW {
                        // if the chargeback is on a withdrawal reimburse the client the amount of the withdrawal.
                        self.available += tx.amount;
                        self.total += tx.amount;
                    }

                    tx.conflict_type_id = TxRecordType::CHARGEBACK;
                    tx_history.set_tx(&tx.type_id, &tx.client_id, &tx.tx_id, &tx.amount);
                    self.locked = true;

                    // println!(
                    //     "chargeback --> tx_id: {}, client {}, available: {}, held: {}, total: {}, locked: {}",
                    //     tx_id, self.client_id, self.available, self.held, self.total, self.locked
                    // );
                }
            }
            _ => {}
        }
    }

    pub fn write_to_csv(&mut self, account_dir: &str) -> Result<(), AppError> {
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
}
