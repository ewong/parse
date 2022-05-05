use std::fs;

use csv::ByteRecord;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};

use super::tx_history::TxHistory;
use super::tx_reader::TxReader;
use super::tx_record::TxRecordType;
use super::tx_writer::TxWriter;
use crate::lib::constants::ACCOUNT_DIR;
use crate::lib::error::AppError;

const PATH: &str = "model/account";

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
    pub fn new(client_id: u16, summary_dir: &str) -> Self {
        let mut user_opt = Self::load_from_file(client_id, summary_dir);

        if user_opt.is_none() {
            user_opt = Self::load_from_file(client_id, ACCOUNT_DIR);
        }

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

    fn load_from_file(client_id: u16, account_dir: &str) -> Option<Account> {
        let file_path = &[account_dir, "/", &client_id.to_string(), ".csv"].join("");
        let result = TxReader::new_reader(&file_path);
        if result.is_err() {
            return None;
        }

        let mut reader = result.unwrap();
        let mut byte_record = ByteRecord::new();
        let result = reader.read_byte_record(&mut byte_record);
        if result.is_err() {
            return None;
        }

        if byte_record.len() == 0 {
            return None;
        }

        let result = byte_record.deserialize::<Account>(None);

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

    pub fn write_to_csv(&self, summary_dir: &str) -> Result<(), AppError> {
        let available_str = format!("{:.4}", self.available);
        let held_str = format!("{:.4}", self.held);
        let total_str = format!("{:.4}", self.total);

        let byte_record = ByteRecord::from(
            &[
                &self.client_id.to_string(),
                &available_str,
                &held_str,
                &total_str,
                &self.locked.to_string(),
            ][..],
        );

        let mut tx_writer = TxWriter::new(summary_dir, &self.client_id.to_string())?;
        tx_writer.write_records(&vec![byte_record])?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct AccountPath {
    pub update_file: bool,
    pub file_path: String,
    pub file_name: String,
}

impl AccountPath {
    pub fn paths(update_file: bool, dir: &str) -> Result<Vec<Vec<AccountPath>>, AppError> {
        let mut row = 0;
        let mut paths: Vec<Vec<AccountPath>> = Vec::new();

        let p =
            fs::read_dir(dir).map_err(|e| AppError::new(PATH, "paths", "00", &e.to_string()))?;

        let mut v = Vec::new();
        for e in p {
            if e.is_err() {
                continue;
            }

            let path = e.unwrap();
            if path.path().is_dir() {
                continue;
            }

            let file_path = path.path().display().to_string();
            let file_name = path
                .path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();

            v.push(AccountPath {
                update_file,
                file_path,
                file_name,
            });

            row += 1;
            if row == 1000 {
                row = 0;
                paths.push(v);
                v = Vec::new();
            }
        }

        if v.len() > 0 {
            paths.push(v);
        }

        Ok(paths)
    }
}
