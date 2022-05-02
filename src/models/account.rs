use std::collections::HashMap;

// use csv::{ByteRecord, Reader, Writer};
use serde::{Deserialize, Serialize};

use super::tx_record::TxRecordType;
// use std::collections::HashSet;
// use std::fs::{self, File};
// use std::io::Write;
// use std::str;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Account {
    #[serde(rename(deserialize = "client", serialize = "client"))]
    client_id: u16,
    available: f64,
    held: f64,
    total: f64,
    locked: bool,

    #[serde(skip)]
    tx_conflict_map: Option<HashMap<u32, f64>>,
}

impl Account {
    // todo: check to see the client has an existing account
    pub fn new(client_id: u16, tx_conflict_map: Option<HashMap<u32, f64>>) -> Self {
        Self {
            client_id,
            available: 0.0,
            held: 0.0,
            total: 0.0,
            locked: false,
            tx_conflict_map,
        }
    }

    pub fn handle_tx(&mut self, tx_type: &TxRecordType, amount: &f64) {
        match *tx_type {
            TxRecordType::DEPOSIT => {
                self.deposit(amount);
            }
            TxRecordType::WITHDRAW => {
                self.widthdraw(amount);
            }
            TxRecordType::DISPUTE => {}
            TxRecordType::RESOLVE => {}
            TxRecordType::CHARGEBACK => {}
            _ => {}
        }

        // if let Some(map) = &mut conflict_map {
        //     if map.contains_key(tx_reader.tx_record_tx())
        //         && !tx_reader.tx_record_type().conflict_type()
        //     {
        //         let amount = tx_reader.tx_record_amount().unwrap();
        //         map.entry(tx_reader.tx_record_tx().clone()).and_modify(|e| {
        //             *e = amount;
        //         });
        //         println!(
        //             "client conflict match --> type: {}, client: {}, tx: {}, amount: {:?}",
        //             tx_reader.tx_record_type().name(),
        //             tx_reader.tx_record_client(),
        //             tx_reader.tx_record_tx(),
        //             tx_reader.tx_record_amount()
        //         );
        //     }
        // }
    }

    fn under_dispute(&self) -> bool {
        self.held > 0.0
    }

    fn frozen(&self) -> bool {
        self.locked
    }

    fn deposit(&mut self, amount: &f64) {
        if self.locked {
            return;
        }
        // account.available += tx_reader.tx_record_amount().unwrap();
        // account.total += tx_reader.tx_record_amount().unwrap();
        // if map.contains(tx_reader.tx_record_tx()
    }

    fn widthdraw(&mut self, amount: &f64) {
        // if account.available >= tx_reader.tx_record_amount().unwrap() {
        //     account.available -= tx_reader.tx_record_amount().unwrap();
        // }
    }
}
