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

    pub fn handle_tx(&mut self, tx_type: &TxRecordType, tx_id: &u32, amount: &f64) {
        match *tx_type {
            TxRecordType::DEPOSIT => {
                if self.locked {
                    return;
                }
                self.available += *amount;
                self.total += *amount;
                self.update_conflicts(tx_type, tx_id, amount);
            }
            TxRecordType::WITHDRAW => {
                if self.locked {
                    return;
                }
                if self.available >= *amount {
                    self.available -= *amount;
                    self.total -= *amount;
                }
                self.update_conflicts(tx_type, tx_id, amount);
            }
            TxRecordType::DISPUTE => {
                if self.locked || self.under_dispute() {
                    return;
                }
            }
            TxRecordType::RESOLVE => {
                if self.locked || !self.under_dispute() {
                    return;
                }
            }
            TxRecordType::CHARGEBACK => {
                if self.locked || self.under_dispute() {
                    return;
                }
            }
            _ => {}
        }
    }

    // change this to check the tx conflict map state for the tx_id!
    fn under_dispute(&self) -> bool {
        self.held > 0.0
    }

    fn update_conflicts(&mut self, tx_type: &TxRecordType, tx_id: &u32, amount: &f64) {
        if let Some(map) = &mut self.tx_conflict_map {
            if tx_type.conflict_type() {
                return;
            }

            if !map.contains_key(&tx_id) {
                return;
            }

            if let Some(value) = map.get(&tx_id) {
                if *value > 0.0 {
                    return;
                }
            }

            map.entry(tx_id.clone()).and_modify(|e| {
                // note: need to apply the opposite sign when reversing the transaction
                if *tx_type == TxRecordType::DEPOSIT {
                    *e = *amount;
                } else {
                    *e = -(*amount);
                }
            });

            println!(
                "client conflict updated --> type: {}, client: {}, tx: {}, amount: {:?}",
                tx_type.name(),
                self.client_id,
                tx_id,
                amount
            );
        }
    }
}
