use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::lib::constants::TRANSACTION_DIR;

use super::tx_record::{TxConflict, TxRecordType, TxRow};

pub struct TxHistory {
    client_id: u16,
    db: sled::Db,
    cache: HashMap<u32, TxRow>,
    conflict_cache: HashMap<String, TxConflict>,
}

impl TxHistory {
    pub fn new(client_id: &u16) -> Self {
        let path: String;
        if cfg!(test) {
            use uuid::Uuid;
            let id = Uuid::new_v4();
            path = [
                TRANSACTION_DIR,
                "/",
                &client_id.to_string(),
                "_db_",
                &id.to_string(),
            ]
            .join("");
        } else {
            path = [TRANSACTION_DIR, "/", &client_id.to_string(), "_db"].join("");
        }

        let db = sled::open(path).unwrap();
        Self {
            client_id: client_id.clone(),
            db,
            cache: HashMap::new(),
            conflict_cache: HashMap::new(),
        }
    }

    // pub fn contains_tx_key(&self, tx_id: u32) -> bool {
    //     if self.cache.contains_key(&tx_id) {
    //         return true;
    //     }

    //     let result = self.db.contains_key(tx_id.to_string().as_bytes());
    //     if result.is_ok() {
    //         return result.unwrap();
    //     }

    //     return false;
    // }

    pub fn get_tx(&mut self, tx_id: &u32) -> Option<TxRow> {
        // check cache
        if let Some(row) = self.cache.get(tx_id) {
            return Some(row.clone());
        }

        if let Ok(row) = self.db.get(tx_id.to_string().as_bytes()) {
            if let Some(data) = row {
                if let Ok(string) = String::from_utf8(data.to_vec()) {
                    let row = TxRow::new_from_string(&string);
                    self.cache.insert(tx_id.clone(), row.clone());
                    return Some(row);
                }
            }
        }
        return None;
    }

    pub fn set_tx(
        &mut self,
        tx_type: &TxRecordType,
        client_id: &u16,
        tx_id: &u32,
        amount: &Decimal,
    ) -> bool {
        if tx_type.conflict_type() {
            return false;
        }

        if *amount == Decimal::new(0, 0) {
            return false;
        }

        if *client_id != self.client_id {
            return false;
        }

        let key = tx_id.to_string();
        let data = TxRow::to_string(tx_type, client_id, tx_id, amount);
        let result = self.db.insert(key.as_bytes(), data.as_bytes());

        result.is_ok()
    }

    pub fn contains_conflict_key(&self, key: &str) -> bool {
        if self.conflict_cache.contains_key(key) {
            return true;
        }

        let result = self.db.contains_key(key.as_bytes());
        if result.is_ok() {
            return result.unwrap();
        }

        return false;
    }

    pub fn get_conflict(&mut self, tx_id: &u32) -> Option<TxConflict> {
        let key = TxConflict::key(&tx_id);
        if let Some(row) = self.conflict_cache.get(&key) {
            return Some(row.clone());
        }

        if let Ok(row) = self.db.get(key.as_bytes()) {
            if let Some(data) = row {
                if let Ok(string) = String::from_utf8(data.to_vec()) {
                    let row = TxConflict::new_from_string(&string);
                    self.conflict_cache.insert(key, row.clone());
                    return Some(row);
                }
            }
        }
        return None;
    }

    pub fn set_conflict(
        &mut self,
        tx_id: &u32,
        type_id: &TxRecordType,
        state_id: &TxRecordType,
        amount: &Decimal,
    ) -> bool {
        let key = TxConflict::key(&tx_id);
        let data = TxConflict::to_string(tx_id, type_id, state_id, amount);
        let result = self.db.insert(key.as_bytes(), data.as_bytes());
        result.is_ok()
    }
}
