use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::lib::constants::TRANSACTION_DIR;

use super::tx_record::{TxRecordType, TxRow};

pub struct TxHistory {
    client_id: u16,
    db: sled::Db,
    cache: HashMap<u32, TxRow>,
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
        let cache = HashMap::new();
        Self {
            client_id: client_id.clone(),
            db,
            cache,
        }
    }

    // pub fn contains_key(&self, tx_id: u32) -> bool {
    //     if self.cache.contains_key(&tx_id) {
    //         return true;
    //     }

    //     let result = self.db.contains_key(tx_id.to_string().as_bytes());
    //     if result.is_ok() {
    //         return result.unwrap();
    //     }

    //     return false;
    // }

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

        println!("tx_id {} updated", tx_id);
        result.is_ok()
    }

    pub fn get_tx(&mut self, tx_id: &u32) -> Option<TxRow> {
        // check cache
        if let Some(row) = self.cache.get(&tx_id) {
            println!("found in cache: {:?}", row);
            return Some(row.clone());
        }

        if let Ok(row) = self.db.get(tx_id.to_string().as_bytes()) {
            if let Some(data) = row {
                if let Ok(string) = String::from_utf8(data.to_vec()) {
                    let row = TxRow::new_from_string(&string);
                    println!("found in db: {:?}", row);
                    self.cache.insert(tx_id.clone(), row.clone());
                    return Some(row);
                }
            }
        }
        return None;
    }
}
