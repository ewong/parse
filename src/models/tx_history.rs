use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::lib::constants::TRANSACTION_DIR;

use super::tx_record::{TxRecord, TxRecordType};

pub struct TxHistory {
    client_id: u16,
    db: sled::Db,
    cache: HashMap<u32, String>,
}

impl TxHistory {
    pub fn new(client_id: &u16, tx_dir: &str) -> Self {
        let path = [TRANSACTION_DIR, "/", &client_id.to_string(), "_db"].join("");
        let db = sled::open(path).unwrap();
        let cache = HashMap::new();
        Self {
            client_id: client_id.clone(),
            db,
            cache,
        }
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
        let result = self.db.contains_key(key.as_bytes());
        if result.is_ok() && result.unwrap() {
            return true;
        }

        let data = format!(
            "{},{},{},{}",
            tx_type.as_u8(),
            client_id.to_string(),
            key,
            format!("{:.5}", amount)
        );

        let result = self.db.insert(key.as_bytes(), data.as_bytes());

        if !self.cache.contains_key(&tx_id) {}

        result.is_ok()
    }

    pub fn contains_key(&self, tx_id: u32) -> bool {
        if self.cache.contains_key(&tx_id) {
            return true;
        }

        let result = self.db.contains_key(tx_id.to_string().as_bytes());
        if result.is_ok() {
            return result.unwrap();
        }

        return false;
    }

    pub fn get_tx(&self, tx_id: &u32) -> Option<TxRecord> {
        // check cache
        if let Some(string) = self.cache.get(&tx_id) {
            println!("found in cache: {}", string);
            return None;
        }

        if let Ok(row) = self.db.get(tx_id.to_string().as_bytes()) {
            if let Some(data) = row {
                if let Ok(string) = String::from_utf8(data.to_vec()) {
                    println!("found in db: {}", string);
                    //let a = string.split(",");
                }
            }
        }
        return None;
    }
}

// insert and get
// db.insert(b"yo!", b"v1");
// assert_eq!(&db.get(b"yo!").unwrap().unwrap(), b"v1");

// // Atomic compare-and-swap.
// db.compare_and_swap(
//     b"yo!",      // key
//     Some(b"v1"), // old value, None for not present
//     Some(b"v2"), // new value, None for delete
// )
// .unwrap();

// // Iterates over key-value pairs, starting at the given key.

// use crate::lib::constants::TRANSACTION_DIR;
// let scan_key: &[u8] = b"a non-present key before yo!";
// let mut iter = db.range(scan_key..);
// assert_eq!(&iter.next().unwrap().unwrap().0, b"yo!");
// assert_eq!(iter.next(), None);

// db.remove(b"yo!");
// assert_eq!(db.get(b"yo!"), Ok(None));

// let other_tree: sled::Tree = db.open_tree(b"cool db facts").unwrap();
// other_tree.insert(
//     b"k1",
//     &b"a Db acts like a Tree due to implementing Deref<Target = Tree>"[..]
// ).unwrap();
