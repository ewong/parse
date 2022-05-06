use std::fs;

use crate::lib::constants::{ACCOUNT_DIR, TRANSACTION_DIR};

#[allow(dead_code)]
pub struct TestHelper;

impl TestHelper {
    #[allow(dead_code)]
    pub fn clean(client_id: &u16) {
        let account = [ACCOUNT_DIR, "/", &client_id.to_string(), ".csv"].join("");
        let _ = fs::remove_file(account);

        let db = [TRANSACTION_DIR, "/", &client_id.to_string(), "_db"].join("");
        let _ = fs::remove_dir_all(db);
    }
}
