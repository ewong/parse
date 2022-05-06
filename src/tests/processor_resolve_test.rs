use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
use crate::lib::constants::{ACCOUNT_DIR, SUMMARY_DIR};
use crate::models::account::Account;
use crate::models::processor::Processor;

/*
    resolve
    - tx doesn't exist
    - dispute on a resolved account
    - resolve on a clean account => ignore
    - resolve on a disputed account => process
    - resolve on a tx that has been resolved => ignore

    chargeback
    - tx doesn't exist
    - deposit on a locked account
    - withdraw on a locked account
    - dispute on a frozen account
    - chargeback on a frozen account
*/

#[test]
fn process_resolve_base_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,9,1,10
    // deposit,9,2,11
    // deposit,9,3,12
    // withdraw,9,4,50
    // withdraw,9,5,11
    // dispute,9,1
    // deposit,9,6,12
    // resolve,9,1

    let client_id = 9;
    let result = Processor::new("src/tests/csv/resolve_base.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    // check balance
    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(34, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(34, 0));
    assert!(!account.locked);

    TestHelper::remove_dir(&[SUMMARY_DIR, &client_id.to_string()].join("/"));
    TestHelper::remove_file(&[ACCOUNT_DIR, "/", &client_id.to_string(), ".csv"].join(""));
}
