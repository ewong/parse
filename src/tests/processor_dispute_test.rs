use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
use crate::lib::constants::{ACCOUNT_DIR, SUMMARY_DIR};
use crate::models::account::Account;
use crate::models::processor::Processor;

// dispute
// - tx exists
// - tx doesn't exist
// - dispute on a disputed account
// - dispute on a resolved account that

#[test]
fn process_dispute_base_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,5,1,1
    // deposit,5,2,1
    // deposit,5,3,1
    // withdraw,5,4,5
    // withdraw,5,5,1
    // dispute,5,1

    let result = Processor::new("src/tests/csv/dispute_base.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    // check balance
    let account = Account::new(5, ACCOUNT_DIR);
    assert_eq!(account.client_id, 5);
    assert_eq!(account.available, Decimal::new(1, 0));
    assert_eq!(account.held, Decimal::new(1, 0));
    assert_eq!(account.total, Decimal::new(2, 0));
    assert!(!account.locked);

    TestHelper::remove_dir(&[SUMMARY_DIR, "5"].join("/"));
    TestHelper::remove_file(&[ACCOUNT_DIR, "5.csv"].join("/"));
}

#[test]
fn process_dispute_tx_dne_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // dispute,6,1
    // deposit,6,2,5
    // deposit,6,3,10
    // withdraw,6,4,1
    // dispute,6,50

    let result = Processor::new("src/tests/csv/dispute_tx_dne.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    // check balance
    let account = Account::new(6, ACCOUNT_DIR);
    assert_eq!(account.client_id, 6);
    assert_eq!(account.available, Decimal::new(14, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(14, 0));
    assert!(!account.locked);

    TestHelper::remove_dir(&[SUMMARY_DIR, "6"].join("/"));
    TestHelper::remove_file(&[ACCOUNT_DIR, "6.csv"].join("/"));
}

/*
    resolve
    - tx doesn't exist
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
