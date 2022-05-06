use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
use crate::lib::constants::ACCOUNT_DIR;
use crate::models::account::Account;
use crate::models::processor::Processor;

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

    TestHelper::remove_file(&[ACCOUNT_DIR, "6.csv"].join("/"));
}

#[test]
fn process_dispute_existing_dispute_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,8,1,5
    // deposit,8,2,10
    // withdraw,8,3,1
    // dispute,8,1
    // deposit,8,4,3
    // dispute,8,1
    // dispute,8,1

    let result = Processor::new("src/tests/csv/dispute_existing_dispute.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    // check balance
    let account = Account::new(8, ACCOUNT_DIR);
    assert_eq!(account.client_id, 8);
    assert_eq!(account.available, Decimal::new(12, 0));
    assert_eq!(account.held, Decimal::new(5, 0));
    assert_eq!(account.total, Decimal::new(17, 0));
    assert!(!account.locked);

    TestHelper::remove_file(&[ACCOUNT_DIR, "8.csv"].join("/"));
}
