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

    let client_id = 5;
    let result = Processor::new("src/tests/csv/dispute_base.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    // check balance
    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(1, 0));
    assert_eq!(account.held, Decimal::new(1, 0));
    assert_eq!(account.total, Decimal::new(2, 0));
    assert!(!account.locked);

    TestHelper::clean(&client_id);
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

    let client_id = 6;
    let result = Processor::new("src/tests/csv/dispute_tx_dne.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    // check balance
    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(14, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(14, 0));
    assert!(!account.locked);

    TestHelper::clean(&client_id);
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

    let client_id = 8;
    let result = Processor::new("src/tests/csv/dispute_existing_dispute.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    // check balance
    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(12, 0));
    assert_eq!(account.held, Decimal::new(5, 0));
    assert_eq!(account.total, Decimal::new(17, 0));
    assert!(!account.locked);

    TestHelper::clean(&client_id);
}

#[test]
fn process_dispute_multi_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,24,1,1
    // deposit,24,2,1
    // deposit,24,3,1

    // type,client,tx,amount
    // withdraw,24,4,5
    // withdraw,24,5,1
    // dispute,24,1

    let client_id = 24;
    let result = Processor::new("src/tests/csv/dispute_multi_0.csv");
    assert!(result.is_ok());

    let mut p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let result = p.set_source_path("src/tests/csv/dispute_multi_1.csv");
    assert!(result.is_ok());

    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(1, 0));
    assert_eq!(account.held, Decimal::new(1, 0));
    assert_eq!(account.total, Decimal::new(2, 0));
    assert!(!account.locked);

    TestHelper::clean(&client_id);
}
