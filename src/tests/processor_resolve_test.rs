use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
use crate::lib::constants::ACCOUNT_DIR;
use crate::models::account::Account;
use crate::models::processor::Processor;

#[test]
fn process_resolve_base_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,9,1,10
    // deposit,9,2,11
    // deposit,9,3,12
    // withdrawal,9,4,50
    // withdrawal,9,5,11
    // dispute,9,1
    // deposit,9,6,12
    // resolve,9,1

    let client_id = 9;
    let result = Processor::new("src/tests/csv/resolve_base.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(34, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(34, 0));
    assert!(!account.locked);

    TestHelper::clean(&client_id);
}

#[test]
fn process_resolve_tx_dne_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,10,1,10
    // deposit,10,2,11
    // deposit,10,3,12
    // withdrawal,10,4,50
    // withdrawal,10,5,11
    // resolve,10,50
    // deposit,10,6,12

    let client_id = 10;
    let result = Processor::new("src/tests/csv/resolve_tx_dne.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(34, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(34, 0));
    assert!(!account.locked);

    TestHelper::clean(&client_id);
}

#[test]
fn process_resolve_dispute_on_resolved_account_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,11,1,10
    // deposit,11,2,11
    // deposit,11,3,12
    // withdrawal,11,4,50
    // withdrawal,11,5,11
    // dispute,11,1
    // deposit,11,6,12
    // resolve,11,1
    // deposit,11,7,1
    // dispute,11,1

    let client_id = 11;
    let result = Processor::new("src/tests/csv/resolve_dispute_on_resolved_account.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(35, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(35, 0));
    assert!(!account.locked);

    TestHelper::clean(&client_id);
}

#[test]
fn process_resolve_multi_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,25,1,10
    // deposit,25,2,11
    
    // type,client,tx,amount
    // dispute,25,1
    // deposit,25,3,12
    // withdrawal,25,4,50

    // type,client,tx,amount
    // withdrawal,25,5,11
    // resolve,25,1
    // deposit,25,6,12

    let client_id = 25;
    let result = Processor::new("src/tests/csv/resolve_multi_0.csv");
    assert!(result.is_ok());

    let mut p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let result = p.set_source_path("src/tests/csv/resolve_multi_1.csv");
    assert!(result.is_ok());

    let result = p.process_data(false);
    assert!(result.is_ok());

    let result = p.set_source_path("src/tests/csv/resolve_multi_2.csv");
    assert!(result.is_ok());

    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(34, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(34, 0));
    assert!(!account.locked);

    TestHelper::clean(&client_id);
}
