use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
use crate::lib::constants::ACCOUNT_DIR;
use crate::models::account::Account;
use crate::models::processor::Processor;

#[test]
fn process_chargeback_base_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,12,1,10
    // deposit,12,2,11
    // deposit,12,3,12
    // withdrawal,12,4,50
    // withdrawal,12,5,11
    // dispute,12,1
    // deposit,12,6,12
    // chargeback,12,1

    let client_id = 12;
    let result = Processor::new("src/tests/csv/chargeback_base.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(24, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(24, 0));
    assert!(account.locked);

    TestHelper::clean(&client_id);
}

#[test]
fn process_chargeback_tx_dne_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,13,1,10
    // deposit,13,2,11
    // deposit,13,3,12
    // withdrawal,13,4,50
    // withdrawal,13,5,11
    // deposit,13,6,12
    // chargeback,13,155

    let client_id = 13;
    let result = Processor::new("src/tests/csv/chargeback_tx_dne.csv");
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
fn process_chargeback_on_non_dispute_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,14,1,10
    // deposit,14,2,11
    // deposit,14,3,12
    // withdrawal,14,4,50
    // withdrawal,14,5,11
    // deposit,14,6,12
    // chargeback,14,1

    let client_id = 14;
    let result = Processor::new("src/tests/csv/chargeback_on_non_dispute.csv");
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
fn process_chargeback_on_withdraw_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,15,1,10
    // deposit,15,2,11
    // deposit,15,3,12
    // withdrawal,15,4,12
    // withdrawal,15,5,11
    // dispute,15,4,
    // deposit,15,6,12
    // chargeback,15,4

    let client_id = 15;
    let result = Processor::new("src/tests/csv/chargeback_on_withdraw.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(34, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(34, 0));
    assert!(account.locked);

    TestHelper::clean(&client_id);
}

#[test]
fn process_chargeback_on_resolve_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,16,1,10
    // deposit,16,2,11
    // deposit,16,3,12
    // withdrawal,16,4,12
    // withdrawal,16,5,11
    // dispute,16,2,
    // deposit,16,6,12
    // resolved,16,2
    // chargeback,16,2

    let client_id = 16;
    let result = Processor::new("src/tests/csv/chargeback_on_resolved.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(22, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(22, 0));
    assert!(!account.locked);

    TestHelper::clean(&client_id);
}

#[test]
fn process_chargeback_deposit_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,17,1,10
    // deposit,17,2,11
    // deposit,17,3,12
    // withdrawal,17,4,50
    // withdrawal,17,5,11
    // dispute,17,1
    // deposit,17,6,12
    // chargeback,17,1
    // deposit,17,7,50

    let client_id = 17;
    let result = Processor::new("src/tests/csv/chargeback_deposit.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(24, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(24, 0));
    assert!(account.locked);

    TestHelper::clean(&client_id);
}

#[test]
fn process_chargeback_withdraw_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,18,1,10
    // deposit,18,2,11
    // deposit,18,3,12
    // withdrawal,18,4,50
    // withdrawal,18,5,11
    // dispute,18,1
    // deposit,18,6,12
    // chargeback,18,1
    // withdrawal,18,7,10

    let client_id = 18;
    let result = Processor::new("src/tests/csv/chargeback_withdraw.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(24, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(24, 0));
    assert!(account.locked);

    TestHelper::clean(&client_id);
}

#[test]
fn process_chargeback_dispute_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,19,1,10
    // deposit,19,2,11
    // deposit,19,3,12
    // withdrawal,19,4,50
    // withdrawal,19,5,11
    // dispute,19,1
    // deposit,19,6,12
    // chargeback,19,1
    // dispute,19,1

    let client_id = 19;
    let result = Processor::new("src/tests/csv/chargeback_dispute.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(24, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(24, 0));
    assert!(account.locked);

    TestHelper::clean(&client_id);
}

#[test]
fn process_chargeback_resolve_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,20,1,10
    // deposit,20,2,11
    // deposit,20,3,12
    // withdrawal,20,4,50
    // withdrawal,20,5,11
    // dispute,20,1
    // deposit,20,6,12
    // chargeback,20,1
    // resolve,20,1

    let client_id = 20;
    let result = Processor::new("src/tests/csv/chargeback_resolve.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(24, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(24, 0));
    assert!(account.locked);

    TestHelper::clean(&client_id);
}

#[test]
fn process_chargeback_chargeback_test() {
    // --------- //
    // input csv //
    // --------- //

    let client_id = 21;
    let result = Processor::new("src/tests/csv/chargeback_chargeback.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(24, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(24, 0));
    assert!(account.locked);

    TestHelper::clean(&client_id);
}

#[test]
fn process_chargeback_multi_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,26,1,10
    // deposit,26,2,11
    
    // type,client,tx,amount
    // dispute,26,1
    // deposit,26,3,12
    // withdrawal,26,4,50

    // type,client,tx,amount
    // withdrawal,26,5,11
    // deposit,26,6,12
    // chargeback,26,1

    let client_id = 26;
    let result = Processor::new("src/tests/csv/chargeback_multi_0.csv");
    assert!(result.is_ok());

    let mut p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let result = p.set_source_path("src/tests/csv/chargeback_multi_1.csv");
    assert!(result.is_ok());

    let result = p.process_data(false);
    assert!(result.is_ok());

    let result = p.set_source_path("src/tests/csv/chargeback_multi_2.csv");
    assert!(result.is_ok());

    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(24, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(24, 0));
    assert!(account.locked);

    TestHelper::clean(&client_id);
}

