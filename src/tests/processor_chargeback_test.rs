use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
use crate::lib::constants::ACCOUNT_DIR;
use crate::models::account::Account;
use crate::models::processor::Processor;

/*
    chargeback
    x - base case - chargeback on a disputed account
    x - tx doesn't exist
    x - chargeback on a non-disputed transaction
    x - chargeback on a withdraw
    - chargeback on a resolved account
    - deposit on a locked account
    - withdraw on a locked account
    - dispute on a frozen account
    - resolve on a frozen account
    - chargeback on a frozen account
*/

#[test]
fn process_chargeback_base_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,12,1,10
    // deposit,12,2,11
    // deposit,12,3,12
    // withdraw,12,4,50
    // withdraw,12,5,11
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

    TestHelper::remove_file(&[ACCOUNT_DIR, "/", &client_id.to_string(), ".csv"].join(""));
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
    // withdraw,13,4,50
    // withdraw,13,5,11
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

    TestHelper::remove_file(&[ACCOUNT_DIR, "/", &client_id.to_string(), ".csv"].join(""));
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
    // withdraw,14,4,50
    // withdraw,14,5,11
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

    TestHelper::remove_file(&[ACCOUNT_DIR, "/", &client_id.to_string(), ".csv"].join(""));
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
    // withdraw,15,4,12
    // withdraw,15,5,11
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

    TestHelper::remove_file(&[ACCOUNT_DIR, "/", &client_id.to_string(), ".csv"].join(""));
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
    // withdraw,16,4,12
    // withdraw,16,5,11
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

    TestHelper::remove_file(&[ACCOUNT_DIR, "/", &client_id.to_string(), ".csv"].join(""));
}
