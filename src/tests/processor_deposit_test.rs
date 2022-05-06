use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
use crate::lib::constants::ACCOUNT_DIR;
use crate::models::account::Account;
use crate::models::processor::Processor;

#[test]
fn process_deposit_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,1,1,1
    // deposit,2,2,1
    // deposit,3,3,1
    // deposit,4,4,1
    // deposit,4,5,2
    // deposit,2,6,2
    // deposit,3,7,2
    // deposit,1,8,2

    let result = Processor::new("src/tests/csv/deposit.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    // result.err().unwrap().show();
    // return;
    assert!(result.is_ok());

    let account = Account::new(4, ACCOUNT_DIR);
    assert_eq!(account.client_id, 4);
    assert_eq!(account.available, Decimal::new(3, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(3, 0));
    assert!(!account.locked);

    TestHelper::remove_file(&[ACCOUNT_DIR, "1.csv"].join("/"));
    TestHelper::remove_file(&[ACCOUNT_DIR, "2.csv"].join("/"));
    TestHelper::remove_file(&[ACCOUNT_DIR, "3.csv"].join("/"));
    TestHelper::remove_file(&[ACCOUNT_DIR, "4.csv"].join("/"));
}


#[test]
fn process_deposit_multi_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,22,1,1
    // deposit,22,2,1
    // deposit,22,3,1
    // deposit,22,4,1

    // type,client,tx,amount
    // deposit,22,5,2
    // deposit,22,6,2
    // deposit,22,7,2
    // deposit,22,8,2
    
    let client_id = 22;
    let result = Processor::new("src/tests/csv/deposit_multi_0.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let result = Processor::new("src/tests/csv/deposit_multi_1.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(12, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(12, 0));
    assert!(!account.locked);

    TestHelper::remove_file(&[ACCOUNT_DIR, "/", &client_id.to_string(), ".csv"].join(""));
}
