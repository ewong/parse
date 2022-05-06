use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
use crate::lib::constants::ACCOUNT_DIR;
use crate::models::account::Account;
use crate::models::processor::Processor;

#[test]
fn process_withdraw_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // withdraw,7,1,5
    // deposit,7,2,2
    // withdraw,7,3,3
    // deposit,7,4,2
    // withdraw,7,5,1
    // deposit,7,6,2
    // withdraw,7,7,1
    // deposit,7,1,5

    let result = Processor::new("src/tests/csv/withdraw.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    // check balance
    let account = Account::new(7, ACCOUNT_DIR);
    assert_eq!(account.client_id, 7);
    assert_eq!(account.available, Decimal::new(9, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(9, 0));
    assert!(!account.locked);

    TestHelper::remove_file(&[ACCOUNT_DIR, "7.csv"].join("/"));
}
