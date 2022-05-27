use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
use crate::lib::constants::ACCOUNT_DIR;
use crate::models::account::Account;
use crate::models::processor::Processor;

#[test]
fn process_base_test() {
    // --------- //
    // input csv //
    // --------- //

    // type, client, tx, amount
    // deposit, 27, 1, 1.0
    // deposit, 28, 2, 2.0
    // deposit, 27, 3, 2.0
    // withdrawal, 27, 4, 1.5
    // withdrawal, 28, 5, 3.0

    let result = Processor::new("src/tests/csv/base.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_data(false);
    assert!(result.is_ok());

    let client_id = 27;
    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(15, 1));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(15, 1));
    assert!(!account.locked);

    TestHelper::clean(&client_id);

    let client_id = 28;
    let account = Account::new(client_id, ACCOUNT_DIR);
    assert_eq!(account.client_id, client_id);
    assert_eq!(account.available, Decimal::new(2, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(2, 0));
    assert!(!account.locked);

    TestHelper::clean(&client_id);
}