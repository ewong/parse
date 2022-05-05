use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
use crate::lib::constants::{CLUSTER_DIR, ACCOUNT_DIR, SUMMARY_DIR};
use crate::models::account::Account;
use crate::models::processor::Processor;
use crate::models::tx_reader::TxRecordReader;
use crate::models::tx_record::TxRecordType;

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
    let result = p.process_csv(false);
    assert!(result.is_ok());

    // check output files
    let cluster_base = [CLUSTER_DIR, "withdraw"].join("/");
    let result = TxRecordReader::new(&[&cluster_base, "7", "0.csv"].join("/"));
    assert!(result.is_ok());

    let mut reader = result.unwrap();

    // withdraw,7,1,5
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::WITHDRAW);
    assert_eq!(reader.tx_record_client(), &7);
    assert_eq!(reader.tx_record_tx(), &1);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(5, 0));

    // deposit,7,2,2
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &7);
    assert_eq!(reader.tx_record_tx(), &2);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(2, 0));

    // withdraw,7,3,3
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::WITHDRAW);
    assert_eq!(reader.tx_record_client(), &7);
    assert_eq!(reader.tx_record_tx(), &3);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(3, 0));

    // deposit,7,4,2
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &7);
    assert_eq!(reader.tx_record_tx(), &4);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(2, 0));

    // withdraw,7,5,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::WITHDRAW);
    assert_eq!(reader.tx_record_client(), &7);
    assert_eq!(reader.tx_record_tx(), &5);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // deposit,7,6,2
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &7);
    assert_eq!(reader.tx_record_tx(), &6);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(2, 0));

    // withdraw,7,7,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::WITHDRAW);
    assert_eq!(reader.tx_record_client(), &7);
    assert_eq!(reader.tx_record_tx(), &7);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // deposit,7,1,5
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &7);
    assert_eq!(reader.tx_record_tx(), &1);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(5, 0));

    // check balance
    let account = Account::new(7, ACCOUNT_DIR);
    assert_eq!(account.client_id, 7);
    assert_eq!(account.available, Decimal::new(9, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(9, 0));
    assert!(!account.locked);

    TestHelper::remove_dir(&cluster_base);
    TestHelper::remove_dir(&[SUMMARY_DIR, "7"].join("/"));
    TestHelper::remove_file(&[ACCOUNT_DIR, "7.csv"].join("/"));
}
