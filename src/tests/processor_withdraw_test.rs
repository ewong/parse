use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
use crate::lib::constants::{CLUSTER_DIR, SUMMARY_DIR};
use crate::models::account::Account;
use crate::models::processor::Processor;
use crate::models::tx_record::{TxRecordReader, TxRecordType};

#[test]
fn process_withdraw_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // withdraw,1,1,5
    // deposit,1,2,2
    // withdraw,1,3,3
    // deposit,1,4,2
    // withdraw,1,5,1
    // deposit,1,6,2
    // withdraw,1,7,1
    // deposit,1,1,5

    let p = Processor::new("src/tests/csv/withdraw.csv");
    let result = p.process_csv(false);
    assert!(result.is_ok());

    // check output files
    let cluster_base = [CLUSTER_DIR, "withdraw"].join("/");
    let result = TxRecordReader::new(&[&cluster_base, "1", "0.csv"].join("/"));
    assert!(result.is_ok());

    let mut reader = result.unwrap();

    // withdraw,1,1,5
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::WITHDRAW);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &1);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(5, 0));

    // deposit,1,2,2
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &2);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(2, 0));

    // withdraw,1,3,3
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::WITHDRAW);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &3);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(3, 0));

    // deposit,1,4,2
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &4);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(2, 0));

    // withdraw,1,5,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::WITHDRAW);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &5);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // deposit,1,6,2
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &6);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(2, 0));

    // withdraw,1,7,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::WITHDRAW);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &7);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // deposit,1,1,5
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &1);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(5, 0));

    // check balance
    let summary_base = [SUMMARY_DIR, "withdraw"].join("/");

    let account = Account::new(1, &summary_base);
    assert_eq!(account.client_id, 1);
    assert_eq!(account.available, Decimal::new(9, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(9, 0));
    assert!(!account.locked);

    TestHelper::remove_dir(&cluster_base);
    TestHelper::remove_dir(&summary_base);
}
