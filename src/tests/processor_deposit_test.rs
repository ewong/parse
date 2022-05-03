use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
use crate::lib::constants::{CLUSTER_DIR, SUMMARY_DIR};
use crate::models::account::Account;
use crate::models::processor::Processor;
use crate::models::tx_record::{TxRecordReader, TxRecordType};

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
    assert!(p.process_csv(false).is_ok());

    // check output files
    let cluster_base = [CLUSTER_DIR, "deposit"].join("/");
    let result = TxRecordReader::new(&[&cluster_base, "1", "0.csv"].join("/"));
    assert!(result.is_ok());

    let mut reader = result.unwrap();

    // -------- //
    // client 1 //
    // -------- //

    // deposit,1,1,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &1);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // deposit,1,8,2
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &8);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(2, 0));

    // check balance
    let summary_base = [SUMMARY_DIR, "deposit"].join("/");

    let mut account = Account::new(1, &summary_base);
    assert_eq!(account.client_id, 1);
    assert_eq!(account.available, Decimal::new(3, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(3, 0));
    assert!(!account.locked);

    // -------- //
    // client 2 //
    // -------- //

    assert!(reader
        .set_reader(&[&cluster_base, "2", "0.csv"].join("/"))
        .is_ok());

    // deposit,2,2,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &2);
    assert_eq!(reader.tx_record_tx(), &2);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // deposit,2,6,2
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &2);
    assert_eq!(reader.tx_record_tx(), &6);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(2, 0));

    account = Account::new(2, &summary_base);
    assert_eq!(account.client_id, 2);
    assert_eq!(account.available, Decimal::new(3, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(3, 0));
    assert!(!account.locked);

    // -------- //
    // client 3 //
    // -------- //

    assert!(reader
        .set_reader(&[&cluster_base, "3", "0.csv"].join("/"))
        .is_ok());

    // deposit,3,3,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &3);
    assert_eq!(reader.tx_record_tx(), &3);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // deposit,3,7,2
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &3);
    assert_eq!(reader.tx_record_tx(), &7);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(2, 0));

    account = Account::new(3, &summary_base);
    assert_eq!(account.client_id, 3);
    assert_eq!(account.available, Decimal::new(3, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(3, 0));
    assert!(!account.locked);

    // -------- //
    // client 4 //
    // -------- //

    assert!(reader
        .set_reader(&[&cluster_base, "4", "0.csv"].join("/"))
        .is_ok());

    // deposit,4,4,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &4);
    assert_eq!(reader.tx_record_tx(), &4);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // deposit,4,5,2
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &4);
    assert_eq!(reader.tx_record_tx(), &5);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(2, 0));

    account = Account::new(4, &summary_base);
    assert_eq!(account.client_id, 4);
    assert_eq!(account.available, Decimal::new(3, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(3, 0));
    assert!(!account.locked);

    TestHelper::remove_dir(&cluster_base);
    TestHelper::remove_dir(&summary_base);
}
