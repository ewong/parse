use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
use crate::lib::constants::{ACCOUNT_DIR, CLUSTER_DIR, SUMMARY_DIR};
use crate::models::account::Account;
use crate::models::processor::Processor;
use crate::models::tx_reader::TxReader;
use crate::models::tx_record::TxRecordType;

// dispute
// - tx exists
// - tx doesn't exist
// - dispute on a disputed account
// - dispute on a resolved account that

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

    let result = Processor::new("src/tests/csv/dispute_base.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_csv(false);
    if result.is_err() {
        result.err().unwrap().show();
        return;
    }
    assert!(result.is_ok());

    // check output files
    let cluster_base = [CLUSTER_DIR, "dispute_base"].join("/");
    let result = TxReader::new(&[&cluster_base, "5", "0.csv"].join("/"));
    assert!(result.is_ok());

    let mut reader = result.unwrap();

    // deposit,5,1,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &5);
    assert_eq!(reader.tx_record_tx(), &1);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // deposit,5,2,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &5);
    assert_eq!(reader.tx_record_tx(), &2);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // deposit,5,3,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &5);
    assert_eq!(reader.tx_record_tx(), &3);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // withdraw,5,4,5
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::WITHDRAW);
    assert_eq!(reader.tx_record_client(), &5);
    assert_eq!(reader.tx_record_tx(), &4);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(5, 0));

    // withdraw,5,5,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::WITHDRAW);
    assert_eq!(reader.tx_record_client(), &5);
    assert_eq!(reader.tx_record_tx(), &5);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // dispute,5,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DISPUTE);
    assert_eq!(reader.tx_record_client(), &5);
    assert_eq!(reader.tx_record_tx(), &1);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(0, 0));

    // check balance
    let account = Account::new(5, ACCOUNT_DIR);
    assert_eq!(account.client_id, 5);
    assert_eq!(account.available, Decimal::new(1, 0));
    assert_eq!(account.held, Decimal::new(1, 0));
    assert_eq!(account.total, Decimal::new(2, 0));
    assert!(!account.locked);

    TestHelper::remove_dir(&cluster_base);
    TestHelper::remove_dir(&[SUMMARY_DIR, "5"].join("/"));
    TestHelper::remove_file(&[ACCOUNT_DIR, "5.csv"].join("/"));
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

    let result = Processor::new("src/tests/csv/dispute_tx_dne.csv");
    assert!(result.is_ok());

    let p = result.unwrap();
    let result = p.process_csv(false);
    if result.is_err() {
        result.err().unwrap().show();
        return;
    }
    assert!(result.is_ok());

    // check output files
    let cluster_base = [CLUSTER_DIR, "dispute_tx_dne"].join("/");
    let result = TxReader::new(&[&cluster_base, "6", "0.csv"].join("/"));
    assert!(result.is_ok());

    let mut reader = result.unwrap();

    // dispute,6,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DISPUTE);
    assert_eq!(reader.tx_record_client(), &6);
    assert_eq!(reader.tx_record_tx(), &1);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(0, 0));

    // deposit,6,2,5
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &6);
    assert_eq!(reader.tx_record_tx(), &2);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(5, 0));

    // deposit,6,3,10
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &6);
    assert_eq!(reader.tx_record_tx(), &3);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(10, 0));

    // withdraw,6,4,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::WITHDRAW);
    assert_eq!(reader.tx_record_client(), &6);
    assert_eq!(reader.tx_record_tx(), &4);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // dispute,6,50
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DISPUTE);
    assert_eq!(reader.tx_record_client(), &6);
    assert_eq!(reader.tx_record_tx(), &50);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(0, 0));

    // check balance
    let account = Account::new(6, ACCOUNT_DIR);
    assert_eq!(account.client_id, 6);
    assert_eq!(account.available, Decimal::new(14, 0));
    assert_eq!(account.held, Decimal::new(0, 0));
    assert_eq!(account.total, Decimal::new(14, 0));
    assert!(!account.locked);

    TestHelper::remove_dir(&cluster_base);
    TestHelper::remove_dir(&[SUMMARY_DIR, "6"].join("/"));
    TestHelper::remove_file(&[ACCOUNT_DIR, "6.csv"].join("/"));
}

/*
    resolve
    - tx doesn't exist
    - resolve on a clean account => ignore
    - resolve on a disputed account => process
    - resolve on a tx that has been resolved => ignore

    chargeback
    - tx doesn't exist
    - deposit on a locked account
    - withdraw on a locked account
    - dispute on a frozen account
    - chargeback on a frozen account
*/
