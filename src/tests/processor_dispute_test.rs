use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
use crate::lib::constants::{CLUSTER_DIR, SUMMARY_DIR};
use crate::models::account::Account;
use crate::models::processor::Processor;
use crate::models::tx_record::{TxRecordReader, TxRecordType};

// dispute
// - tx doesn't exist
// - tx exists
// - dispute on a disputed account
// - dispute on a resolved account that

#[test]
fn process_dispute_base_test() {
    // --------- //
    // input csv //
    // --------- //

    // type,client,tx,amount
    // deposit,1,1,1
    // deposit,1,2,1
    // deposit,1,3,1
    // withdraw,1,4,5
    // withdraw,1,5,1
    // dispute,1,1
    
    let p = Processor::new("src/tests/csv/dispute_base.csv");
    let result = p.process_csv(false);
    assert!(result.is_ok());

    // check output files
    let cluster_base = [CLUSTER_DIR, "dispute_base"].join("/");
    let result = TxRecordReader::new(&[&cluster_base, "1", "0.csv"].join("/"));
    assert!(result.is_ok());

    let mut reader = result.unwrap();

    // deposit,1,1,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &1);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // deposit,1,2,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &2);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // deposit,1,3,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &3);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // withdraw,1,4,5
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::WITHDRAW);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &4);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(5, 0));

    // withdraw,1,5,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::WITHDRAW);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &5);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(1, 0));

    // dispute,1,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DISPUTE);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &1);
    assert_eq!(reader.tx_record_amount(), &Decimal::new(0, 0));

    // check balance
    let summary_base = [SUMMARY_DIR, "dispute_base"].join("/");

    let account = Account::new(1, &summary_base);
    assert_eq!(account.client_id, 1);
    assert_eq!(account.available, Decimal::new(1, 0));
    assert_eq!(account.held, Decimal::new(1, 0));
    assert_eq!(account.total, Decimal::new(2, 0));
    assert!(!account.locked);

    TestHelper::remove_dir(&cluster_base);
    TestHelper::remove_dir(&summary_base);
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