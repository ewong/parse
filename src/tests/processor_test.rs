use rust_decimal::Decimal;

use super::helpers::helper::TestHelper;
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

    let p = Processor::new("src/tests/csv/deposit.csv");
    assert!(p.process_csv(false).is_ok());

    // check output files
    let base = "data/deposit";
    let result = TxRecordReader::new(&[base, "1", "0.csv"].join("/"));
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

    // -------- //
    // client 2 //
    // -------- //

    assert!(reader.set_reader(&[base, "2", "0.csv"].join("/")).is_ok());

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

    // -------- //
    // client 3 //
    // -------- //

    assert!(reader.set_reader(&[base, "3", "0.csv"].join("/")).is_ok());

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

    // -------- //
    // client 4 //
    // -------- //

    assert!(reader.set_reader(&[base, "4", "0.csv"].join("/")).is_ok());

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

    // test account balances
    // available should be 3 for all clients

    TestHelper::cleanup("data/deposit");
}

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
    let base = "data/withdraw";
    let result = TxRecordReader::new(&[base, "1", "0.csv"].join("/"));
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

    // test account balances
    // available should be 9

    TestHelper::cleanup("data/withdraw");
}

// fn process_dispute_test() {

// }
