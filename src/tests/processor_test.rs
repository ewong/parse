use crate::models::{
    processor::Processor,
    tx_record::{TxRecordReader, TxRecordType},
};

#[test]
fn process_deposit_test() {
    let p = Processor::new("src/tests/csv/deposit.csv");
    let result = p.process_csv(false);
    assert!(result.is_ok());

    // check output files
    let base = "data/deposit";
    let result = TxRecordReader::new(&[base, "1", "0.csv"].join("/"));
    assert!(result.is_ok());

    let mut reader = result.unwrap();

    // deposit,1,1,1
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &1);
    // assert_eq!(reader.tx_record_amount(), &1.0);

    // deposit,1,8,2
    assert!(reader.next_record());
    assert_eq!(reader.tx_record_type(), &TxRecordType::DEPOSIT);
    assert_eq!(reader.tx_record_client(), &1);
    assert_eq!(reader.tx_record_tx(), &8);
    // assert_eq!(reader.tx_record_amount(), &2.0);
}

// fn process_withdraw_test() {

// }

// fn process_dispute_test() {

// }
