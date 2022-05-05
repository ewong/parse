use std::fs::{self, File};

use csv::{ByteRecord, Reader, Trim};
use rust_decimal::Decimal;

use super::tx_record::{TxRecord, TxRecordSmall, TxRecordType};
use crate::lib::{constants::TYPE_POS, error::AppError};

const PATH: &str = "model/tx_reader";
pub struct TxReader {
    reader: Reader<File>,
    tx_record_type: TxRecordType,
    tx_record_client: u16,
    tx_record_tx: u32,
    tx_record_amount: Decimal,
    byte_record: ByteRecord,
    error: Option<String>,
}

impl TxReader {
    pub fn new(csv_path: &str) -> Result<Self, AppError> {
        let reader = Self::new_reader(csv_path)?;
        Ok(Self {
            reader,
            tx_record_type: TxRecordType::NONE,
            tx_record_client: 0,
            tx_record_tx: 0,
            tx_record_amount: Decimal::new(0, 0),
            byte_record: ByteRecord::new(),
            error: None,
        })
    }

    pub fn new_reader(csv_path: &str) -> Result<Reader<File>, AppError> {
        let f = fs::File::open(&csv_path)
            .map_err(|e| AppError::new(PATH, "csv_reader", &["00", csv_path].join("| "), &e.to_string()))?;
        // println!("size of file: {}", f.metadata().unwrap().len());
        Ok(csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .trim(Trim::All)
            .from_reader(f))
    }

    pub fn byte_record(&self) -> &ByteRecord {
        &self.byte_record
    }

    pub fn tx_record_type(&self) -> &TxRecordType {
        &self.tx_record_type
    }

    pub fn tx_record_client(&self) -> &u16 {
        &self.tx_record_client
    }

    pub fn tx_record_tx(&self) -> &u32 {
        &self.tx_record_tx
    }

    pub fn tx_record_amount(&self) -> &Decimal {
        &self.tx_record_amount
    }

    pub fn error(&self) -> &Option<String> {
        &self.error
    }

    pub fn next_record(&mut self) -> bool {
        let result = self.reader.read_byte_record(&mut self.byte_record);
        if result.is_err() {
            let e = result.err().unwrap();
            self.error = Some(e.to_string());
            return false;
        }

        if self.byte_record.len() == 0 {
            // end of file
            return false;
        }

        // todo: trap for blank lines

        // validate
        let mut tx_record_type = TxRecordType::from_binary(&self.byte_record[TYPE_POS]);
        if tx_record_type == TxRecordType::NONE {
            if TxRecordType::header_type(&self.byte_record) {
                // skip header + read the next row
                let result = self.reader.read_byte_record(&mut self.byte_record);
                if result.is_err() {
                    let e = result.err().unwrap();
                    self.error = Some(e.to_string());
                    return false;
                }
                // reset tx_record_type value
                tx_record_type = TxRecordType::from_binary(&self.byte_record[TYPE_POS]);
            } else {
                self.error = Some("invalid transaction record type".to_string());
                return false;
            }
        }

        if tx_record_type.conflict_type() {
            let result = self.byte_record.deserialize::<TxRecordSmall>(None);
            if result.is_err() {
                self.error = Some("invalid transaction record".to_string());
                return false;
            }
            let tx_record = result.unwrap();
            self.tx_record_type = tx_record_type;
            self.tx_record_client = tx_record.client_id.clone();
            self.tx_record_tx = tx_record.tx_id.clone();
            self.tx_record_amount = Decimal::new(0, 0);
            return true;
        }

        let result = self.byte_record.deserialize::<TxRecord>(None);
        if result.is_err() {
            println!("{:?}, {:?}", self.byte_record(), tx_record_type);
            let e = result.err().unwrap();
            self.error = Some(e.to_string());
            return false;
        }

        let tx_record = result.unwrap();
        self.tx_record_type = TxRecordType::from_binary(tx_record.type_id);
        self.tx_record_client = tx_record.client_id;
        self.tx_record_tx = tx_record.tx_id;
        self.tx_record_amount = tx_record.amount;

        true
    }
}
