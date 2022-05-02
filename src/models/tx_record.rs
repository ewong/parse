use csv::{ByteRecord, Reader, Writer};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Write;
use std::str;

use crate::lib::error::AppError;

const PATH: &str = "models/tx_record";

const TYPE_POS: usize = 0;
// const CLIENT_POS: usize = 1;

const TYPE_COL: &str = "type";
const CLIENT_COL: &str = "client";
const TX_COL: &str = "tx";
const AMOUNT_COL: &str = "amount";

const FN_NEW: &str = "new";
const FN_WRITE_RECORDS: &str = "writer_records";

const B_DEPOSIT: &[u8] = b"deposit";
const B_WITHDRAW: &[u8] = b"withdraw";
const B_DISPUTE: &[u8] = b"dispute";
const B_RESOLVE: &[u8] = b"resolve";
const B_CHARGEBACK: &[u8] = b"chargeback";

#[derive(PartialEq, Debug, Clone)]
pub enum TxRecordType {
    DEPOSIT = 0,
    WITHDRAW,
    DISPUTE,
    RESOLVE,
    CHARGEBACK,
    NONE,
}

impl TxRecordType {
    pub fn from_binary(binary: &[u8]) -> Self {
        let initial_type = match binary {
            B_DEPOSIT => Self::DEPOSIT,
            B_WITHDRAW => Self::WITHDRAW,
            B_DISPUTE => Self::DISPUTE,
            B_RESOLVE => Self::RESOLVE,
            B_CHARGEBACK => Self::CHARGEBACK,
            _ => Self::NONE,
        };

        if initial_type != Self::NONE {
            return initial_type;
        }

        // try to convert it to a lower case str + retest
        if let Ok(str) = str::from_utf8(binary) {
            return match str.to_lowercase().as_str() {
                "deposit" => Self::DEPOSIT,
                "withdraw" => Self::WITHDRAW,
                "dispute" => Self::DISPUTE,
                "resolve" => Self::RESOLVE,
                "chargeback" => Self::CHARGEBACK,
                _ => Self::NONE,
            };
        }

        // return none
        Self::NONE
    }

    pub fn conflict_type(&self) -> bool {
        *self == Self::DISPUTE || *self == Self::RESOLVE || *self == Self::CHARGEBACK
    }

    // pub fn name(&self) -> &str {
    //     return match self {
    //         Self::DEPOSIT => "deposit",
    //         Self::WITHDRAW => "withdraw",
    //         Self::DISPUTE => "dispute",
    //         Self::RESOLVE => "resolve",
    //         Self::CHARGEBACK => "chargeback",
    //         _ => "none",
    //     };
    // }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TxRecord<'a> {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_id: &'a [u8],
    #[serde(rename(deserialize = "client", serialize = "client"))]
    pub client_id: u16,
    #[serde(rename(deserialize = "tx", serialize = "tx"))]
    pub tx_id: u32,
    #[serde(rename(deserialize = "amount", serialize = "amount"))]
    pub amount: Option<f64>,
}

pub struct TxRecordReader {
    reader: Reader<File>,
    headers: ByteRecord,
    tx_record_type: TxRecordType,
    tx_record_client: u16,
    tx_record_tx: u32,
    tx_record_amount: Option<f64>,
    byte_record: ByteRecord,
    error: Option<String>,
}

impl TxRecordReader {
    pub fn new(csv_path: &str) -> Result<Self, AppError> {
        let mut reader = Self::csv_reader(csv_path)?;
        let headers = reader
            .byte_headers()
            .map_err(|e| AppError::new(PATH, FN_NEW, "02", &e.to_string()))?
            .clone();
        Ok(Self {
            reader,
            headers,
            tx_record_type: TxRecordType::NONE,
            tx_record_client: 0,
            tx_record_tx: 0,
            tx_record_amount: None,
            byte_record: ByteRecord::new(),
            error: None,
        })
    }

    pub fn set_reader(&mut self, csv_path: &str) -> Result<(), AppError> {
        self.reader = Self::csv_reader(csv_path)?;
        Ok(())
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

    pub fn tx_record_amount(&self) -> &Option<f64> {
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
        let tx_record_type = TxRecordType::from_binary(&self.byte_record[TYPE_POS]);
        if tx_record_type == TxRecordType::NONE {
            self.error = Some("invalid transaction record type".to_string());
            return false;
        }

        let result = self
            .byte_record
            .deserialize::<TxRecord>(Some(&self.headers));

        if result.is_err() {
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

    fn csv_reader(csv_path: &str) -> Result<Reader<File>, AppError> {
        let f = fs::File::open(&csv_path)
            .map_err(|e| AppError::new(PATH, "csv_reader", "00", &e.to_string()))?;
        Ok(csv::Reader::from_reader(f))
    }
}

pub struct TxRecordWriter {
    writer: Writer<File>,
}

impl TxRecordWriter {
    pub fn new(dir_path: &str, file_name: &str) -> Result<Self, AppError> {
        let file_path = Self::file_path(dir_path, file_name)?;
        let writer = csv::Writer::from_path(&file_path)
            .map_err(|e| AppError::new(PATH, FN_NEW, "02", &e.to_string()))?;
        Ok(Self { writer })
    }

    pub fn set_writer(&mut self, dir_path: &str, file_name: &str) -> Result<(), AppError> {
        let file_path = Self::file_path(dir_path, file_name)?;
        self.writer = csv::Writer::from_path(&file_path)
            .map_err(|e| AppError::new(PATH, FN_NEW, "02", &e.to_string()))?;
        Ok(())
    }

    pub fn write_records(&mut self, records: &Vec<ByteRecord>) -> Result<(), AppError> {
        self.writer
            .write_byte_record(&ByteRecord::from(
                &[TYPE_COL, CLIENT_COL, TX_COL, AMOUNT_COL][..],
            ))
            .map_err(|e| AppError::new(PATH, FN_WRITE_RECORDS, "03", &e.to_string()))?;

        for record in records {
            self.writer
                .write_byte_record(record)
                .map_err(|e| AppError::new(PATH, FN_WRITE_RECORDS, "04", &e.to_string()))?;
        }

        self.writer
            .flush()
            .map_err(|e| AppError::new(PATH, FN_WRITE_RECORDS, "05", &e.to_string()))?;

        Ok(())
    }

    pub fn write_conflicted_tx_ids(
        &self,
        dir_path: &str,
        file_name: &str,
        set: &HashSet<u32>,
    ) -> Result<(), AppError> {
        if set.is_empty() {
            return Ok(());
        }
        let path = Self::file_path(dir_path, file_name)?;
        let mut output = fs::File::create(path)
            .map_err(|e| AppError::new(PATH, "write_conflicted_tx_ids", "00", &e.to_string()))?;
        write!(output, "{:?}", set)
            .map_err(|e| AppError::new(PATH, "write_conflicted_tx_ids", "01", &e.to_string()))?;
        Ok(())
    }

    fn file_path(dir_path: &str, file_name: &str) -> Result<String, AppError> {
        fs::create_dir_all(dir_path)
            .map_err(|e| AppError::new(PATH, "file_path", "00", &e.to_string()))?;
        Ok([dir_path, "/", file_name, ".csv"].join(""))
    }
}
