use csv::{ByteRecord, Reader, Writer};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::str;

use crate::lib::error::AppError;

const PATH: &str = "models/tx_record";

const TYPE_COL: &str = "type";
const CLIENT_COL: &str = "client";
const TX_COL: &str = "tx";
const AMOUNT_COL: &str = "amount";

pub const CLIENT_POS: usize = 1;
const TX_POS: usize = 2;
const AMOUNT_POS: usize = 3;

const FN_NEW: &str = "new";
const FN_NEXT: &str = "next";
const FN_WRITE_RECORDS: &str = "writer_records";

const B_DEPOSIT: &[u8] = b"deposit";
const B_WITHDRAW: &[u8] = b"withdraw";
const B_DISPUTE: &[u8] = b"dispute";
const B_RESOLVE: &[u8] = b"resolve";
const B_CHARGEBACK: &[u8] = b"chargeback";
const B_NONE: &[u8] = b"";

#[derive(PartialEq)]
enum TxRecordType {
    DEPOSIT = 0,
    WITHDRAW,
    DISPUTE,
    RESOLVE,
    CHARGEBACK,
    NONE,
}

impl TxRecordType {
    pub fn byte_position() -> usize {
        0
    }

    pub fn from_binary(binary: &[u8]) -> Self {
        let initial_type = match binary {
            B_DEPOSIT => TxRecordType::DEPOSIT,
            B_WITHDRAW => TxRecordType::WITHDRAW,
            B_DISPUTE => TxRecordType::DISPUTE,
            B_RESOLVE => TxRecordType::RESOLVE,
            B_CHARGEBACK => TxRecordType::CHARGEBACK,
            _ => TxRecordType::NONE,
        };

        if initial_type != TxRecordType::NONE {
            return initial_type;
        }

        // try to convert it to a lower case str + retest
        if let Ok(str) = str::from_utf8(binary) {
            return match str.to_lowercase().as_str() {
                "deposit" => TxRecordType::DEPOSIT,
                "withdraw" => TxRecordType::WITHDRAW,
                "dispute" => TxRecordType::DISPUTE,
                "resolve" => TxRecordType::RESOLVE,
                "chargeback" => TxRecordType::CHARGEBACK,
                _ => TxRecordType::NONE,
            };
        }

        // return none
        TxRecordType::NONE
    }

    pub fn as_binary(&self) -> &[u8] {
        match self {
            DEPOSIT => B_DEPOSIT,
            WITHDRAW => B_WITHDRAW,
            DISPUTE => B_DISPUTE,
            RESOLVE => B_RESOLVE,
            CHARGEBACK => B_CHARGEBACK,
            NONE => B_NONE,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TxRecord<'a> {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_id: &'a [u8],
    #[serde(rename(deserialize = "client", serialize = "client"))]
    pub client_id: u16,
    #[serde(rename(deserialize = "tx", serialize = "tx"))]
    pub tx_id: f32,
    #[serde(rename(deserialize = "amount", serialize = "amount"))]
    pub amount: Option<f64>,
}

impl<'a> TxRecord<'a> {
    pub fn new() -> Self {
        Self {
            type_id: b"",
            client_id: 0,
            tx_id: 0.0,
            amount: None,
        }
    }
}

pub struct TxRecordReader {
    reader: Reader<File>,
    headers: ByteRecord,
    byte_record: ByteRecord,
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
            byte_record: ByteRecord::new(),
        })
    }

    pub fn set_reader(&mut self, csv_path: &str) -> Result<(), AppError> {
        self.reader = Self::csv_reader(csv_path)?;
        Ok(())
    }

    pub fn byte_record(&self) -> &ByteRecord {
        &self.byte_record
    }

    pub fn next_byte_record(&mut self) -> Result<bool, AppError> {
        self.reader
            .read_byte_record(&mut self.byte_record)
            .map_err(|e| AppError::new(PATH, FN_NEXT, "00", &e.to_string()))?;

        if self.byte_record.len() == 0 {
            // end of file
            return Ok(false);
        }

        // validate
        let tx_record_type =
            TxRecordType::from_binary(&self.byte_record[TxRecordType::byte_position()]);
        if tx_record_type == TxRecordType::NONE {
            return Err(AppError::new(
                PATH,
                FN_NEXT,
                "01",
                "invalid transaction record type",
            ));
        }

        let _ = self
            .byte_record
            .deserialize(Some(&self.headers))
            .map_err(|e| {
                println!("{}", &e.to_string());
                AppError::new(PATH, FN_NEXT, "04", &e.to_string())
            })?;
        Ok(true)
    }

    pub fn next_tx_record(&mut self, tx_record: &mut TxRecord) -> Result<bool, AppError> {
        self.reader
            .read_byte_record(&mut self.byte_record)
            .map_err(|e| AppError::new(PATH, FN_NEXT, "00", &e.to_string()))?;

        if self.byte_record.len() == 0 {
            // end of file
            return Ok(false);
        }

        // add validation
        let tx_record_type =
            TxRecordType::from_binary(&self.byte_record[TxRecordType::byte_position()]);
        if tx_record_type == TxRecordType::NONE {
            return Err(AppError::new(
                PATH,
                FN_NEXT,
                "01",
                "invalid transaction record type",
            ));
        }

        let tx_record = self
            .byte_record
            .deserialize(Some(&self.headers))
            .map_err(|e| {
                println!("{}", &e.to_string());
                AppError::new(PATH, FN_NEXT, "04", &e.to_string())
            })?;

        Ok(true)
    }

    fn csv_reader(csv_path: &str) -> Result<Reader<File>, AppError> {
        let f = fs::File::open(&csv_path)
            .map_err(|e| AppError::new(PATH, FN_NEW, "00", &e.to_string()))?;
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

    fn file_path(dir_path: &str, file_name: &str) -> Result<String, AppError> {
        fs::create_dir_all(dir_path)
            .map_err(|e| AppError::new(PATH, FN_NEW, "00", &e.to_string()))?;
        Ok([dir_path, "/", file_name, ".csv"].join(""))
    }
}
