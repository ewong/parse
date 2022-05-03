use csv::{ByteRecord, Reader, Trim, Writer};
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::str;

use crate::lib::constants::FN_NEW;
use crate::lib::error::AppError;

const PATH: &str = "models/tx_record";

const TYPE_POS: usize = 0;

const TYPE_COL: &str = "type";
const CLIENT_COL: &str = "client";
const TX_COL: &str = "tx";
const AMOUNT_COL: &str = "amount";

const FN_WRITE_RECORDS: &str = "writer_records";
const FN_WRITE_CONFLICTS: &str = "write_conflicts";

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
            return match str.to_lowercase().replace(" ", "").as_str() {
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
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TxRecord<'a> {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_id: &'a [u8],
    #[serde(rename(deserialize = "client", serialize = "client"))]
    pub client_id: u16,
    #[serde(rename(deserialize = "tx", serialize = "tx"))]
    pub tx_id: u32,
    #[serde(
        rename(deserialize = "amount", serialize = "amount"),
        with = "rust_decimal::serde::str"
    )]
    pub amount: Decimal,
}

#[derive(Deserialize, Serialize)]
pub struct TxRecordSmall<'a> {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_id: &'a [u8],
    #[serde(rename(deserialize = "client", serialize = "client"))]
    pub client_id: u16,
    #[serde(rename(deserialize = "tx", serialize = "tx"))]
    pub tx_id: u32,
}

pub struct TxRecordReader {
    reader: Reader<File>,
    tx_record_type: TxRecordType,
    tx_record_client: u16,
    tx_record_tx: u32,
    tx_record_amount: Decimal,
    byte_record: ByteRecord,
    error: Option<String>,
}

impl TxRecordReader {
    pub fn new(csv_path: &str) -> Result<Self, AppError> {
        let reader = Self::csv_reader(csv_path)?;
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
        let tx_record_type = TxRecordType::from_binary(&self.byte_record[TYPE_POS]);
        if tx_record_type == TxRecordType::NONE {
            self.error = Some("invalid transaction record type".to_string());
            return false;
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
        // println!("size of file: {}", f.metadata().unwrap().len());
        Ok(csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .trim(Trim::All)
            .from_reader(f))
    }
}

pub struct TxRecordWriter {
    writer: Writer<File>,
}

impl TxRecordWriter {
    pub fn new(dir_path: &str, file_name: &str) -> Result<Self, AppError> {
        let file_path = Self::file_path(dir_path, file_name)?;
        let writer = csv::WriterBuilder::new()
            .flexible(true)
            .from_path(&file_path)
            .map_err(|e| AppError::new(PATH, FN_NEW, "00", &e.to_string()))?;
        Ok(Self { writer })
    }

    pub fn set_writer(&mut self, dir_path: &str, file_name: &str) -> Result<(), AppError> {
        let file_path = Self::file_path(dir_path, file_name)?;
        self.writer = csv::WriterBuilder::new()
            .flexible(true)
            .from_path(&file_path)
            .map_err(|e| AppError::new(PATH, FN_NEW, "01", &e.to_string()))?;
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

    pub fn write_conflicts(
        &self,
        dir_path: &str,
        file_name: &str,
        conflict_set: &HashSet<u32>,
        deposit_withdraw_map: &HashMap<u32, usize>,
        records: &Vec<ByteRecord>,
    ) -> Result<(), AppError> {
        let mut byte_records: Vec<&ByteRecord> = Vec::new();
        let mut orphans = Vec::new();

        for tx_id in conflict_set {
            if let Some(row) = deposit_withdraw_map.get(tx_id) {
                if let Some(record) = records.get(*row) {
                    byte_records.push(record);
                    continue;
                }
            }
            orphans.push(tx_id.clone());
        }

        // flush csv into file
        if byte_records.len() > 0 {
            let csv_dir = [dir_path, "csv"].join("/");
            let file_path = Self::file_path(&csv_dir, file_name)?;

            let mut writer = csv::WriterBuilder::new()
                .flexible(true)
                .from_path(&file_path)
                .map_err(|e| AppError::new(PATH, FN_WRITE_CONFLICTS, "00", &e.to_string()))?;

            writer
                .write_byte_record(&ByteRecord::from(
                    &[TYPE_COL, CLIENT_COL, TX_COL, AMOUNT_COL][..],
                ))
                .map_err(|e| AppError::new(PATH, FN_WRITE_CONFLICTS, "01", &e.to_string()))?;

            for record in byte_records {
                writer
                    .write_byte_record(record)
                    .map_err(|e| AppError::new(PATH, FN_WRITE_CONFLICTS, "02", &e.to_string()))?;
            }

            writer
                .flush()
                .map_err(|e| AppError::new(PATH, FN_WRITE_CONFLICTS, "03", &e.to_string()))?;
        }

        // write orphans
        if orphans.len() > 0 {
            let csv_dir = [dir_path, "txt"].join("/");
            let file_path = Self::file_path(&csv_dir, file_name)?;

            let mut output = fs::File::create(file_path.replace(".csv", ".txt"))
                .map_err(|e| AppError::new(PATH, FN_WRITE_CONFLICTS, "04", &e.to_string()))?;
            write!(output, "{:?}", orphans)
                .map_err(|e| AppError::new(PATH, FN_WRITE_CONFLICTS, "05", &e.to_string()))?;
        }

        Ok(())
    }

    fn file_path(dir_path: &str, file_name: &str) -> Result<String, AppError> {
        fs::create_dir_all(dir_path)
            .map_err(|e| AppError::new(PATH, "file_path", "00", &e.to_string()))?;
        Ok([dir_path, "/", file_name, ".csv"].join(""))
    }
}
