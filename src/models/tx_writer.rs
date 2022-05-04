use csv::{ByteRecord, Writer};
use std::fs::{self, File};

use crate::lib::{constants::FN_NEW, error::AppError};

const PATH: &str = "models/tx_record";

const TYPE_COL: &str = "type";
const CLIENT_COL: &str = "client";
const TX_COL: &str = "tx";
const AMOUNT_COL: &str = "amount";

const FN_WRITE_RECORDS: &str = "writer_records";

pub struct TxWriter {
    writer: Writer<File>,
}

impl TxWriter {
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

    fn file_path(dir_path: &str, file_name: &str) -> Result<String, AppError> {
        fs::create_dir_all(dir_path)
            .map_err(|e| AppError::new(PATH, "file_path", "00", &e.to_string()))?;
        Ok([dir_path, "/", file_name, ".csv"].join(""))
    }
}
