use csv::{ByteRecord, Writer};
use std::fs::{self, File};

use crate::lib::{constants::FN_NEW, error::AppError};

const PATH: &str = "models/tx_record";

const FN_WRITE_RECORDS: &str = "writer_records";

pub struct TxWriter {
    writer: Writer<File>,
}

impl TxWriter {
    pub fn new(dir_path: &str, file_name: &str) -> Result<Self, AppError> {
        let writer = Self::new_writer(dir_path, file_name)?;
        Ok(Self { writer })
    }

    pub fn set_writer(&mut self, dir_path: &str, file_name: &str) -> Result<(), AppError> {
        self.writer = Self::new_writer(dir_path, file_name)?;
        Ok(())
    }

    pub fn write_records(&mut self, records: &Vec<ByteRecord>) -> Result<(), AppError> {
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

    fn new_writer(dir_path: &str, file_name: &str) -> Result<Writer<File>, AppError> {
        let file_path = Self::file_path(dir_path, file_name)?;
        let writer = csv::WriterBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_path(&file_path)
            .map_err(|e| AppError::new(PATH, FN_NEW, "00", &e.to_string()))?;
        Ok(writer)
    }
}
