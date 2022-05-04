use csv::{ByteRecord, Writer};
// use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
// use std::io::Write;

use crate::lib::{constants::FN_NEW, error::AppError};

const PATH: &str = "models/tx_record";

// const TYPE_POS: usize = 0;

const TYPE_COL: &str = "type";
const CLIENT_COL: &str = "client";
const TX_COL: &str = "tx";
const AMOUNT_COL: &str = "amount";

const FN_WRITE_RECORDS: &str = "writer_records";
// const FN_WRITE_CONFLICTS: &str = "write_conflicts";

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

    // pub fn write_conflicts(
    //     &self,
    //     dir_path: &str,
    //     file_name: &str,
    //     conflict_set: &HashSet<u32>,
    //     deposit_withdraw_map: &HashMap<u32, usize>,
    //     records: &Vec<ByteRecord>,
    // ) -> Result<(), AppError> {
    //     let mut byte_records: Vec<&ByteRecord> = Vec::new();
    //     let mut orphans = Vec::new();

    //     for tx_id in conflict_set {
    //         if let Some(row) = deposit_withdraw_map.get(tx_id) {
    //             if let Some(record) = records.get(*row) {
    //                 byte_records.push(record);
    //                 continue;
    //             }
    //         }
    //         orphans.push(tx_id.clone());
    //     }

    //     // flush csv into file
    //     if byte_records.len() > 0 {
    //         let csv_dir = [dir_path, "csv"].join("/");
    //         let file_path = Self::file_path(&csv_dir, file_name)?;

    //         let mut writer = csv::WriterBuilder::new()
    //             .flexible(true)
    //             .from_path(&file_path)
    //             .map_err(|e| AppError::new(PATH, FN_WRITE_CONFLICTS, "00", &e.to_string()))?;

    //         writer
    //             .write_byte_record(&ByteRecord::from(
    //                 &[TYPE_COL, CLIENT_COL, TX_COL, AMOUNT_COL][..],
    //             ))
    //             .map_err(|e| AppError::new(PATH, FN_WRITE_CONFLICTS, "01", &e.to_string()))?;

    //         for record in byte_records {
    //             writer
    //                 .write_byte_record(record)
    //                 .map_err(|e| AppError::new(PATH, FN_WRITE_CONFLICTS, "02", &e.to_string()))?;
    //         }

    //         writer
    //             .flush()
    //             .map_err(|e| AppError::new(PATH, FN_WRITE_CONFLICTS, "03", &e.to_string()))?;
    //     }

    //     // write orphans
    //     if orphans.len() > 0 {
    //         let csv_dir = [dir_path, "txt"].join("/");
    //         let file_path = Self::file_path(&csv_dir, file_name)?;

    //         let mut output = fs::File::create(file_path.replace(".csv", ".txt"))
    //             .map_err(|e| AppError::new(PATH, FN_WRITE_CONFLICTS, "04", &e.to_string()))?;
    //         write!(output, "{:?}", orphans)
    //             .map_err(|e| AppError::new(PATH, FN_WRITE_CONFLICTS, "05", &e.to_string()))?;
    //     }

    //     Ok(())
    // }

    fn file_path(dir_path: &str, file_name: &str) -> Result<String, AppError> {
        fs::create_dir_all(dir_path)
            .map_err(|e| AppError::new(PATH, "file_path", "00", &e.to_string()))?;
        Ok([dir_path, "/", file_name, ".csv"].join(""))
    }
}
