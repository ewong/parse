use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs};

use super::error::AppError;

const PATH: &str = "model/transactions";
const OUTPUT_DIR: &str = "data/transactions";
const FN_WRITE_CSV: &str = "write_client_txns";

pub struct WriteQueue {
    q: HashMap<Vec<u8>, u32>,
}

impl WriteQueue {
    pub fn new() -> Self {
        Self { q: HashMap::new() }
    }

    pub fn write_client_txns(
        &self,
        client_id: &u32,
        block_id: &usize,
        chunk_id: &usize,
        tx_rows: &Vec<TxRow>,
    ) -> Result<(), AppError> {
        let client_str = ["client", &client_id.to_string()].join("_");
        let dir_path = [OUTPUT_DIR, &client_str].join("/");
        fs::create_dir_all(&dir_path)
            .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "00", &e.to_string()))?;

        let file_path = [
            &dir_path,
            "/",
            &client_str,
            "_block_",
            &block_id.to_string(),
            "_chunk_",
            &chunk_id.to_string(),
            ".csv",
        ]
        .join("");

        let mut wtr = csv::Writer::from_path(&file_path)
            .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "01", &e.to_string()))?;

        wtr.write_record(&["userId", "movieId", "rating", "timestamp"])
            .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "02", &e.to_string()))?;

        for row in tx_rows {
            wtr.write_record(&[
                &row.type_id.to_string(),
                &row.client_id.to_string(),
                &row.tx_id.to_string(),
                &row.amount.unwrap_or(0).to_string(),
            ])
            .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "03", &e.to_string()))?;
        }

        wtr.flush()
            .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "04", &e.to_string()))?;

        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TxRow<'a> {
    #[serde(rename(deserialize = "userId", serialize = "userId"))]
    pub type_id: &'a str,
    #[serde(rename(deserialize = "movieId", serialize = "movieId"))]
    pub client_id: u32,
    #[serde(rename(deserialize = "rating", serialize = "rating"))]
    pub tx_id: f32,
    #[serde(rename(deserialize = "timestamp", serialize = "timestamp"))]
    pub amount: Option<u32>,
}

impl<'a> TxRow<'a> {
    pub fn new() -> Self {
        Self {
            type_id: "",
            client_id: 0,
            tx_id: 0.0,
            amount: None,
        }
    }
}
