use csv::ByteRecord;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

use super::error::AppError;
use super::timer::Timer;
use super::tx_cluster_queue::{TxClusterQueue, TxClusterQueueBlock};
use super::tx_merge_queue::TxMergeQueue;
use super::tx_queue::TxQueue;
use super::tx_row::CLIENT_POS;

const PATH: &str = "model/parser";
const FN_PROCESS_CSV: &str = "group_transactions_by_client";
const FN_MERGE_TXNS: &str = "merge_transactions_by_client";
const OUTPUT_ROOT_DIR: &str = "data";
const ACCOUNT_DIR: &str = "data/accounts";

const BLOCK_SIZE: usize = 1_000_000;

pub struct Processor<'a> {
    file_path: &'a str,
}

impl<'a> Processor<'a> {
    pub fn new(file_path: &'a str) -> Self {
        Self { file_path }
    }

    pub fn process_csv(&self) -> Result<(), AppError> {
        let result = self.cluster_transactions_by_client();
        if result.is_err() {
            println!("rollback");
            return result;
        }

        let result = self.flatten_transactions_by_client();
        if result.is_err() {
            print!("rollback");
            return result;
        }

        Ok(())
    }

    fn file_dir(&self) -> Result<String, AppError> {
        let v: Vec<&str> = self.file_path.split("/").collect();
        if v.len() > 0 {
            let file_name = v[v.len() - 1];
            if file_name.len() > 0 {
                let v: Vec<&str> = file_name.split(".").collect();
                return Ok([OUTPUT_ROOT_DIR, v[0]].join("/"));
            }
        }
        Err(AppError::new(PATH, "file_dir", "01", "invalid file path"))
    }

    fn cluster_transactions_by_client(&self) -> Result<(), AppError> {
        let timer = Timer::start();
        let f = fs::File::open(self.file_path)
            .map_err(|e| AppError::new(PATH, FN_PROCESS_CSV, "00", &e.to_string()))?;

        let mut rdr = csv::Reader::from_reader(f);
        let mut block: usize = 0;
        let mut rows: usize = 0;

        let mut map: HashMap<Vec<u8>, Vec<ByteRecord>> = HashMap::new();
        let out_dir = self.file_dir()?;
        let mut q = TxClusterQueue::new(&out_dir);
        let mut record = ByteRecord::new();

        let mut block_timer = Timer::start();

        while rdr.read_byte_record(&mut record).map_err(|e| {
            let line_num = (block * BLOCK_SIZE + rows).to_string();
            let err = [
                "read_byte_record failed in line ",
                &line_num,
                " --> ",
                &e.to_string(),
            ]
            .join("");
            AppError::new(PATH, FN_PROCESS_CSV, "03", &err)
        })? {
            // serde serialize validation

            if map.contains_key(&record[CLIENT_POS]) {
                map.entry(record[CLIENT_POS].to_vec()).and_modify(|e| {
                    e.push(record.clone());
                });
            } else {
                let mut v = Vec::new();
                v.push(record.clone());
                map.insert(record[CLIENT_POS].to_vec(), v);
            }

            rows += 1;
            if rows == BLOCK_SIZE {
                if block == 0 {
                    // println!("----------------------------------------------------");
                    q.start()?;
                }

                // println!("add to q --> block: {}, num clients: {}", block, map.len());
                q.add(TxClusterQueueBlock::new(block, map))?;
                map = HashMap::new();

                rows = 0;
                block += 1;

                block_timer.stop();
                // println!("----------------------------------------------------");

                block_timer = Timer::start();
            }
        }

        // send remaining data to write queue
        if map.len() > 0 {
            // println!(
            //     "add to q --> block: {}, num clients: {}",
            //     block + 1,
            //     map.len()
            // );
            q.add(TxClusterQueueBlock::new(block + 1, map))?;
        }

        q.stop()?;
        timer.stop();
        Ok(())
    }

    fn flatten_transactions_by_client(&self) -> Result<(), AppError> {
        let timer = Timer::start();
        let in_dir = self.file_dir()?; // in_dir is the out_dir of group_transactions_by_client

        let paths = fs::read_dir(&in_dir)
            .map_err(|e| AppError::new(PATH, FN_MERGE_TXNS, "00", &e.to_string()))?;

        let dir_paths: Vec<String> = paths
            .map(|e| {
                if let Ok(path) = e {
                    if path.path().is_dir() {
                        return path.path().display().to_string();
                    }
                }
                "".to_string()
            })
            .filter(|s| s.len() > 0)
            .collect();

        // println!("num clients: {}", dir_paths.len());
        let mut q = TxMergeQueue::new(ACCOUNT_DIR, dir_paths);
        q.start()?;
        q.stop()?;
        timer.stop();
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
struct CsvRow<'a> {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    type_id: Option<&'a [u8]>,
    #[serde(rename(deserialize = "client", serialize = "client"))]
    client_id: Option<u16>,
    #[serde(rename(deserialize = "tx", serialize = "tx"))]
    tx_id: Option<f32>,
    #[serde(rename(deserialize = "amount", serialize = "amount"))]
    amount: Option<f64>,
}
