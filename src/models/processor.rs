use csv::ByteRecord;
use std::collections::HashMap;
use std::fs;

use super::client_queue::ClientQueue;
use super::constant::{ACCOUNT_DIR, CLIENT_POS, OUTPUT_ROOT_DIR};
use super::error::AppError;
use super::queue::Queue;
use super::timer::Timer;
use super::tx_queue::{TxBlock, TxQueue};

const PATH: &str = "model/parser";
const FN_PROCESS_CSV: &str = "group_transactions_by_client";
const FN_MERGE_TXNS: &str = "merge_transactions_by_client";

const BLOCK_SIZE: usize = 1_000_000;

pub struct Processor<'a> {
    file_path: &'a str,
}

impl<'a> Processor<'a> {
    pub fn new(file_path: &'a str) -> Self {
        Self { file_path }
    }

    pub fn process_csv(&self) -> Result<(), AppError> {
        self.group_transactions_by_client()?;
        self.merge_transactions_by_client()
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

    fn group_transactions_by_client(&self) -> Result<(), AppError> {
        let timer = Timer::start();
        let f = fs::File::open(self.file_path)
            .map_err(|e| AppError::new(PATH, FN_PROCESS_CSV, "00", &e.to_string()))?;

        let mut rdr = csv::Reader::from_reader(f);
        let mut block: usize = 0;
        let mut rows: usize = 0;

        let mut map: HashMap<Vec<u8>, Vec<ByteRecord>> = HashMap::new();
        let out_dir = self.file_dir()?;
        let mut tq = TxQueue::new(&out_dir);
        let mut record = ByteRecord::new();

        let mut block_timer = Timer::start();

        while rdr
            .read_byte_record(&mut record)
            .map_err(|e| AppError::new(PATH, FN_PROCESS_CSV, "03", &e.to_string()))?
        {
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
                    println!("----------------------------------------------------");
                    tq.start()?;
                }

                println!("add to tq --> block: {}, num clients: {}", block, map.len());
                tq.add(TxBlock::new(block, map))?;
                map = HashMap::new();

                rows = 0;
                block += 1;

                block_timer.stop();
                println!("----------------------------------------------------");

                block_timer = Timer::start();
            }
        }

        // send remaining data to write queue
        if map.len() > 0 {
            println!(
                "add to tq --> block: {}, num clients: {}",
                block + 1,
                map.len()
            );
            tq.add(TxBlock::new(block + 1, map))?;
        }

        tq.stop()?;
        timer.stop();
        Ok(())
    }

    fn merge_transactions_by_client(&self) -> Result<(), AppError> {
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
        let mut cq = ClientQueue::new(ACCOUNT_DIR, dir_paths);
        cq.start()?;
        cq.stop()?;
        timer.stop();
        Ok(())
    }
}
