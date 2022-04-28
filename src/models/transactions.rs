use csv::ByteRecord;
use std::collections::HashMap;
use std::fs;

use super::error::AppError;
use super::timer::Timer;
use super::write_queue::{WriteQueue, WriteQueueEntry};

const PATH: &str = "model/transactions";
const FN_PROCESS_CSV: &str = "linear_group_txns_by_client";

const CLIENT_ID_POS: usize = 1;
const BLOCK_SIZE: usize = 1_000_000;
const NUM_THREADS: u8 = 4;

pub struct Transactions;

impl Transactions {
    pub fn new() -> Self {
        Self {}
    }

    pub fn group_txns_by_client(&self, csv_path: &str) -> Result<(), AppError> {
        let timer = Timer::start();
        let f = fs::File::open(csv_path)
            .map_err(|e| AppError::new(PATH, FN_PROCESS_CSV, "00", &e.to_string()))?;

        let mut rdr = csv::Reader::from_reader(f);
        let mut block: usize = 0;
        let mut rows: usize = 0;

        let mut map: HashMap<Vec<u8>, Vec<ByteRecord>> = HashMap::new();
        let mut wq = WriteQueue::new(NUM_THREADS);
        let mut record = ByteRecord::new();
        let mut block_timer = Timer::start();

        while rdr
            .read_byte_record(&mut record)
            .map_err(|e| AppError::new(PATH, FN_PROCESS_CSV, "01", &e.to_string()))?
        {
            if map.contains_key(&record[CLIENT_ID_POS]) {
                map.entry(record[CLIENT_ID_POS].to_vec()).and_modify(|e| {
                    e.push(record.clone());
                });
            } else {
                let mut v = Vec::new();
                v.push(record.clone());
                map.insert(record[CLIENT_ID_POS].to_vec(), v);
            }

            rows += 1;
            if rows == BLOCK_SIZE {
                if block == 0 {
                    println!("----------------------------------------------------");
                    wq.start()?;
                }

                println!("add to wq --> block: {}, num clients: {}", block, map.len());
                wq.add(TxBlock::new(block, map))?;
                map = HashMap::new();

                rows = 0;
                block += 1;

                block_timer.stop();
                println!("----------------------------------------------------");

                block_timer = Timer::start();
            }
        }

        // send remaining data to write queue
        wq.stop()?;
        timer.stop();
        Ok(())
    }
}

struct TxBlock {
    block: usize,
    map: HashMap<Vec<u8>, Vec<ByteRecord>>,
}

impl TxBlock {
    fn new(block: usize, map: HashMap<Vec<u8>, Vec<ByteRecord>>) -> Self {
        Self { block, map }
    }
}

impl WriteQueueEntry for TxBlock {
    fn block(&self) -> usize {
        self.block
    }

    fn map(&self) -> &HashMap<Vec<u8>, Vec<ByteRecord>> {
        &self.map
    }
}
