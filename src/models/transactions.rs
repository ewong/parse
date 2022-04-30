use csv::ByteRecord;
use std::collections::HashMap;
use std::fs;

use super::error::AppError;
use super::queue::Queue;
use super::timer::Timer;
use super::tx_queue::{TxBlock, TxQueue};

const PATH: &str = "model/transactions";
const FN_PROCESS_CSV: &str = "linear_group_txns_by_client";

const BLOCK_SIZE: usize = 1_000_000;

// const TYPE_POS: usize = 0;
const CLIENT_POS: usize = 1;
// const TX_POS: usize = 2;
// const AMOUNT_POS: usize = 3;

pub struct Transactions;

impl Transactions {
    pub fn group_txns_by_client(csv_path: &str) -> Result<(), AppError> {
        let timer = Timer::start();
        let f = fs::File::open(csv_path)
            .map_err(|e| AppError::new(PATH, FN_PROCESS_CSV, "00", &e.to_string()))?;

        let mut rdr = csv::Reader::from_reader(f);
        let mut block: usize = 0;
        let mut rows: usize = 0;

        let mut map: HashMap<Vec<u8>, Vec<ByteRecord>> = HashMap::new();
        let mut tq = TxQueue::new();
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
}
