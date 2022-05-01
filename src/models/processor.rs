use std::fs;

use super::tx_cluster::{TxCluster, TxClusterData};
use super::tx_cluster_queue::TxClusterQueue;
use super::tx_record::TxRecordReader;
use super::tx_summary_queue::TxSummaryQueue;
use crate::lib::error::AppError;
use crate::lib::timer::Timer;
use crate::lib::tx_queue::TxQueue;

const PATH: &str = "model/processor";
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
        let timer = Timer::start();
        let working_dir = self.file_dir()?;
        self.cluster_transactions_by_client(&working_dir)?;
        self.summarize_transactions_by_client(&working_dir)?;
        println!("removing working directory");
        let _ = fs::remove_dir_all(working_dir);
        timer.stop();
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

    fn cluster_transactions_by_client(&self, working_dir: &str) -> Result<(), AppError> {
        // let timer = Timer::start();

        let mut tx_cluster = TxCluster::new(0);
        let mut rdr = TxRecordReader::new(self.file_path)?;
        let mut q = TxClusterQueue::new(working_dir);
        // let mut block_timer = Timer::start();

        q.start()?;
        let mut block: usize = 0;
        let mut rows: usize = 0;

        while rdr.next_record() {
            tx_cluster.add(
                rdr.byte_record_client(),
                rdr.byte_record_tx(),
                rdr.byte_record(),
            );

            rows += 1;
            if rows == BLOCK_SIZE {
                if block == 0 {
                    println!("----------------------------------------------------");
                }

                println!(
                    "add to q --> block: {}, num clients: {}",
                    block,
                    tx_cluster.client_txns().len()
                );

                rows = 0;
                block += 1;
                q.add(tx_cluster)?;
                tx_cluster = TxCluster::new(block);
                // block_timer.stop();
                println!("----------------------------------------------------");

                // block_timer = Timer::start();
            }
        }

        // handle rollback
        if let Some(error) = rdr.error() {
            let _ = q.stop();
            let _ = fs::remove_dir_all(working_dir);
            return Err(AppError::new(
                PATH,
                "cluster_transactions_by_client",
                "00",
                error,
            ));
        }

        // send remaining data to write queue
        if tx_cluster.client_txns().len() > 0 {
            if block > 0 {
                block += 1;
            }

            println!(
                "send remaining data to write queue --> block: {}, num clients: {}",
                block,
                tx_cluster.client_txns().len()
            );
            q.add(tx_cluster)?;
        }

        q.stop()?;
        // timer.stop();
        Ok(())
    }

    fn summarize_transactions_by_client(&self, working_dir: &str) -> Result<(), AppError> {
        // let timer = Timer::start();
        let paths = fs::read_dir(working_dir)
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

        let mut q = TxSummaryQueue::new(ACCOUNT_DIR, dir_paths);
        q.start()?;
        q.stop()?;
        // timer.stop();
        Ok(())
    }
}

// fn cluster_transactions_by_client(&self) -> Result<(), AppError> {
//     let timer = Timer::start();
//     let f = fs::File::open(self.file_path)
//         .map_err(|e| AppError::new(PATH, FN_PROCESS_CSV, "00", &e.to_string()))?;

//     let mut rdr = csv::Reader::from_reader(f);
//     let mut block: usize = 0;
//     let mut rows: usize = 0;

//     let mut map: HashMap<Vec<u8>, Vec<ByteRecord>> = HashMap::new();
//     let out_dir = self.file_dir()?;
//     let mut q = TxClusterQueue::new(&out_dir);
//     let mut record = ByteRecord::new();
//     let mut block_timer = Timer::start();

//     q.start()?;
//     while rdr.read_byte_record(&mut record).map_err(|e| {
//         let line_num = (block * BLOCK_SIZE + rows).to_string();
//         let err = [
//             "read_byte_record failed in line ",
//             &line_num,
//             " --> ",
//             &e.to_string(),
//         ]
//         .join("");
//         AppError::new(PATH, FN_PROCESS_CSV, "03", &err)
//     })? {
//         // serde serialize validation
//         if map.contains_key(&record[CLIENT_POS]) {
//             map.entry(record[CLIENT_POS].to_vec()).and_modify(|e| {
//                 e.push(record.clone());
//             });
//         } else {
//             let mut v = Vec::new();
//             v.push(record.clone());
//             map.insert(record[CLIENT_POS].to_vec(), v);
//         }

//         rows += 1;
//         if rows == BLOCK_SIZE {
//             if block == 0 {
//                 println!("----------------------------------------------------");
//             }

//             println!("add to q --> block: {}, num clients: {}", block, map.len());
//             q.add(TxClusterQueueBlock::new(block, map))?;
//             map = HashMap::new();

//             rows = 0;
//             block += 1;

//             block_timer.stop();
//             println!("----------------------------------------------------");

//             block_timer = Timer::start();
//         }
//     }

//     // send remaining data to write queue
//     if map.len() > 0 {
//         if block > 0 {
//             block += 1;
//         }

//         println!(
//             "send remaining data to write queue --> block: {}, num clients: {}",
//             block,
//             map.len()
//         );
//         q.add(TxClusterQueueBlock::new(block, map))?;
//     }

//     q.stop()?;
//     timer.stop();
//     Ok(())
// }
