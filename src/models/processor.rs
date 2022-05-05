use std::fs;

use super::balance_queue::{BalanceQueue, SummaryPath};
use super::tx_cluster::{TxCluster, TxClusterData, TxClusterPath};
use super::tx_cluster_queue::TxClusterQueue;
use super::tx_reader::TxRecordReader;
use super::tx_summary_queue::TxSummaryQueue;
use crate::lib::constants::{ACCOUNT_DIR, CLUSTER_DIR, FN_NEW, SUMMARY_DIR, TRANSACTION_DIR};
use crate::lib::error::AppError;
use crate::lib::tx_queue::TxQueue;

const PATH: &str = "model/processor";
const BLOCK_SIZE: usize = 1_000_000;
const MIN_CLUSTER_THREADS: u16 = 4;
const MAX_SUMMARY_THREADS: u16 = 64;
const MIN_SUMMARY_THREADS: u16 = 4;

pub struct Processor<'a> {
    source_csv_path: &'a str,
    csv_cluster_dir: String,
    csv_summary_dir: String,
}

impl<'a> Processor<'a> {
    pub fn new(source_csv_path: &'a str) -> Result<Self, AppError> {
        let csv_cluster_dir = Self::csv_base_dir(source_csv_path, CLUSTER_DIR)?;
        let csv_summary_dir = Self::csv_base_dir(source_csv_path, SUMMARY_DIR)?;

        fs::create_dir_all(csv_cluster_dir.clone())
            .map_err(|e| AppError::new(PATH, FN_NEW, "0", &e.to_string()))?;

        fs::create_dir_all(csv_summary_dir.clone())
            .map_err(|e| AppError::new(PATH, FN_NEW, "1", &e.to_string()))?;

        fs::create_dir_all(ACCOUNT_DIR.clone())
            .map_err(|e| AppError::new(PATH, FN_NEW, "2", &e.to_string()))?;

        fs::create_dir_all(TRANSACTION_DIR.clone())
            .map_err(|e| AppError::new(PATH, FN_NEW, "3", &e.to_string()))?;

        Ok(Self {
            source_csv_path,
            csv_cluster_dir,
            csv_summary_dir,
        })
    }

    pub fn process_csv(&self, enable_cleanup: bool) -> Result<(), AppError> {
        // let timer = Timer::start();

        let result = self.cluster_transactions_by_client();
        if result.is_err() {
            self.cleanup(enable_cleanup);
            return result;
        }

        let result = self.summarize_transactions_by_client();
        if result.is_err() {
            self.cleanup(enable_cleanup);
            return result;
        }

        let result = self.update_balances();
        // timer.stop();
        result
    }

    fn cluster_transactions_by_client(&self) -> Result<(), AppError> {
        let mut tx_cluster = TxCluster::new(0);
        let mut tx_reader = TxRecordReader::new(&self.source_csv_path)?;
        let mut q = TxClusterQueue::new(&self.csv_cluster_dir, MIN_CLUSTER_THREADS);

        q.start()?;
        let mut block: usize = 0;
        let mut rows: usize = 0;

        while tx_reader.next_record() {
            tx_cluster.add(tx_reader.tx_record_client(), tx_reader.byte_record());

            rows += 1;
            if rows == BLOCK_SIZE {
                // if block == 0 {
                //     println!("----------------------------------------------------");
                // }

                // println!(
                //     "add to q --> block: {}, num clients: {}",
                //     block,
                //     tx_cluster.tx_map().len()
                // );

                rows = 0;
                block += 1;
                q.add(tx_cluster)?;
                tx_cluster = TxCluster::new(block);
                // println!("----------------------------------------------------");
            }
        }

        // handle rollback
        if let Some(error) = tx_reader.error() {
            let _ = q.stop();
            return Err(AppError::new(
                PATH,
                &["cluster_transactions_by_client", &block.to_string()].join(" | "),
                "00",
                error,
            ));
        }

        // send remaining data to write queue
        if tx_cluster.tx_map().len() > 0 {
            if block > 0 {
                block += 1;
            }

            // println!(
            //     "send remaining data to write queue --> block: {}, num clients: {}",
            //     block,
            //     tx_cluster.tx_map().len()
            // );
            q.add(tx_cluster)?;
        }

        q.stop()?;
        Ok(())
    }

    fn summarize_transactions_by_client(&self) -> Result<(), AppError> {
        let cluster_paths = TxClusterPath::paths(&self.csv_cluster_dir)?;

        let num_summary_threads: u16;
        if cluster_paths.len() > u16::MAX as usize {
            num_summary_threads = MAX_SUMMARY_THREADS;
        } else {
            num_summary_threads = ((cluster_paths.len() as u16) / 1000) + MIN_SUMMARY_THREADS;
        }

        let mut q = TxSummaryQueue::new(&self.csv_summary_dir, cluster_paths, num_summary_threads);
        q.start()?;
        q.stop()?;
        Ok(())
    }

    fn update_balances(&self) -> Result<(), AppError> {
        let summary_files = SummaryPath::paths(true, &self.csv_summary_dir)?;
        let account_file_count = SummaryPath::count(ACCOUNT_DIR)?;

        let num_threads: u16;
        if summary_files.len() + account_file_count > u16::MAX as usize {
            num_threads = MAX_SUMMARY_THREADS;
        } else {
            num_threads = ((summary_files.len() as u16 + account_file_count as u16) / 1000)
                + MIN_SUMMARY_THREADS;
        }

        let mut q = BalanceQueue::new(summary_files, num_threads);
        q.start()?;
        q.stop()?;

        let mut account_files = SummaryPath::paths(false, ACCOUNT_DIR)?;
        let _ = q.add_block(&mut account_files);
        q.start()?;
        q.stop()?;
        Ok(())
    }

    fn csv_base_dir(source_csv_path: &str, base: &str) -> Result<String, AppError> {
        let v: Vec<&str> = source_csv_path.split("/").collect();
        if v.len() > 0 {
            let file_name = v[v.len() - 1];
            if file_name.len() > 0 {
                let v: Vec<&str> = file_name.split(".").collect();
                return Ok([base, v[0]].join("/"));
            }
        }
        Err(AppError::new(PATH, "file_dir", "01", "invalid file path"))
    }

    fn cleanup(&self, enable_cleanup: bool) {
        if enable_cleanup {
            let _ = fs::remove_dir_all(&self.csv_cluster_dir);
            let _ = fs::remove_dir_all(&self.csv_summary_dir);
        }
    }
}
