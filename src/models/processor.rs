use std::fs;

use super::tx_cluster::{TxCluster, TxClusterData};
use super::tx_cluster_queue::TxClusterQueue;
use super::tx_record::TxRecordReader;
use super::tx_summary::TxSummary;
use super::tx_summary_queue::TxSummaryQueue;
use crate::lib::constants::{CLUSTER_DIR, SUMMARY_DIR};
use crate::lib::error::AppError;
use crate::lib::timer::Timer;
use crate::lib::tx_queue::TxQueue;

const PATH: &str = "model/processor";
const BLOCK_SIZE: usize = 1_000_000;

pub struct Processor<'a> {
    source_csv_path: &'a str,
}

impl<'a> Processor<'a> {
    pub fn new(source_csv_path: &'a str) -> Self {
        Self { source_csv_path }
    }

    pub fn process_csv(&self, cleanup: bool) -> Result<(), AppError> {
        let timer = Timer::start();
        let working_dir = self.file_dir(CLUSTER_DIR)?;
        self.cluster_transactions_by_client(&working_dir)?;
        self.summarize_transactions_by_client(&working_dir)?;
        if cleanup {
            let account_dir = self.file_dir(SUMMARY_DIR)?;
            let _ = fs::remove_dir_all(working_dir);
            let _ = fs::remove_dir_all(account_dir);
        }
        timer.stop();
        Ok(())
    }

    fn file_dir(&self, base: &str) -> Result<String, AppError> {
        let v: Vec<&str> = self.source_csv_path.split("/").collect();
        if v.len() > 0 {
            let file_name = v[v.len() - 1];
            if file_name.len() > 0 {
                let v: Vec<&str> = file_name.split(".").collect();
                return Ok([base, v[0]].join("/"));
            }
        }
        Err(AppError::new(PATH, "file_dir", "01", "invalid file path"))
    }

    fn cluster_transactions_by_client(&self, working_dir: &str) -> Result<(), AppError> {
        let mut tx_cluster = TxCluster::new(0);
        let mut tx_reader = TxRecordReader::new(self.source_csv_path)?;
        let mut q = TxClusterQueue::new(working_dir);

        q.start()?;
        let mut block: usize = 0;
        let mut rows: usize = 0;

        while tx_reader.next_record() {
            tx_cluster.add_tx(tx_reader.tx_record_client(), tx_reader.byte_record());
            if tx_reader.tx_record_type().conflict_type() {
                tx_cluster.add_conflict(tx_reader.tx_record_client(), tx_reader.tx_record_tx());
            }

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
        if let Some(error) = tx_reader.error() {
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
        let summaries = TxSummary::summaries(working_dir)?;
        let account_dir = self.file_dir(SUMMARY_DIR)?;
        let mut q = TxSummaryQueue::new(&account_dir, summaries);
        q.start()?;
        q.stop()?;
        Ok(())
    }
}
