use std::fs;

use super::account::AccountPath;
use super::balancer::Balancer;
use super::tx_cluster::TxCluster;
use super::tx_reader::TxReader;
use super::tx_record::TxRow;
use super::updater::Updater;
use crate::lib::constants::{
    ACCOUNT_BACKUP_DIR, ACCOUNT_DIR, FN_NEW, SUMMARY_DIR, TRANSACTION_DIR,
};
use crate::lib::error::AppError;

const PATH: &str = "model/processor";
const BLOCK_SIZE: usize = 1_000_000;

pub struct Processor<'a> {
    source_csv_path: &'a str,
    csv_summary_dir: String,
}

impl<'a> Processor<'a> {
    pub fn new(source_csv_path: &'a str) -> Result<Self, AppError> {
        let csv_summary_dir = Self::csv_base_dir(source_csv_path, SUMMARY_DIR)?;

        let _ = fs::remove_dir_all(&csv_summary_dir);

        fs::create_dir_all(csv_summary_dir.clone())
            .map_err(|e| AppError::new(PATH, FN_NEW, "1", &e.to_string()))?;

        fs::create_dir_all(ACCOUNT_DIR.clone())
            .map_err(|e| AppError::new(PATH, FN_NEW, "2", &e.to_string()))?;

        fs::create_dir_all(ACCOUNT_BACKUP_DIR.clone())
            .map_err(|e| AppError::new(PATH, FN_NEW, "2", &e.to_string()))?;

        fs::create_dir_all(TRANSACTION_DIR.clone())
            .map_err(|e| AppError::new(PATH, FN_NEW, "3", &e.to_string()))?;

        Ok(Self {
            source_csv_path,
            csv_summary_dir,
        })
    }

    #[allow(dead_code)]
    pub fn set_source_path(&mut self, source_csv_path: &'a str) -> Result<(), AppError> {
        let csv_summary_dir = Self::csv_base_dir(source_csv_path, SUMMARY_DIR)?;
        let _ = fs::remove_dir_all(&csv_summary_dir);
        fs::create_dir_all(csv_summary_dir.clone())
            .map_err(|e| AppError::new(PATH, FN_NEW, "1", &e.to_string()))?;
        self.source_csv_path = source_csv_path;
        self.csv_summary_dir = csv_summary_dir;
        Ok(())
    }

    pub fn process_data(&self, enable_cleanup: bool) -> Result<(), AppError> {
        let result = self.cluster_transactions();
        if result.is_err() {
            self.cleanup(enable_cleanup);
            return result;
        }

        let result = self.update_accounts();
        if result.is_err() {
            self.cleanup(enable_cleanup);
            return result;
        }

        let result = self.show_accounts();
        result
    }

    pub fn cluster_transactions(&self) -> Result<(), AppError> {
        let mut tx_cluster = TxCluster::new();
        let mut tx_reader = TxReader::new(&self.source_csv_path)?;
        let mut balancer = Balancer::new(&self.csv_summary_dir);

        balancer.start()?;
        let mut rows: usize = 0;

        while tx_reader.next_record() {
            let tx_id = tx_reader.tx_record_type().clone();
            let tx_row = TxRow::new(
                tx_id,
                tx_reader.tx_record_client().clone(),
                tx_reader.tx_record_tx().clone(),
                tx_reader.tx_record_amount().clone(),
            );
            tx_cluster.add(tx_row);

            rows += 1;
            if rows == BLOCK_SIZE {
                rows = 0;
                balancer.add(tx_cluster)?;
                tx_cluster = TxCluster::new();
            }
        }

        // handle rollback
        if let Some(error) = tx_reader.error() {
            let _ = balancer.stop();
            return Err(AppError::new(
                PATH,
                "cluster_transactions_by_client",
                "00",
                error,
            ));
        }

        // send remaining data to write queue
        if tx_cluster.tx_row_map.len() > 0 {
            balancer.add(tx_cluster)?;
        }

        balancer.stop()?;
        Ok(())
    }

    fn update_accounts(&self) -> Result<(), AppError> {
        let mut updater = Updater::new();
        let batches = AccountPath::paths(true, &self.csv_summary_dir)?;

        updater.start()?;
        for files in batches {
            updater.add(files)?;
        }
        updater.stop()?;
        Ok(())
    }

    fn show_accounts(&self) -> Result<(), AppError> {
        let mut updater = Updater::new();
        let batches = AccountPath::paths(false, ACCOUNT_DIR)?;
        updater.start()?;
        for files in batches {
            updater.add(files)?;
        }
        updater.stop()?;

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
            let _ = fs::remove_dir_all(&self.csv_summary_dir);
        }
    }
}
