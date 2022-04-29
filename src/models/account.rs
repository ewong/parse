use std::fs;

use super::client_queue::ClientQueue;
use super::error::AppError;
use super::queue::Queue;
use super::timer::Timer;

const PATH: &str = "models/account";
const INPUT_DIR: &str = "data/transactions";
// const OUTPUT_DIR: &str = "data/accounts";

const FN_MERGE_TXNS: &str = "merge_txns_by_client";

pub struct Account;

impl Account {
    pub fn merge_txns_by_client() -> Result<(), AppError> {
        let timer = Timer::start();
        let paths = fs::read_dir(INPUT_DIR)
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
        let mut cq = ClientQueue::new(dir_paths);
        cq.start()?;
        cq.stop()?;
        timer.stop();
        Ok(())
    }
}
