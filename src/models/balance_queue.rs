use crossbeam_channel::{Receiver, Sender};
use std::fs;
use std::sync::{Arc, RwLock};

use crate::lib::constants::ACCOUNT_DIR;
use crate::lib::error::AppError;
use crate::lib::tx_queue::TxQueue;

const PATH: &str = "model/balance_queue";

const THREAD_SLEEP_DURATION: u64 = 100;

pub trait SummaryPathData: Send + Sync + 'static {
    fn update_file(&self) -> bool;
    fn file_path(&self) -> &str;
}

#[derive(Debug)]
pub struct SummaryPath {
    update_file: bool,
    file_path: String,
}

impl SummaryPath {
    pub fn new() -> Self {
        Self {
            update_file: true,
            file_path: "".to_string(),
        }
    }

    pub fn paths(update_file: bool, dir: &str) -> Result<Vec<SummaryPath>, AppError> {
        let p =
            fs::read_dir(dir).map_err(|e| AppError::new(PATH, "paths", "00", &e.to_string()))?;

        let paths: Vec<SummaryPath> = p
            .map(|e| {
                if let Ok(path) = e {
                    if path.path().is_dir() {
                        return SummaryPath::new();
                    }

                    return SummaryPath {
                        update_file,
                        file_path: path.path().display().to_string(),
                    };
                }
                SummaryPath::new()
            })
            .filter(|s| s.file_path.len() > 0)
            .collect();

        Ok(paths)
    }
}

impl SummaryPathData for SummaryPath {
    fn update_file(&self) -> bool {
        self.update_file
    }

    fn file_path(&self) -> &str {
        &self.file_path
    }
}

pub struct BalanceQueue<T> {
    started: bool,
    tx: Option<Sender<bool>>,
    rx: Option<Receiver<Result<u16, AppError>>>,
    num_threads: u16,
    arc_q: Arc<RwLock<Vec<T>>>,
}

impl<T> BalanceQueue<T>
where
    T: SummaryPathData,
{
    pub fn new(file_paths: Vec<T>, num_threads: u16) -> Self {
        Self {
            started: false,
            tx: None,
            rx: None,
            num_threads,
            arc_q: Arc::new(RwLock::new(file_paths)),
        }
    }
}

impl<T> TxQueue<T> for BalanceQueue<T>
where
    T: SummaryPathData,
{
    fn max_queue_len() -> usize {
        64000
    }

    fn num_threads(&self) -> u16 {
        self.num_threads
    }

    fn thread_sleep_duration() -> u64 {
        THREAD_SLEEP_DURATION
    }

    fn started(&self) -> bool {
        self.started
    }

    fn set_started(&mut self, value: bool) {
        self.started = value;
    }

    fn mtx_q(&self) -> &Arc<RwLock<Vec<T>>> {
        &self.arc_q
    }

    fn rx(&self) -> &Option<Receiver<Result<u16, AppError>>> {
        &self.rx
    }

    fn set_rx(&mut self, rx: Option<Receiver<Result<u16, AppError>>>) {
        self.rx = rx;
    }

    fn tx(&self) -> &Option<Sender<bool>> {
        &self.tx
    }

    fn set_tx(&mut self, tx: Option<Sender<bool>>) {
        self.tx = tx;
    }

    fn out_dir(&self) -> &str {
        ACCOUNT_DIR
    }

    fn process_entry(_out_dir: &str, entry: &T) -> Result<(), AppError> {
        println!(
            "updating balances !!!! {}, {}",
            entry.update_file(),
            entry.file_path()
        );
        Ok(())
    }
}
