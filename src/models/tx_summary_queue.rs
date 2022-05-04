use crossbeam_channel::{Receiver, Sender};
use std::fs;
use std::sync::{Arc, RwLock};

use crate::lib::constants::ACCOUNT_DIR;
use crate::lib::error::AppError;
use crate::lib::tx_queue::TxQueue;

use super::account::Account;
use super::tx_cluster::TxClusterPathData;
use super::tx_history::TxHistory;
use super::tx_reader::TxRecordReader;

const PATH: &str = "model/client_queue";
const FN_PROCESS_ENTRY: &str = "process_entry";

const THREAD_SLEEP_DURATION: u64 = 100;

pub struct TxSummaryQueue<T> {
    started: bool,
    tx: Option<Sender<bool>>,
    rx: Option<Receiver<Result<u16, AppError>>>,
    num_threads: u16,
    summary_dir: String,
    arc_q: Arc<RwLock<Vec<T>>>,
}

impl<T> TxSummaryQueue<T>
where
    T: TxClusterPathData,
{
    pub fn new(summary_dir: &str, dir_paths: Vec<T>, num_threads: u16) -> Self {
        Self {
            started: false,
            tx: None,
            rx: None,
            num_threads,
            summary_dir: summary_dir.to_owned(),
            arc_q: Arc::new(RwLock::new(dir_paths)),
        }
    }

    fn file_path_and_names_in_client_tx_dir(entry: &T) -> Result<Vec<String>, AppError> {
        let paths = fs::read_dir(entry.dir_path())
            .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "00", &e.to_string()))?;

        let mut file_paths: Vec<(String, String)> = paths
            .map(|e| {
                if e.is_err() {
                    return ("".to_string(), "".to_string());
                }

                let path = e.unwrap();
                if path.path().file_name().is_none() {
                    return ("".to_string(), "".to_string());
                }

                if !path.path().is_file() {
                    return ("".to_string(), "".to_string());
                }

                (
                    path.path().display().to_string(),
                    path.path()
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string(),
                )
            })
            .filter(|s| s.0.len() > 0)
            .collect();

        if file_paths.len() == 0 {
            return Ok(Vec::new());
        }

        file_paths.sort_by(|a, b| {
            // all client tx csv files are of the format client_id.csv
            // println!("a: {:?}, b: {:?}", a, b);

            let x: Vec<&str> = a.1.split(".").collect();
            let y: Vec<&str> = b.1.split(".").collect();

            let client_id0 = x[0].to_string().parse::<u16>().unwrap();
            let client_id1 = y[0].to_string().parse::<u16>().unwrap();

            client_id0.cmp(&client_id1)
        });

        let fps: Vec<String> = file_paths.iter().map(|e| e.0.clone()).collect();
        Ok(fps)
    }
}

impl<T> TxQueue<T> for TxSummaryQueue<T>
where
    T: TxClusterPathData,
{
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
        &self.summary_dir
    }

    fn process_entry(out_dir: &str, entry: &T) -> Result<(), AppError> {
        let file_paths = Self::file_path_and_names_in_client_tx_dir(entry)?;

        if file_paths.len() == 0 {
            return Ok(());
        }

        let mut tx_history = TxHistory::new(&entry.client_id());
        let mut account = Account::new(entry.client_id(), ACCOUNT_DIR);
        let mut tx_reader = TxRecordReader::new(&file_paths.get(0).unwrap())?;
        let mut initial_loop = true;

        for path in file_paths {
            if initial_loop {
                tx_reader.set_reader(&path)?;
                initial_loop = false;
            }

            while tx_reader.next_record() {
                if let Some(e) = tx_reader.error() {
                    return Err(AppError::new(PATH, FN_PROCESS_ENTRY, "01", &e.to_string()));
                }

                if *tx_reader.tx_record_client() != entry.client_id() {
                    continue;
                }

                account.handle_tx(
                    tx_reader.tx_record_type(),
                    tx_reader.tx_record_tx(),
                    tx_reader.tx_record_amount(),
                    &mut tx_history,
                );

                tx_history.set_tx(
                    tx_reader.tx_record_type(),
                    tx_reader.tx_record_client(),
                    tx_reader.tx_record_tx(),
                    tx_reader.tx_record_amount(),
                );

                tx_history.get_tx(tx_reader.tx_record_tx());
            }
        }

        account.write_to_csv(out_dir)?;
        Ok(())
    }
}
