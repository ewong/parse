use crossbeam_channel::Receiver;
use csv::ByteRecord;
use std::fmt::{Debug, Display};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use super::error::AppError;
use super::timer::Timer;
use super::tx_zip_row::TxZipInputRow;
use crate::traits::tx_queue::TxQueue;

const PATH: &str = "model/client_queue";
const FN_PROCESS_ENTRY: &str = "process_entry";

const NUM_THREADS: u16 = 64;
const THREAD_SLEEP_DURATION: u64 = 100;

pub struct TxZipQueue<T> {
    started: bool,
    rx: Option<Receiver<bool>>,
    out_dir: String,
    arc_shutdown: Arc<Mutex<bool>>,
    arc_q: Arc<Mutex<Vec<T>>>,
}

impl<T> TxZipQueue<T>
where
    T: Send + Sync + Display + Debug + AsRef<Path> + 'static,
{
    pub fn new(out_dir: &str, dir_paths: Vec<T>) -> Self {
        Self {
            started: false,
            rx: None,
            out_dir: out_dir.to_owned(),
            arc_shutdown: Arc::new(Mutex::new(true)),
            arc_q: Arc::new(Mutex::new(dir_paths)),
        }
    }
}

impl<T> TxQueue<T> for TxZipQueue<T>
where
    T: Send + Sync + Display + Debug + AsRef<Path> + 'static,
{
    fn num_threads() -> u16 {
        NUM_THREADS
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

    fn mtx_q(&self) -> &Arc<Mutex<Vec<T>>> {
        &self.arc_q
    }

    fn mtx_shutdown(&self) -> &Arc<Mutex<bool>> {
        &self.arc_shutdown
    }

    fn rx(&self) -> &Option<Receiver<bool>> {
        &self.rx
    }

    fn set_rx(&mut self, rx: Option<Receiver<bool>>) {
        self.rx = rx;
    }

    fn out_dir(&self) -> &str {
        &self.out_dir
    }

    fn process_entry(out_dir: &str, entry: &T, wid: u16) -> Result<(), AppError> {
        let timer = Timer::start();
        let paths = fs::read_dir(entry)
            .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "00", &e.to_string()))?;

        let mut file_paths: Vec<String> = paths
            .map(|e| {
                if let Ok(path) = e {
                    if !path.path().is_dir() {
                        return path.path().display().to_string();
                    }
                }
                "".to_string()
            })
            .filter(|s| s.len() > 0)
            .collect();
        file_paths.sort_by(|a, b| a.cmp(b));

        let mut count = 0.0;
        let mut record = ByteRecord::new();
        let mut tx_row = TxZipInputRow::new();

        for path in file_paths {
            let f = fs::File::open(&path)
                .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "00", &e.to_string()))?;

            let mut rdr = csv::Reader::from_reader(f);
            let headers = rdr
                .byte_headers()
                .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "02", &e.to_string()))?
                .clone();

            while rdr
                .read_byte_record(&mut record)
                .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "03", &e.to_string()))?
            {
                // validate type id
                // if &record[TYPE_POS] == TYPE_DEPOSIT || &record[TYPE_POS] == TYPE_WITHDRAW {
                //     println!("capture tx id ({:?}) in a separate file", &record[TX_POS]);
                // } else if &record[TYPE_POS] == TYPE_DISPUTE
                //     || &record[TYPE_POS] == TYPE_RESOLVE
                //     || &record[TYPE_POS] == TYPE_CHARGEBACK
                // {
                //     println!("write tx id ({:?}) in a separate file", &record[TX_POS]);
                // } else {
                //     println!(
                //         "write client id ({:?}) in tx_error.csv",
                //         &record[CLIENT_POS]
                //     );
                // }

                let row: TxZipInputRow = record.deserialize(Some(&headers)).map_err(|e| {
                    println!("{}", &e.to_string());
                    AppError::new(PATH, FN_PROCESS_ENTRY, "04", &e.to_string())
                })?;
                // if fail => write user id in tx_error.csv

                if tx_row.client_id == 0 {
                    tx_row.client_id = row.client_id;
                }

                tx_row.tx_id += row.tx_id;
                count += 1.0;
            }

            tx_row.tx_id = tx_row.tx_id / count;
        }

        println!(
            "worker {} wrote --> client {:?}, out_dir: {}",
            wid, tx_row, out_dir
        );
        timer.stop();
        Ok(())
    }
}
