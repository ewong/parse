use crossbeam_channel::Receiver;
use csv::ByteRecord;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, Mutex};

use super::error::AppError;
use super::queue::Queue;
use super::timer::Timer;

const PATH: &str = "model/write_queue";
const TX_DIR: &str = "data/transactions";
const FN_WRITE_TX: &str = "write_tx";
const EXT: &str = ".csv";

pub trait TxQueueEntry: Send + Sync + 'static {
    fn block(&self) -> usize;
    fn map(&self) -> &HashMap<Vec<u8>, Vec<ByteRecord>>;
}

pub struct TxBlock {
    block: usize,
    map: HashMap<Vec<u8>, Vec<ByteRecord>>,
}

impl TxBlock {
    pub fn new(block: usize, map: HashMap<Vec<u8>, Vec<ByteRecord>>) -> Self {
        Self { block, map }
    }
}

impl TxQueueEntry for TxBlock {
    fn block(&self) -> usize {
        self.block
    }

    fn map(&self) -> &HashMap<Vec<u8>, Vec<ByteRecord>> {
        &self.map
    }
}

pub struct TxQueue<E> {
    started: bool,
    num_threads: u8,
    rx: Option<Receiver<bool>>,
    arc_shutdown: Arc<Mutex<bool>>,
    arc_q: Arc<Mutex<Vec<E>>>,
}

impl<E> TxQueue<E>
where
    E: TxQueueEntry,
{
    pub fn new(num_threads: u8) -> Self {
        Self {
            started: false,
            num_threads,
            rx: None,
            arc_shutdown: Arc::new(Mutex::new(true)),
            arc_q: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl<T> Queue<T> for TxQueue<T>
where
    T: TxQueueEntry,
{
    fn num_threads(&self) -> u8 {
        self.num_threads
    }

    fn started(&self) -> bool {
        println!("workers started: {}", self.started);
        self.started
    }

    fn set_started(&mut self, value: bool) {
        println!("workers started: {}", value);
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

    fn process_entry(i: u8, entry: &T) -> Result<(), AppError> {
        for (k, v) in entry.map() {
            let timer = Timer::start();

            let client_id_str = String::from_utf8(k.to_vec())
                .map_err(|e| AppError::new(PATH, FN_WRITE_TX, "00", &e.to_string()))?;

            let dir_path = [TX_DIR, "/cid_", &client_id_str].join("");
            fs::create_dir_all(&dir_path)
                .map_err(|e| AppError::new(PATH, FN_WRITE_TX, "01", &e.to_string()))?;

            let file_path = [
                &dir_path,
                "/cid_",
                &client_id_str,
                "_blk_",
                &entry.block().to_string(),
                EXT,
            ]
            .join("");

            let mut wtr = csv::Writer::from_path(&file_path)
                .map_err(|e| AppError::new(PATH, FN_WRITE_TX, "02", &e.to_string()))?;
            wtr.write_byte_record(&ByteRecord::from(
                &["userId", "movieId", "rating", "timestamp"][..],
            ))
            .map_err(|e| AppError::new(PATH, FN_WRITE_TX, "03", &e.to_string()))?;

            let mut rows = 0;
            for record in v {
                wtr.write_byte_record(record)
                    .map_err(|e| AppError::new(PATH, FN_WRITE_TX, "04", &e.to_string()))?;
                rows += 1;
            }
            wtr.flush()
                .map_err(|e| AppError::new(PATH, FN_WRITE_TX, "05", &e.to_string()))?;
            println!(
                "worker {} wrote --> block: {}, client: {:?}, num rows: {}",
                i,
                entry.block(),
                client_id_str,
                rows
            );
            timer.stop();
        }

        Ok(())
    }
}
