use crossbeam_channel::Receiver;
use csv::ByteRecord;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, Mutex};

use super::error::AppError;
use super::queue::Queue;
use super::timer::Timer;

const PATH: &str = "model/write_queue";
const FN_PROCESS_ENTRY: &str = "process_entry";

const CLIENT_PARTITION: &str = "/client_";
const BLOCK_PARTITION: &str = "_block_";
const WORKER_PARTITION: &str = "_worker_";

const NUM_THREADS: u16 = 3;
const THREAD_SLEEP_DURATION: u64 = 500;

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
    rx: Option<Receiver<bool>>,
    out_dir: String,
    arc_shutdown: Arc<Mutex<bool>>,
    arc_q: Arc<Mutex<Vec<E>>>,
}

impl<E> TxQueue<E>
where
    E: TxQueueEntry,
{
    pub fn new(out_dir: &str) -> Self {
        Self {
            started: false,
            rx: None,
            out_dir: out_dir.to_owned(),
            arc_shutdown: Arc::new(Mutex::new(true)),
            arc_q: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl<T> Queue<T> for TxQueue<T>
where
    T: TxQueueEntry,
{
    fn num_threads() -> u16 {
        NUM_THREADS
    }

    fn thread_sleep_duration() -> u64 {
        THREAD_SLEEP_DURATION
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

    fn out_dir(&self) -> &str {
        &self.out_dir
    }

    fn process_entry(out_dir: &str, entry: &T, wid: u16) -> Result<(), AppError> {
        for (k, v) in entry.map() {
            let timer = Timer::start();

            let client_id_str = String::from_utf8(k.to_vec())
                .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "00", &e.to_string()))?;

            let dir_path = [out_dir, &client_id_str].join(CLIENT_PARTITION);
            fs::create_dir_all(&dir_path)
                .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "01", &e.to_string()))?;

            let file_path = [
                &dir_path,
                CLIENT_PARTITION,
                &client_id_str,
                BLOCK_PARTITION,
                &entry.block().to_string(),
                WORKER_PARTITION,
                &wid.to_string(),
                ".csv",
            ]
            .join("");

            let mut wtr = csv::Writer::from_path(&file_path)
                .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "02", &e.to_string()))?;
            wtr.write_byte_record(&ByteRecord::from(
                &["userId", "movieId", "rating", "timestamp"][..],
            ))
            .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "03", &e.to_string()))?;

            let mut rows = 0;
            for record in v {
                wtr.write_byte_record(record)
                    .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "04", &e.to_string()))?;
                rows += 1;
            }
            wtr.flush()
                .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "05", &e.to_string()))?;
            println!(
                "worker {} wrote --> block: {}, client: {:?}, num rows: {}",
                wid,
                entry.block(),
                client_id_str,
                rows
            );
            timer.stop();
        }

        Ok(())
    }
}
