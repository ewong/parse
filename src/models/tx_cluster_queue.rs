use crossbeam_channel::Receiver;
use std::sync::{Arc, Mutex};

use super::tx_cluster::TxClusterData;
use super::tx_record::TxRecordWriter;
use crate::lib::error::AppError;
// use crate::lib::timer::Timer;
use crate::lib::tx_queue::TxQueue;

// const PATH: &str = "model/write_queue";
// const FN_PROCESS_ENTRY: &str = "process_entry";

const NUM_THREADS: u16 = 3;
const THREAD_SLEEP_DURATION: u64 = 500;

pub struct TxClusterQueue<E> {
    started: bool,
    rx: Option<Receiver<bool>>,
    out_dir: String,
    arc_shutdown: Arc<Mutex<bool>>,
    arc_q: Arc<Mutex<Vec<E>>>,
}

impl<E> TxClusterQueue<E>
where
    E: TxClusterData,
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

impl<T> TxQueue<T> for TxClusterQueue<T>
where
    T: TxClusterData,
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

    fn process_entry(out_dir: &str, entry: &T) -> Result<(), AppError> {
        let mut wtr_opt: Option<TxRecordWriter> = None;

        for (client_id, records) in entry.client_txns() {
            let dir_path = [out_dir, "/", &client_id.to_string()].join("");
            let file_name = &entry.block().to_string();

            if wtr_opt.is_none() {
                wtr_opt = Some(TxRecordWriter::new(&dir_path, &file_name)?);
            }

            if let Some(wtr) = &mut wtr_opt {
                wtr.set_writer(&dir_path, &file_name)?;
                wtr.write_records(records)?;
                if let Some(set) = entry.client_conflicts().get(client_id) {
                    let conflict_dir = [out_dir, &client_id.to_string(), "conflicts"].join("/");
                    wtr.write_conflicted_tx_ids(&conflict_dir, &file_name, set)?;
                }
            }
        }

        Ok(())
    }
}