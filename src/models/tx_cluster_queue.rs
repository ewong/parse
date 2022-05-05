use crossbeam_channel::{Receiver, Sender};
use std::sync::{Arc, RwLock};

use super::tx_cluster::TxClusterData;
use super::tx_writer::TxWriter;
use crate::lib::error::AppError;
use crate::lib::tx_queue::TxQueue;

const THREAD_SLEEP_DURATION: u64 = 500;

pub struct TxClusterQueue<E> {
    started: bool,
    tx: Option<Sender<bool>>,
    rx: Option<Receiver<Result<u16, AppError>>>,
    num_threads: u16,
    csv_cluster_dir: String,
    arc_q: Arc<RwLock<Vec<E>>>,
}

impl<E> TxClusterQueue<E>
where
    E: TxClusterData,
{
    pub fn new(csv_cluster_dir: &str, num_threads: u16) -> Self {
        Self {
            started: false,
            tx: None,
            rx: None,
            num_threads,
            csv_cluster_dir: csv_cluster_dir.to_owned(),
            arc_q: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl<T> TxQueue<T> for TxClusterQueue<T>
where
    T: TxClusterData,
{
    fn max_queue_len() -> usize {
        5
    }

    fn num_threads(&self) -> u16 {
        self.num_threads
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
        &self.csv_cluster_dir
    }

    fn process_entry(out_dir: &str, entry: &T) -> Result<(), AppError> {
        let mut wtr_opt: Option<TxWriter> = None;

        for (client_id, records) in entry.tx_map() {
            let dir_path = [out_dir, "/", &client_id.to_string()].join("");
            let file_name = &entry.block().to_string();

            if wtr_opt.is_none() {
                wtr_opt = Some(TxWriter::new(&dir_path, &file_name)?);
            }

            if let Some(wtr) = &mut wtr_opt {
                wtr.set_writer(&dir_path, &file_name)?;
                wtr.write_records(records)?;
                // if let Some(set) = entry.tx_conflict_map().get(client_id) {
                //     if let Some(deposit_withdraw_map) =
                //         entry.tx_deposit_withdraw_map().get(client_id)
                //     {
                //         wtr.write_conflicts(
                //             &dir_path,
                //             &file_name,
                //             set,
                //             deposit_withdraw_map,
                //             records,
                //         )?;
                //     }
                // }
            }
        }

        Ok(())
    }
}
