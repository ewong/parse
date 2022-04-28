use crossbeam_channel::{unbounded, Receiver};
use csv::ByteRecord;
// use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use super::error::AppError;
use super::timer::Timer;

const PATH: &str = "model/write_queue";
// const TXN_DIR: &str = "parse/txns";
// const FN_WRITE_CSV: &str = "write_client_txns";

const MTX_NUM_TRIES: u8 = 3;
const MTX_SLEEP_DURATION: u64 = 20;

pub trait WriteQueueEntry: Send + Sync + 'static {
    fn tx_type() -> bool {
        true
    }
    fn block(&self) -> usize {
        0
    }
    fn map(&self) -> &HashMap<Vec<u8>, Vec<ByteRecord>>;
}

pub struct WriteQueue<T> {
    started: bool,
    num_threads: u8,
    rx: Option<Receiver<bool>>,
    mtx_shutdown: Arc<Mutex<bool>>,
    // mtx_q: Arc<Mutex<Vec<HashMap<Vec<u8>, Vec<ByteRecord>>>>>,
    mtx_q: Arc<Mutex<Vec<T>>>,
}

impl<T> WriteQueue<T>
where
    T: WriteQueueEntry,
{
    pub fn new(num_threads: u8) -> Self {
        Self {
            started: false,
            num_threads,
            rx: None,
            mtx_shutdown: Arc::new(Mutex::new(true)),
            mtx_q: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn start(&mut self) -> Result<(), AppError> {
        if self.is_shutdown()? {
            self.started = true;
            self.set_shutdown(false)?;
            self.spawn_workers()?;
            println!("WriteQueue has started!");
        }
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), AppError> {
        if self.started {
            self.started = false;
            self.set_shutdown(true)?;

            // block until all threads are done
            if let Some(rx) = &self.rx {
                for _ in 0..self.num_threads {
                    rx.recv().unwrap();
                }
            }
        }
        Ok(())
    }

    // pub fn add(&self, map: HashMap<Vec<u8>, Vec<ByteRecord>>) -> Result<(), AppError> {
    pub fn add(&self, map: T) -> Result<(), AppError> {
        let mut mtx_q = self.get_queue()?;
        let q = &mut (*mtx_q);
        q.push(map);
        Ok(())
    }

    // private methods

    fn get_shutdown(&self) -> Result<MutexGuard<bool>, AppError> {
        for _ in 1..MTX_NUM_TRIES {
            if let Ok(mtx_shutdown) = self.mtx_shutdown.lock() {
                return Ok(mtx_shutdown);
            } else {
                thread::sleep(Duration::from_millis(MTX_SLEEP_DURATION));
            }
        }
        let msg = "error accessing shutdown lock";
        Err(AppError::new(PATH, "get_shutdown", "00", msg))
    }

    fn set_shutdown(&self, value: bool) -> Result<(), AppError> {
        let mut mtx_shutdown = self.get_shutdown()?;
        *mtx_shutdown = value;
        Ok(())
    }

    fn is_shutdown(&self) -> Result<bool, AppError> {
        let mtx_shutdown = self.get_shutdown()?;
        let result = *mtx_shutdown;
        Ok(result)
    }

    // fn get_queue(&self) -> Result<MutexGuard<Vec<HashMap<Vec<u8>, Vec<ByteRecord>>>>, AppError> {
    fn get_queue(&self) -> Result<MutexGuard<Vec<T>>, AppError> {
        for _ in 1..MTX_NUM_TRIES {
            if let Ok(mtx_q) = self.mtx_q.lock() {
                return Ok(mtx_q);
            } else {
                thread::sleep(Duration::from_millis(MTX_SLEEP_DURATION));
            }
        }
        let msg = "error accessing mtx_q lock";
        Err(AppError::new(PATH, "get_queue", "00", msg))
    }

    fn spawn_workers(&mut self) -> Result<(), AppError> {
        let (s, r) = unbounded();
        self.rx = Some(r);

        for i in 0..self.num_threads {
            let mtx_q = Arc::clone(&self.mtx_q);
            let mtx_shutdown = Arc::clone(&self.mtx_shutdown);
            let tx = s.clone();

            thread::spawn(move || loop {
                // check if there are items to process
                if let Ok(mq) = &mut mtx_q.lock() {
                    let q = &mut (*mq);
                    if let Some(entry) = q.pop() {
                        drop(q);
                        drop(mq);
                        for (k, v) in entry.map() {
                            let timer = Timer::start();
                            let client = String::from_utf8(k.clone()).unwrap();
                            let mut rows = 0;
                            for _ in v {
                                rows += 1;
                            }
                            println!(
                                "worker {} wrote --> block: {}, client: {:?}, num rows: {}",
                                i,
                                entry.block(),
                                client,
                                rows
                            );
                            timer.stop();
                        }
                    }
                }

                // check if need to shut down
                if let Ok(shutdown) = mtx_shutdown.lock() {
                    if *shutdown {
                        println!("worker {} is shutdown", i);
                        tx.send(true).unwrap();
                        return;
                    }
                }

                // sleep
                thread::sleep(Duration::from_millis(2000));
            });
            println!("spawned worker {}", i);
        }

        Ok(())
    }

    //     pub fn write_client_txns(
    //         &self,
    //         client_id: &u32,
    //         block_id: &usize,
    //         tx_rows: &Vec<TxRow>,
    //     ) -> Result<(), AppError> {
    //         let client_str = ["client", &client_id.to_string()].join("_");
    //         let dir_path = [OUTPUT_DIR, &client_str].join("/");
    //         fs::create_dir_all(&dir_path)
    //             .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "00", &e.to_string()))?;

    //         let file_path = [
    //             &dir_path,
    //             "/",
    //             &client_str,
    //             "_block_",
    //             &block_id.to_string(),
    //             ".csv",
    //         ]
    //         .join("");

    //         let mut wtr = csv::Writer::from_path(&file_path)
    //             .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "01", &e.to_string()))?;

    //         wtr.write_record(&["userId", "movieId", "rating", "timestamp"])
    //             .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "02", &e.to_string()))?;

    //         for row in tx_rows {
    //             wtr.write_record(&[
    //                 &row.type_id.to_string(),
    //                 &row.client_id.to_string(),
    //                 &row.tx_id.to_string(),
    //                 &row.amount.unwrap_or(0).to_string(),
    //             ])
    //             .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "03", &e.to_string()))?;
    //         }

    //         wtr.flush()
    //             .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "04", &e.to_string()))?;

    //         Ok(())
    //     }
}

// #[derive(Debug, Deserialize, Serialize, Clone)]
// pub struct TxRow<'a> {
//     #[serde(rename(deserialize = "userId", serialize = "userId"))]
//     pub type_id: &'a str,
//     #[serde(rename(deserialize = "movieId", serialize = "movieId"))]
//     pub client_id: u32,
//     #[serde(rename(deserialize = "rating", serialize = "rating"))]
//     pub tx_id: f32,
//     #[serde(rename(deserialize = "timestamp", serialize = "timestamp"))]
//     pub amount: Option<u32>,
// }
