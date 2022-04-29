use crossbeam_channel::{unbounded, Receiver};
use csv::ByteRecord;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use super::error::AppError;
use super::timer::Timer;

const PATH: &str = "model/write_queue";
const TX_DIR: &str = "data/transactions";
const FN_WRITE_TX: &str = "write_tx";
const EXT: &str = ".csv";

const MTX_NUM_TRIES: u8 = 3;
const MTX_SLEEP_DURATION: u64 = 20;
const THREAD_SLEEP_DURATION: u64 = 2000;

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
                let res = mtx_q.lock();
                if res.is_err() {
                    thread::sleep(Duration::from_millis(4 * MTX_SLEEP_DURATION));
                    continue;
                }

                let mut mgq = res.unwrap();
                let q = &mut (*mgq);

                if q.len() == 0 {
                    drop(q);
                    drop(mgq);
                    if let Ok(shutdown) = mtx_shutdown.lock() {
                        if *shutdown {
                            println!("worker {} is shutdown", i);
                            tx.send(true).unwrap();
                            return;
                        }
                    }
                    continue;
                }

                if let Some(entry) = q.pop() {
                    drop(q);
                    drop(mgq);
                    let _ = WriteQueue::write_tx(i, &entry);
                }

                // sleep
                thread::sleep(Duration::from_millis(THREAD_SLEEP_DURATION));
            });
            println!("spawned worker {}", i);
        }

        Ok(())
    }

    fn write_tx(i: u8, entry: &T) -> Result<(), AppError> {
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
