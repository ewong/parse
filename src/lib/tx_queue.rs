use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use crate::lib::error::AppError;

const PATH: &str = "model/queue";
const MTX_SLEEP_DURATION: u64 = 20;

pub trait TxQueue<T: Send + Sync + 'static> {
    fn max_queue_len() -> usize;
    fn num_threads(&self) -> u16;
    fn thread_sleep_duration() -> u64;
    fn started(&self) -> bool;
    fn set_started(&mut self, value: bool);
    fn mtx_q(&self) -> &Arc<RwLock<Vec<T>>>;

    fn tx(&self) -> &Option<Sender<bool>>;
    fn set_tx(&mut self, tx: Option<Sender<bool>>);

    fn rx(&self) -> &Option<Receiver<Result<u16, AppError>>>;
    fn set_rx(&mut self, rx: Option<Receiver<Result<u16, AppError>>>);

    fn out_dir(&self) -> &str;
    fn process_entry(out_dir: &str, entry: &T) -> Result<(), AppError>;

    fn start(&mut self) -> Result<(), AppError> {
        if !self.started() {
            self.set_started(true);
            self.spawn_workers()?;
        }
        Ok(())
    }

    fn stop(&mut self) -> Result<(), AppError> {
        if self.started() {
            self.set_started(false);

            let mut err: Option<AppError> = None;
            if let Some(tx) = self.tx() {
                tx.send(true).unwrap();
                // block until all threads are done
                if let Some(rx) = self.rx() {
                    for _ in 0..self.num_threads() {
                        if let Ok(wid) = rx.recv() {
                            if wid.is_err() {
                                err = wid.err();
                            } else {
                                // println!("worker {} is shutdown", wid.unwrap());
                            }
                        }
                    }
                }
            }
            if err.is_some() {
                return Err(err.unwrap());
            }
        }
        Ok(())
    }

    fn add(&self, entry: T) -> Result<(), AppError> {
        let mut ok = false;
        loop {
            if let Ok(mtx_q) = &mut self.mtx_q().write() {
                let q = &mut (*mtx_q);
                q.push(entry);
                if q.len() >= Self::max_queue_len() {
                    thread::sleep(Duration::from_millis(1000));
                }
                ok = true;
                break;
            } else {
                thread::sleep(Duration::from_millis(MTX_SLEEP_DURATION));
            }
        }

        if ok {
            return Ok(());
        }
        let msg = "error accessing mtx_q lock";
        return Err(AppError::new(PATH, "get_queue", "00", msg));
    }

    fn add_block(&self, block: &mut Vec<T>) -> Result<(), AppError> {
        let mut ok = false;
        loop {
            if let Ok(mtx_q) = &mut self.mtx_q().write() {
                let q = &mut (*mtx_q);
                q.append(block);
                if q.len() >= Self::max_queue_len() {
                    thread::sleep(Duration::from_millis(1000));
                }
                ok = true;
                break;
            } else {
                thread::sleep(Duration::from_millis(MTX_SLEEP_DURATION));
            }
        }

        if ok {
            return Ok(());
        }

        let msg = "error accessing mtx_q lock";
        Err(AppError::new(PATH, "get_queue", "00", msg))
    }

    fn spawn_workers(&mut self) -> Result<(), AppError> {
        let (child_tx, parent_rx) = unbounded();
        let (parent_tx, child_rx) = bounded(1);

        self.set_tx(Some(parent_tx));
        self.set_rx(Some(parent_rx));

        for wid in 0..self.num_threads() {
            let mtx_q = Arc::clone(&self.mtx_q());
            let out_dir_path = self.out_dir().to_owned();
            let tx = child_tx.clone();
            let rx = child_rx.clone();

            thread::spawn(move || loop {
                let res = mtx_q.write();
                if res.is_err() {
                    thread::sleep(Duration::from_millis(MTX_SLEEP_DURATION));
                    continue;
                }

                let mut mgq = res.unwrap();
                let q = &mut (*mgq);

                if q.len() == 0 {
                    drop(q);
                    drop(mgq);
                    if rx.len() > 0 {
                        tx.send(Ok(wid)).unwrap();
                        break;
                    }
                    continue;
                }

                if let Some(entry) = q.pop() {
                    drop(q);
                    drop(mgq);
                    let result = Self::process_entry(&out_dir_path, &entry);
                    if result.is_err() {
                        // println!("worker {} failed. rolling back", wid);
                        tx.send(Err(result.err().unwrap())).unwrap();
                        break;
                    }
                }

                // sleep
                thread::sleep(Duration::from_millis(Self::thread_sleep_duration()));
            });
            // println!("spawned worker {}", wid);
        }

        Ok(())
    }
}
