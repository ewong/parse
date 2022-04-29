use crossbeam_channel::{unbounded, Receiver};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use super::error::AppError;

const PATH: &str = "model/proc_queue";
const MTX_NUM_TRIES: u8 = 3;
const MTX_SLEEP_DURATION: u64 = 20;
const THREAD_SLEEP_DURATION: u64 = 2000;

pub trait Queue<T: Send + Sync + 'static> {
    fn num_threads(&self) -> u8;
    fn started(&self) -> bool;
    fn set_started(&mut self, value: bool);
    fn mtx_q(&self) -> &Arc<Mutex<Vec<T>>>;
    fn mtx_shutdown(&self) -> &Arc<Mutex<bool>>;
    fn rx(&self) -> &Option<Receiver<bool>>;
    fn set_rx(&mut self, rx: Option<Receiver<bool>>);
    fn process_entry(i: u8, entry: &T) -> Result<(), AppError>;

    fn start(&mut self) -> Result<(), AppError> {
        if self.is_shutdown()? {
            self.set_started(true);
            self.set_shutdown(false)?;
            self.spawn_workers()?;
            println!("WriteQueue has started!");
        }
        Ok(())
    }

    fn stop(&mut self) -> Result<(), AppError> {
        if self.started() {
            self.set_started(false);
            self.set_shutdown(true)?;

            // block until all threads are done
            if let Some(rx) = self.rx() {
                for _ in 0..self.num_threads() {
                    rx.recv().unwrap();
                }
            }
        }
        Ok(())
    }

    fn add(&self, map: T) -> Result<(), AppError> {
        let mut mtx_q = self.get_queue()?;
        let q = &mut (*mtx_q);
        q.push(map);
        Ok(())
    }

    fn get_queue(&self) -> Result<MutexGuard<Vec<T>>, AppError> {
        for _ in 1..MTX_NUM_TRIES {
            if let Ok(mtx_q) = self.mtx_q().lock() {
                return Ok(mtx_q);
            } else {
                thread::sleep(Duration::from_millis(MTX_SLEEP_DURATION));
            }
        }
        let msg = "error accessing mtx_q lock";
        Err(AppError::new(PATH, "get_queue", "00", msg))
    }

    fn get_shutdown(&self) -> Result<MutexGuard<bool>, AppError> {
        for _ in 1..MTX_NUM_TRIES {
            if let Ok(mtx_shutdown) = self.mtx_shutdown().lock() {
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

    fn spawn_workers(&mut self) -> Result<(), AppError> {
        let (s, r) = unbounded();
        self.set_rx(Some(r));

        for i in 0..self.num_threads() {
            let mtx_q = Arc::clone(&self.mtx_q());
            let mtx_shutdown = Arc::clone(&self.mtx_shutdown());
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
                    let _ = Self::process_entry(i, &entry);
                }

                // sleep
                thread::sleep(Duration::from_millis(THREAD_SLEEP_DURATION));
            });
            println!("spawned worker {}", i);
        }

        Ok(())
    }
}
