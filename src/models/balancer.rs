use crossbeam_channel::{bounded, select, unbounded, Receiver, Sender};
use std::collections::{HashMap, HashSet};
use std::thread;
use std::time::Duration;

use crate::lib::error::AppError;
use crate::models::tx_record::TxRecord;

const PATH: &str = "model/balancer";
const MIN_NUM_THREADS: u16 = 4;
const MAX_NUM_RECORDS: usize = 64_000;
const THREAD_SLEEP_DURATION: u64 = 250;

pub struct Balancer {
    started: bool,
    tx: Option<Sender<Option<&'static TxRecord<'static>>>>,
    rx: Option<Receiver<Result<bool, AppError>>>,
}

impl Balancer {
    pub fn new() -> Self {
        Self {
            started: false,
            tx: None,
            rx: None,
        }
    }

    pub fn start(&mut self) -> Result<(), AppError> {
        if !self.started {
            self.started = true;
            self.spawn_load_balancer()?;
        }
        Ok(())
    }

    pub fn add(&self, tx_record: &'static TxRecord) -> Result<(), AppError> {
        if let Some(tx) = &self.tx {
            loop {
                if tx.len() >= MAX_NUM_RECORDS {
                    thread::sleep(Duration::from_millis(THREAD_SLEEP_DURATION));
                } else {
                    break;
                }
            }
            tx.send(Some(tx_record))
                .map_err(|e| AppError::new(PATH, "add", "00", &e.to_string()))?;
        }
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), AppError> {
        if !self.started {
            return Ok(());
        }

        self.started = false;
        if let Some(tx) = &self.tx {
            tx.send(None).unwrap();
            if let Some(rx) = &self.rx {
                if let Ok(result) = rx.recv() {
                    if result.is_err() {
                        return Err(result.err().unwrap());
                    }
                }
            }
        }

        Ok(())
    }

    fn spawn_load_balancer(&mut self) -> Result<(), AppError> {
        let (child_tx, parent_rx) = bounded(1);
        let (parent_tx, child_rx) = unbounded();

        self.tx = Some(parent_tx);
        self.rx = Some(parent_rx);

        let from_parent = child_rx.clone();
        let to_parent = child_tx.clone();

        thread::spawn(move || {
            let mut num_threads = MIN_NUM_THREADS;

            let mut tx_vec: Vec<Sender<Option<TxRecord>>> = Vec::new();
            let mut rx_vec: Vec<Receiver<Result<bool, AppError>>> = Vec::new();

            // start with 4 workers
            for wid in 0..num_threads {
                let (worker_tx, balancer_rx) = bounded(1);
                let (balancer_tx, worker_rx) = unbounded();

                tx_vec.push(balancer_tx);
                rx_vec.push(balancer_rx);

                let tx = worker_tx.clone();
                let rx = worker_rx.clone();

                thread::spawn(move || loop {
                    select! {
                        recv(rx) -> packet => {
                            if let Some(record) = packet.unwrap() {
                                println!("{:?}", record);
                            } else {
                                tx.send(Ok(true)).unwrap();
                                println!("worker {} shutdown", wid);
                                break;
                            }
                        },
                    }
                });
                println!("spawned worker {}", wid);
                num_threads += 1;
            }

            // let mut map: HashMap<u16, HashSet<u16>> = HashMap::new(); // <worker id, client id>
            loop {
                select! {
                    // recv() -> _ => STAT.print(),
                    recv(from_parent) -> packet => {
                        if packet.unwrap().is_none() {
                            for t in tx_vec {
                                t.send(None).unwrap();
                            }
                            for r in rx_vec {
                                let _ = r.recv().unwrap();
                            }
                            to_parent.send(Ok(true)).unwrap();
                            println!("balancer shutting down");
                            break;
                        }
                    },
                }
            }
        });
        println!("spawned load balancer");

        Ok(())
    }
}
