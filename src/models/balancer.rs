use crossbeam_channel::{bounded, select, unbounded, Receiver, Sender};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use super::account::Account;
use super::tx_cluster::TxCluster;
use super::tx_history::TxHistory;
use super::tx_record::TxRow;
use crate::lib::error::AppError;

const MAX_NUM_RECORDS: usize = 10;
const THREAD_SLEEP_DURATION: u64 = 250;

const PATH: &str = "model/balancer";

pub struct Balancer {
    started: bool,
    summary_dir: String,
    tx: Sender<Option<TxCluster>>,
    rx: Receiver<Result<(), AppError>>,
}

impl Balancer {
    pub fn new(summary_dir: &str) -> Self {
        let (tx, _) = bounded(0);
        let (_, rx) = bounded(0);
        Self {
            started: false,
            summary_dir: summary_dir.to_string(),
            tx,
            rx,
        }
    }

    pub fn start(&mut self) -> Result<(), AppError> {
        if !self.started {
            self.started = true;
            self.spawn_manager()?;
        }
        Ok(())
    }

    pub fn add(&self, tx_cluster: TxCluster) -> Result<(), AppError> {
        self.tx
            .send(Some(tx_cluster))
            .map_err(|e| AppError::new(PATH, "add", "00", &e.to_string()))?;
        loop {
            if self.tx.len() >= MAX_NUM_RECORDS {
                println!("balancer is sleeping");
                thread::sleep(Duration::from_millis(THREAD_SLEEP_DURATION));
            } else {
                break;
            }
        }

        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), AppError> {
        if !self.started {
            return Ok(());
        }

        self.started = false;
        self.tx.send(None).unwrap();
        if let Ok(result) = self.rx.recv() {
            if result.is_err() {
                return Err(result.err().unwrap());
            }
        }

        Ok(())
    }

    fn spawn_manager(&mut self) -> Result<(), AppError> {
        let (child_tx, parent_rx) = bounded(1);
        let (parent_tx, child_rx) = unbounded();

        self.tx = parent_tx;
        self.rx = parent_rx;

        let mut manager = LoadManager::new(&self.summary_dir, child_tx, child_rx);
        thread::spawn(move || {
            for _ in 0..63 {
                manager.spawn_worker();
            }
            manager.listen();
        });
        println!("spawned load balancer");

        Ok(())
    }
}

struct LoadManager {
    tx: Sender<Result<(), AppError>>,
    rx: Receiver<Option<TxCluster>>,
    summary_dir: String,

    num_clients: u16,
    num_workers: u16,

    worker_id_ptr: u16,
    worker_map: HashMap<u16, u16>,
    worker_tx_channels: Vec<Sender<Option<(u16, Vec<TxRow>)>>>,
    worker_rx_channels: Vec<Receiver<Result<u16, AppError>>>,
}

impl LoadManager {
    fn new(
        summary_dir: &str,
        tx: Sender<Result<(), AppError>>,
        rx: Receiver<Option<TxCluster>>,
    ) -> Self {
        Self {
            tx,
            rx,
            summary_dir: summary_dir.to_string(),
            num_clients: 0,
            num_workers: 0,
            worker_id_ptr: 0,
            worker_map: HashMap::new(),
            worker_tx_channels: Vec::new(),
            worker_rx_channels: Vec::new(),
        }
    }

    fn listen(&mut self) {
        loop {
            select! {
                recv(self.rx) -> packet => {
                    if let Ok(block) = packet {
                        if let Some(tx_cluster) = block {
                            for (client_id, tx_rows) in tx_cluster.tx_row_map {
                                if self.worker_map.contains_key(&client_id) {
                                    let worker_id = self.worker_map.get(&client_id).unwrap().clone();
                                    let tx = self.worker_tx_channels.get(worker_id as usize).unwrap();
                                    tx.send(Some((client_id, tx_rows))).unwrap();
                                } else {
                                    let worker_id_ptr = self.worker_id_ptr.clone();
                                    let tx = self.worker_tx_channels.get(worker_id_ptr as usize).unwrap();
                                    tx.send(Some((client_id, tx_rows))).unwrap();

                                    self.worker_map.insert(client_id.clone(),worker_id_ptr.clone());
                                    if (self.worker_id_ptr as usize) == self.worker_tx_channels.len() - 1 {
                                        self.worker_id_ptr = 0;
                                    } else {
                                        self.worker_id_ptr += 1;
                                    }
                                }
                            }
                        } else {
                            self.shutdown();
                            return;
                        }
                    } else {
                        self.shutdown();
                        return;
                    }
                },
            }
        }
    }

    fn shutdown(&mut self) {
        for t in &self.worker_tx_channels {
            t.send(None).unwrap();
        }
        for r in &self.worker_rx_channels {
            let _ = r.recv().unwrap();
        }
        self.tx.send(Ok(())).unwrap();
        println!("balancer shutting down");
    }

    fn spawn_worker(&mut self) {
        let (worker_tx, manager_rx) = bounded(1);
        let (manager_tx, worker_rx) = unbounded();
        self.worker_tx_channels.push(manager_tx);
        self.worker_rx_channels.push(manager_rx);
        let wid = self.num_workers;
        let tx = worker_tx.clone();
        let rx = worker_rx.clone();
        let mut worker = Worker::new(wid, &self.summary_dir, tx, rx);
        thread::spawn(move || worker.listen());
        println!("spawned worker {}", wid);
        self.num_workers += 1;
    }
}

struct Worker {
    id: u16,
    summary_dir: String,
    account_map: HashMap<u16, Account>,
    tx: Sender<Result<u16, AppError>>,
    rx: Receiver<Option<(u16, Vec<TxRow>)>>,
}

impl Worker {
    fn new(
        id: u16,
        summary_dir: &str,
        tx: Sender<Result<u16, AppError>>,
        rx: Receiver<Option<(u16, Vec<TxRow>)>>,
    ) -> Self {
        Self {
            id,
            summary_dir: summary_dir.to_string(),
            tx,
            rx,
            account_map: HashMap::new(),
        }
    }

    fn listen(&mut self) {
        loop {
            select! {
                recv(self.rx) -> packet => {
                    if let Ok(block) = packet {
                        if let Some(tuple) = block {
                            //println!("worker {} got client id: {}, num row: {}", self.id, tuple.0, tuple.1.len());
                            let client_id = tuple.0;
                            let tx_rows = tuple.1;
                            let mut account: Account;
                            if self.account_map.contains_key(&client_id) {
                                account = self.account_map.get(&client_id).unwrap().clone();
                            } else {
                                account = Account::new_for_balancer(client_id, &self.summary_dir);
                            }
                            let mut tx_history = TxHistory::new(&client_id);
                            for row in &tx_rows {
                                account.handle_tx(&row.type_id, &row.tx_id, &row.amount, &mut tx_history);
                            }
                            let result = account.write_to_csv(&self.summary_dir);
                            if result.is_err() {
                                self.tx.send(Err(result.err().unwrap())).unwrap();
                                self.shutdown();
                                break;
                            }
                        } else {
                            self.shutdown();
                            break;
                        }
                    } else {
                        self.shutdown();
                        break;
                    }
                },
            }
        }
    }

    fn shutdown(&mut self) {
        self.tx.send(Ok(self.id)).unwrap();
        println!("worker {} shutdown", self.id);
    }
}