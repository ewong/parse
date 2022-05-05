use chrono::Utc;
use crossbeam_channel::{bounded, select, unbounded, Receiver, Sender};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use std::{fs, thread};

use super::account::{Account, AccountPath};
use crate::lib::constants::{ACCOUNT_BACKUP_DIR, ACCOUNT_DIR};
use crate::lib::error::AppError;

const MAX_NUM_RECORDS: usize = 64_000;
const THREAD_SLEEP_DURATION: u64 = 250;

const PATH: &str = "model/updater";

pub struct Updater {
    started: bool,
    tx: Sender<Option<Vec<AccountPath>>>,
    rx: Receiver<Result<(), AppError>>,
}

impl Updater {
    pub fn new() -> Self {
        let (tx, _) = bounded(0);
        let (_, rx) = bounded(0);
        Self {
            started: false,
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

    pub fn add(&self, account_paths: Vec<AccountPath>) -> Result<(), AppError> {
        self.tx
            .send(Some(account_paths))
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

        let mut manager = LoadManager::new(child_tx, child_rx);
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
    rx: Receiver<Option<Vec<AccountPath>>>,
    num_workers: u16,
    worker_id_ptr: u16,
    worker_tx_channels: Vec<Sender<Option<Vec<AccountPath>>>>,
    worker_rx_channels: Vec<Receiver<Result<u16, AppError>>>,
}

impl LoadManager {
    fn new(tx: Sender<Result<(), AppError>>, rx: Receiver<Option<Vec<AccountPath>>>) -> Self {
        Self {
            tx,
            rx,
            num_workers: 0,
            worker_id_ptr: 0,
            worker_tx_channels: Vec::new(),
            worker_rx_channels: Vec::new(),
        }
    }

    fn listen(&mut self) {
        loop {
            select! {
                recv(self.rx) -> packet => {
                    if let Ok(block) = packet {
                        if let Some(account_paths) = block {
                            let worker_id_ptr = self.worker_id_ptr.clone();
                            let tx = self.worker_tx_channels.get(worker_id_ptr as usize).unwrap();
                            tx.send(Some(account_paths)).unwrap();

                            if (self.worker_id_ptr as usize) == self.worker_tx_channels.len() - 1 {
                                self.worker_id_ptr = 0;
                            } else {
                                self.worker_id_ptr += 1;
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
        let mut worker = Worker::new(wid, tx, rx);
        thread::spawn(move || worker.listen());
        println!("spawned worker {}", wid);
        self.num_workers += 1;
    }
}

struct Worker {
    id: u16,
    account_map: HashMap<u16, Account>,
    tx: Sender<Result<u16, AppError>>,
    rx: Receiver<Option<Vec<AccountPath>>>,
}

impl Worker {
    fn new(
        id: u16,
        tx: Sender<Result<u16, AppError>>,
        rx: Receiver<Option<Vec<AccountPath>>>,
    ) -> Self {
        Self {
            id,
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
                        if let Some(account_paths) = block {
                            for entry in account_paths {

                            if entry.update_file {
                                let account_file = [ACCOUNT_DIR, &entry.file_name].join("/");
                                if Path::new(&account_file).exists() {
                                    let backup_file = [
                                        ACCOUNT_BACKUP_DIR,
                                        "/",
                                        &entry.file_name.replace(
                                            ".csv",
                                            &["_", &Utc::now().timestamp_millis().to_string(), ".csv"].join(""),
                                        ),
                                    ]
                                    .join("");
                                    let _ = fs::copy(&account_file, backup_file);
                                    let _ = fs::remove_file(&account_file);
                                }

                                let _ = fs::copy(&entry.file_path, &account_file);
                                let _ = fs::remove_file(&entry.file_path);
                                continue;
                            }

                            let result = fs::read_to_string(&entry.file_path).map_err(|e| {
                                AppError::new(
                                    PATH,
                                    "process_entry",
                                    &["00", &entry.file_path].join(" | "),
                                    &e.to_string(),
                                )
                            });

                            if result.is_ok() {
                                println!("{}", result.unwrap().replace("\n", ""));
                            }

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
