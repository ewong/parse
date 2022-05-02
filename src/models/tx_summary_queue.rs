use crossbeam_channel::Receiver;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::lib::error::AppError;
// use crate::lib::timer::Timer;
use crate::lib::tx_queue::TxQueue;
use crate::models::tx_record::{TxRecordReader, TxRecordType};

const PATH: &str = "model/client_queue";
const FN_PROCESS_ENTRY: &str = "process_entry";

const NUM_THREADS: u16 = 64;
const THREAD_SLEEP_DURATION: u64 = 100;

pub struct TxSummaryQueue<T> {
    started: bool,
    rx: Option<Receiver<bool>>,
    out_dir: String,
    arc_shutdown: Arc<Mutex<bool>>,
    arc_q: Arc<Mutex<Vec<T>>>,
}

impl<T> TxSummaryQueue<T>
where
    T: Send + Sync + Display + Debug + AsRef<Path> + 'static,
{
    pub fn new(out_dir: &str, dir_paths: Vec<T>) -> Self {
        Self {
            started: false,
            rx: None,
            out_dir: out_dir.to_owned(),
            arc_shutdown: Arc::new(Mutex::new(true)),
            arc_q: Arc::new(Mutex::new(dir_paths)),
        }
    }

    fn file_path_and_names_in_client_tx_dir(entry: &T) -> Result<(String, Vec<String>), AppError> {
        let paths = fs::read_dir(entry)
            .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "00", &e.to_string()))?;

        let mut file_paths: Vec<(String, String)> = paths
            .map(|e| {
                if e.is_err() {
                    return ("".to_string(), "".to_string());
                }

                let path = e.unwrap();
                if path.path().file_name().is_none() {
                    return ("".to_string(), "".to_string());
                }

                if !path.path().is_file() {
                    return ("".to_string(), "".to_string());
                }

                (
                    path.path().display().to_string(),
                    path.path()
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string(),
                )
            })
            .filter(|s| s.0.len() > 0)
            .collect();

        if file_paths.len() == 0 {
            return Ok(("".to_string(), Vec::new()));
        }

        file_paths.sort_by(|a, b| {
            // all client tx csv files are of the format client_id.csv
            println!("a: {:?}, b: {:?}", a, b);

            let x: Vec<&str> = a.1.split(".").collect();
            let y: Vec<&str> = b.1.split(".").collect();

            let client_id0 = x[0].to_string().parse::<u16>().unwrap();
            let client_id1 = y[0].to_string().parse::<u16>().unwrap();

            client_id0.cmp(&client_id1)
        });

        let fp = file_paths.get(0).unwrap();
        let dir = fp.0.replace(&fp.1, "");

        let fps: Vec<String> = file_paths.iter().map(|e| e.0.clone()).collect();
        Ok((dir, fps))
    }

    fn tx_ids_in_client_conflict_dir(tx_dir: &str) -> Result<Option<HashMap<u32, f64>>, AppError> {
        let conflict_dir = [tx_dir, "conflicts"].join("/");
        let paths = fs::read_dir(&conflict_dir)
            .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "00", &e.to_string()))?;

        let conflict_paths: Vec<String> = paths
            .map(|e| {
                if e.is_err() {
                    return "".to_string();
                }

                let path = e.unwrap();
                if path.path().file_name().is_none() {
                    return "".to_string();
                }

                if !path.path().is_file() {
                    return "".to_string();
                }

                path.path().display().to_string()
            })
            .filter(|s| s.len() > 0)
            .collect();

        if conflict_paths.len() == 0 {
            return Ok(None);
        }

        let mut map: HashMap<u32, f64> = HashMap::new();
        for path in conflict_paths {
            let result = fs::File::open(&path);
            if result.is_err() {
                continue;
            }

            let mut f = result.unwrap();
            let mut s = String::new();
            let result = f.read_to_string(&mut s);

            if result.is_ok() {
                let list = s.replace("{", "").replace("}", "").replace(" ", "");
                for x in list.split(",") {
                    let tx_id = x.to_string().parse::<u32>().unwrap();
                    if !map.contains_key(&tx_id) {
                        map.insert(tx_id, 0.0);
                    }
                }
            }
        }

        if map.len() == 0 {
            return Ok(None);
        }

        Ok(Some(map))
    }
}

impl<T> TxQueue<T> for TxSummaryQueue<T>
where
    T: Send + Sync + Display + Debug + AsRef<Path> + 'static,
{
    fn num_threads() -> u16 {
        NUM_THREADS
    }

    fn thread_sleep_duration() -> u64 {
        THREAD_SLEEP_DURATION
    }

    fn started(&self) -> bool {
        self.started
    }

    fn set_started(&mut self, value: bool) {
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

    fn process_entry(_out_dir: &str, entry: &T) -> Result<(), AppError> {
        let (dir, file_paths) = Self::file_path_and_names_in_client_tx_dir(entry)?;

        if file_paths.len() == 0 {
            return Ok(());
        }

        let mut initial_loop = true;
        let mut conflict_map = Self::tx_ids_in_client_conflict_dir(&dir)?;
        let mut tx_reader = TxRecordReader::new(&file_paths.get(0).unwrap())?;

        for path in file_paths {
            if initial_loop {
                tx_reader.set_reader(&path)?;
                initial_loop = false;
            }

            while tx_reader.next_record() {
                if let Some(map) = &mut conflict_map {
                    if map.contains_key(tx_reader.tx_record_tx())
                        && !tx_reader.tx_record_type().conflict_type()
                    {
                        let amount = tx_reader.tx_record_amount().unwrap();
                        map.entry(tx_reader.tx_record_tx().clone()).and_modify(|e| {
                            *e = amount;
                        });
                        println!(
                            "client conflict match --> type: {}, client: {}, tx: {}, amount: {:?}",
                            tx_reader.tx_record_type().name(),
                            tx_reader.tx_record_client(),
                            tx_reader.tx_record_tx(),
                            tx_reader.tx_record_amount()
                        );
                    }
                }

                // handle rollback
                if let Some(e) = tx_reader.error() {
                    println!("fatal error. rolling back");
                    return Err(AppError::new(PATH, FN_PROCESS_ENTRY, "01", &e.to_string()));
                }
            }
        }

        // timer.stop();
        Ok(())
    }
}

// let paths = fs::read_dir(entry)
//     .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "00", &e.to_string()))?;

// let mut file_paths: Vec<(String, String)> = paths
//     .map(|e| {
//         if e.is_err() {
//             return ("".to_string(), "".to_string());
//         }

//         let path = e.unwrap();
//         if path.path().file_name().is_none() {
//             return ("".to_string(), "".to_string());
//         }

//         if !path.path().is_file() {
//             return ("".to_string(), "".to_string());
//         }

//         (
//             path.path().display().to_string(),
//             path.path()
//                 .file_name()
//                 .unwrap()
//                 .to_str()
//                 .unwrap()
//                 .to_string(),
//         )
//     })
//     .filter(|s| s.0.len() > 0)
//     .collect();

// if file_paths.len() == 0 {
//     return Ok(());
// }

// file_paths.sort_by(|a, b| {
//     // all client tx csv files are of the format client_id.csv
//     println!("a: {:?}, b: {:?}", a, b);

//     let x: Vec<&str> = a.1.split(".").collect();
//     let y: Vec<&str> = b.1.split(".").collect();

//     let client_id0 = x[0].to_string().parse::<u16>().unwrap();
//     let client_id1 = y[0].to_string().parse::<u16>().unwrap();

//     client_id0.cmp(&client_id1)
// });
