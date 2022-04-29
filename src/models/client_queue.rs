use crossbeam_channel::Receiver;
use csv::ByteRecord;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use super::error::AppError;
use super::queue::Queue;
use super::timer::Timer;

const PATH: &str = "model/client_queue";
const TX_DIR: &str = "data/balances";
const FN_PROCESS_ENTRY: &str = "process_entry";

const TYPE_POS: usize = 0;
const CLIENT_POS: usize = 1;
const TX_POS: usize = 2;
const AMOUNT_POS: usize = 3;

const TYPE_DEPOSIT: &[u8] = b"deposit";
const TYPE_WITHDRAW: &[u8] = b"withdraw";
const TYPE_DISPUTE: &[u8] = b"dispute";
const TYPE_RESOLVE: &[u8] = b"resolve";
const TYPE_CHARGEBACK: &[u8] = b"chargeback";

const NUM_THREADS: u16 = 256;
const THREAD_SLEEP_DURATION: u64 = 100;

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TxRow<'a> {
    #[serde(rename(deserialize = "userId", serialize = "type"))]
    type_id: &'a [u8],
    #[serde(rename(deserialize = "movieId", serialize = "client"))]
    client_id: u32,
    #[serde(rename(deserialize = "rating", serialize = "tx"))]
    tx_id: f32,
    #[serde(rename(deserialize = "timestamp", serialize = "amount"))]
    amount: Option<u32>,
}

pub struct ClientQueue<T> {
    started: bool,
    rx: Option<Receiver<bool>>,
    arc_shutdown: Arc<Mutex<bool>>,
    arc_q: Arc<Mutex<Vec<T>>>,
}

impl<T> ClientQueue<T>
where
    T: Send + Sync + Display + Debug + AsRef<Path> + 'static,
{
    pub fn new(dir_paths: Vec<T>) -> Self {
        Self {
            started: false,
            rx: None,
            arc_shutdown: Arc::new(Mutex::new(true)),
            arc_q: Arc::new(Mutex::new(dir_paths)),
        }
    }
}

impl<T> Queue<T> for ClientQueue<T>
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

    fn process_entry(i: u16, entry: &T) -> Result<(), AppError> {
        let timer = Timer::start();
        let paths = fs::read_dir(entry)
            .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "00", &e.to_string()))?;

        let mut file_paths: Vec<String> = paths
            .map(|e| {
                if let Ok(path) = e {
                    if !path.path().is_dir() {
                        return path.path().display().to_string();
                    }
                }
                "".to_string()
            })
            .filter(|s| s.len() > 0)
            .collect();
        file_paths.sort_by(|a, b| a.cmp(b));

        for path in file_paths {
            let f = fs::File::open(&path)
                .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "00", &e.to_string()))?;

            let mut rdr = csv::Reader::from_reader(f);
            let mut record = ByteRecord::new();

            let headers = rdr
                .byte_headers()
                .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "02", &e.to_string()))?
                .clone();

            while rdr
                .read_byte_record(&mut record)
                .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "03", &e.to_string()))?
            {
                // validate type id
                // if &record[TYPE_POS] == TYPE_DEPOSIT || &record[TYPE_POS] == TYPE_WITHDRAW {
                //     println!("capture tx id ({:?}) in a separate file", &record[TX_POS]);
                // } else if &record[TYPE_POS] == TYPE_DISPUTE
                //     || &record[TYPE_POS] == TYPE_RESOLVE
                //     || &record[TYPE_POS] == TYPE_CHARGEBACK
                // {
                //     println!("write tx id ({:?}) in a separate file", &record[TX_POS]);
                // } else {
                //     println!(
                //         "write client id ({:?}) in tx_error.csv",
                //         &record[CLIENT_POS]
                //     );
                // }

                let _: TxRow = record
                    .deserialize(Some(&headers))
                    .map_err(|e| AppError::new(PATH, FN_PROCESS_ENTRY, "04", &e.to_string()))?;
                // if fail => write user id in tx_error.csv
            }
            println!("worker {} wrote --> processed client file: {}", i, path);
        }

        timer.stop();
        Ok(())
    }
}

// for pres in paths {
//     pool.spawn(move || {
//         if pres.is_err() {
//             println!("dead0");
//             return;
//         }

//         let res = pres.unwrap().path();
//         if !res.is_dir() {
//             println!("dead1");
//             return;
//         }

//         let opt = res.to_str();
//         if opt.is_none() {
//             println!("dead2");
//             return;
//         }

//         let dir = opt.unwrap().to_string();
//         let res = fs::read_dir(&dir);
//         if res.is_err() {
//             println!("dead3");
//             return;
//         }
//         // println!("{}", dir);

//         let mut tx_row = TxRow::new();
//         let mut count = 0.0;

//         for file_path in res.unwrap() {
//             if file_path.is_err() {
//                 println!("dead4");
//                 continue;
//             }
//             let fp = [&dir, file_path.unwrap().file_name().to_str().unwrap()].join("/");
//             let f = fs::File::open(&fp).unwrap();

//             let mut rdr = csv::Reader::from_reader(f);
//             for rdrres in rdr.deserialize() {
//                 let row: TxRow = rdrres.unwrap();
//                 if tx_row.client_id == 0 {
//                     tx_row.type_id = row.type_id;
//                     tx_row.client_id = row.client_id;
//                 }

//                 tx_row.tx_id += row.tx_id;
//                 count += 1.0;
//             }
//         }
//         tx_row.tx_id = tx_row.tx_id / count;
//         println!("{:?}, count: {}", tx_row, count);

//         // write to file
//     });
// }
