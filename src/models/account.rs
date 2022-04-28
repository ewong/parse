use std::fs;
use std::time::{Duration, Instant};

use crate::models::tx_row::TxRow;

use super::error::AppError;

const PATH: &str = "models/account";
const INPUT_DIR: &str = "data/transactions";
const OUTPUT_DIR: &str = "data/accounts";

const FN_MERGE_TXNS: &str = "merge_txns";

/*
todo:
- get previous summary as a starting point
*/

pub struct Account {}

impl Account {
    pub fn new() -> Self {
        Self {}
    }

    pub fn merge_txns(&self) -> Result<(), AppError> {
        let start = Instant::now();
        let paths = fs::read_dir(INPUT_DIR)
            .map_err(|e| AppError::new(PATH, FN_MERGE_TXNS, "00", &e.to_string()))?;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();

        for res in paths {
            pool.spawn(move || {
                let pbuf = res.unwrap().path();
                if !pbuf.is_dir() {
                    return;
                }
                let dir = pbuf.to_str().unwrap().to_string();
                let result = fs::read_dir(&dir);
                let mut tx_row = TxRow::new();
                let mut count = 0.0;
                for file_path in result.unwrap() {
                    if file_path.is_err() {
                        continue;
                    }
                    let fp = [&dir, file_path.unwrap().file_name().to_str().unwrap()].join("/");
                    let f = fs::File::open(&fp).unwrap();

                    let mut rdr = csv::Reader::from_reader(f);
                    for rdrres in rdr.deserialize() {
                        let row: TxRow = rdrres.unwrap();
                        if tx_row.client_id == 0 {
                            tx_row.type_id = row.type_id;
                            tx_row.client_id = row.client_id;
                        }

                        tx_row.tx_id += row.tx_id;
                        count += 1.0;
                    }

                    tx_row.tx_id = tx_row.tx_id / count;
                    println!("Name: {:?}", tx_row);
                }
            });
        }

        let duration = start.elapsed();
        println!("Time elapsed in expensive_function() is: {:?}", duration);

        Ok(())
    }
}
