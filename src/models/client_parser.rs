use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

use super::error::AppError;

const PATH: &str = "model/client_parser";

const FN_PROCESS_CSV: &str = "process_csv";
const FN_WRITE_CSV: &str = "write_csv";

const BLOCK_SIZE: usize = 1_000_000;
const CHUNK_SIZE: usize = 200_000;

/*
todo:
- use tx.clone() when switching to multi threaded chunking
- split paraellel chunking using iterator
- client field in TxRow change to u16 later

considerations
- check memory usage & chunk accordingly
- fail out gracefully after
- allow resuming afterwards

test cases
- malformed type, client, tx, or amount
- invalid number of fields
*/

pub struct ClientParser;

impl ClientParser {
    pub fn new() -> Self {
        Self {}
    }

    pub fn linear_chunk_csv(&self, csv_path: &str) -> Result<(), AppError> {
        let f = fs::File::open(csv_path)
            .map_err(|e| AppError::new(PATH, FN_PROCESS_CSV, "00", &e.to_string()))?;

        let mut rdr = csv::Reader::from_reader(f);
        let mut block_id: usize = 0;
        let mut chunk_id: usize = 0;
        let mut rows: usize = 0;

        let mut map: HashMap<u32, Vec<TxRow>> = HashMap::new();
        for result in rdr.deserialize() {
            if result.is_err() {
                return Err(AppError::new(
                    PATH,
                    FN_PROCESS_CSV,
                    "01",
                    &result.err().unwrap().to_string(),
                ));
            }

            let row: TxRow = result.unwrap();
            let client_id = row.client_id.clone();

            if map.contains_key(&client_id) {
                map.entry(client_id.clone()).and_modify(|e| {
                    e.push(row);
                });
            } else {
                let v = vec![row];
                map.insert(client_id, v);
            }

            rows += 1;
            if rows >= CHUNK_SIZE {
                for client_id in map.keys() {
                    if let Some(tx_rows) = map.get(client_id) {
                        self.write_csv(client_id, &block_id, &chunk_id, tx_rows)?;
                    }
                }
                map.clear();

                rows = 0;
                chunk_id += 1;
            }

            if chunk_id * CHUNK_SIZE >= BLOCK_SIZE {
                chunk_id = 0;
                block_id += 1;
            }
        }

        // write any remaining data
        for client_id in map.keys() {
            if let Some(tx_rows) = map.get(client_id) {
                self.write_csv(client_id, &block_id, &chunk_id, tx_rows)?;
            }
        }

        Ok(())
    }

    fn write_csv(
        &self,
        client_id: &u32,
        block_id: &usize,
        chunk_id: &usize,
        tx_rows: &Vec<TxRow>,
    ) -> Result<(), AppError> {
        let client_str = ["client", &client_id.to_string()].join("_");
        let dir_path = ["data", "clients", &client_str].join("/");
        fs::create_dir_all(&dir_path)
            .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "00", &e.to_string()))?;

        let file_path = [
            &dir_path,
            "/",
            &client_str,
            "_block_",
            &block_id.to_string(),
            "_chunk_",
            &chunk_id.to_string(),
            ".csv",
        ]
        .join("");

        let mut wtr = csv::Writer::from_path(&file_path)
            .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "01", &e.to_string()))?;

        wtr.write_record(&["userId", "movieId", "rating", "timestamp"])
            .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "02", &e.to_string()))?;

        for row in tx_rows {
            wtr.write_record(&[
                &row.type_id.to_string(),
                &row.client_id.to_string(),
                &row.tx_id.to_string(),
                &row.amount.unwrap_or(0).to_string(),
            ])
            .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "03", &e.to_string()))?;
        }

        wtr.flush()
            .map_err(|e| AppError::new(PATH, FN_WRITE_CSV, "04", &e.to_string()))?;

        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct TxRow {
    #[serde(rename(deserialize = "userId", serialize = "userId"))]
    type_id: String,
    #[serde(rename(deserialize = "movieId", serialize = "movieId"))]
    client_id: u32,
    #[serde(rename(deserialize = "rating", serialize = "rating"))]
    tx_id: f32,
    #[serde(rename(deserialize = "timestamp", serialize = "timestamp"))]
    amount: Option<u32>,
}

// pub fn process_csv(&self, csv_path: &str) -> Result<(), AppError> {
//     let (tx, rx) = mpsc::channel();

//     let path = csv_path.to_string();
//     thread::spawn(move || {
//         let res = File::open(&path);
//         if res.is_err() {
//             let e = AppError::new(PATH, FN_PROCESS_CSV, "00", &res.err().unwrap().to_string());
//             tx.send(e.msg).unwrap();
//             return;
//         }

//         let f = res.unwrap();
//         let mut rdr = csv::Reader::from_reader(f);
//         for res in rdr.records() {
//             if res.is_err() {
//                 let e =
//                     AppError::new(PATH, FN_PROCESS_CSV, "01", &res.err().unwrap().to_string());
//                 tx.send(e.msg).unwrap();
//                 return;
//             }
//             println!("{:?}", res.unwrap());
//         }
//     });

//     for received in rx {
//         print!("{}", received);
//     }

//     Ok(())
// }
